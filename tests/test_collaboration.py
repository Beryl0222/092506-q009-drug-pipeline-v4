"""合作管线服务验收演练。

覆盖：并行提交、部分达成、区域拆分、确认撤销、证据撤回冻结、
跨时区截止日、故障恢复、付款决定还原。
"""
import json
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from drug_pipeline.api import handle
from drug_pipeline.domain import Actor, DomainError
from drug_pipeline.service import Service
from drug_pipeline.store import Store

UTC = timezone.utc

# 双方操作人：甲方（中国药企）与乙方（海外伙伴），各自持有提交/确认/财务角色。
A_SUB = Actor("u-a-sub", "party_a", ("submitter",))
A_FIN = Actor("u-a-fin", "party_a", ("finance",))
A_JSC = Actor("u-a-jsc", "party_a", ("jsc",))
B_SUB = Actor("u-b-sub", "party_b", ("submitter",))
B_FIN = Actor("u-b-fin", "party_b", ("finance",))
B_JSC = Actor("u-b-jsc", "party_b", ("jsc",))


def make_service(path=":memory:", start="2026-01-01T00:00:00+00:00"):
    """返回 (service, clock)，测试通过 clock[0] 控制当前时间。"""
    clock = [datetime.fromisoformat(start)]
    service = Service(Store(path), clock=lambda: clock[0])
    return service, clock


def setup_project(service, reqs=("endpoint",), milestones=2):
    """登记项目、适应症与一串里程碑，返回里程碑 id 列表。"""
    service.register_project("P1", "共同开发项目", ["party_a", "party_b"], actor=A_JSC)
    service.add_indication("P1", "I1", "晚期实体瘤", A_JSC)
    service.grant_region_right("P1", "RR-APAC", "APAC", "party_a", A_JSC)
    ids = []
    for i in range(1, milestones + 1):
        mid = f"M{i}"
        service.define_milestone(
            mid, "P1", "I1", seq=i, title=f"第{i}期里程碑", amount=100.0 * i,
            currency="USD", deadline="2026-06-01T09:00:00+08:00",
            requirements=list(reqs), actor=A_JSC)
        ids.append(mid)
    return ids


def confirm(service, evidence_id, decision="confirm", actor=B_JSC):
    return service.confirm_evidence(evidence_id, decision, actor)


class 并行提交演练(unittest.TestCase):
    def test_双方并行提交证据且各自确认(self):
        service, _ = make_service()
        service.register_project("P1", "共同开发项目", ["party_a", "party_b"], actor=A_JSC)
        service.add_indication("P1", "I1", "晚期实体瘤", A_JSC)
        service.define_milestone("M1", "P1", "I1", 1, "二期主要终点", 100.0, "USD",
                                 "2026-06-01T09:00:00+08:00",
                                 ["endpoint", "safety"], A_JSC)

        barrier = threading.Barrier(2)
        results, errors = {}, {}

        def submit(key, evidence_id, requirement, actor):
            try:
                barrier.wait(timeout=5)
                results[key] = service.submit_evidence(
                    evidence_id, "M1", requirement, "met", f"{requirement} 达标", actor)
            except Exception as exc:  # pragma: no cover - 失败时暴露
                errors[key] = exc

        threads = [
            threading.Thread(target=submit, args=("a", "E-A", "endpoint", A_SUB)),
            threading.Thread(target=submit, args=("b", "E-B", "safety", B_SUB)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        self.assertFalse(errors)
        self.assertEqual(results["a"]["version"], 1)
        self.assertEqual(results["b"]["version"], 1)

        # 交叉确认：乙确认甲的证据，甲确认乙的证据。
        confirm(service, "E-A", actor=B_JSC)
        confirm(service, "E-B", actor=A_JSC)
        status = service.milestone_status("M1")
        self.assertEqual(status["achievement"], "achieved")
        self.assertEqual(len(status["supporting_evidence"]), 2)

    def test_自我确认被拒绝(self):
        service, _ = make_service()
        setup_project(service)
        service.submit_evidence("E1", "M1", "endpoint", "met", "终点达标", A_SUB)
        with self.assertRaises(DomainError) as ctx:
            confirm(service, "E1", actor=A_JSC)
        self.assertEqual(ctx.exception.code, "self_confirm_forbidden")


class 部分达成演练(unittest.TestCase):
    def test_部分达成与限额付款(self):
        service, _ = make_service()
        service.register_project("P1", "共同开发项目", ["party_a", "party_b"], actor=A_JSC)
        service.add_indication("P1", "I1", "晚期实体瘤", A_JSC)
        service.define_milestone("M1", "P1", "I1", 1, "二期", 100.0, "USD",
                                 "2026-06-01T09:00:00+08:00",
                                 ["endpoint", "safety"], A_JSC)
        service.submit_evidence("E1", "M1", "endpoint", "met", "终点达标", A_SUB)
        service.submit_evidence("E2", "M1", "safety", "partial", "安全性部分达标", B_SUB)
        confirm(service, "E1", actor=B_JSC)
        confirm(service, "E2", actor=A_JSC)
        self.assertEqual(service.milestone_status("M1")["achievement"],
                         "partially_achieved")

        # 部分达成只能请求低于全额的款项，且需对方财务确认。
        with self.assertRaises(DomainError) as ctx:
            service.request_payment("PAY-1", "M1", 100.0, "key-1", A_FIN)
        self.assertEqual(ctx.exception.code, "amount_mismatch")
        payment = service.request_payment("PAY-1", "M1", 40.0, "key-1", A_FIN)
        self.assertEqual(payment["state"], "requested")
        with self.assertRaises(DomainError) as ctx:
            service.confirm_payment("PAY-1", A_FIN)  # 请求方不能自确认
        self.assertEqual(ctx.exception.code, "self_confirm_forbidden")
        decision = service.confirm_payment("PAY-1", B_FIN)
        self.assertEqual(decision["payment"]["state"], "confirmed")

        # 幂等重试同一键返回首次结论；超额第二笔被拒绝，避免重复触发。
        retry = service.request_payment("PAY-X", "M1", 40.0, "key-1", A_FIN)
        self.assertEqual(retry["payment_id"], "PAY-1")
        self.assertTrue(retry["duplicated"])
        with self.assertRaises(DomainError) as ctx:
            service.request_payment("PAY-2", "M1", 61.0, "key-2", A_FIN)
        self.assertEqual(ctx.exception.code, "duplicate_payment")

        # 回调成功 → paid；重试回调不改变结论。
        resp = service.payment_callback("key-1", "success")
        self.assertEqual(resp["state"], "paid")
        again = service.payment_callback("key-1", "success")
        self.assertEqual(again, resp)


class 区域拆分演练(unittest.TestCase):
    def test_区域拆分与责任方查询(self):
        service, _ = make_service()
        setup_project(service)
        service.split_region_right("RR-APAC", [
            {"right_id": "RR-CN", "region": "CN", "party": "party_a"},
            {"right_id": "RR-JP", "region": "JP", "party": "party_b"},
        ], A_JSC)

        rights = {r["region"]: r["party"] for r in service.project_rights("P1")["rights"]}
        self.assertEqual(rights, {"CN": "party_a", "JP": "party_b"})

        history = service.right_history("P1")["history"]
        apac = [h for h in history if h["right_id"] == "RR-APAC"]
        self.assertEqual(apac[-1]["status"], "superseded")
        jp = [h for h in history if h["right_id"] == "RR-JP"][0]
        self.assertEqual(jp["supersedes"], "RR-APAC")

        # 已拆分的权利不能再次拆分或转让。
        with self.assertRaises(DomainError) as ctx:
            service.reassign_region_right("RR-APAC", "party_b", A_JSC)
        self.assertEqual(ctx.exception.code, "right_not_active")
        moved = service.reassign_region_right("RR-JP", "party_a", B_JSC)
        self.assertEqual(moved["version"], 2)


class 确认撤销演练(unittest.TestCase):
    def test_撤销确认冻结后续且已确认付款只能冲正(self):
        service, _ = make_service()
        m1, m2, m3 = setup_project(service, milestones=3)
        service.submit_evidence("E1", m1, "endpoint", "met", "终点达标", A_SUB)
        cfm = confirm(service, "E1", actor=B_JSC)
        service.request_payment("PAY-1", m1, 100.0, "key-1", A_FIN)
        service.confirm_payment("PAY-1", B_FIN)

        # 乙方撤销确认：M1 达成依据消失，M2/M3 被冻结。
        service.revoke_confirmation(cfm["confirmation_id"], "数据复核发现偏差", B_JSC)
        self.assertEqual(service.milestone_status(m1)["achievement"], "open")
        chain = {m["milestone_id"]: m["state"]
                 for m in service.indication_chain("P1", "I1")["milestones"]}
        self.assertEqual(chain[m2], "frozen")
        self.assertEqual(chain[m3], "frozen")

        # 历史结论不变：已确认的付款仍是 confirmed，决定快照完整。
        decision = service.payment_decision("PAY-1")
        self.assertEqual(decision["payment"]["state"], "confirmed")
        self.assertEqual(decision["decision"]["evidence"][0]["evidence_id"], "E1")

        # 不能删除，只能冲正；冲正后原记录与抵销记录都在。
        reversed_view = service.reverse_payment("PAY-1", "确认被撤销，双方同意冲正", B_FIN)
        self.assertEqual(reversed_view["payment"]["state"], "reversed")
        self.assertEqual(reversed_view["reversals"][0]["amount"], -100.0)
        self.assertIsNotNone(reversed_view["decision"])  # 快照保留
        with self.assertRaises(DomainError) as ctx:
            service.reverse_payment("PAY-1", "重复冲正", B_FIN)
        self.assertEqual(ctx.exception.code, "payment_not_reversible")

        # 冻结的里程碑禁止提交证据，需指定角色解冻。
        with self.assertRaises(DomainError) as ctx:
            service.submit_evidence("E2", m2, "endpoint", "met", "x", A_SUB)
        self.assertEqual(ctx.exception.code, "milestone_frozen")
        service.unfreeze_milestone(m2, "复核完成，恢复推进", A_JSC)
        self.assertEqual(service.milestone_status(m2)["state"], "open")


class 证据撤回演练(unittest.TestCase):
    def test_撤回证据冻结后续里程碑(self):
        service, _ = make_service()
        m1, m2 = setup_project(service, milestones=2)
        service.submit_evidence("E1", m1, "endpoint", "met", "终点达标", A_SUB)
        confirm(service, "E1", actor=B_JSC)
        self.assertEqual(service.milestone_status(m1)["achievement"], "achieved")

        result = service.withdraw_evidence("E1", "原始数据无法溯源", A_SUB)
        self.assertEqual(result["frozen"], [m2])
        self.assertEqual(service.evidence_view("E1")["state"], "withdrawn")
        self.assertEqual(service.milestone_status(m1)["achievement"], "open")
        self.assertEqual(service.milestone_status(m2)["state"], "frozen")

        # 撤回是终态，不能再修订。
        with self.assertRaises(DomainError):
            service.submit_evidence("E1", m1, "endpoint", "met", "修订", A_SUB)


class 跨时区截止日演练(unittest.TestCase):
    def test_截止日归一与逾期结论固化(self):
        # 截止日 2026-06-01 09:00 +08:00 == 2026-06-01T01:00:00Z。
        service, clock = make_service()
        m1, m2 = setup_project(service, milestones=2)
        status = service.milestone_status(m1)
        self.assertEqual(status["deadline_utc"], "2026-06-01T01:00:00+00:00")

        # 截止前 1 秒（UTC）提交：在截止日内。
        clock[0] = datetime(2026, 6, 1, 0, 59, 59, tzinfo=UTC)
        on_time = service.submit_evidence("E1", m1, "endpoint", "met", "按时", A_SUB)
        self.assertTrue(on_time["within_deadline"])
        # 截止后 1 秒（UTC）提交：逾期。
        clock[0] = datetime(2026, 6, 1, 1, 0, 1, tzinfo=UTC)
        late = service.submit_evidence("E2", m2, "endpoint", "met", "迟到", B_SUB)
        self.assertFalse(late["within_deadline"])

        # 逾期的证据可以被确认，但不能据此确认付款。
        confirm(service, "E2", actor=A_JSC)
        service.request_payment("PAY-LATE", m2, 200.0, "key-late", B_FIN)
        with self.assertRaises(DomainError) as ctx:
            service.confirm_payment("PAY-LATE", A_FIN)
        self.assertEqual(ctx.exception.code, "deadline_exceeded")

        # 时间流逝与重复查询不改变已固化的判定。
        clock[0] = datetime(2026, 7, 1, tzinfo=UTC)
        versions = service.evidence_view("E2")["versions"]
        self.assertEqual(versions[0]["within_deadline"], 0)


class 故障恢复演练(unittest.TestCase):
    def test_崩溃后重启且回调重试不重复入账(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "pipeline.db")
            service, clock = make_service(path)
            setup_project(service)
            service.submit_evidence("E1", "M1", "endpoint", "met", "终点达标", A_SUB)
            confirm(service, "E1", actor=B_JSC)
            service.request_payment("PAY-1", "M1", 100.0, "key-1", A_FIN)
            service.store.close()  # 模拟进程退出

            # 重启：同一数据库文件，状态完整恢复。
            service2, _ = make_service(path)
            self.assertEqual(service2.milestone_status("M1")["achievement"], "achieved")
            decision = service2.confirm_payment("PAY-1", B_FIN)
            self.assertEqual(decision["payment"]["state"], "confirmed")
            resp = service2.payment_callback("key-1", "success")
            self.assertEqual(resp["state"], "paid")
            service2.store.close()  # 再次崩溃

            # 再次重启后重试回调：返回首次结论，不产生第二个 paid 事件。
            service3, _ = make_service(path)
            again = service3.payment_callback("key-1", "success")
            self.assertEqual(again, resp)
            events = service3.payment_decision("PAY-1")["events"]
            self.assertEqual([e["event"] for e in events],
                             ["requested", "confirmed", "paid"])
            service3.store.close()

    def test_事务中途失败不留半截写入(self):
        service, _ = make_service()
        setup_project(service)
        service.submit_evidence("E1", "M1", "endpoint", "met", "终点达标", A_SUB)

        original = service.store.log_audit
        def boom(*args, **kwargs):
            raise RuntimeError("模拟写库故障")
        service.store.log_audit = boom
        with self.assertRaises(RuntimeError):
            confirm(service, "E1", actor=B_JSC)
        service.store.log_audit = original

        # 回滚干净：证据仍在待确认状态，没有确认单，可以重试。
        self.assertEqual(service.evidence_view("E1")["state"], "submitted")
        self.assertEqual(service.evidence_view("E1")["confirmations"], [])
        cfm = confirm(service, "E1", actor=B_JSC)
        self.assertEqual(cfm["decision"], "confirm")


class 付款决定还原演练(unittest.TestCase):
    def test_还原证据版本与审批链(self):
        service, _ = make_service()
        setup_project(service, milestones=1)
        # 证据先提交 v1，修订为 v2 后才被确认 —— 快照必须绑定 v2。
        service.submit_evidence("E1", "M1", "endpoint", "partial", "初步数据", A_SUB)
        service.submit_evidence("E1", "M1", "endpoint", "met", "补充后达标", A_SUB)
        cfm = confirm(service, "E1", actor=B_JSC)
        service.request_payment("PAY-1", "M1", 100.0, "key-1", A_FIN, note="里程碑款")
        service.confirm_payment("PAY-1", B_FIN)
        service.payment_callback("key-1", "success")
        service.reverse_payment("PAY-1", "汇率条款重议", B_FIN)

        view = service.payment_decision("PAY-1")
        snapshot = view["decision"]
        self.assertEqual(snapshot["milestone_version"], 1)
        self.assertEqual(snapshot["achievement"], "achieved")
        evidence = snapshot["evidence"][0]
        self.assertEqual((evidence["evidence_id"], evidence["version"]), ("E1", 2))
        self.assertEqual(evidence["confirmations"][0]["confirmation_id"],
                         cfm["confirmation_id"])
        self.assertEqual(evidence["confirmations"][0]["party"], "party_b")
        self.assertEqual([a["action"] for a in snapshot["approvals"]],
                         ["request", "confirm"])
        self.assertEqual([e["event"] for e in view["events"]],
                         ["requested", "confirmed", "paid", "reversed"])
        self.assertEqual(view["reversals"][0]["reason"], "汇率条款重议")

        # 审计链可按实体回放。
        trail = service.audit_trail("payment", "PAY-1")["entries"]
        self.assertEqual([e["action"] for e in trail],
                         ["request_payment", "confirm_payment", "reverse_payment"])


class 接口层演练(unittest.TestCase):
    def test_api_动作分发与错误封装(self):
        service, _ = make_service()
        actor = {"id": "u-a-jsc", "party": "party_a", "roles": ["jsc", "finance"]}

        def call(body):
            return json.loads(handle(json.dumps(body), service))

        self.assertEqual(call({"action": "health"})["status"], "ok")
        call({"action": "register_project", "project_id": "P1", "name": "共同开发",
              "parties": ["party_a", "party_b"], "actor": actor})
        call({"action": "add_indication", "project_id": "P1", "indication_id": "I1",
              "name": "实体瘤", "actor": actor})
        call({"action": "define_milestone", "milestone_id": "M1", "project_id": "P1",
              "indication_id": "I1", "seq": 1, "title": "二期", "amount": 100.0,
              "currency": "USD", "deadline": "2026-06-01T09:00:00+08:00",
              "requirements": ["endpoint"], "actor": actor})
        sub = call({"action": "submit_evidence", "evidence_id": "E1", "milestone_id": "M1",
                    "requirement": "endpoint", "result": "met", "summary": "达标",
                    "actor": actor})
        self.assertTrue(sub["within_deadline"])

        # 业务错误返回结构化错误而不是异常。
        err = call({"action": "confirm_evidence", "evidence_id": "E1",
                    "decision": "confirm", "actor": actor})
        self.assertEqual(err["error"], "self_confirm_forbidden")

        # 查询动作还原付款链（此处尚无付款，验证查询通路）。
        self.assertEqual(call({"action": "milestone_status", "milestone_id": "M1"})
                         ["achievement"], "open")
        with self.assertRaises(ValueError):
            call({"action": "delete_payment", "payment_id": "PAY-1"})


if __name__ == "__main__":
    unittest.main()
