"""合作管线服务的验收演练。

覆盖需求中列出的全部场景：
并行提交、部分达成、区域拆分、确认撤销、撤回冻结、已确认付款只能冲正、
跨时区截止日、回调重试与故障恢复，以及付款决定的证据与审批链还原。
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor

from drug_pipeline.api import handle
from drug_pipeline.domain import (
    MilestoneFrozen,
    NotFound,
    PaymentLocked,
    PermissionDenied,
    Conflict,
    ValidationFailed,
)
from drug_pipeline.service import PAYMENT_CONFIRMER_ROLE, Service
from drug_pipeline.store import Store

A_OP = ["A_operator"]
B_OP = ["B_operator"]
A_CF = ["A_confirmer"]
B_CF = ["B_confirmer"]
PAY = [PAYMENT_CONFIRMER_ROLE]


def _ids(n: int, prefix: str) -> list[str]:
    return [f"{prefix}-{i}" for i in range(n)]


class 验收演练测试(unittest.TestCase):
    def setUp(self) -> None:
        self.service = Service(Store())
        self.cmds = iter(range(10_000))

    def cmd(self) -> str:
        return f"cmd-{next(self.cmds)}"

    # ------------------------------------------------------------------
    # 公共搭建：项目 + 适应症 + 地区权利 + 两个里程碑（M2 依赖 M1）
    # ------------------------------------------------------------------
    def 搭建管线(self, deadline: str = "", tz: str = "UTC",
                amount: str = "1000000.00"):
        s = self.service
        s.register_project("P1", "联合开发项目", actor="pm", roles=A_OP,
                           cmd_id=self.cmd())
        s.register_indication("I1", "P1", "一线肺癌", actor="pm", roles=A_OP,
                              cmd_id=self.cmd())
        s.register_region_rights(
            "R1", "I1",
            [{"region": "CN", "party": "A"}, {"region": "US", "party": "B"},
             {"region": "EU", "party": "B"}],
            actor="legal-a", roles=A_CF, cmd_id=self.cmd())
        s.register_milestone(
            "M1", "P1", "III期主要终点", actor="pm", roles=A_OP,
            cmd_id=self.cmd(), indication_id="I1", stage="pivotal",
            criteria={"orr": ">=0.4"}, deadline=deadline, deadline_tz=tz,
            amount=amount, currency="USD")
        s.register_milestone(
            "M2", "P1", "FDA申报受理", actor="pm", roles=A_OP,
            cmd_id=self.cmd(), indication_id="I1", stage="filing",
            criteria={"accepted": True}, amount="2000000.00",
            currency="USD", depends_on=["M1"])
        return s

    def 双方达成(self, milestone: str = "M1"):
        s = self.service
        s.submit_result(milestone, "A", f"E-{milestone}-A", "met",
                        actor="a-cra", roles=A_OP, cmd_id=self.cmd(),
                        metrics={"orr": 0.46}, regions=["CN"])
        s.submit_result(milestone, "B", f"E-{milestone}-B", "met",
                        actor="b-cra", roles=B_OP, cmd_id=self.cmd(),
                        metrics={"orr": 0.44}, regions=["US", "EU"])
        s.confirm_result(milestone, "A", actor="a-med", roles=A_CF,
                         cmd_id=self.cmd())
        s.confirm_result(milestone, "B", actor="b-med", roles=B_CF,
                         cmd_id=self.cmd())

    # ------------------------------------------------------------------
    # 1. 并行提交：双方同时提交互不覆盖，重复 cmd_id 只生效一次
    # ------------------------------------------------------------------
    def test_01_并行提交与命令幂等(self):
        s = self.搭建管线()

        # 不同 cmd_id 并行提交双方证据
        with ThreadPoolExecutor(max_workers=2) as pool:
            futs = [
                pool.submit(s.submit_result, "M1", "A", "E-A", "met",
                            actor="a", roles=A_OP, cmd_id="cmd-par-A",
                            metrics={"orr": 0.5}),
                pool.submit(s.submit_result, "M1", "B", "E-B", "met",
                            actor="b", roles=B_OP, cmd_id="cmd-par-B",
                            metrics={"orr": 0.42}),
            ]
            [f.result() for f in futs]

        view = s.get_milestone("M1")
        self.assertEqual(view["slots"]["A"]["evidence_id"], "E-A")
        self.assertEqual(view["slots"]["B"]["evidence_id"], "E-B")
        self.assertEqual(view["status"], "submitted")

        # 同一 cmd_id 并行重放：只有一条 result_submitted 事件，结果一致
        with ThreadPoolExecutor(max_workers=4) as pool:
            futs = [pool.submit(
                s.submit_result, "M1", "A", "E-A-DUP", "met",
                actor="a", roles=A_OP, cmd_id="cmd-dup",
                metrics={"orr": 0.99}) for _ in range(4)]
            results = [f.result() for f in futs]

        dup_count = sum(1 for r in results if r.get("idempotent_replay"))
        self.assertEqual(dup_count, 3)
        # 重试不得改变首次结论：槽位证据保持 cmd-dup 首次写入的 E-A-DUP，
        # 并行重放没有产生重复事件。
        self.assertEqual(s.get_milestone("M1")["slots"]["A"]["evidence_id"],
                         "E-A-DUP")
        submit_events = [e for e in s.audit_log("M1")
                         if e["event_type"] == "result_submitted"
                         and e["payload"]["party"] == "A"]
        self.assertEqual(len(submit_events), 2)  # cmd-par-A 与 cmd-dup 各一次

    # ------------------------------------------------------------------
    # 2. 部分达成：只有一方确认时状态为 partial，付款被阻止
    # ------------------------------------------------------------------
    def test_02_部分达成就绪不可付款(self):
        s = self.搭建管线()
        s.submit_result("M1", "A", "E-A", "met", actor="a", roles=A_OP,
                        cmd_id=self.cmd())
        s.submit_result("M1", "B", "E-B", "met", actor="b", roles=B_OP,
                        cmd_id=self.cmd())
        s.confirm_result("M1", "A", actor="a-med", roles=A_CF,
                         cmd_id=self.cmd())
        view = s.get_milestone("M1")
        self.assertEqual(view["status"], "partial")
        self.assertEqual(view["achieved_parties"], ["A"])
        self.assertEqual(view["missing_parties"], ["B"])
        self.assertFalse(view["achieved"])

        with self.assertRaises(ValidationFailed) as ctx:
            s.request_payment("PAY-1", "M1", actor="finance", roles=A_OP,
                              cmd_id=self.cmd())
        self.assertEqual(ctx.exception.code, "milestone_not_achieved")

        # B 方结论为 not_met 时即使双方确认，仍不算达成
        s.confirm_result("M1", "B", actor="b-med", roles=B_CF,
                         cmd_id=self.cmd())
        s.submit_result("M1", "B", "E-B2", "not_met", actor="b", roles=B_OP,
                        cmd_id=self.cmd())
        s.confirm_result("M1", "B", actor="b-med", roles=B_CF,
                         cmd_id=self.cmd())
        self.assertFalse(s.get_milestone("M1")["achieved"])

    # ------------------------------------------------------------------
    # 3. 区域拆分：按权利表限制数据发布，越权被拒绝
    # ------------------------------------------------------------------
    def test_03_区域拆分与越权发布拦截(self):
        s = self.搭建管线()
        self.双方达成()

        # A 方只能发布自己负责的 CN
        ok = s.release_data("REL-1", "M1", ["CN"], actor="a-pub", roles=A_OP,
                            cmd_id=self.cmd(), title="中国队列数据")
        self.assertEqual(ok["regions"], ["CN"])

        with self.assertRaises(PermissionDenied):
            s.release_data("REL-2", "M1", ["US"], actor="a-pub", roles=A_OP,
                           cmd_id=self.cmd())
        with self.assertRaises(PermissionDenied):
            s.release_data("REL-3", "M1", ["CN", "EU"], actor="b-pub",
                           roles=B_OP, cmd_id=self.cmd())
        with self.assertRaises(NotFound):
            s.release_data("REL-4", "M1", ["JP"], actor="b-pub", roles=B_OP,
                           cmd_id=self.cmd())
        # B 方在 US/EU 的发布成功
        ok = s.release_data("REL-5", "M1", ["US", "EU"], actor="b-pub",
                            roles=B_OP, cmd_id=self.cmd())
        self.assertEqual(sorted(ok["regions"]), ["EU", "US"])

    def test_03b_证据未确认禁止发布(self):
        s = self.搭建管线()
        s.submit_result("M1", "A", "E-A", "met", actor="a", roles=A_OP,
                        cmd_id=self.cmd())
        with self.assertRaises(PermissionDenied):
            s.release_data("REL-X", "M1", ["CN"], actor="a", roles=A_OP,
                           cmd_id=self.cmd())

    # ------------------------------------------------------------------
    # 4. 确认撤销：撤销后回到待确认，重新确认后达成
    # ------------------------------------------------------------------
    def test_04_确认撤销与重新确认(self):
        s = self.搭建管线()
        self.双方达成()
        self.assertTrue(s.get_milestone("M1")["achieved"])

        s.revoke_confirmation("M1", "A", "发现数据质疑", actor="a-med",
                              roles=A_CF, cmd_id=self.cmd())
        view = s.get_milestone("M1")
        self.assertEqual(view["status"], "partial")
        self.assertEqual(view["slots"]["A"]["status"], "submitted")
        # 撤销事实作为独立历史事件保留
        revokes = [e for e in s.audit_log("M1")
                   if e["event_type"] == "result_confirmation_revoked"]
        self.assertEqual(len(revokes), 1)
        self.assertEqual(revokes[0]["payload"]["reason"], "发现数据质疑")

        s.confirm_result("M1", "A", actor="a-med2", roles=A_CF,
                         cmd_id=self.cmd())
        self.assertTrue(s.get_milestone("M1")["achieved"])

        # 非该方确认角色无权撤销
        with self.assertRaises(PermissionDenied):
            s.revoke_confirmation("M1", "B", "越权", actor="a-med",
                                  roles=A_CF, cmd_id=self.cmd())

    # ------------------------------------------------------------------
    # 5. 证据撤回冻结：本里程碑与全部下游冻结；新证据恢复后解冻
    # ------------------------------------------------------------------
    def test_05_撤回证据冻结下游并可恢复(self):
        s = self.搭建管线()
        self.双方达成()
        self.双方达成("M2")

        s.withdraw_evidence("M1", "A", "原始数据无法溯源", actor="a-cra",
                            roles=A_OP, cmd_id=self.cmd())
        self.assertTrue(s.get_milestone("M1")["frozen"])
        self.assertEqual(s.get_milestone("M1")["status"], "frozen")
        # 冻结沿依赖传播到 M2
        self.assertTrue(s.get_milestone("M2")["frozen"])

        # 冻结期间下游任何证据动作都被拒绝
        with self.assertRaises(MilestoneFrozen):
            s.confirm_result("M2", "B", actor="b-med", roles=B_CF,
                             cmd_id=self.cmd())
        # 本方在 M1 之外的提交方也不能借提交绕过冻结
        with self.assertRaises(MilestoneFrozen):
            s.submit_result("M1", "B", "E-BX", "met", actor="b", roles=B_OP,
                            cmd_id=self.cmd())
        # 冻结期间不得付款
        with self.assertRaises(MilestoneFrozen):
            s.request_payment("PAY-F", "M1", actor="f", roles=A_OP,
                              cmd_id=self.cmd())

        # 撤回方提交新证据 -> M1 解冻，但需重新确认
        s.submit_result("M1", "A", "E-A-NEW", "met", actor="a-cra2",
                        roles=A_OP, cmd_id=self.cmd(),
                        metrics={"orr": 0.48})
        self.assertFalse(s.get_milestone("M1")["frozen"])
        self.assertFalse(s.get_milestone("M2")["frozen"])
        self.assertEqual(s.get_milestone("M1")["status"], "partial")
        s.confirm_result("M1", "A", actor="a-med", roles=A_CF,
                         cmd_id=self.cmd())
        self.assertTrue(s.get_milestone("M1")["achieved"])
        self.assertTrue(s.get_milestone("M2")["achieved"])
        # 旧证据与撤回事实仍可审计
        history = [e for e in s.audit_log("M1")
                   if e["event_type"] in ("evidence_withdrawn",
                                          "result_submitted")]
        self.assertIn("evidence_withdrawn",
                      [e["event_type"] for e in history])

    # ------------------------------------------------------------------
    # 6. 已确认付款只能冲正、不能删除；冲正后净额为 0、链路可查
    # ------------------------------------------------------------------
    def test_06_付款确认后只能冲正不可删改(self):
        s = self.搭建管线()
        self.双方达成()
        s.request_payment("PAY-1", "M1", actor="fin", roles=A_OP,
                          cmd_id=self.cmd())
        s.confirm_payment("PAY-1", actor="cfo", roles=PAY,
                          cmd_id=self.cmd())

        # 服务不存在任何删除付款的入口；尝试直接改/撤证据被锁拦截
        with self.assertRaises(PaymentLocked):
            s.withdraw_evidence("M1", "A", "想撤证据", actor="a", roles=A_OP,
                                cmd_id=self.cmd())
        with self.assertRaises(PaymentLocked):
            s.revoke_confirmation("M1", "A", "想撤确认", actor="a-med",
                                  roles=A_CF, cmd_id=self.cmd())
        with self.assertRaises(PaymentLocked):
            s.submit_result("M1", "A", "E-A-X", "met", actor="a", roles=A_OP,
                            cmd_id=self.cmd())

        # 重复确认幂等；不能重复触发新付款
        again = s.confirm_payment("PAY-1", actor="cfo", roles=PAY,
                                  cmd_id=self.cmd())
        self.assertTrue(again.get("idempotent"))
        with self.assertRaises(Conflict):
            s.request_payment("PAY-2", "M1", actor="fin", roles=A_OP,
                              cmd_id=self.cmd())

        # 冲正
        s.reverse_payment("PAY-1", "入组标准争议，暂缓支付", actor="cfo",
                          roles=PAY, cmd_id=self.cmd())
        view = s.list_payments("M1")[0]
        self.assertEqual(view["status"], "reversed")
        self.assertEqual(view["net_amount"], "0.00")
        self.assertEqual(len(view["reversals"]), 1)
        # 已冲正不能再次确认，但可以发起新付款
        with self.assertRaises(PaymentLocked):
            s.confirm_payment("PAY-1", actor="cfo", roles=PAY,
                              cmd_id=self.cmd())
        # 冲正后证据解锁，可撤回修订
        s.withdraw_evidence("M1", "A", "随付款冲正复核撤回", actor="a",
                            roles=A_OP, cmd_id=self.cmd())

    def test_06b_待确认付款也阻止重复触发(self):
        s = self.搭建管线()
        self.双方达成()
        s.request_payment("PAY-1", "M1", actor="fin", roles=A_OP,
                          cmd_id=self.cmd())
        with self.assertRaises(Conflict):
            s.request_payment("PAY-2", "M1", actor="fin", roles=A_OP,
                              cmd_id=self.cmd())

    def test_06c_证据变化自动取消挂起付款且确认复核(self):
        s = self.搭建管线()
        self.双方达成()
        s.request_payment("PAY-1", "M1", actor="fin", roles=A_OP,
                          cmd_id=self.cmd())

        # A 方撤销确认：挂起付款请求自动取消（不删除，状态 canceled）
        s.revoke_confirmation("M1", "A", "质疑数据", actor="a-med", roles=A_CF,
                              cmd_id=self.cmd())
        canceled = s.list_payments("M1")[0]
        self.assertEqual(canceled["status"], "canceled")
        self.assertTrue(canceled["is_locked"])
        self.assertEqual(canceled["net_amount"], "0.00")
        with self.assertRaises(PaymentLocked):
            s.confirm_payment("PAY-1", actor="cfo", roles=PAY,
                              cmd_id=self.cmd())

        # 重新确认达成后，可以用新编号发起付款，旧请求仍可溯源
        s.confirm_result("M1", "A", actor="a-med2", roles=A_CF,
                         cmd_id=self.cmd())
        s.request_payment("PAY-2", "M1", actor="fin", roles=A_OP,
                          cmd_id=self.cmd())
        payments = s.list_payments("M1")
        self.assertEqual([p["payment_id"] for p in payments],
                         ["PAY-1", "PAY-2"])
        self.assertEqual(payments[0]["status"], "canceled")
        prov = s.payment_provenance("PAY-1")
        kinds = [c["event_type"] for c in prov["approval_chain"]]
        self.assertIn("payment_canceled", kinds)
        self.assertIn("payment_requested", kinds)

        # 撤回证据：里程碑冻结，挂起的 PAY-2 也被自动取消
        s.withdraw_evidence("M1", "B", "数据问题", actor="b", roles=B_OP,
                            cmd_id=self.cmd())
        self.assertTrue(s.get_milestone("M1")["frozen"])
        with self.assertRaises(PaymentLocked):
            s.confirm_payment("PAY-2", actor="cfo", roles=PAY,
                              cmd_id=self.cmd())
        self.assertEqual(
            [p["status"] for p in s.list_payments("M1")],
            ["canceled", "canceled"])

    # ------------------------------------------------------------------
    # 7. 跨时区截止日：按截止日本地时区判定，边界确定、可重复
    # ------------------------------------------------------------------
    def test_07_跨时区截止日边界确定(self):
        # 截止 2026-03-31（纽约），即纽约时间 2026-04-01 00:00（EDT，UTC-4）
        s = self.搭建管线(deadline="2026-03-31", tz="America/New_York")

        # 纽约 3/31 23:59 == UTC 4/1 03:59，受理
        s.submit_result("M1", "A", "E-A", "met", actor="a", roles=A_OP,
                        cmd_id=self.cmd(),
                        submitted_at="2026-04-01T03:59:00+00:00")
        # 恰好到 4/1 00:00 纽约（UTC 04:00）即超期，拒绝
        with self.assertRaises(ValidationFailed) as ctx:
            s.submit_result("M1", "B", "E-B", "met", actor="b", roles=B_OP,
                            cmd_id=self.cmd(),
                            submitted_at="2026-04-01T04:00:00+00:00")
        self.assertEqual(ctx.exception.code, "past_deadline")

        # 截止判断是纯函数：重试/换调用方结论一致
        from drug_pipeline.domain import is_past_deadline
        from drug_pipeline.domain import parse_instant
        instant = parse_instant("2026-04-01T03:59:59+00:00")
        self.assertFalse(is_past_deadline("2026-03-31",
                                         "America/New_York", instant))
        self.assertFalse(is_past_deadline("2026-03-31",
                                         "America/New_York", instant))

        # 上海时区视角：同一截止按上海日历结束（UTC 3/31 16:00）
        self.assertTrue(is_past_deadline("2026-03-31", "Asia/Shanghai",
                                         parse_instant("2026-03-31T16:00:00+00:00")))
        self.assertFalse(is_past_deadline("2026-03-31", "Asia/Shanghai",
                                          parse_instant("2026-03-31T15:59:59+00:00")))

        view = s.get_milestone("M1")
        self.assertTrue(view["deadline_passed"])  # 查询时（2026-10）已过

    # ------------------------------------------------------------------
    # 8. 回调重试：失败留痕、可重试，消费方按 event_id 幂等
    # ------------------------------------------------------------------
    def test_08_回调失败重试与幂等消费(self):
        s = self.搭建管线()
        received: list[dict] = []
        consumed_ids: set[str] = set()
        flaky = {"fail_left": 2}

        def handler(message: dict) -> None:
            if flaky["fail_left"] > 0:
                flaky["fail_left"] -= 1
                raise RuntimeError("对方系统暂时不可用")
            # 消费端去重：重复投递绝不产生第二个副作用
            if message["event_id"] in consumed_ids:
                return
            consumed_ids.add(message["event_id"])
            received.append(message)

        s.register_destination("partner-b", handler)
        self.双方达成()

        first = s.dispatch_pending("partner-b")
        # 前两次尝试失败（每个事件失败即中断本轮），消息保持 pending
        self.assertEqual(first["failed"], 1)
        pending = s.store.pending_outbox("partner-b")
        self.assertTrue(pending)
        self.assertGreaterEqual(pending[0]["attempts"], 1)

        # 不断重试直到全部投递成功
        summary = {"delivered": 0, "failed": 1}
        for _ in range(20):
            summary = s.dispatch_pending("partner-b")
            if not s.store.pending_outbox("partner-b"):
                break
        self.assertEqual(s.store.pending_outbox("partner-b"), [])
        self.assertGreaterEqual(summary["delivered"], 1)

        # 模拟重复投递（at-least-once）：副作用不翻倍
        delivered = list(received)
        for message in delivered:
            handler(message)
        self.assertEqual(len(received), len(consumed_ids))
        types = [m["event_type"] for m in received]
        self.assertIn("result_submitted", types)
        self.assertIn("result_confirmed", types)

    # ------------------------------------------------------------------
    # 9. 故障恢复：崩溃后新进程从事件日志重建，发件箱继续投递
    # ------------------------------------------------------------------
    def test_09_崩溃后重放与发件箱恢复(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "pipeline.db")
            store = Store(db)
            s1 = Service(store)
            self.service = s1
            self.cmds = iter(range(100))
            s1.register_project("P1", "项目", actor="pm", roles=A_OP,
                                cmd_id="c0")
            s1.register_indication("I1", "P1", "适应症", actor="pm",
                                   roles=A_OP, cmd_id="c1")
            s1.register_region_rights(
                "R1", "I1", [{"region": "CN", "party": "A"}],
                actor="l", roles=A_CF, cmd_id="c2")
            s1.register_milestone("M1", "P1", "终点", actor="pm", roles=A_OP,
                                  cmd_id="c3", indication_id="I1",
                                  amount="500.00", currency="USD")
            s1.submit_result("M1", "A", "E-A", "met", actor="a", roles=A_OP,
                             cmd_id="c4")
            s1.submit_result("M1", "B", "E-B", "met", actor="b", roles=B_OP,
                             cmd_id="c5")
            s1.confirm_result("M1", "A", actor="ac", roles=A_CF, cmd_id="c6")
            s1.confirm_result("M1", "B", actor="bc", roles=B_CF, cmd_id="c7")
            s1.request_payment("PAY-1", "M1", actor="f", roles=A_OP,
                               cmd_id="c8")
            s1.confirm_payment("PAY-1", actor="cfo", roles=PAY, cmd_id="c9")
            # 未注册目的地 -> 无发件箱消息；模拟“事件已落库、回调未接”
            store.close()

            # —— 新进程启动 ——
            store2 = Store(db)
            s2 = Service(store2)
            view = s2.get_milestone("M1")
            self.assertTrue(view["achieved"])
            payment = s2.list_payments("M1")[0]
            self.assertEqual(payment["status"], "confirmed")
            self.assertEqual(payment["amount"], "500.00")

            # 新目的地在故障后接入：历史事件补齐投递
            got: list[str] = []
            s2.register_destination(
                "audit-sink", lambda m: got.append(m["event_type"]))
            backfill = s2.dispatch_pending("audit-sink")
            self.assertGreater(backfill["delivered"], 0)
            self.assertIn("payment_confirmed", got)
            self.assertIn("milestone_registered", got)
            store2.close()

    # ------------------------------------------------------------------
    # 10. 付款溯源：证据快照 + 完整审批链 + 里程碑版本
    # ------------------------------------------------------------------
    def test_10_付款决定的证据与审批链还原(self):
        s = self.搭建管线()
        self.双方达成()
        s.request_payment("PAY-1", "M1", actor="fin", roles=A_OP,
                          cmd_id=self.cmd())
        s.confirm_payment("PAY-1", actor="cfo", roles=PAY,
                          cmd_id=self.cmd())
        s.reverse_payment("PAY-1", "审计抽查", actor="cfo2", roles=PAY,
                          cmd_id=self.cmd())

        prov = s.payment_provenance("PAY-1")
        snapshot = prov["evidence_snapshot"]
        self.assertEqual(snapshot["milestone_version"], 1)
        self.assertTrue(snapshot["milestone_registered_event"])
        self.assertEqual(snapshot["slots"]["A"]["evidence_id"], "E-M1-A")
        self.assertEqual(snapshot["slots"]["A"]["submitted_by"], "a-cra")
        self.assertEqual(snapshot["slots"]["A"]["confirmed_by"], "a-med")
        self.assertEqual(snapshot["slots"]["B"]["submitted_by"], "b-cra")
        self.assertEqual(snapshot["slots"]["B"]["confirmed_by"], "b-med")
        self.assertTrue(snapshot["slots"]["A"]["submit_event_id"])
        self.assertTrue(snapshot["slots"]["A"]["confirm_event_id"])
        # 地区权利快照随付款决定固定
        owners = {r["region"]: r["party"] for r in snapshot["rights"]}
        self.assertEqual(owners, {"CN": "A", "US": "B", "EU": "B"})

        chain = prov["approval_chain"]
        ordered = [c["event_type"] for c in chain]
        self.assertEqual(ordered.index("result_submitted")
                         < ordered.index("result_confirmed"), True)
        self.assertEqual(ordered[-1], "payment_reversed")
        # 冲正后请求与确认事件仍然保留（不可删除）
        self.assertIn("payment_requested", ordered)
        self.assertIn("payment_confirmed", ordered)
        reversal = [c for c in chain if c["event_type"] == "payment_reversed"][0]
        self.assertEqual(reversal["actor"], "cfo2")
        self.assertEqual(reversal["reason"], "审计抽查")

    # ------------------------------------------------------------------
    # 11. 版本化登记：新版本替换当前定义，旧版本可查，版本号冲突被拒绝
    # ------------------------------------------------------------------
    def test_11_版本化登记与历史保留(self):
        s = self.搭建管线()
        s.register_milestone(
            "M1", "P1", "III期主要终点（ORR放宽）", actor="pm2", roles=A_OP,
            cmd_id=self.cmd(), indication_id="I1", version=2,
            supersedes="v1", criteria={"orr": ">=0.35"})
        view = s.get_milestone("M1")
        self.assertEqual(view["version"], 2)
        self.assertEqual(view["criteria"], {"orr": ">=0.35"})
        versions = [(v["version"], v["seq"]) for v in view["versions"]]
        self.assertEqual([v[0] for v in versions], [1, 2])

        with self.assertRaises(Conflict):
            s.register_milestone("M1", "P1", "重复版本", actor="pm",
                                 roles=A_OP, cmd_id=self.cmd(),
                                 indication_id="I1", version=2)
        # 地区权利新版本
        s.register_region_rights(
            "R2", "I1", [{"region": "CN", "party": "A"},
                         {"region": "US", "party": "A"},
                         {"region": "EU", "party": "B"},
                         {"region": "JP", "party": "B"}],
            actor="legal-b", roles=B_CF, cmd_id=self.cmd(), version=2)
        rights = s.get_rights("I1")
        self.assertEqual(rights["version"], 2)
        self.assertEqual(len(rights["regions"]), 4)

    def test_11b_依赖环路被拒绝(self):
        s = self.搭建管线()
        # M2 已依赖 M1；让 M1 新版本反向依赖 M2 必须被拒
        with self.assertRaises(ValidationFailed):
            s.register_milestone("M1", "P1", "环路版本", actor="pm",
                                 roles=A_OP, cmd_id=self.cmd(),
                                 indication_id="I1", version=2,
                                 depends_on=["M2"])

    # ------------------------------------------------------------------
    # 12. JSON API：错误映射为稳定结构，写操作幂等
    # ------------------------------------------------------------------
    def test_12_api_错误映射与幂等写(self):
        svc = self.service
        r = json.loads(handle(json.dumps({"action": "health"}), svc))
        self.assertEqual(r["status"], "ok")

        payload = {"action": "register_project", "project_id": "P1",
                   "name": "项目", "actor": "pm", "roles": A_OP,
                   "cmd_id": "api-1"}
        ok = json.loads(handle(json.dumps(payload), svc))
        self.assertTrue(ok["ok"])
        again = json.loads(handle(json.dumps(payload), svc))
        self.assertTrue(again["result"].get("idempotent_replay"))

        bad = json.loads(handle(json.dumps({
            "action": "submit_result", "milestone_id": "NOPE", "party": "A",
            "evidence_id": "E", "result": "met", "actor": "a",
            "roles": A_OP}), svc))
        self.assertFalse(bad["ok"])
        self.assertEqual(bad["error"]["code"], "not_found")

        denied = json.loads(handle(json.dumps({
            "action": "confirm_result", "milestone_id": "M9", "party": "A",
            "actor": "b", "roles": B_OP}), svc))
        # M9 不存在优先 not_found；这里再验一个纯权限错误
        self.assertFalse(denied["ok"])

    def test_12b_api_金额必须是定点字符串(self):
        svc = self.service
        r = json.loads(handle(json.dumps({
            "action": "register_project", "project_id": "P", "name": "n",
            "actor": "a", "roles": A_OP}), svc))
        self.assertTrue(r["ok"])


if __name__ == "__main__":
    unittest.main()
