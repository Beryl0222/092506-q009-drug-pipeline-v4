"""合作管线应用服务。

写入路径全部走“命令 -> 校验 -> 追加事件 -> 同事务写发件箱/幂等记录”：

1. 命令携带 ``cmd_id``。同一 cmd_id 的并行提交、超时重试只会生效一次，
   重复调用永远返回第一次的结果——这是“重试不改变历史结论”的第一道保证。
2. 所有事实都只追加事件。撤回、撤销确认、冲正都产生新事件，旧事件不可变。
3. 回调走事务发件箱：事件已提交而回调尚未送达时崩溃，重启后从发件箱继续；
   消费方按 event_id 幂等，重复投递不产生第二个副作用。
4. 查询侧由事件回放重建（:mod:`drug_pipeline.projection`），付款决定所用的
   证据快照与审批链随付款事件永久保存。
"""
from __future__ import annotations

import json
import uuid
from collections.abc import Callable

from .domain import (
    PARTIES,
    Conflict,
    MilestoneFrozen,
    NotFound,
    PaymentLocked,
    PermissionDenied,
    ValidationFailed,
    CONFIRM_ROLE,
    SUBMIT_ROLE,
    Role,
    is_past_deadline,
    money,
    now_iso,
    require_party,
)
from .projection import State
from .store import Store

PAYMENT_CONFIRMER_ROLE = "payment_confirmer"


class _NewEvent:
    __slots__ = ("event_type", "payload")

    def __init__(self, event_type: str, payload: dict) -> None:
        self.event_type = event_type
        self.payload = payload


class Service:
    def __init__(self, store: Store | None = None) -> None:
        self.store = store or Store()
        # destination 名称 -> 投递回调。消息持久化在 outbox，处理器可在重启后重绑。
        self._destinations: dict[str, Callable[[dict], None]] = {}

    # ==================================================================
    # 健康检查与遗留登记
    # ==================================================================
    def health(self) -> dict:
        return {"service": "drug_pipeline", "status": "ok"}

    def register(self, record_id: str, owner_id: str) -> dict:
        from .domain import Record

        record = self.store.save(Record(record_id, owner_id))
        return {"record_id": record.record_id, "owner_id": record.owner_id,
                "state": record.state, "created_at": record.created_at}

    def find(self, record_id: str) -> dict | None:
        record = self.store.get(record_id)
        return record.__dict__.copy() if record else None

    # ==================================================================
    # 命令执行骨架：事务 + 幂等 + 发件箱
    # ==================================================================
    def _replay(self) -> State:
        # 与写事务共用同一把锁：查询不会读到其他线程尚未提交的中间状态。
        with self.store.lock:
            state = State()
            for event in self.store.all_events():
                state.apply(event)
        return state

    def _commit(self, cmd_id: str, actor: str, roles: tuple,
                produce: Callable[[State], tuple[list[_NewEvent], dict]]) -> dict:
        """在一个事务里校验并追加事件。

        produce 基于事务内回放的状态做校验、返回待追加事件与结果；
        抛 DomainError 则回滚。成功后事件、命令结果、发件箱消息同事务可见。
        """
        if not cmd_id:
            raise ValidationFailed("命令必须携带 cmd_id 以支持幂等重试")
        store = self.store
        with store.lock:
            store.begin()
            try:
                existing = store.find_command(cmd_id)
                if existing is not None:
                    # 首次结论即最终结论：重试/并行重复提交直接返回缓存结果。
                    store.rollback()
                    cached = json.loads(existing["result"] or "{}")
                    cached["idempotent_replay"] = True
                    return cached

                state = self._replay()
                events, result = produce(state)
                result.setdefault("ok", True)

                first_seq = 0
                appended = []
                for new in events:
                    event_id = uuid.uuid4().hex
                    created_at = now_iso()
                    seq = store.insert_event(_StoredEvent(
                        event_id=event_id, cmd_id=cmd_id,
                        event_type=new.event_type, payload=new.payload,
                        actor=actor, actor_roles=tuple(roles),
                        created_at=created_at))
                    first_seq = first_seq or seq
                    appended.append({"seq": seq, "event_id": event_id,
                                     "event_type": new.event_type,
                                     "payload": new.payload, "created_at": created_at})

                # 同事务扇出到全部已登记目的地。
                for destination in self._destinations:
                    for item in appended:
                        message = {
                            "event_id": item["event_id"],
                            "seq": item["seq"],
                            "event_type": item["event_type"],
                            "payload": item["payload"],
                            "destination": destination,
                        }
                        store.add_outbox(item["seq"], item["event_id"],
                                         destination, message)

                result["events"] = [{"seq": i["seq"], "event_id": i["event_id"],
                                     "event_type": i["event_type"]} for i in appended]
                store.insert_command(cmd_id, first_seq, result)
                store.commit()
                return dict(result)
            except Exception:
                store.rollback()
                raise

    # ==================================================================
    # 角色辅助
    # ==================================================================
    @staticmethod
    def _require_any(roles: tuple, needed, action: str) -> None:
        needed = tuple(needed)
        if not any(r in needed for r in roles):
            raise PermissionDenied(f"执行 {action} 需要角色 {needed}，当前 {roles}")

    @staticmethod
    def _party_of(roles: tuple) -> str:
        if Role.PARTY_A_OPERATOR.value in roles or Role.PARTY_A_CONFIRMER.value in roles:
            return "A"
        if Role.PARTY_B_OPERATOR.value in roles or Role.PARTY_B_CONFIRMER.value in roles:
            return "B"
        raise PermissionDenied("无法从角色判断所属合作方")

    # ==================================================================
    # 版本化登记：项目 / 适应症 / 地区权利 / 里程碑
    # ==================================================================
    def register_project(self, project_id: str, name: str, *,
                         actor: str, roles: list[str] | tuple, cmd_id: str,
                         version: int = 1, supersedes: str = "") -> dict:
        roles = tuple(roles)
        self._require_any(roles, (Role.PARTY_A_OPERATOR.value,
                                  Role.PARTY_B_OPERATOR.value), "登记项目")
        if not project_id or not name:
            raise ValidationFailed("项目编号与名称必填")

        def produce(state: State):
            self._check_version(state.projects.get(project_id), version, project_id)
            event = _NewEvent("project_registered", {
                "project_id": project_id, "name": name, "version": version,
                "supersedes": supersedes})
            return [event], {"project_id": project_id, "version": version}

        return self._commit(cmd_id, actor, roles, produce)

    def register_indication(self, indication_id: str, project_id: str, name: str, *,
                            actor: str, roles: list[str] | tuple, cmd_id: str,
                            version: int = 1, supersedes: str = "") -> dict:
        roles = tuple(roles)
        self._require_any(roles, (Role.PARTY_A_OPERATOR.value,
                                  Role.PARTY_B_OPERATOR.value), "登记适应症")

        def produce(state: State):
            if state.project(project_id) is None:
                raise NotFound(f"项目不存在: {project_id}")
            self._check_version(state.indications.get(indication_id), version,
                                indication_id)
            event = _NewEvent("indication_registered", {
                "indication_id": indication_id, "project_id": project_id,
                "name": name, "version": version, "supersedes": supersedes})
            return [event], {"indication_id": indication_id, "version": version}

        return self._commit(cmd_id, actor, roles, produce)

    def register_region_rights(self, rights_id: str, indication_id: str,
                               regions: list[dict], *,
                               actor: str, roles: list[str] | tuple, cmd_id: str,
                               version: int = 1, supersedes: str = "") -> dict:
        """登记某适应症下的地区权利拆分。

        regions 形如 [{"region": "CN", "party": "A"}, {"region": "EU", "party": "B"}]。
        新版本整体替换旧版本；同一版本内地区不得重复。
        """
        roles = tuple(roles)
        self._require_any(roles, (Role.PARTY_A_CONFIRMER.value,
                                  Role.PARTY_B_CONFIRMER.value), "登记地区权利")
        normalized, seen = [], set()
        for item in regions or []:
            region = str(item.get("region", "")).strip()
            party = require_party(str(item.get("party", "")).strip())
            if not region:
                raise ValidationFailed("地区编码必填")
            if region in seen:
                raise ValidationFailed(f"地区重复: {region}")
            seen.add(region)
            normalized.append({"region": region, "party": party})
        if not normalized:
            raise ValidationFailed("至少需要一条地区权利")

        def produce(state: State):
            if state.indication(indication_id) is None:
                raise NotFound(f"适应症不存在: {indication_id}")
            rows = [v for group in state.rights.values() for v in group
                    if v.get("indication_id") == indication_id]
            self._check_version(rows, version, rights_id)
            event = _NewEvent("region_rights_registered", {
                "rights_id": rights_id, "indication_id": indication_id,
                "regions": normalized, "version": version,
                "supersedes": supersedes})
            events = [event]
            if version > 1:
                # 权利归属变化使挂起付款请求中的地区快照失效。
                for mid in state.milestones:
                    if state.milestone(mid).get("indication_id") == indication_id:
                        events += self._cancel_pending_payments(
                            state, mid, f"适应症 {indication_id} 登记了地区权利新版本 v{version}")
            return events, {"rights_id": rights_id, "version": version,
                            "regions": normalized}

        return self._commit(cmd_id, actor, roles, produce)

    def register_milestone(self, milestone_id: str, project_id: str, name: str, *,
                           actor: str, roles: list[str] | tuple, cmd_id: str,
                           indication_id: str = "", stage: str = "",
                           required_parties: list[str] | tuple = PARTIES,
                           criteria: dict | None = None,
                           deadline: str = "", deadline_tz: str = "UTC",
                           amount: str | int = "", currency: str = "",
                           depends_on: list[str] | tuple | None = None,
                           version: int = 1, supersedes: str = "") -> dict:
        roles = tuple(roles)
        self._require_any(roles, (Role.PARTY_A_OPERATOR.value,
                                  Role.PARTY_B_OPERATOR.value), "登记里程碑")
        required = tuple(require_party(p) for p in (required_parties or PARTIES))
        if deadline:
            # 提前校验，错误时区/日期不允许进入历史。
            from .domain import deadline_instance

            deadline_instance(deadline, deadline_tz)
        amount_text = str(money(amount)) if amount != "" else ""

        def produce(state: State):
            if state.project(project_id) is None:
                raise NotFound(f"项目不存在: {project_id}")
            if indication_id and state.indication(indication_id) is None:
                raise NotFound(f"适应症不存在: {indication_id}")
            self._check_version(state.milestones.get(milestone_id), version,
                                milestone_id)
            deps = list(depends_on or [])
            for dep in deps:
                if state.milestone(dep) is None:
                    raise NotFound(f"依赖里程碑不存在: {dep}")
                if dep == milestone_id:
                    raise ValidationFailed("里程碑不能依赖自身")
            # 依赖图必须无环：把新版本假设加入后做 DFS。
            self._assert_no_cycle(state, milestone_id, deps)
            event = _NewEvent("milestone_registered", {
                "milestone_id": milestone_id, "project_id": project_id,
                "indication_id": indication_id, "name": name, "stage": stage,
                "required_parties": list(required),
                "criteria": criteria or {}, "deadline": deadline,
                "deadline_tz": deadline_tz, "amount": amount_text,
                "currency": currency, "depends_on": deps,
                "version": version, "supersedes": supersedes})
            events = [event]
            # 新版本可能改变达成条件：挂起付款依据的是旧定义，自动作废。
            if version > 1:
                events += self._cancel_pending_payments(
                    state, milestone_id, f"里程碑登记了新版本 v{version}")
            return events, {"milestone_id": milestone_id, "version": version}

        return self._commit(cmd_id, actor, roles, produce)

    @staticmethod
    def _check_version(existing: list[dict] | None, version: int, entity_id: str):
        versions = {(e.get("version", 1)) for e in (existing or [])}
        if version in versions:
            raise Conflict(f"{entity_id} 的版本 {version} 已存在，历史版本不可覆盖")

    @staticmethod
    def _assert_no_cycle(state: State, milestone_id: str, deps: list[str]):
        graph = {}
        for mid in state.milestones:
            graph[mid] = list(state.milestone(mid).get("depends_on", []))
        graph[milestone_id] = deps  # 假设新版本生效后的图
        visiting: set[str] = set()

        def walk(node: str) -> None:
            if node in visiting:
                raise ValidationFailed(f"里程碑依赖存在环路，涉及: {node}")
            visiting.add(node)
            for nxt in graph.get(node, []):
                walk(nxt)
            visiting.discard(node)

        for node in graph:
            walk(node)

    # ==================================================================
    # 双方证据：提交 / 确认 / 撤销确认 / 撤回
    # ==================================================================
    def _require_milestone_active(self, state: State, milestone_id: str,
                                  *, party: str = "") -> dict:
        view = state.milestone_view(milestone_id)
        if view is None:
            raise NotFound(f"里程碑不存在: {milestone_id}")
        if view["frozen"] and not self._can_resubmit(state, view, party):
            raise MilestoneFrozen(
                f"里程碑 {milestone_id} 已被证据撤回冻结，须待上游恢复后再操作")
        return view

    @staticmethod
    def _can_resubmit(state: State, view: dict, party: str) -> bool:
        """撤回方用新证据恢复：冻结只能源于本方撤回，不能是上游传播。"""
        if not party or any(d in state.frozen_milestones()
                            for d in view["depends_on"]):
            return False
        slot = view["slots"].get(party)
        return bool(slot and slot["status"] == "withdrawn")

    def _guard_confirmed_payment(self, state: State, milestone_id: str) -> None:
        for payment in state.payments.values():
            if payment["milestone_id"] == milestone_id \
                    and payment["status"] == "confirmed":
                raise PaymentLocked(
                    f"里程碑 {milestone_id} 已有确认付款 {payment['payment_id']}，"
                    "变更证据前必须先冲正")

    @staticmethod
    def _cancel_pending_payments(state: State, milestone_id: str,
                                 reason: str) -> list["_NewEvent"]:
        """证据基础一变，该里程碑所有挂起付款请求自动作废（只追加，不删除）。"""
        events = []
        for payment in state.payments.values():
            if payment["milestone_id"] == milestone_id \
                    and payment["status"] == "requested":
                events.append(_NewEvent("payment_canceled", {
                    "payment_id": payment["payment_id"],
                    "milestone_id": milestone_id, "reason": reason,
                    "canceled_at": now_iso()}))
        return events

    def submit_result(self, milestone_id: str, party: str, evidence_id: str,
                      result: str, *, actor: str, roles: list[str] | tuple,
                      cmd_id: str, metrics: dict | None = None,
                      regions: list[str] | None = None,
                      submitted_at: str = "") -> dict:
        """一方提交临床/注册结果证据。"""
        roles = tuple(roles)
        party = require_party(party)
        self._require_any(roles, (SUBMIT_ROLE[party], CONFIRM_ROLE[party]),
                          f"提交 {party} 方结果")
        if result not in ("met", "not_met"):
            raise ValidationFailed("result 必须为 met 或 not_met")
        if not evidence_id:
            raise ValidationFailed("证据编号必填")

        def produce(state: State):
            view = self._require_milestone_active(state, milestone_id, party=party)
            self._guard_confirmed_payment(state, milestone_id)
            if view["deadline"]:
                from .domain import parse_instant

                at = parse_instant(submitted_at) if submitted_at else None
                if is_past_deadline(view["deadline"], view["deadline_tz"], at):
                    raise ValidationFailed(
                        f"证据提交晚于截止日 {view['deadline']} "
                        f"({view['deadline_tz']})", code="past_deadline")
            payload = {
                "milestone_id": milestone_id, "party": party,
                "evidence_id": evidence_id, "result": result,
                "metrics": metrics or {}, "regions": list(regions or []),
                "submitted_at": submitted_at or now_iso()}
            events = [_NewEvent("result_submitted", payload)]
            # 新证据改变决定基础：挂起付款请求自动作废，须重新发起。
            events += self._cancel_pending_payments(
                state, milestone_id, f"{party} 方提交了新证据 {evidence_id}")
            return events, {
                "milestone_id": milestone_id, "party": party,
                "evidence_id": evidence_id, "state": "submitted"}

        return self._commit(cmd_id, actor, roles, produce)

    def confirm_result(self, milestone_id: str, party: str, *,
                       actor: str, roles: list[str] | tuple, cmd_id: str,
                       confirmed_at: str = "") -> dict:
        """由该方指定确认角色确认证据。重复确认幂等成功。"""
        roles = tuple(roles)
        party = require_party(party)
        self._require_any(roles, (CONFIRM_ROLE[party],), f"确认 {party} 方结果")

        def produce(state: State):
            view = self._require_milestone_active(state, milestone_id)
            slot = view["slots"][party]
            if slot["status"] == "confirmed":
                return [], {"milestone_id": milestone_id, "party": party,
                            "state": "confirmed", "idempotent": True}
            if slot["status"] != "submitted":
                raise ValidationFailed(
                    f"{party} 方证据当前状态为 {slot['status'] or 'draft'}，"
                    "必须先提交才能确认")
            payload = {"milestone_id": milestone_id, "party": party,
                       "evidence_id": slot["evidence_id"],
                       "confirmed_at": confirmed_at or now_iso()}
            return [_NewEvent("result_confirmed", payload)], {
                "milestone_id": milestone_id, "party": party,
                "state": "confirmed"}

        return self._commit(cmd_id, actor, roles, produce)

    def revoke_confirmation(self, milestone_id: str, party: str, reason: str, *,
                            actor: str, roles: list[str] | tuple,
                            cmd_id: str) -> dict:
        """撤销本方确认（确认撤销），槽位回到已提交待确认。"""
        roles = tuple(roles)
        party = require_party(party)
        self._require_any(roles, (CONFIRM_ROLE[party],), f"撤销 {party} 方确认")

        def produce(state: State):
            view = self._require_milestone_active(state, milestone_id)
            self._guard_confirmed_payment(state, milestone_id)
            slot = view["slots"][party]
            if slot["status"] != "confirmed":
                raise ValidationFailed(
                    f"{party} 方证据未处于已确认状态，无法撤销确认")
            payload = {"milestone_id": milestone_id, "party": party,
                       "evidence_id": slot["evidence_id"], "reason": reason}
            events = [_NewEvent("result_confirmation_revoked", payload)]
            events += self._cancel_pending_payments(
                state, milestone_id, f"{party} 方撤销了证据确认")
            return events, {
                "milestone_id": milestone_id, "party": party,
                "state": "confirmation_revoked"}

        return self._commit(cmd_id, actor, roles, produce)

    def withdraw_evidence(self, milestone_id: str, party: str, reason: str, *,
                          actor: str, roles: list[str] | tuple,
                          cmd_id: str) -> dict:
        """撤回证据：该里程碑及其所有下游里程碑立即派生为冻结。"""
        roles = tuple(roles)
        party = require_party(party)
        self._require_any(roles, (SUBMIT_ROLE[party], CONFIRM_ROLE[party]),
                          f"撤回 {party} 方证据")

        def produce(state: State):
            view = self._require_milestone_active(state, milestone_id)
            self._guard_confirmed_payment(state, milestone_id)
            slot = view["slots"][party]
            if slot["status"] == "draft" or not slot["evidence_id"]:
                raise ValidationFailed(f"{party} 方没有可撤回的证据")
            payload = {"milestone_id": milestone_id, "party": party,
                       "evidence_id": slot["evidence_id"], "reason": reason,
                       "withdrawn_at": now_iso()}
            events = [_NewEvent("evidence_withdrawn", payload)]
            events += self._cancel_pending_payments(
                state, milestone_id, f"{party} 方撤回了证据 {slot['evidence_id']}")
            return events, {
                "milestone_id": milestone_id, "party": party,
                "state": "withdrawn", "frozen_downstream":
                    self._downstream(state, milestone_id)}

        return self._commit(cmd_id, actor, roles, produce)

    @staticmethod
    def _downstream(state: State, milestone_id: str) -> list[str]:
        result = []
        for mid in state.milestones:
            if milestone_id in state.milestone(mid).get("depends_on", []):
                result.append(mid)
        return result

    # ==================================================================
    # 付款：请求 / 确认 / 冲正（已确认不可删除）
    # ==================================================================
    def request_payment(self, payment_id: str, milestone_id: str, *,
                        actor: str, roles: list[str] | tuple, cmd_id: str,
                        amount: str | int = "", currency: str = "",
                        regions: list[str] | None = None) -> dict:
        roles = tuple(roles)
        self._require_any(roles, (Role.PARTY_A_OPERATOR.value,
                                  Role.PARTY_B_OPERATOR.value), "请求付款")

        def produce(state: State):
            if payment_id in state.payments:
                raise Conflict(f"付款请求已存在: {payment_id}")
            view = state.milestone_view(milestone_id)
            if view is None:
                raise NotFound(f"里程碑不存在: {milestone_id}")
            if view["frozen"]:
                raise MilestoneFrozen("里程碑已冻结，不能触发付款")
            if not view["achieved"]:
                raise ValidationFailed(
                    f"里程碑尚未全部达成（已达成 {list(view['achieved_parties'])}，"
                    f"缺失 {list(view['missing_parties'])}），不能触发付款",
                    code="milestone_not_achieved")
            for payment in state.payments.values():
                if payment["milestone_id"] == milestone_id \
                and payment["status"] in ("requested", "confirmed"):
                    raise Conflict(
                        f"里程碑 {milestone_id} 已有未结付款 {payment['payment_id']}，"
                        "禁止重复触发")

            final_amount = money(amount) if amount != "" else money(view["amount"])
            final_currency = currency or view["currency"]
            # 证据快照：付款决定用了哪一版里程碑定义、哪些证据、谁提交谁确认。
            slots_snapshot = {}
            for party in view["required_parties"]:
                slot = state.slot(milestone_id, party)
                slots_snapshot[party] = {
                    "evidence_id": slot["evidence_id"],
                    "result": slot["result"],
                    "metrics": slot["metrics"],
                    "regions": slot["regions"],
                    "submitted_by": slot["submitted_by"],
                    "submitted_at": slot["submitted_at"],
                    "confirmed_by": slot["confirmed_by"],
                    "confirmed_at": slot["confirmed_at"],
                    "submit_event_id": slot.get("submit_event_id", ""),
                    "confirm_event_id": slot.get("confirm_event_id", ""),
                }
            rights = []
            if view["indication_id"]:
                current_rights = state.rights_for(view["indication_id"])
                if current_rights:
                    rights = list(current_rights["regions"])
            payload = {
                "payment_id": payment_id,
                "milestone_id": milestone_id,
                "amount": str(final_amount),
                "currency": final_currency,
                "regions": list(regions or []),
                "evidence": {
                    "milestone_version": view["version"],
                    "milestone_registered_event": view["registered_event_id"],
                    "rights": rights,
                    "slots": slots_snapshot,
                },
            }
            return [_NewEvent("payment_requested", payload)], {
                "payment_id": payment_id, "milestone_id": milestone_id,
                "amount": str(final_amount), "state": "requested"}

        return self._commit(cmd_id, actor, roles, produce)

    def confirm_payment(self, payment_id: str, *,
                        actor: str, roles: list[str] | tuple,
                        cmd_id: str) -> dict:
        roles = tuple(roles)
        self._require_any(roles, (PAYMENT_CONFIRMER_ROLE,), "确认付款")

        def produce(state: State):
            payment = state.payments.get(payment_id)
            if payment is None:
                raise NotFound(f"付款请求不存在: {payment_id}")
            if payment["status"] == "confirmed":
                return [], {"payment_id": payment_id, "state": "confirmed",
                            "idempotent": True}
            if payment["status"] == "reversed":
                raise PaymentLocked("付款已冲正，不能再次确认；请发起新的付款请求")
            if payment["status"] == "canceled":
                raise PaymentLocked(
                    "付款请求因其证据基础变化已自动取消，请重新发起付款")
            # 确认是决定时刻：请求之后证据可能已被撤销/撤回，
            # 必须按当前派生结论重新校验，防止按过期结论放款。
            view = state.milestone_view(payment["milestone_id"])
            if view["frozen"]:
                raise MilestoneFrozen(
                    "付款对应里程碑已被证据撤回冻结，不能确认付款")
            if not view["achieved"]:
                raise ValidationFailed(
                    "付款确认时里程碑已不再满足达成条件"
                    f"（缺失 {list(view['missing_parties'])}），"
                    "待证据恢复后再确认",
                    code="milestone_no_longer_achieved")
            payload = {"payment_id": payment_id,
                       "milestone_id": payment["milestone_id"]}
            return [_NewEvent("payment_confirmed", payload)], {
                "payment_id": payment_id, "state": "confirmed"}

        return self._commit(cmd_id, actor, roles, produce)

    def reverse_payment(self, payment_id: str, reason: str, *,
                        actor: str, roles: list[str] | tuple,
                        cmd_id: str) -> dict:
        """冲正已确认付款。冲正只追加，原始确认链永久保留。"""
        roles = tuple(roles)
        self._require_any(roles, (PAYMENT_CONFIRMER_ROLE,), "冲正付款")
        if not reason:
            raise ValidationFailed("冲正必须填写原因")

        def produce(state: State):
            payment = state.payments.get(payment_id)
            if payment is None:
                raise NotFound(f"付款请求不存在: {payment_id}")
            if payment["status"] != "confirmed":
                raise PaymentLocked(
                    f"付款状态为 {payment['status']}，只有已确认付款可以冲正")
            payload = {"payment_id": payment_id,
                       "milestone_id": payment["milestone_id"],
                       "reason": reason, "reversed_at": now_iso()}
            return [_NewEvent("payment_reversed", payload)], {
                "payment_id": payment_id, "state": "reversed"}

        return self._commit(cmd_id, actor, roles, produce)

    # ==================================================================
    # 数据发布的越权防护
    # ==================================================================
    def release_data(self, release_id: str, milestone_id: str, regions: list[str],
                     *, actor: str, roles: list[str] | tuple,
                     cmd_id: str, title: str = "") -> dict:
        roles = tuple(roles)
        party = self._party_of(roles)
        regions = list(dict.fromkeys(str(r) for r in (regions or [])))
        if not regions:
            raise ValidationFailed("发布范围至少包含一个地区")

        def produce(state: State):
            view = state.milestone_view(milestone_id)
            if view is None:
                raise NotFound(f"里程碑不存在: {milestone_id}")
            if view["frozen"]:
                raise MilestoneFrozen("里程碑已冻结，禁止发布数据")
            indication_id = view["indication_id"]
            if not indication_id or state.rights_for(indication_id) is None:
                raise ValidationFailed("该里程碑未登记地区权利，无法判定发布权限")
            slot = view["slots"].get(party)
            if not slot or slot["status"] != "confirmed":
                raise PermissionDenied(
                    f"{party} 方证据尚未确认，不得发布本方地区数据")
            allowed, denied, unknown = [], [], []
            for region in regions:
                owner = state.region_owner(indication_id, region)
                if owner is None:
                    unknown.append(region)
                elif owner == party:
                    allowed.append(region)
                else:
                    denied.append({"region": region, "owner": owner})
            if unknown:
                raise NotFound(f"地区未在权利表中登记: {unknown}")
            if denied:
                raise PermissionDenied(
                    f"地区 {[d['region'] for d in denied]} 由对方负责，"
                    f"{party} 方无权发布")
            payload = {"release_id": release_id, "milestone_id": milestone_id,
                       "party": party, "regions": allowed, "title": title,
                       "evidence_id": slot["evidence_id"],
                       "released_at": now_iso()}
            return [_NewEvent("data_released", payload)], {
                "release_id": release_id, "regions": allowed,
                "state": "released"}

        return self._commit(cmd_id, actor, roles, produce)

    # ==================================================================
    # 查询：状态还原与付款溯源
    # ==================================================================
    def get_project(self, project_id: str) -> dict | None:
        state = self._replay()
        project = state.project(project_id)
        if project is None:
            return None
        milestones = [state.milestone_view(mid) for mid in state.milestones
                      if state.milestone(mid)["project_id"] == project_id]
        indications = [v for group in state.indications.values() for v in group
                       if v["project_id"] == project_id]
        return {"project": self._public(project),
                "indications": [self._public(state.indication(i["indication_id"]))
                                for i in indications],
                "milestones": milestones}

    def get_milestone(self, milestone_id: str) -> dict | None:
        state = self._replay()
        view = state.milestone_view(milestone_id)
        if view is None:
            return None
        view = dict(view)
        view["versions"] = [
            {"version": v["version"], "seq": v["seq"], "event_id": v["event_id"],
             "at": v["at"], "supersedes": v.get("supersedes", "")}
            for v in state.milestone_versions(milestone_id)]
        if view["deadline"]:
            view["deadline_passed"] = is_past_deadline(
                view["deadline"], view["deadline_tz"])
        indication_id = view.get("indication_id")
        rights = state.rights_for(indication_id) if indication_id else None
        view["region_rights"] = (
            {"rights_id": rights["rights_id"], "version": rights["version"],
             "regions": rights["regions"]} if rights else None)
        # tuple 转 list，方便 JSON 序列化
        for key in ("achieved_parties", "missing_parties", "required_parties",
                    "depends_on"):
            view[key] = list(view[key])
        return view

    def get_rights(self, indication_id: str) -> dict | None:
        state = self._replay()
        rights = state.rights_for(indication_id)
        return dict(rights) if rights else None

    def list_payments(self, milestone_id: str | None = None) -> list[dict]:
        state = self._replay()
        result = []
        for payment in state.payments.values():
            if milestone_id and payment["milestone_id"] != milestone_id:
                continue
            view = state.payment_view(payment["payment_id"])
            view["reversals"] = list(payment["reversals"])
            result.append(view)
        return sorted(result, key=lambda p: p["seq"])

    def payment_provenance(self, payment_id: str) -> dict:
        """还原一笔付款决定使用的全部证据与审批链。"""
        state = self._replay()
        payment = state.payments.get(payment_id)
        if payment is None:
            raise NotFound(f"付款不存在: {payment_id}")
        milestone_id = payment["milestone_id"]
        chain = []
        with self.store.lock:
            events = self.store.all_events()
        for event in events:
            p = event["payload"]
            related = (
                event["event_type"] in ("payment_requested", "payment_confirmed",
                                        "payment_reversed", "payment_canceled")
                and p.get("payment_id") == payment_id
            ) or (
                event["event_type"] in ("result_submitted", "result_confirmed",
                                        "result_confirmation_revoked",
                                        "evidence_withdrawn")
                and p.get("milestone_id") == milestone_id
            ) or (
                event["event_type"] == "milestone_registered"
                and p.get("milestone_id") == milestone_id
            )
            if related:
                chain.append({
                    "seq": event["seq"], "event_id": event["event_id"],
                    "cmd_id": event["cmd_id"], "event_type": event["event_type"],
                    "actor": event["actor"], "actor_roles": list(event["actor_roles"]),
                    "at": event["created_at"],
                    "party": p.get("party", ""),
                    "evidence_id": p.get("evidence_id", ""),
                    "reason": p.get("reason", ""),
                    "version": p.get("version", ""),
                })
        return {
            "payment": state.payment_view(payment_id),
            "evidence_snapshot": payment["evidence"],
            "approval_chain": chain,
        }

    def audit_log(self, milestone_id: str | None = None) -> list[dict]:
        with self.store.lock:
            events = self.store.all_events()
        if milestone_id:
            events = [e for e in events
                      if e["payload"].get("milestone_id") == milestone_id]
        return [{"seq": e["seq"], "event_id": e["event_id"],
                 "cmd_id": e["cmd_id"], "event_type": e["event_type"],
                 "payload": e["payload"], "actor": e["actor"],
                 "actor_roles": list(e["actor_roles"]), "at": e["created_at"]}
                for e in events]

    @staticmethod
    def _public(version: dict) -> dict:
        return {k: v for k, v in version.items() if k not in ("seq", "at")}

    # ==================================================================
    # 回调发件箱：重试、故障恢复
    # ==================================================================
    def register_destination(self, destination: str,
                             handler: Callable[[dict], None],
                             replay_missing: bool = True) -> dict:
        self._destinations[destination] = handler
        backfilled = 0
        if replay_missing:
            # 目的地下线期间产生的事件也要补齐：为缺失的事件补建发件箱消息。
            store = self.store
            with store.lock:
                store.begin()
                try:
                    existing = {
                        row["event_seq"] for row in store.connection.execute(
                            "SELECT event_seq FROM outbox WHERE destination=?",
                            (destination,)).fetchall()
                    }
                    for event in store.all_events():
                        if event["seq"] not in existing:
                            message = {
                                "event_id": event["event_id"], "seq": event["seq"],
                                "event_type": event["event_type"],
                                "payload": event["payload"],
                                "destination": destination}
                            store.add_outbox(event["seq"], event["event_id"],
                                             destination, message)
                            backfilled += 1
                    store.commit()
                except Exception:
                    store.rollback()
                    raise
        return {"destination": destination, "backfilled": backfilled}

    def dispatch_pending(self, destination: str | None = None) -> dict:
        """尝试投递全部待发消息。失败只增加 attempts，消息保持 pending 可重试。

        投递语义是至少一次：处理器必须按 event_id 幂等。
        事件结论在事件提交时已经确定，投递成功与否都不改变历史。
        """
        targets = [destination] if destination else list(self._destinations)
        summary = {"delivered": 0, "failed": 0, "skipped": 0}
        for name in targets:
            handler = self._destinations.get(name)
            if handler is None:
                summary["skipped"] += len(self.store.pending_outbox(name))
                continue
            while True:
                pending = self.store.pending_outbox(name)
                if not pending:
                    break
                item = pending[0]
                try:
                    handler(item["payload"])
                except Exception as exc:  # 投递失败：留待重试
                    with self.store.lock:
                        self.store.begin()
                        try:
                            self.store.mark_outbox_attempt(item["id"], repr(exc))
                            self.store.commit()
                        except Exception:
                            self.store.rollback()
                            raise
                    summary["failed"] += 1
                    # 一次 dispatch 中每条失败只重试一轮，其余等下次调用。
                    break
                with self.store.lock:
                    self.store.begin()
                    try:
                        self.store.mark_outbox_done(item["id"])
                        self.store.advance_delivery(name, item["event_seq"])
                        self.store.commit()
                    except Exception:
                        self.store.rollback()
                        raise
                summary["delivered"] += 1
        return summary

    def outbox_status(self) -> list[dict]:
        rows = self.store.connection.execute(
            "SELECT destination, status, COUNT(*) AS n, SUM(attempts) AS attempts "
            "FROM outbox GROUP BY destination, status ORDER BY destination, status"
        ).fetchall()
        return [dict(r) for r in rows]


class _StoredEvent:
    """insert_event 需要的最小事件形状。"""

    def __init__(self, *, event_id, cmd_id, event_type, payload, actor,
                 actor_roles, created_at) -> None:
        self.event_id = event_id
        self.cmd_id = cmd_id
        self.event_type = event_type
        self.payload = payload
        self.actor = actor
        self.actor_roles = actor_roles
        self.created_at = created_at
