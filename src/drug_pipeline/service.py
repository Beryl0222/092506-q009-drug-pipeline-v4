"""合作管线与里程碑的应用服务入口。

核心规则：
- 项目、适应症、地区权利、里程碑、证据全部版本化登记，历史不可改。
- 证据由任一方提交，必须由对方持有项目指定确认角色（默认 jsc）的人确认。
- 证据被撤回（或确认被撤销导致达成依据消失）时，同一适应症链上序号更靠后的
  里程碑一律冻结，需指定角色显式解冻。
- 付款请求确认后只能冲正（reversal），不能删除；冲正生成抵销记录。
- 截止日在登记时归一到 UTC，提交是否在截止日内只判定一次并随证据版本固化；
  付款回调按幂等键记录，重试直接返回首次结论，均不改变历史。
"""
import json
import uuid

from .domain import (
    Achievement,
    Actor,
    ConfirmationStatus,
    DEFAULT_CONFIRM_ROLES,
    DEFAULT_FINANCE_ROLES,
    DomainError,
    EVIDENCE_RESULTS,
    EvidenceState,
    MilestoneState,
    PaymentState,
    Record,
    iso,
    parse_instant,
    round_money,
    utc_now,
)
from .store import Store


class Service:
    def __init__(self, store: Store | None = None, clock=None) -> None:
        self.store = store or Store()
        # clock 返回带时区的 datetime，便于演练跨时区截止日与故障恢复。
        self.clock = clock or utc_now

    # ---------------------------------------------------------------- 基线

    def health(self) -> dict:
        return {"service": "drug_pipeline", "status": "ok"}

    def register(self, record_id: str, owner_id: str) -> dict:
        record = self.store.save(Record(record_id, owner_id))
        return {"record_id": record.record_id, "owner_id": record.owner_id,
                "state": record.state, "created_at": record.created_at}

    def find(self, record_id: str) -> dict | None:
        record = self.store.get(record_id)
        return record.__dict__.copy() if record else None

    # ---------------------------------------------------------------- 工具

    def _now(self):
        moment = self.clock()
        return iso(parse_instant(moment.isoformat()))

    def _require_project(self, project_id: str) -> dict:
        project = self.store.get_project(project_id)
        if not project:
            raise DomainError("project_not_found", f"项目不存在: {project_id}")
        project["parties"] = json.loads(project["parties"])
        project["confirm_roles"] = json.loads(project["confirm_roles"])
        project["finance_roles"] = json.loads(project["finance_roles"])
        return project

    @staticmethod
    def _require_party(actor: Actor, project: dict) -> None:
        if actor.party not in project["parties"]:
            raise DomainError("party_not_in_project",
                              f"合作方 {actor.party} 不属于项目 {project['project_id']}")

    @staticmethod
    def _require_role(actor: Actor, roles, action: str) -> None:
        if not actor.has_any(roles):
            raise DomainError("role_not_allowed",
                              f"{actor.actor_id} 缺少执行 {action} 所需角色: {'/'.join(roles)}")

    def _require_milestone(self, milestone_id: str):
        milestone = self.store.get_milestone(milestone_id)
        if not milestone:
            raise DomainError("milestone_not_found", f"里程碑不存在: {milestone_id}")
        milestone["requirements"] = json.loads(milestone["requirements"])
        state = self.store.get_milestone_state(milestone_id) or {
            "state": MilestoneState.OPEN, "achievement": Achievement.OPEN}
        return milestone, state

    def _audit(self, actor: Actor, action: str, entity_type: str,
               entity_id: str, detail: dict, role: str | None = None) -> None:
        self.store.log_audit(self._now(), actor.actor_id, role, action,
                             entity_type, entity_id, detail)

    # ---------------------------------------------------------------- 登记

    def register_project(self, project_id: str, name: str, parties,
                         confirm_roles=None, finance_roles=None, actor: Actor = None) -> dict:
        actor = actor or Actor("system", "system", ("admin",))
        parties = [str(p) for p in parties]
        if len(parties) < 2:
            raise DomainError("invalid_parties", "共同开发项目至少需要两个合作方")
        confirm_roles = list(confirm_roles or DEFAULT_CONFIRM_ROLES)
        finance_roles = list(finance_roles or DEFAULT_FINANCE_ROLES)
        with self.store.transaction():
            existing = self.store.get_project(project_id)
            version = (existing["version"] + 1) if existing else 1
            self.store.insert_project_version(project_id, version, name, parties,
                                              confirm_roles, finance_roles,
                                              self._now(), actor.actor_id)
            self._audit(actor, "register_project", "project", project_id,
                        {"version": version, "name": name, "parties": parties})
        return {"project_id": project_id, "version": version, "name": name,
                "parties": parties, "confirm_roles": confirm_roles,
                "finance_roles": finance_roles}

    def add_indication(self, project_id: str, indication_id: str, name: str,
                       actor: Actor) -> dict:
        project = self._require_project(project_id)
        self._require_party(actor, project)
        with self.store.transaction():
            existing = self.store.get_indication(indication_id)
            version = (existing["version"] + 1) if existing else 1
            self.store.insert_indication_version(indication_id, project_id, version,
                                                 name, self._now(), actor.actor_id)
            self._audit(actor, "add_indication", "indication", indication_id,
                        {"project_id": project_id, "version": version, "name": name})
        return {"indication_id": indication_id, "project_id": project_id,
                "version": version, "name": name}

    # ---------------------------------------------------------------- 地区权利

    def grant_region_right(self, project_id: str, right_id: str, region: str,
                           party: str, actor: Actor) -> dict:
        project = self._require_project(project_id)
        self._require_party(actor, project)
        if party not in project["parties"]:
            raise DomainError("party_not_in_project", f"受让方 {party} 不属于项目")
        with self.store.transaction():
            if self.store.get_right(right_id):
                raise DomainError("right_exists", f"权利记录已存在: {right_id}")
            self.store.insert_right_version(right_id, project_id, 1, region, party,
                                            "active", "", self._now(), actor.actor_id)
            self._audit(actor, "grant_region_right", "region_right", right_id,
                        {"project_id": project_id, "region": region, "party": party})
        return {"right_id": right_id, "region": region, "party": party, "version": 1}

    def reassign_region_right(self, right_id: str, party: str, actor: Actor) -> dict:
        right = self.store.get_right(right_id)
        if not right:
            raise DomainError("right_not_found", f"权利记录不存在: {right_id}")
        project = self._require_project(right["project_id"])
        self._require_party(actor, project)
        if party not in project["parties"]:
            raise DomainError("party_not_in_project", f"受让方 {party} 不属于项目")
        if right["status"] != "active":
            raise DomainError("right_not_active", f"权利 {right_id} 已被拆分或失效")
        with self.store.transaction():
            version = right["version"] + 1
            self.store.insert_right_version(right_id, right["project_id"], version,
                                            right["region"], party, "active", "",
                                            self._now(), actor.actor_id)
            self._audit(actor, "reassign_region_right", "region_right", right_id,
                        {"version": version, "party": party})
        return {"right_id": right_id, "region": right["region"],
                "party": party, "version": version}

    def split_region_right(self, right_id: str, splits, actor: Actor) -> dict:
        """区域拆分：原权利标记 superseded，新权利记录各自的责任方。"""
        right = self.store.get_right(right_id)
        if not right:
            raise DomainError("right_not_found", f"权利记录不存在: {right_id}")
        project = self._require_project(right["project_id"])
        self._require_party(actor, project)
        if right["status"] != "active":
            raise DomainError("right_not_active", f"权利 {right_id} 已被拆分或失效")
        splits = list(splits or ())
        if len(splits) < 2:
            raise DomainError("invalid_split", "区域拆分至少需要两个新权利")
        with self.store.transaction():
            created = []
            for item in splits:
                new_id = str(item["right_id"])
                region = str(item["region"])
                party = str(item["party"])
                if party not in project["parties"]:
                    raise DomainError("party_not_in_project", f"受让方 {party} 不属于项目")
                if self.store.get_right(new_id):
                    raise DomainError("right_exists", f"权利记录已存在: {new_id}")
                self.store.insert_right_version(new_id, right["project_id"], 1, region,
                                                party, "active", right_id,
                                                self._now(), actor.actor_id)
                created.append({"right_id": new_id, "region": region, "party": party})
            self.store.insert_right_version(right_id, right["project_id"],
                                            right["version"] + 1, right["region"],
                                            right["party"], "superseded", "",
                                            self._now(), actor.actor_id)
            self._audit(actor, "split_region_right", "region_right", right_id,
                        {"splits": created})
        return {"superseded": right_id, "created": created}

    def project_rights(self, project_id: str) -> dict:
        self._require_project(project_id)
        return {"project_id": project_id,
                "rights": [{"right_id": r["right_id"], "region": r["region"],
                            "party": r["party"], "version": r["version"],
                            "supersedes": r["supersedes"]}
                           for r in self.store.active_rights(project_id)]}

    def right_history(self, project_id: str) -> dict:
        self._require_project(project_id)
        return {"project_id": project_id, "history": self.store.right_history(project_id)}

    # ---------------------------------------------------------------- 里程碑

    def define_milestone(self, milestone_id: str, project_id: str, indication_id: str,
                         seq: int, title: str, amount, currency: str, deadline: str,
                         requirements, actor: Actor) -> dict:
        project = self._require_project(project_id)
        self._require_party(actor, project)
        indication = self.store.get_indication(indication_id)
        if not indication or indication["project_id"] != project_id:
            raise DomainError("indication_not_found",
                              f"适应症 {indication_id} 不属于项目 {project_id}")
        requirements = [str(r) for r in requirements]
        if not requirements:
            raise DomainError("invalid_requirements", "里程碑至少需要一个达成要求")
        deadline_utc = iso(parse_instant(deadline))  # 登记时归一，之后不再重解释
        amount = round_money(amount)
        with self.store.transaction():
            existing = self.store.get_milestone(milestone_id)
            version = (existing["version"] + 1) if existing else 1
            self.store.insert_milestone_version(
                milestone_id, project_id, indication_id, int(seq), title, amount,
                str(currency), deadline_utc, requirements, version,
                self._now(), actor.actor_id)
            if not existing:
                self.store.set_milestone_state(milestone_id, MilestoneState.OPEN,
                                               Achievement.OPEN, self._now())
            self._audit(actor, "define_milestone", "milestone", milestone_id,
                        {"version": version, "seq": int(seq), "amount": amount,
                         "currency": currency, "deadline_utc": deadline_utc,
                         "requirements": requirements})
        return {"milestone_id": milestone_id, "version": version,
                "deadline_utc": deadline_utc, "amount": amount, "currency": currency}

    # ---------------------------------------------------------------- 证据

    def submit_evidence(self, evidence_id: str, milestone_id: str, requirement: str,
                        result: str, summary: str, actor: Actor) -> dict:
        milestone, state = self._require_milestone(milestone_id)
        project = self._require_project(milestone["project_id"])
        self._require_party(actor, project)
        if state["state"] == MilestoneState.FROZEN:
            raise DomainError("milestone_frozen", f"里程碑 {milestone_id} 已冻结，禁止提交证据")
        if requirement not in milestone["requirements"]:
            raise DomainError("unknown_requirement",
                              f"要求 {requirement} 不在里程碑 {milestone_id} 的定义中")
        if result not in EVIDENCE_RESULTS:
            raise DomainError("invalid_result", f"无效的证据结论: {result}")
        submitted_at = self._now()
        # 截止日判定只在提交时做一次，随版本固化，永不重算。
        within_deadline = parse_instant(submitted_at) <= parse_instant(milestone["deadline_utc"])
        with self.store.transaction():
            latest = self.store.get_evidence_version(evidence_id)
            if latest:
                current = self.store.get_evidence_state(evidence_id)
                if current in (EvidenceState.CONFIRMED, EvidenceState.WITHDRAWN):
                    raise DomainError("evidence_locked",
                                      f"证据 {evidence_id} 已{current}，不能再修订")
                if latest["milestone_id"] != milestone_id:
                    raise DomainError("evidence_conflict", "证据编号已被其他里程碑使用")
                version = latest["version"] + 1
            else:
                version = 1
            self.store.insert_evidence_version(
                evidence_id, milestone_id, requirement, version, actor.party, result,
                str(summary), submitted_at, within_deadline, submitted_at, actor.actor_id)
            self.store.set_evidence_state(evidence_id, EvidenceState.SUBMITTED, submitted_at)
            self._audit(actor, "submit_evidence", "evidence", evidence_id,
                        {"milestone_id": milestone_id, "requirement": requirement,
                         "version": version, "result": result,
                         "within_deadline": within_deadline})
        return {"evidence_id": evidence_id, "version": version,
                "state": EvidenceState.SUBMITTED, "submitted_at": submitted_at,
                "within_deadline": within_deadline}

    def confirm_evidence(self, evidence_id: str, decision: str, actor: Actor,
                         reason: str = "") -> dict:
        if decision not in ("confirm", "reject"):
            raise DomainError("invalid_decision", "确认结论只能是 confirm 或 reject")
        latest = self.store.get_evidence_version(evidence_id)
        if not latest:
            raise DomainError("evidence_not_found", f"证据不存在: {evidence_id}")
        milestone, mstate = self._require_milestone(latest["milestone_id"])
        project = self._require_project(milestone["project_id"])
        self._require_role(actor, project["confirm_roles"], "confirm_evidence")
        if actor.party == latest["party"]:
            raise DomainError("self_confirm_forbidden",
                              "证据必须由对方指定角色确认，不能自我确认")
        if mstate["state"] == MilestoneState.FROZEN:
            raise DomainError("milestone_frozen", f"里程碑已冻结，禁止确认")
        if self.store.get_evidence_state(evidence_id) != EvidenceState.SUBMITTED:
            raise DomainError("evidence_not_submitted", "证据当前不在待确认状态")
        confirmation_id = f"cfm-{uuid.uuid4().hex[:12]}"
        now = self._now()
        with self.store.transaction():
            new_state = (EvidenceState.CONFIRMED if decision == "confirm"
                         else EvidenceState.REJECTED)
            self.store.insert_confirmation(confirmation_id, evidence_id, latest["version"],
                                           actor.party, actor.roles[0] if actor.roles else "",
                                           decision, ConfirmationStatus.ACTIVE,
                                           str(reason), now, actor.actor_id)
            self.store.set_evidence_state(evidence_id, new_state, now)
            self._refresh_achievement(milestone)
            self._audit(actor, f"{decision}_evidence", "evidence", evidence_id,
                        {"confirmation_id": confirmation_id,
                         "evidence_version": latest["version"]},
                        role=actor.roles[0] if actor.roles else None)
        return {"confirmation_id": confirmation_id, "evidence_id": evidence_id,
                "evidence_version": latest["version"], "decision": decision}

    def revoke_confirmation(self, confirmation_id: str, reason: str, actor: Actor) -> dict:
        conf = self.store.get_confirmation(confirmation_id)
        if not conf:
            raise DomainError("confirmation_not_found", f"确认单不存在: {confirmation_id}")
        if conf["status"] != ConfirmationStatus.ACTIVE:
            raise DomainError("confirmation_not_active", "确认单已被撤销")
        evidence = self.store.get_evidence_version(conf["evidence_id"],
                                                   conf["evidence_version"])
        milestone, _ = self._require_milestone(evidence["milestone_id"])
        project = self._require_project(milestone["project_id"])
        self._require_role(actor, project["confirm_roles"], "revoke_confirmation")
        if actor.party != conf["party"]:
            raise DomainError("party_mismatch", "只能由作出确认的一方撤销")
        if not reason:
            raise DomainError("reason_required", "撤销确认必须说明原因")
        now = self._now()
        with self.store.transaction():
            self.store.mark_confirmation_revoked(confirmation_id,
                                                 ConfirmationStatus.REVOKED, now,
                                                 actor.actor_id, str(reason))
            if self.store.get_evidence_state(conf["evidence_id"]) == EvidenceState.CONFIRMED:
                self.store.set_evidence_state(conf["evidence_id"],
                                              EvidenceState.SUBMITTED, now)
            old, new = self._refresh_achievement(milestone)
            # 达成依据被抽走时，冻结后续里程碑，等待双方复核。
            if old != Achievement.OPEN and new != old:
                self._freeze_downstream(milestone, actor, f"确认 {confirmation_id} 被撤销")
            self._audit(actor, "revoke_confirmation", "confirmation",
                        confirmation_id, {"reason": str(reason),
                                          "evidence_id": conf["evidence_id"]})
        return {"confirmation_id": confirmation_id, "status": ConfirmationStatus.REVOKED}

    def withdraw_evidence(self, evidence_id: str, reason: str, actor: Actor) -> dict:
        latest = self.store.get_evidence_version(evidence_id)
        if not latest:
            raise DomainError("evidence_not_found", f"证据不存在: {evidence_id}")
        milestone, _ = self._require_milestone(latest["milestone_id"])
        project = self._require_project(milestone["project_id"])
        current = self.store.get_evidence_state(evidence_id)
        if current not in (EvidenceState.SUBMITTED, EvidenceState.CONFIRMED):
            raise DomainError("evidence_not_withdrawable", f"证据状态 {current} 不可撤回")
        # 提交方可以撤回自己的证据；指定确认角色也可以代表双方强制撤回。
        if actor.party != latest["party"] and not actor.has_any(project["confirm_roles"]):
            raise DomainError("role_not_allowed", "只有提交方或指定确认角色可以撤回证据")
        if not reason:
            raise DomainError("reason_required", "撤回证据必须说明原因")
        now = self._now()
        with self.store.transaction():
            self.store.set_evidence_state(evidence_id, EvidenceState.WITHDRAWN, now)
            self._refresh_achievement(milestone)
            # 证据撤回一律冻结同一适应症链上的后续里程碑。
            frozen = self._freeze_downstream(milestone, actor, f"证据 {evidence_id} 被撤回")
            self._audit(actor, "withdraw_evidence", "evidence", evidence_id,
                        {"reason": str(reason), "frozen": frozen})
        return {"evidence_id": evidence_id, "state": EvidenceState.WITHDRAWN,
                "frozen": frozen}

    # ---------------------------------------------------------------- 达成与冻结

    def _supporting_evidence(self, milestone: dict):
        """每个要求取最新一条仍有效的已确认证据版本，作为达成依据。"""
        supporting = []
        for req in milestone["requirements"]:
            rows = self.store.confirmed_evidence(milestone["milestone_id"], req)
            if rows:
                rows.sort(key=lambda r: (r["submitted_at"], r["evidence_id"]))
                supporting.append(rows[-1])
        return supporting

    def _evaluate(self, milestone: dict):
        supporting = self._supporting_evidence(milestone)
        requirements = milestone["requirements"]
        met = [s for s in supporting if s["result"] == "met"]
        if requirements and len(met) == len(requirements):
            achievement = Achievement.FULL
        elif any(s["result"] in ("met", "partial") for s in supporting):
            achievement = Achievement.PARTIAL
        else:
            achievement = Achievement.OPEN
        return achievement, supporting

    def _refresh_achievement(self, milestone: dict):
        state = self.store.get_milestone_state(milestone["milestone_id"])
        old = state["achievement"] if state else Achievement.OPEN
        achievement, _ = self._evaluate(milestone)
        self.store.set_milestone_state(
            milestone["milestone_id"],
            state["state"] if state else MilestoneState.OPEN,
            achievement, self._now())
        return old, achievement

    def _freeze_downstream(self, milestone: dict, actor: Actor, reason: str):
        frozen = []
        for other in self.store.milestones_for_indication(milestone["project_id"],
                                                          milestone["indication_id"]):
            if other["seq"] <= milestone["seq"] or other["state"] == MilestoneState.FROZEN:
                continue
            self.store.set_milestone_state(other["milestone_id"], MilestoneState.FROZEN,
                                           other["achievement"], self._now())
            self._audit(actor, "freeze_milestone", "milestone",
                        other["milestone_id"], {"reason": reason})
            frozen.append(other["milestone_id"])
        return frozen

    def unfreeze_milestone(self, milestone_id: str, reason: str, actor: Actor) -> dict:
        milestone, state = self._require_milestone(milestone_id)
        project = self._require_project(milestone["project_id"])
        self._require_role(actor, project["confirm_roles"], "unfreeze_milestone")
        if state["state"] != MilestoneState.FROZEN:
            raise DomainError("milestone_not_frozen", "里程碑未处于冻结状态")
        if not reason:
            raise DomainError("reason_required", "解冻必须说明原因")
        with self.store.transaction():
            self.store.set_milestone_state(milestone_id, MilestoneState.OPEN,
                                           state["achievement"], self._now())
            self._audit(actor, "unfreeze_milestone", "milestone", milestone_id,
                        {"reason": str(reason)})
        return {"milestone_id": milestone_id, "state": MilestoneState.OPEN}

    # ---------------------------------------------------------------- 付款

    def request_payment(self, payment_id: str, milestone_id: str, amount,
                        idempotency_key: str, actor: Actor, note: str = "") -> dict:
        amount = round_money(amount)
        milestone, state = self._require_milestone(milestone_id)
        project = self._require_project(milestone["project_id"])
        self._require_party(actor, project)
        self._require_role(actor, project["finance_roles"], "request_payment")
        with self.store.transaction():
            existing = self.store.payment_by_key(idempotency_key)
            if existing:
                # 幂等重试：同一键直接返回首次结论，不重复触发付款。
                if (existing["milestone_id"] != milestone_id
                        or round(existing["amount"], 2) != amount):
                    raise DomainError("idempotency_conflict",
                                      "幂等键已被不同金额的请求占用")
                return self._payment_view(existing, duplicated=True)
            if state["state"] == MilestoneState.FROZEN:
                raise DomainError("milestone_frozen", "里程碑已冻结，不能发起付款")
            full = round(float(milestone["amount"]), 2)
            if state["achievement"] == Achievement.FULL:
                if amount != full:
                    raise DomainError("amount_mismatch",
                                      "全部达成时付款金额必须等于里程碑金额")
            elif state["achievement"] == Achievement.PARTIAL:
                if amount >= full:
                    raise DomainError("amount_mismatch",
                                      "部分达成时付款金额必须小于里程碑金额")
            else:
                raise DomainError("milestone_not_achieved", "里程碑尚未达成，不能发起付款")
            total = self.store.active_payment_total(milestone_id)
            if total + amount > full + 1e-9:
                raise DomainError("duplicate_payment",
                                  "累计付款将超过里程碑金额，疑似重复触发")
            now = self._now()
            self.store.insert_payment(payment_id, milestone_id, amount,
                                      milestone["currency"], PaymentState.REQUESTED,
                                      str(idempotency_key), str(note),
                                      actor.actor_id, actor.party, now)
            self.store.insert_payment_event(payment_id, "requested", actor.actor_id,
                                            now, {"amount": amount, "note": str(note)})
            self._audit(actor, "request_payment", "payment", payment_id,
                        {"milestone_id": milestone_id, "amount": amount})
        return self._payment_view(self.store.get_payment(payment_id))

    def confirm_payment(self, payment_id: str, actor: Actor) -> dict:
        payment = self._require_payment(payment_id)
        if payment["state"] != PaymentState.REQUESTED:
            raise DomainError("payment_not_requested",
                              f"付款状态 {payment['state']} 不可确认")
        milestone, mstate = self._require_milestone(payment["milestone_id"])
        project = self._require_project(milestone["project_id"])
        self._require_role(actor, project["finance_roles"], "confirm_payment")
        if actor.party == payment["request_party"]:
            raise DomainError("self_confirm_forbidden", "付款必须由对方财务角色确认")
        if mstate["state"] == MilestoneState.FROZEN:
            raise DomainError("milestone_frozen", "里程碑已冻结，付款待双方复核")
        achievement, supporting = self._evaluate(milestone)
        late = [s["evidence_id"] for s in supporting if not s["within_deadline"]]
        if late:
            raise DomainError("deadline_exceeded",
                              f"证据 {late} 提交超过截止日，不能据此付款")
        now = self._now()
        snapshot = self._build_snapshot(payment, milestone, achievement, supporting, actor, now)
        with self.store.transaction():
            self.store.set_payment_state(payment_id, PaymentState.CONFIRMED,
                                         actor.actor_id, actor.party, now)
            self.store.insert_payment_decision(payment_id, snapshot, now)
            self.store.insert_payment_event(payment_id, "confirmed", actor.actor_id,
                                            now, {"achievement": achievement})
            self._audit(actor, "confirm_payment", "payment", payment_id,
                        {"milestone_id": payment["milestone_id"],
                         "evidence": [s["evidence_id"] for s in supporting]})
        return self.payment_decision(payment_id)

    def reverse_payment(self, payment_id: str, reason: str, actor: Actor,
                        reversal_id: str | None = None) -> dict:
        """冲正：保留原付款与决定快照，追加一笔抵销记录。"""
        payment = self._require_payment(payment_id)
        if payment["state"] not in (PaymentState.CONFIRMED, PaymentState.PAID):
            raise DomainError("payment_not_reversible",
                              f"付款状态 {payment['state']} 不可冲正")
        milestone, _ = self._require_milestone(payment["milestone_id"])
        project = self._require_project(milestone["project_id"])
        self._require_role(actor, project["finance_roles"], "reverse_payment")
        if not reason:
            raise DomainError("reason_required", "冲正必须说明原因")
        reversal_id = reversal_id or f"rev-{payment_id}"
        if self.store.reversals_for(payment_id):
            raise DomainError("already_reversed", "该付款已冲正")
        now = self._now()
        with self.store.transaction():
            self.store.insert_reversal(reversal_id, payment_id,
                                       -round(float(payment["amount"]), 2),
                                       str(reason), actor.actor_id, now)
            self.store.set_payment_state(payment_id, PaymentState.REVERSED)
            self.store.insert_payment_event(payment_id, "reversed", actor.actor_id,
                                            now, {"reversal_id": reversal_id,
                                                  "reason": str(reason)})
            self._audit(actor, "reverse_payment", "payment", payment_id,
                        {"reversal_id": reversal_id, "reason": str(reason)})
        return self.payment_decision(payment_id)

    def payment_callback(self, idempotency_key: str, result: str) -> dict:
        """网关回调：首次记录结论，重试原样返回，绝不改写历史。"""
        with self.store.transaction():
            stored = self.store.get_callback(idempotency_key)
            if stored:
                return json.loads(stored["response"])
            payment = self.store.payment_by_key(idempotency_key)
            if not payment:
                raise DomainError("payment_not_found",
                                  f"找不到幂等键对应的付款: {idempotency_key}")
            now = self._now()
            if payment["state"] == PaymentState.CONFIRMED and result == "success":
                self.store.set_payment_state(payment["payment_id"], PaymentState.PAID)
                self.store.insert_payment_event(payment["payment_id"], "paid",
                                                "gateway", now, {"result": result})
            else:
                self.store.insert_payment_event(payment["payment_id"], "callback_ignored",
                                                "gateway", now,
                                                {"result": result,
                                                 "state": payment["state"]})
            payment = self.store.get_payment(payment["payment_id"])
            response = {"payment_id": payment["payment_id"], "state": payment["state"],
                        "result": result, "handled_at": now}
            self.store.insert_callback(idempotency_key, payment["payment_id"],
                                       result, response, now)
        return response

    # ---------------------------------------------------------------- 查询

    def _require_payment(self, payment_id: str) -> dict:
        payment = self.store.get_payment(payment_id)
        if not payment:
            raise DomainError("payment_not_found", f"付款不存在: {payment_id}")
        return payment

    def _payment_view(self, payment: dict, duplicated: bool = False) -> dict:
        view = {k: payment[k] for k in
                ("payment_id", "milestone_id", "amount", "currency", "state",
                 "idempotency_key", "requested_by", "request_party", "request_ts",
                 "confirmed_by", "confirm_party", "confirm_ts")}
        if duplicated:
            view["duplicated"] = True
        return view

    def _build_snapshot(self, payment, milestone, achievement, supporting, actor, now):
        evidence_entries = []
        for item in supporting:
            confirmations = [
                {"confirmation_id": c["confirmation_id"], "actor": c["actor"],
                 "party": c["party"], "role": c["role"], "ts": c["ts"]}
                for c in self.store.confirmations_for(item["evidence_id"],
                                                      item["version"], active_only=True)
                if c["decision"] == "confirm"
            ]
            evidence_entries.append({
                "evidence_id": item["evidence_id"], "version": item["version"],
                "requirement": item["requirement"], "result": item["result"],
                "party": item["party"], "submitted_at": item["submitted_at"],
                "within_deadline": bool(item["within_deadline"]),
                "confirmations": confirmations,
            })
        return {
            "payment_id": payment["payment_id"],
            "milestone_id": milestone["milestone_id"],
            "milestone_version": milestone["version"],
            "achievement": achievement,
            "amount": payment["amount"], "currency": payment["currency"],
            "deadline_utc": milestone["deadline_utc"],
            "evidence": evidence_entries,
            "approvals": [
                {"action": "request", "actor": payment["requested_by"],
                 "party": payment["request_party"], "ts": payment["request_ts"]},
                {"action": "confirm", "actor": actor.actor_id,
                 "party": actor.party, "ts": now},
            ],
            "decided_at": now,
        }

    def milestone_status(self, milestone_id: str) -> dict:
        milestone, state = self._require_milestone(milestone_id)
        _, supporting = self._evaluate(milestone)
        return {
            "milestone_id": milestone_id, "version": milestone["version"],
            "seq": milestone["seq"], "title": milestone["title"],
            "amount": milestone["amount"], "currency": milestone["currency"],
            "deadline_utc": milestone["deadline_utc"],
            "requirements": milestone["requirements"],
            "state": state["state"], "achievement": state["achievement"],
            "supporting_evidence": [
                {"evidence_id": s["evidence_id"], "version": s["version"],
                 "requirement": s["requirement"], "result": s["result"],
                 "party": s["party"], "within_deadline": bool(s["within_deadline"])}
                for s in supporting],
        }

    def indication_chain(self, project_id: str, indication_id: str) -> dict:
        self._require_project(project_id)
        rows = self.store.milestones_for_indication(project_id, indication_id)
        return {"project_id": project_id, "indication_id": indication_id,
                "milestones": [
                    {"milestone_id": r["milestone_id"], "seq": r["seq"],
                     "title": r["title"], "state": r["state"],
                     "achievement": r["achievement"],
                     "deadline_utc": r["deadline_utc"]} for r in rows]}

    def evidence_view(self, evidence_id: str) -> dict:
        versions = self.store.evidence_versions(evidence_id)
        if not versions:
            raise DomainError("evidence_not_found", f"证据不存在: {evidence_id}")
        return {"evidence_id": evidence_id,
                "state": self.store.get_evidence_state(evidence_id),
                "versions": versions,
                "confirmations": self.store.confirmations_for(evidence_id)}

    def payment_decision(self, payment_id: str) -> dict:
        """还原一笔付款决定使用的证据版本与完整审批链。"""
        payment = self._require_payment(payment_id)
        decision = self.store.get_payment_decision(payment_id)
        events = self.store.payment_events(payment_id)
        for event in events:
            event["detail"] = json.loads(event["detail"])
        return {
            "payment": self._payment_view(payment),
            "decision": decision["snapshot"] if decision else None,
            "events": events,
            "reversals": self.store.reversals_for(payment_id),
        }

    def audit_trail(self, entity_type: str | None = None,
                    entity_id: str | None = None) -> dict:
        rows = self.store.audit_trail(entity_type, entity_id)
        for row in rows:
            row["detail"] = json.loads(row["detail"])
        return {"entries": rows}
