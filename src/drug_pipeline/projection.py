"""事件回放与派生结论。

投影是纯函数式的：给定同一个事件序列，必然得到同一个状态。它不做任何
I/O，也不做权限判断——服务层在写入事件前负责校验，投影只负责解释历史。

关键不变量：

- 当前定义（项目/适应症/地区权利/里程碑）= 该实体编号下版本号最大的登记，
  旧版本仍然保留，可随时还原。
- 槽位（milestone × 提交方）记录双方各自的证据生命周期。
- 冻结是派生结论：某方证据撤回，或其依赖的上游里程碑被冻结，则该里程碑冻结。
- 已确认付款的状态只被 ``payment_reversed`` 翻转，任何删除路径都不存在。
"""
from __future__ import annotations

from typing import Any

from .domain import (
    PARTIES,
    PAYMENT_CANCELED,
    PAYMENT_CONFIRMED,
    PAYMENT_REQUESTED,
    PAYMENT_REVERSED,
    STATUS_CONFIRMED,
    STATUS_DRAFT,
    STATUS_FROZEN,
    STATUS_PARTIAL,
    STATUS_SUBMITTED,
    STATUS_WITHDRAWN,
)


def _empty_slot() -> dict[str, Any]:
    return {
        "evidence_id": "",
        "result": "",              # met / not_met
        "metrics": {},
        "regions": [],
        "status": STATUS_DRAFT,
        "submitted_by": "",
        "submitted_at": "",
        "submit_event_id": "",
        "confirmed_by": "",
        "confirmed_at": "",
        "confirm_event_id": "",
        "withdrawn_by": "",
        "withdrawn_at": "",
        "evidence_history": [],    # 每个证据的完整生命周期，旧证据不被抹掉
    }


class State:
    def __init__(self) -> None:
        self.projects: dict[str, list[dict]] = {}
        self.indications: dict[str, list[dict]] = {}
        self.rights: dict[str, list[dict]] = {}
        self.milestones: dict[str, list[dict]] = {}
        self.slots: dict[str, dict[str, dict]] = {}   # milestone_id -> party -> slot
        self.payments: dict[str, dict] = {}
        self.releases: list[dict] = []
        self.seq = 0

    # ------------------------------------------------------------------
    # 回放
    # ------------------------------------------------------------------
    def apply(self, event: dict) -> None:
        seq = event["seq"]
        self.seq = seq
        p = event["payload"]
        kind = event["event_type"]
        at = event["created_at"]

        if kind == "project_registered":
            self.projects.setdefault(p["project_id"], []).append(
                {**p, "seq": seq, "at": at, "event_id": event["event_id"]})
        elif kind == "indication_registered":
            self.indications.setdefault(p["indication_id"], []).append(
                {**p, "seq": seq, "at": at, "event_id": event["event_id"]})
        elif kind == "region_rights_registered":
            self.rights.setdefault(p["rights_id"], []).append(
                {**p, "seq": seq, "at": at, "event_id": event["event_id"]})
        elif kind == "milestone_registered":
            self.milestones.setdefault(p["milestone_id"], []).append(
                {**p, "seq": seq, "at": at, "event_id": event["event_id"]})

        elif kind == "result_submitted":
            slot = self._slot(p["milestone_id"], p["party"])
            entry = {
                "evidence_id": p["evidence_id"],
                "result": p["result"],
                "metrics": p.get("metrics", {}),
                "regions": list(p.get("regions", [])),
                "submitted_by": event["actor"],
                "submitted_at": p.get("submitted_at", at),
                "submit_event_id": event["event_id"],
                "seq": seq,
            }
            slot.update(entry)
            slot["status"] = STATUS_SUBMITTED
            slot["confirmed_by"] = slot["confirmed_at"] = ""
            slot["confirm_event_id"] = ""
            # 新证据覆盖当前槽位，撤回状态解除；撤回事实仍留在事件历史中。
            slot["withdrawn_by"] = slot["withdrawn_at"] = ""
            slot["evidence_history"].append(entry)

        elif kind == "result_confirmed":
            slot = self._slot(p["milestone_id"], p["party"])
            slot["status"] = STATUS_CONFIRMED
            slot["confirmed_by"] = event["actor"]
            slot["confirmed_at"] = p.get("confirmed_at", at)
            slot["confirm_event_id"] = event["event_id"]

        elif kind == "result_confirmation_revoked":
            slot = self._slot(p["milestone_id"], p["party"])
            # 回到“已提交待确认”，确认事实被追加记录为撤销而非删除。
            slot["status"] = STATUS_SUBMITTED
            slot["confirmed_by"] = ""
            slot["confirmed_at"] = ""
            slot.setdefault("revocations", []).append({
                "by": event["actor"], "at": at, "reason": p.get("reason", ""),
                "seq": seq})

        elif kind == "evidence_withdrawn":
            slot = self._slot(p["milestone_id"], p["party"])
            slot["status"] = STATUS_WITHDRAWN
            slot["withdrawn_by"] = event["actor"]
            slot["withdrawn_at"] = p.get("withdrawn_at", at)
            slot["confirmed_by"] = slot["confirmed_at"] = ""

        elif kind in ("payment_requested", "payment_confirmed",
                      "payment_reversed", "payment_canceled"):
            self._apply_payment(kind, p, event)

        elif kind == "data_released":
            self.releases.append({**p, "seq": seq, "at": at,
                                  "event_id": event["event_id"]})

    def _apply_payment(self, kind: str, p: dict, event: dict) -> None:
        pid = p["payment_id"]
        payment = self.payments.get(pid)
        if kind == "payment_requested":
            self.payments[pid] = {
                "payment_id": pid,
                "milestone_id": p["milestone_id"],
                "status": PAYMENT_REQUESTED,
                "amount": p["amount"],
                "currency": p.get("currency", ""),
                "evidence": p.get("evidence", {}),
                "requested_by": event["actor"],
                "requested_at": event["created_at"],
                "confirmed_by": "",
                "confirmed_at": "",
                "reversals": [],
                "seq": event["seq"],
            }
            return
        if payment is None:
            # 历史不会出现这种情况（服务层保证），防御性处理。
            raise ValueError(f"付款 {pid} 不存在，无法应用 {kind}")
        if kind == "payment_confirmed":
            payment["status"] = PAYMENT_CONFIRMED
            payment["confirmed_by"] = event["actor"]
            payment["confirmed_at"] = event["created_at"]
        elif kind == "payment_reversed":
            # 冲正追加为独立记录；付款保留可追溯，净额为 0。
            payment["status"] = PAYMENT_REVERSED
            payment["reversals"].append({
                "by": event["actor"], "reason": p.get("reason", ""),
                "at": event["created_at"], "seq": event["seq"]})
        elif kind == "payment_canceled":
            # 挂起请求所依据的证据发生变化，请求自动作废；记录保留可审计。
            payment["status"] = PAYMENT_CANCELED
            payment["canceled_at"] = event["created_at"]
            payment["cancel_reason"] = p.get("reason", "")
            payment["cancel_seq"] = event["seq"]

    def _slot(self, milestone_id: str, party: str) -> dict:
        return self.slots.setdefault(milestone_id, {}).setdefault(
            party, _empty_slot())

    # ------------------------------------------------------------------
    # 当前定义查询（取最大版本）
    # ------------------------------------------------------------------
    @staticmethod
    def _current(versions: dict[str, list[dict]], entity_id: str) -> dict | None:
        rows = versions.get(entity_id)
        if not rows:
            return None
        return max(rows, key=lambda r: (r.get("version", 1), r["seq"]))

    def project(self, project_id: str) -> dict | None:
        return self._current(self.projects, project_id)

    def indication(self, indication_id: str) -> dict | None:
        return self._current(self.indications, indication_id)

    def rights_for(self, indication_id: str) -> dict | None:
        rows = [v for group in self.rights.values()
                for v in group if v.get("indication_id") == indication_id]
        if not rows:
            return None
        return max(rows, key=lambda r: (r.get("version", 1), r["seq"]))

    def milestone(self, milestone_id: str) -> dict | None:
        return self._current(self.milestones, milestone_id)

    def milestone_versions(self, milestone_id: str) -> list[dict]:
        return list(self.milestones.get(milestone_id, ()))

    def slot(self, milestone_id: str, party: str) -> dict | None:
        group = self.slots.get(milestone_id)
        return group.get(party) if group else None

    # ------------------------------------------------------------------
    # 派生结论
    # ------------------------------------------------------------------
    def required_parties(self, milestone: dict) -> tuple[str, ...]:
        return tuple(milestone.get("required_parties") or PARTIES)

    def _conclusions(self) -> tuple[set[str], set[str]]:
        """返回 (冻结集, 达成集)，两者都传播到不动点。

        - 冻结：自身证据撤回，或传递依赖到被冻结里程碑。
        - 达成：未冻结、要求各方确认达成、且全部（传递）依赖达成。
        图在登记时保证无环，不动点迭代必然收敛。
        """
        frozen: set[str] = set()
        for mid, group in self.slots.items():
            if any(s["status"] == STATUS_WITHDRAWN for s in group.values()):
                frozen.add(mid)
        changed = True
        while changed:
            changed = False
            for mid in self.milestones:
                if mid in frozen:
                    continue
                current = self.milestone(mid)
                if any(d in frozen for d in (current or {}).get("depends_on", [])):
                    frozen.add(mid)
                    changed = True

        def slots_confirmed(mid: str) -> bool:
            current = self.milestone(mid)
            for party in self.required_parties(current):
                slot = self.slot(mid, party)
                if not slot or slot["status"] != STATUS_CONFIRMED \
                        or slot["result"] != "met":
                    return False
            return True

        achieved: set[str] = set()
        changed = True
        while changed:
            changed = False
            for mid in self.milestones:
                if mid in achieved or mid in frozen:
                    continue
                current = self.milestone(mid)
                deps = current.get("depends_on", [])
                if any(d in frozen for d in deps):
                    continue
                if slots_confirmed(mid) and all(d in achieved for d in deps):
                    achieved.add(mid)
                    changed = True
        return frozen, achieved

    def frozen_milestones(self) -> set[str]:
        return self._conclusions()[0]

    def achieved_milestones(self) -> set[str]:
        return self._conclusions()[1]

    def milestone_view(self, milestone_id: str) -> dict | None:
        milestone = self.milestone(milestone_id)
        if milestone is None:
            return None
        required = self.required_parties(milestone)
        frozen_set, achieved_set = self._conclusions()
        is_frozen = milestone_id in frozen_set

        confirmed_met, missing, slot_views = [], [], {}
        for party in required:
            slot = self.slot(milestone_id, party) or _empty_slot()
            slot_views[party] = {
                "evidence_id": slot["evidence_id"],
                "result": slot["result"],
                "metrics": slot["metrics"],
                "regions": slot["regions"],
                "status": slot["status"],
                "submitted_by": slot["submitted_by"],
                "submitted_at": slot["submitted_at"],
                "confirmed_by": slot["confirmed_by"],
                "confirmed_at": slot["confirmed_at"],
                "withdrawn_by": slot["withdrawn_by"],
                "withdrawn_at": slot["withdrawn_at"],
            }
            if slot["status"] == STATUS_CONFIRMED and slot["result"] == "met":
                confirmed_met.append(party)
            else:
                missing.append(party)

        achieved = milestone_id in achieved_set
        if is_frozen:
            status = STATUS_FROZEN
        elif achieved:
            status = STATUS_CONFIRMED
        elif confirmed_met:
            status = STATUS_PARTIAL
        elif any(slot_views[p]["status"] == STATUS_SUBMITTED for p in required):
            status = STATUS_SUBMITTED
        else:
            status = STATUS_DRAFT

        deps = milestone.get("depends_on", [])
        deps_achieved = all(d in achieved_set for d in deps) if deps else True

        return {
            "milestone_id": milestone_id,
            "project_id": milestone["project_id"],
            "indication_id": milestone.get("indication_id", ""),
            "name": milestone["name"],
            "stage": milestone.get("stage", ""),
            "version": milestone.get("version", 1),
            "supersedes": milestone.get("supersedes", ""),
            "registered_event_id": milestone["event_id"],
            "registered_at": milestone["at"],
            "status": status,
            "achieved": achieved,
            "frozen": is_frozen,
            "achieved_parties": tuple(confirmed_met),
            "missing_parties": tuple(missing),
            "required_parties": required,
            "slots": slot_views,
            "criteria": milestone.get("criteria", {}),
            "deadline": milestone.get("deadline", ""),
            "deadline_tz": milestone.get("deadline_tz", ""),
            "depends_on": deps,
            "deps_achieved": deps_achieved,
            "amount": milestone.get("amount", ""),
            "currency": milestone.get("currency", ""),
        }

    def payment_view(self, payment_id: str) -> dict | None:
        payment = self.payments.get(payment_id)
        if payment is None:
            return None
        view = {k: v for k, v in payment.items()}
        # 净额：只有已确认（未冲正/取消）才计入。
        view["net_amount"] = payment["amount"] \
            if payment["status"] == PAYMENT_CONFIRMED else "0.00"
        # 一旦离开 requested，付款决定即锁定：确认/冲正/取消都只能追加历史。
        view["is_locked"] = payment["status"] != PAYMENT_REQUESTED
        return view

    def region_owner(self, indication_id: str, region: str) -> str | None:
        rights = self.rights_for(indication_id)
        if rights is None:
            return None
        for item in rights.get("regions", []):
            if item["region"] == region:
                return item["party"]
        return None
