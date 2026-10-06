"""合作管线与里程碑的领域对象、角色与时间处理。

约定：
- 所有持久化的时间一律为 UTC 的 ISO-8601 字符串；带偏移量的输入在登记时归一，
  之后不再按任何本地时区重新解释，保证跨时区截止日结论稳定。
- 状态机取值集中在下方常量类，持久层只保存字符串。
"""
from dataclasses import dataclass
from datetime import datetime, timezone


class DomainError(Exception):
    """业务规则错误。code 供调用方程序化处理，message 面向协作团队。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class Record:
    record_id: str
    owner_id: str
    state: str = "draft"
    created_at: str = ""

    def with_timestamp(self) -> "Record":
        return Record(self.record_id, self.owner_id, self.state,
                      self.created_at or datetime.now(timezone.utc).isoformat())


@dataclass(frozen=True)
class Actor:
    """一次操作的执行人：所属合作方与角色决定其权限。"""

    actor_id: str
    party: str
    roles: tuple = ()

    @classmethod
    def from_payload(cls, payload) -> "Actor":
        if not isinstance(payload, dict):
            raise DomainError("actor_required", "请求缺少操作人信息 actor")
        actor_id = str(payload.get("id") or "").strip()
        party = str(payload.get("party") or "").strip()
        roles = tuple(str(r) for r in (payload.get("roles") or ()))
        if not actor_id or not party:
            raise DomainError("actor_required", "操作人必须包含 id 与 party")
        return cls(actor_id, party, roles)

    def has_any(self, roles) -> bool:
        return bool(set(self.roles) & set(roles))


class EvidenceState:
    SUBMITTED = "submitted"      # 已提交，待对方确认
    CONFIRMED = "confirmed"      # 指定角色已确认
    REJECTED = "rejected"        # 指定角色已驳回，可修订后再次提交新版本
    WITHDRAWN = "withdrawn"      # 提交方撤回，终态


class MilestoneState:
    OPEN = "open"
    FROZEN = "frozen"            # 上游证据失效后冻结，禁止新证据与付款


class Achievement:
    OPEN = "open"
    PARTIAL = "partially_achieved"
    FULL = "achieved"


class PaymentState:
    REQUESTED = "requested"
    CONFIRMED = "confirmed"
    PAID = "paid"
    REVERSED = "reversed"        # 已冲正；原记录保留，永不删除


class ConfirmationStatus:
    ACTIVE = "active"
    REVOKED = "revoked"


EVIDENCE_RESULTS = ("met", "partial", "not_met")
DEFAULT_CONFIRM_ROLES = ("jsc",)     # 联合指导委员会
DEFAULT_FINANCE_ROLES = ("finance",)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_instant(value) -> datetime:
    """把带时区偏移的 ISO-8601 时间归一到 UTC；裸时间按 UTC 处理。"""
    text = str(value).strip()
    if not text:
        raise DomainError("invalid_time", "时间不能为空")
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        moment = datetime.fromisoformat(text)
    except ValueError as exc:
        raise DomainError("invalid_time", f"无法解析时间: {value!r}") from exc
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat()


def round_money(amount) -> float:
    value = round(float(amount), 2)
    if value <= 0:
        raise DomainError("invalid_amount", "金额必须为正数")
    return value
