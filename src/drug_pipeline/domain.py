"""合作管线与里程碑的领域对象与规则。

本模块只描述领域概念，不做持久化：

- 角色与提交方：合作双方各自有提交方标识与确认角色。
- 截止日：按截止日所在时区判定，避免跨时区争议；判定函数是纯函数，
  重复调用（例如重试回调）必然得到同一结论。
- 金额：统一使用定点十进制字符串，禁止二进制浮点参与金额比较。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from zoneinfo import ZoneInfo


# ---------------------------------------------------------------------------
# 异常
# ---------------------------------------------------------------------------
class DomainError(Exception):
    """所有可预期的业务违规都使用该异常，携带稳定的错误码。"""

    code = "domain_error"

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        if code:
            self.code = code


class NotFound(DomainError):
    code = "not_found"


class Conflict(DomainError):
    """并发冲突：命令基于的版本已经过期。"""

    code = "conflict"


class PermissionDenied(DomainError):
    code = "permission_denied"


class ValidationFailed(DomainError):
    code = "validation_failed"


class MilestoneFrozen(DomainError):
    """证据撤回后，依赖证据的后续里程碑被冻结。"""

    code = "milestone_frozen"


class PaymentLocked(DomainError):
    """已确认的付款请求只能冲正，不能修改或删除。"""

    code = "payment_locked"


# ---------------------------------------------------------------------------
# 角色
# ---------------------------------------------------------------------------
PARTY_A = "A"  # 例如：中方药企
PARTY_B = "B"  # 例如：海外伙伴
PARTIES = (PARTY_A, PARTY_B)


class Role(str, Enum):
    PARTY_A_OPERATOR = "A_operator"   # A 方提交人
    PARTY_B_OPERATOR = "B_operator"   # B 方提交人
    PARTY_A_CONFIRMER = "A_confirmer"  # A 方指定确认角色
    PARTY_B_CONFIRMER = "B_confirmer"  # B 方指定确认角色
    STEERING = "steering"             # 联合指导委员会（双方确认齐备）


# 提交方 -> 可提交该方结果的角色
SUBMIT_ROLE = {
    PARTY_A: Role.PARTY_A_OPERATOR.value,
    PARTY_B: Role.PARTY_B_OPERATOR.value,
}
# 提交方 -> 有权确认该方结果的指定角色
CONFIRM_ROLE = {
    PARTY_A: Role.PARTY_A_CONFIRMER.value,
    PARTY_B: Role.PARTY_B_CONFIRMER.value,
}

# 证据/槽位/付款的生命周期状态
STATUS_DRAFT = "draft"
STATUS_SUBMITTED = "submitted"
STATUS_CONFIRMED = "confirmed"
STATUS_WITHDRAWN = "withdrawn"
STATUS_PARTIAL = "partial"
STATUS_FROZEN = "frozen"

PAYMENT_REQUESTED = "requested"
PAYMENT_CONFIRMED = "confirmed"
PAYMENT_REVERSED = "reversed"
PAYMENT_CANCELED = "canceled"  # 挂起请求因证据基础变化被自动取消


# ---------------------------------------------------------------------------
# 时间与金额规则
# ---------------------------------------------------------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_utc().isoformat()


def parse_instant(value: str) -> datetime:
    """把 ISO-8601 字符串解析为带时区的 datetime；缺失时区按 UTC 处理。"""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def deadline_instance(date_text: str, tz_name: str) -> datetime:
    """截止日 = 该日期在截止时区的 24:00（即次日 00:00）。

    例如截止日 2026-03-31、时区 America/New_York，
    指的是纽约时间 2026-04-01 00:00 这一瞬间。
    """
    try:
        tz = ZoneInfo(tz_name)
    except Exception as exc:  # pragma: no cover - 防御
        raise ValidationFailed(f"未知时区: {tz_name}") from exc
    try:
        day = datetime.strptime(date_text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValidationFailed(f"截止日格式应为 YYYY-MM-DD: {date_text}") from exc
    next_day = datetime(day.year, day.month, day.day, tzinfo=tz)
    from datetime import timedelta

    return next_day + timedelta(days=1)


def is_past_deadline(date_text: str, tz_name: str, at: datetime | None = None) -> bool:
    """纯函数：给定判断时刻（默认现在），截止日是否已过。

    结论只取决于参数，因此重试、跨时区调用都不会改变历史结论。
    """
    at = at or now_utc()
    if at.tzinfo is None:
        at = at.replace(tzinfo=timezone.utc)
    return at >= deadline_instance(date_text, tz_name)


def money(value: str | int | Decimal) -> Decimal:
    """把金额统一规整为两位小数的 Decimal；拒绝浮点字面量与非法输入。"""
    if isinstance(value, float):
        raise ValidationFailed("金额禁止使用二进制浮点，请以字符串提交")
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValidationFailed(f"非法金额: {value!r}") from exc
    if amount < 0:
        raise ValidationFailed("金额不能为负")
    return amount.quantize(Decimal("0.01"))


def require_party(party: str) -> str:
    if party not in PARTIES:
        raise ValidationFailed(f"提交方必须是 {PARTIES} 之一: {party!r}")
    return party


def require_roles(roles) -> list[str]:
    if isinstance(roles, str):
        roles = [roles]
    roles = [str(r) for r in roles]
    if not roles:
        raise ValidationFailed("至少需要一个角色")
    return roles


# ---------------------------------------------------------------------------
# 事件
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Event:
    """不可变领域事件。

    seq 在提交存储时分配；事件一旦写入即为历史事实，
    任何后续命令（包括撤回、冲正、回调重试）都只能追加新事件，不能改写旧事件。
    """

    event_type: str
    payload: dict
    actor: str = ""
    actor_roles: tuple = ()
    cmd_id: str = ""
    event_id: str = ""
    seq: int = 0
    created_at: str = ""

    def as_row(self) -> dict:
        return {
            "seq": self.seq,
            "event_id": self.event_id,
            "cmd_id": self.cmd_id,
            "event_type": self.event_type,
            "payload": self.payload,
            "actor": self.actor,
            "actor_roles": list(self.actor_roles),
            "created_at": self.created_at,
        }


# ---------------------------------------------------------------------------
# 遗留记录对象（保留既有登记能力）
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Record:
    record_id: str
    owner_id: str
    state: str = "draft"
    created_at: str = ""

    def with_timestamp(self) -> "Record":
        return Record(self.record_id, self.owner_id, self.state,
                      self.created_at or now_iso())


