"""供进程内调用的轻量请求适配层。

请求与响应都是 JSON 字符串。写操作需要 ``actor``、``roles`` 与 ``cmd_id``
（cmd_id 缺失时自动生成；调用方重试时应带上同一 cmd_id 才能幂等）。

业务违规返回 ``{"ok": false, "error": {"code", "message"}}``，不抛到边界外。
"""
from __future__ import annotations

import json
import uuid

from .domain import DomainError
from .service import Service


def _take(body: dict, *names, default=None):
    return {n: body[n] for n in names if n in body}


def _write_context(body: dict) -> dict:
    return {
        "actor": str(body.get("actor", "")),
        "roles": list(body.get("roles", []) or []),
        "cmd_id": str(body.get("cmd_id") or uuid.uuid4().hex),
    }


# 每个动作：调用方法、固定字段透传、是否写操作（注入 actor/roles/cmd_id）。
_ACTIONS = {
    "register_project": ("register_project",
                         ("project_id", "name", "version", "supersedes"), True),
    "register_indication": ("register_indication",
                            ("indication_id", "project_id", "name",
                             "version", "supersedes"), True),
    "register_region_rights": ("register_region_rights",
                               ("rights_id", "indication_id", "regions",
                                "version", "supersedes"), True),
    "register_milestone": ("register_milestone",
                           ("milestone_id", "project_id", "name",
                            "indication_id", "stage", "required_parties",
                            "criteria", "deadline", "deadline_tz", "amount",
                            "currency", "depends_on", "version", "supersedes"),
                           True),
    "submit_result": ("submit_result",
                      ("milestone_id", "party", "evidence_id", "result",
                       "metrics", "regions", "submitted_at"), True),
    "confirm_result": ("confirm_result",
                       ("milestone_id", "party", "confirmed_at"), True),
    "revoke_confirmation": ("revoke_confirmation",
                            ("milestone_id", "party", "reason"), True),
    "withdraw_evidence": ("withdraw_evidence",
                          ("milestone_id", "party", "reason"), True),
    "request_payment": ("request_payment",
                        ("payment_id", "milestone_id", "amount", "currency",
                         "regions"), True),
    "confirm_payment": ("confirm_payment", ("payment_id",), True),
    "reverse_payment": ("reverse_payment", ("payment_id", "reason"), True),
    "release_data": ("release_data",
                     ("release_id", "milestone_id", "regions", "title"), True),
    "dispatch_pending": ("dispatch_pending", ("destination",), False),
}

_QUERIES = {
    "get_project": ("get_project", ("project_id",)),
    "get_milestone": ("get_milestone", ("milestone_id",)),
    "get_rights": ("get_rights", ("indication_id",)),
    "list_payments": ("list_payments", ("milestone_id",)),
    "payment_provenance": ("payment_provenance", ("payment_id",)),
    "audit_log": ("audit_log", ("milestone_id",)),
    "outbox_status": ("outbox_status", ()),
}


def handle(payload: str, service: Service | None = None) -> str:
    service = service or Service()
    try:
        body = json.loads(payload)
    except json.JSONDecodeError as exc:
        return _error("invalid_json", str(exc))
    action = body.get("action")

    if action == "health":
        return json.dumps(service.health(), ensure_ascii=False)
    if action == "register":
        # 遗留登记能力保持原有签名。
        return json.dumps(service.register(str(body["record_id"]),
                                           str(body["owner_id"])),
                          ensure_ascii=False)
    if action == "find":
        return json.dumps(service.find(str(body["record_id"])), ensure_ascii=False)

    try:
        if action in _ACTIONS:
            method_name, fields, is_write = _ACTIONS[action]
            kwargs = _take(body, *fields)
            if is_write:
                kwargs.update(_write_context(body))
            result = getattr(service, method_name)(**kwargs)
            return json.dumps({"ok": True, "result": result}, ensure_ascii=False,
                              default=_json_default)
        if action in _QUERIES:
            method_name, fields = _QUERIES[action]
            result = getattr(service, method_name)(**_take(body, *fields))
            if result is None:
                return _error("not_found", f"未找到资源: {action}", found=False)
            return json.dumps({"ok": True, "result": result}, ensure_ascii=False,
                              default=_json_default)
    except DomainError as exc:
        return _error(exc.code, str(exc))
    except TypeError as exc:
        return _error("bad_request", str(exc))

    return _error("unsupported_action", f"不支持的请求动作: {action!r}")


def _json_default(value):
    if isinstance(value, (set, tuple)):
        return list(value)
    raise TypeError(f"不可序列化的类型: {type(value)!r}")


def _error(code: str, message: str, *, found: bool = True) -> str:
    return json.dumps({"ok": False, "found": found,
                       "error": {"code": code, "message": message}},
                      ensure_ascii=False)
