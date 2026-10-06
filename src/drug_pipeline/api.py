"""供进程内调用的轻量请求适配层。

请求为 JSON：{"action": ..., 业务字段..., "actor": {"id","party","roles":[...]}}。
业务规则错误返回 {"error": code, "message": ...}，未知动作抛 ValueError。
"""
import json

from .domain import Actor, DomainError
from .service import Service


def _actor(body) -> Actor:
    return Actor.from_payload(body.get("actor"))


def _dispatch(action, body, service: Service):
    # 基线动作
    if action == "health":
        return service.health()
    if action == "register":
        return service.register(str(body["record_id"]), str(body["owner_id"]))
    if action == "find":
        return service.find(str(body["record_id"]))

    # 登记
    if action == "register_project":
        return service.register_project(
            str(body["project_id"]), str(body["name"]), body["parties"],
            body.get("confirm_roles"), body.get("finance_roles"), _actor(body))
    if action == "add_indication":
        return service.add_indication(str(body["project_id"]), str(body["indication_id"]),
                                      str(body["name"]), _actor(body))
    if action == "grant_region_right":
        return service.grant_region_right(str(body["project_id"]), str(body["right_id"]),
                                          str(body["region"]), str(body["party"]),
                                          _actor(body))
    if action == "reassign_region_right":
        return service.reassign_region_right(str(body["right_id"]), str(body["party"]),
                                             _actor(body))
    if action == "split_region_right":
        return service.split_region_right(str(body["right_id"]), body["splits"],
                                          _actor(body))
    if action == "define_milestone":
        return service.define_milestone(
            str(body["milestone_id"]), str(body["project_id"]),
            str(body["indication_id"]), int(body["seq"]), str(body["title"]),
            body["amount"], str(body["currency"]), str(body["deadline"]),
            body["requirements"], _actor(body))

    # 证据与确认
    if action == "submit_evidence":
        return service.submit_evidence(
            str(body["evidence_id"]), str(body["milestone_id"]),
            str(body["requirement"]), str(body["result"]),
            str(body.get("summary", "")), _actor(body))
    if action == "confirm_evidence":
        return service.confirm_evidence(str(body["evidence_id"]), str(body["decision"]),
                                        _actor(body), str(body.get("reason", "")))
    if action == "revoke_confirmation":
        return service.revoke_confirmation(str(body["confirmation_id"]),
                                           str(body["reason"]), _actor(body))
    if action == "withdraw_evidence":
        return service.withdraw_evidence(str(body["evidence_id"]), str(body["reason"]),
                                         _actor(body))
    if action == "unfreeze_milestone":
        return service.unfreeze_milestone(str(body["milestone_id"]), str(body["reason"]),
                                          _actor(body))

    # 付款
    if action == "request_payment":
        return service.request_payment(
            str(body["payment_id"]), str(body["milestone_id"]), body["amount"],
            str(body["idempotency_key"]), _actor(body), str(body.get("note", "")))
    if action == "confirm_payment":
        return service.confirm_payment(str(body["payment_id"]), _actor(body))
    if action == "reverse_payment":
        return service.reverse_payment(str(body["payment_id"]), str(body["reason"]),
                                       _actor(body), body.get("reversal_id"))
    if action == "payment_callback":
        return service.payment_callback(str(body["idempotency_key"]), str(body["result"]))

    # 查询
    if action == "milestone_status":
        return service.milestone_status(str(body["milestone_id"]))
    if action == "indication_chain":
        return service.indication_chain(str(body["project_id"]), str(body["indication_id"]))
    if action == "project_rights":
        return service.project_rights(str(body["project_id"]))
    if action == "right_history":
        return service.right_history(str(body["project_id"]))
    if action == "evidence_view":
        return service.evidence_view(str(body["evidence_id"]))
    if action == "payment_decision":
        return service.payment_decision(str(body["payment_id"]))
    if action == "audit_trail":
        return service.audit_trail(body.get("entity_type"), body.get("entity_id"))

    raise ValueError("不支持的请求动作")


def handle(payload: str, service: Service | None = None) -> str:
    service = service or Service()
    body = json.loads(payload)
    action = body.get("action")
    try:
        result = _dispatch(action, body, service)
    except DomainError as exc:
        return json.dumps({"error": exc.code, "message": exc.message},
                          ensure_ascii=False)
    return json.dumps(result, ensure_ascii=False)
