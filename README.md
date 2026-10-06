# 创新药合作管线服务

面向“中国药企 × 海外伙伴共同开发”场景的协作服务端。同一条管线同时承载
临床证据、注册结论、区域权利与多阶段付款，服务回答三类问题：

- 某个里程碑**现在是否达成**、哪些合作方已经确认；
- 某个地区**由谁负责**、谁有权发布该地区的数据；
- 每笔付款决定**使用了哪一版定义、哪些证据、经过谁的审批**。

所有写入都是只追加的领域事件，撤回、撤销确认、冲正都不会改写或删除历史。

## 设计要点

| 需求 | 实现 |
| --- | --- |
| 版本化登记 | 项目/适应症/地区权利/里程碑均带 `version`；当前定义取最大版本，旧版本与登记事件永久保留；重复版本号被 `conflict` 拒绝 |
| 双方分别提交、指定角色确认 | 每个里程碑按合作方（A/B）各有证据槽位；`A_operator/B_operator` 提交，`A_confirmer/B_confirmer` 确认 |
| 部分达成 | 只有一方确认达成时状态为 `partial`，付款被阻止；双方都确认 `met` 且依赖达成才 `achieved` |
| 证据撤回冻结后续里程碑 | 撤回产生 `evidence_withdrawn`，冻结集沿 `depends_on` 传播到不动点；冻结期间禁止证据操作、付款与数据发布；撤回方可用新证据恢复 |
| 确认撤销 | 撤销产生 `result_confirmation_revoked`，槽位回到待确认，撤销事实留在审批链 |
| 付款不可删除 | `payment_requested → confirmed → reversed`，确认后只能冲正，冲正后净额为 0 且原请求/确认事件保留；证据在确认付款存在时被锁定 |
| 决定基础变化 | 证据重提/撤销/撤回、里程碑或地区权利出新版本时，挂起的付款请求自动 `canceled`（追加事件），确认时还要复核当前派生结论 |
| 跨时区截止日 | 截止日按其声明时区的本地午夜判定（`zoneinfo`），判定是纯函数，重试不改变结论 |
| 并行/重试安全 | 每条命令带 `cmd_id`，`commands` 表保证同 ID 只生效一次；事件与发件箱同事务提交（SQLite 单写者串行化 + busy_timeout） |
| 回调故障恢复 | 事务发件箱 + 每目的地投递游标，至少投递一次；消费方按 `event_id` 幂等；进程崩溃后重启自动继续，新目的地接入可补齐历史 |
| 越权发布防护 | `release_data` 校验本方证据已确认，且发布地区在权利表中归属于本方 |
| 付款溯源 | `payment_provenance` 返回付款时固化的证据/权利快照与按序审批链 |

## 目录

- `src/drug_pipeline/domain.py`：角色、错误、时区截止日、定点金额、事件对象。
- `src/drug_pipeline/store.py`：SQLite 事件日志、幂等命令表、事务发件箱、投递游标。
- `src/drug_pipeline/projection.py`：事件回放与派生结论（槽位、部分达成、冻结/达成不动点）。
- `src/drug_pipeline/service.py`：命令编排、权限、付款生命周期、回调重试与溯源查询。
- `src/drug_pipeline/api.py`：JSON 动作适配层（写操作需要 `actor/roles/cmd_id`）。
- `tests/test_acceptance.py`：验收演练（并行提交、部分达成、区域拆分、确认撤销、
  撤回冻结、冲正、跨时区截止、回调重试、崩溃恢复、付款溯源、版本化）。

## 运行

```bash
PYTHONPATH=src python3 -m unittest discover -s tests   # 全部测试
python3 -m compileall src                              # 语法检查
```

只使用 Python 3.10+ 标准库（`zoneinfo` 需系统时区数据），无需其他服务。

## 典型流程（服务 API）

```python
from drug_pipeline.service import Service
from drug_pipeline.store import Store

svc = Service(Store("pipeline.db"))
svc.register_project("P1", "联合开发", actor="pm", roles=["A_operator"], cmd_id="c1")
svc.register_indication("I1", "P1", "一线肺癌", actor="pm",
                        roles=["A_operator"], cmd_id="c2")
svc.register_region_rights("R1", "I1",
    [{"region": "CN", "party": "A"}, {"region": "US", "party": "B"}],
    actor="legal", roles=["A_confirmer"], cmd_id="c3")
svc.register_milestone("M1", "P1", "III期主要终点", actor="pm",
    roles=["A_operator"], cmd_id="c4", indication_id="I1",
    deadline="2026-03-31", deadline_tz="America/New_York",
    amount="1000000.00", currency="USD")

# 双方各自提交、各自的指定角色确认
svc.submit_result("M1", "A", "EV-A-1", "met", actor="cra-a",
                  roles=["A_operator"], cmd_id="c5", regions=["CN"])
svc.submit_result("M1", "B", "EV-B-1", "met", actor="cra-b",
                  roles=["B_operator"], cmd_id="c6", regions=["US"])
svc.confirm_result("M1", "A", actor="med-a", roles=["A_confirmer"], cmd_id="c7")
svc.confirm_result("M1", "B", actor="med-b", roles=["B_confirmer"], cmd_id="c8")

# 全部达成后请求、确认付款
svc.request_payment("PAY-1", "M1", actor="fin", roles=["A_operator"], cmd_id="c9")
svc.confirm_payment("PAY-1", actor="cfo", roles=["payment_confirmer"], cmd_id="c10")

# 只能冲正
svc.reverse_payment("PAY-1", "入组争议暂缓", actor="cfo",
                    roles=["payment_confirmer"], cmd_id="c11")

# 追溯这笔付款的证据快照与审批链
svc.payment_provenance("PAY-1")
```

JSON 适配层：

```python
handle(json.dumps({"action": "submit_result", "milestone_id": "M1",
                   "party": "A", "evidence_id": "EV-A-1", "result": "met",
                   "actor": "cra-a", "roles": ["A_operator"],
                   "cmd_id": "c5"}))
# 成功: {"ok": true, "result": {...}}；业务违规: {"ok": false, "error": {"code", "message"}}
```

可用动作见 `api.py` 中的 `_ACTIONS`（写入）与 `_QUERIES`（查询）。
