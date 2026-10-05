# 市场价格异常中止

本项目维护市场价格异常中止的领域约定、角色边界与样例数据，供后端服务、接口和自动化验证统一使用。当前契约覆盖企业申报员、核算专员、交易运营员、监管审计员，并明确异常规则版本、市场状态屏障、撮合事务边界、分阶段恢复审计等关键约束。

## 目录

- `domain/contract.json`：领域角色、状态、约束和样例。
- `src/domain_contract/`：契约读取与确定性校验。
- `src/market_halt/`：异常中止服务端（纯标准库实现）。
- `tools/check_contract.py`：命令行摘要检查。
- `tools/run_server.py`：启动 HTTP 服务。
- `tests/`：契约完整性回归测试与服务端行为测试。

## 服务端（src/market_halt/）

交易时段内出现短时异常价格或成交量时，引擎在一致边界停止新撮合，
对当前撮合事务按规则配置执行提交或回滚，绝不留下状态不明的成交；
已进入队列的订单全部保留，恢复时按原订单序列重新进场。

- **异常规则版本**（`rules.py`）：价格偏离、窗口成交量上限、基线倍数三类阈值；
  规则按版本登记（草稿）→ 激活 → 被取代，激活后不可变；每次中止记录触发时的规则版本。
- **市场状态屏障**（`engine.py`）：`OPEN → HALTED → RECOVERING → OPEN`。
  中止期间报单滞留（PARKED）、撤单冻结；恢复分三阶段「仅撤单 → 可报单不撮合 → 恢复撮合」。
- **撮合事务边界**（`engine.py`）：每次撮合是一个事务，引擎在锁内完成整个事务，
  人工触发总是落在事务边界上；自动触发按规则的 `boundary_policy`
  回滚当前事务（`ROLLBACK_CURRENT`，默认）或提交已成交部分（`COMMIT_CURRENT`）。
  提交的成交进入待清算台账，核算专员逐笔清算后才能进入恢复。
- **分阶段恢复审计**（`audit.py`）：触发、人工复核、误报撤销、重复触发
  （并发重复与幂等重放）、每次阶段推进、订单滞留与重新进场，全部留痕。
- **中止事件生命周期**复用契约状态机：草稿 → 待核算 → 已确认 → 执行中 → 已封存。

### 启动

```bash
python3 tools/run_server.py --port 8080
```

### API 摘要

| 方法 | 路径 | 说明 | 角色 |
| --- | --- | --- | --- |
| POST | `/rules` / `/rules/{v}/activate` | 登记规则版本 / 激活 | 交易运营员、监管审计员 / 监管审计员 |
| GET | `/rules` | 规则版本列表 | - |
| POST | `/market/reference` | 设定参考价 | 交易运营员 |
| GET | `/market/state` | 市场状态、恢复阶段、在途事件 | - |
| POST | `/orders` | 报单（`client_order_id` 幂等） | 企业申报员 |
| POST | `/orders/{id}/cancel` | 撤单（中止期冻结，恢复阶段一开放） | 企业申报员 |
| GET | `/orders` / `/orders/{id}` | 订单及逐笔处置记录 | - |
| POST | `/halts` | 人工触发中止（`idempotency_key` 幂等，重复触发只审计） | 交易运营员、监管审计员 |
| GET | `/halts` / `/halts/{id}` | 中止事件与暂停依据（规则版本、违例度量、事务边界处置） | - |
| POST | `/halts/{id}/review` | 人工复核：`confirm` 进入核算，`revoke` 误报撤销并恢复 | 监管审计员 |
| POST | `/halts/{id}/confirm-reconciliation` | 核算确认（存在未清算成交则拒绝） | 核算专员 |
| POST | `/halts/{id}/begin-recovery` / `/advance` | 开始 / 推进分阶段恢复 | 交易运营员 |
| GET | `/trades` / POST `/trades/{id}/clear` | 待清算成交查询 / 逐笔清算 | 核算专员 |
| GET | `/audit` | 审计日志（可按 action、subject 过滤） | - |

错误响应统一为 `{"error": {"code", "message"}}`，领域错误码映射到 400/403/404/409。

## 验证

测试命令：`python3 -m unittest discover -s tests -v`

编译命令：`python3 -m compileall -q src tools tests`

命令行检查：`python3 tools/check_contract.py domain/contract.json`
