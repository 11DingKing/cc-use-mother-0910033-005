# 市场价格异常中止

交易时段内短时间出现异常价格或成交量时，运营人员需要暂停撮合并保留已进入队列的订单。直接停止进程会留下状态不明的成交，因此本项目以**一致事务边界 + 追加式审计**实现服务端，而不是依赖进程退出。

## 领域不变量

对应 `domain/contract.json` 的四条不变量：

1. **异常规则版本**：规则只追加、不改写；每笔待清算成交永久记录撮合时的规则版本与阈值指纹，每次中止事件留存命中版本的完整快照。
2. **市场状态屏障**：正常交易为 `TRADING`；触发后切换为 `CANCEL_ONLY`（仅撤单）；人工确认后分阶段推进到 `LIMIT_ONLY`（限价恢复 ±2% 带宽）再回到 `TRADING`。屏障只在撮合成交流程的事务间隙生效，保证在一致边界停止新撮合。
3. **撮合事务边界**：每笔候选成交先过屏障、再做异常检测（评估只看窗口行情，不落账）。命中即在**提交前回滚**——不生成成交、不写入行情、不扣减队列数量，订单原样留在队首；未命中才提交为 PENDING_SETTLEMENT 的待清算成交，由核算专员逐笔清算。
4. **分阶段恢复审计**：人工复核（确认 / 误报撤销）、三次阶段推进、重复触发、事务回滚、撤单、清算全部写入只追加的审计日志，按事件可回溯。

## 状态机

与契约五状态对齐：`草稿 →（发布规则）→ 待核算 → 已确认 → 执行中 → 已封存`；复核为误报时从待核算直接封存并恢复交易。封存后再次命中会开启新事件。重复触发是幂等的：已有活动事件时屏障与队列不变，仅经 `/market/signals` 或撮合路径追加 `REPEATED_TRIGGER_IGNORED` 审计。

## 目录

- `domain/contract.json`：领域角色、状态、约束和样例。
- `src/domain_contract/`：契约读取与确定性校验。
- `src/market_halt/`：服务端实现。
  - `models.py`：规则版本、订单、待清算成交、五状态枚举。
  - `detector.py`：短时间窗口价格偏移与成交量骤增检测（提交前评估）。
  - `audit.py`：追加式审计日志。
  - `engine.py`：规则版本、状态屏障、事务提交/回滚、复核与分阶段恢复。
  - `api.py`：零依赖 HTTP API（标准库）与启动入口。
- `tools/check_contract.py`：命令行摘要检查。
- `tests/`：契约回归测试 + 异常中止全流程测试（含真实 HTTP 层）。

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
 | GET | `/status` | 市场状态、屏障阶段、队列余量、待清算计数 |
| POST | `/admin/rules` | 发布规则版本（`window_ms` / `max_price_move` / `max_volume_multiple`） |
| GET | `/admin/rules` | 规则版本历史与指纹 |
| POST | `/orders` | 提交订单并立即尝试撮合 |
| POST | `/orders/{id}/cancel` | 仅撤单阶段也允许的撤单 |
| GET | `/orders/{id}` | 单笔订单完整处置轨迹与暂停依据 |
| POST | `/market/signals` | 外部行情异常信号（重复触发经此入口留审计） |
| GET | `/trades?pending=1` | 待清算成交 |
| POST | `/trades/{id}/settle` | 完成当前事务：逐笔清算 |
| GET | `/incidents` | 中止事件列表 |
| GET | `/incidents/{id}` | 暂停依据：命中版本、指纹、观测值/阈值、回滚事务 |
| GET | `/incidents/{id}/orders` | 事件影响的每笔订单及其处置 |
| POST | `/incidents/{id}/review` | 人工复核 `CONFIRMED` / `FALSE_POSITIVE` |
| POST | `/incidents/{id}/advance` | 分阶段恢复推进一阶段（1→2→3） |
| GET | `/audit?incident_id=&action=&actor=` | 审计事件查询 |

操作人身份取请求体 `actor`（推荐，支持中文）或 `X-Actor` 头（latin-1）。

## 验证

测试命令：`python3 -m unittest discover -s tests -v`

编译命令：`python3 -m compileall -q src tools tests`

命令行检查：`python3 tools/check_contract.py domain/contract.json`

启动服务：`PYTHONPATH=src python3 -m market_halt.api --host 127.0.0.1 --port 8080`

### 端到端示例

```bash
# 发布规则：1 秒窗口内价格偏移 5% 即中止
curl -s localhost:8080/admin/rules -d '{"actor":"风控管理员","window_ms":1000,"max_price_move":0.05}'
# 挂两笔 100 元的正常成交，再挂 106 元的对 → 事务回滚、进入仅撤单
curl -s localhost:8080/orders -d '{"order_id":"S1","symbol":"600000","side":"SELL","price":100,"quantity":10}'
curl -s localhost:8080/orders -d '{"order_id":"B1","symbol":"600000","side":"BUY","price":100,"quantity":10}'
# ... 提交 S2/B2 建立基准，随后 S3/B3 @106 触发 ...
# 查询暂停依据与每笔订单处置
curl -s localhost:8080/incidents/<incident_id>
curl -s localhost:8080/incidents/<incident_id>/orders
# 人工复核 → 分阶段恢复
curl -s localhost:8080/incidents/<inc_id>/review  -d '{"actor":"交易运营员","verdict":"CONFIRMED","comment":"已核实"}'
curl -s localhost:8080/incidents/<inc_id>/advance  -d '{"actor":"交易运营员"}'   # 仅撤单
curl -s localhost:8080/incidents/<inc_id>/advance  -d '{"actor":"交易运营员"}'   # 限价恢复
curl -s localhost:8080/incidents/<inc_id>/advance  -d '{"actor":"交易运营员"}'   # 全量恢复、封存
```
