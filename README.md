# 策略版本与批准链业务服务

这是一个使用 Python、FastAPI 与 SQLite 实现的纯后端业务服务，包含领域模型、数据访问、业务编排、接口和异常路径测试。项目可在单个 Linux 应用容器内离线运行，使用本地 SQLite 或内存替身，不依赖外部运行服务。

## 安装

```bash
python3 -m pip install -r requirements.txt
```

## 测试

```bash
python3 -m pytest -q
```

## 构建检查

```bash
python3 -m compileall -q app
```

## API 导入冒烟

```bash
python3 -c "from app.main import app; print(len(app.routes))"
```

## 启动

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 策略版本生命周期

研究版本必须经过复核发布后才能进入自动交易。系统实现了草稿、复核、
发布、撤回、回滚、运行快照的完整生命周期：

- **草稿**：`POST /api/strategies/{key}/versions` 创建版本（版本号单调递增，
  参数与风险参数生成 SHA-256 内容指纹），提交复核前可修改。
- **复核（批准链）**：`POST .../versions/{id}/submit` 固定有序复核人列表，
  `POST .../versions/{id}/decisions` 必须按顺序逐级批准；任一级驳回为终态，
  批准链不完整的版本无法发布。
- **发布/撤回/回滚**：`publish`、`POST /api/strategies/{key}/withdraw`、
  `POST /api/strategies/{key}/rollback`；发布/撤回/回滚是追加到时间线上的
  不可变事件，按 `(effective_at, id)` 确定性折叠。请求可带 `effective_at`
  实现定时切换（不能早于当前时间），到期前可
  `DELETE /api/strategies/events/{event_id}` 取消；时间线可通过
  `GET /api/strategies/{key}/timeline` 与 `GET .../active?at=` 查询，
  历史查询结果不随后续发布改变。
- **运行快照**：`POST /api/strategies/{key}/runs`（或
  `POST /api/trading/strategy-runs/{key}/start`）启动任务时冻结版本参数、
  风险参数、内容指纹与完整批准依据。运行期间发布的新版本不影响该任务，
  风控也使用快照中的风险参数；`heartbeat` 上报心跳，后台推进器
  （`STRATEGY_TICK_SECONDS`，默认 5s）负责激活定时事件并将心跳超时
  （`STRATEGY_STALE_RUN_SECONDS`，默认 60s）的运行标记为 `crashed`。
- **批准依据还原**：每个信号（仅追加）与每笔订单（提交时写一次，永不更新）
  都携带运行快照与内容指纹；`GET /api/strategies/orders/{order_id}/provenance`
  可从订单还原 订单 → 信号 → 运行快照 → 版本 → 批准链。

典型流程：

```bash
# 1. 草稿
curl -X POST localhost:8000/api/strategies/chan-a/versions \
  -H 'Content-Type: application/json' \
  -d '{"params":{"ma_period":10},"risk_params":{"stop_loss_ratio":0.08}}'

# 2. 提交两级复核并逐级批准
curl -X POST localhost:8000/api/strategies/versions/1/submit \
  -H 'Content-Type: application/json' -d '{"reviewers":["alice","bob"]}'
curl -X POST localhost:8000/api/strategies/versions/1/decisions \
  -H 'Content-Type: application/json' -d '{"reviewer":"alice","decision":"approved"}'
curl -X POST localhost:8000/api/strategies/versions/1/decisions \
  -H 'Content-Type: application/json' -d '{"reviewer":"bob","decision":"approved"}'

# 3. 发布（可带 effective_at 定时切换）
curl -X POST localhost:8000/api/strategies/versions/1/publish \
  -H 'Content-Type: application/json' -d '{}'

# 4. 启动自动交易任务（固定版本与风险），之后交易的每笔订单均可追溯
curl -X POST localhost:8000/api/trading/strategy-runs/chan-a/start \
  -H 'Content-Type: application/json' -d '{}'
```
