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

## 策略版本与批准链生命周期

研究版本必须经过完整批准链、发布后才能进入自动交易。系统在任务启动时
**固定策略参数与风险设置（运行快照）**，运行期间发布/回滚/撤回新版本不影响
运行中的任务。

### 生命周期

```
草稿 draft ──提交──▶ 复核中 in_review ──批准链全部 approved──▶ approved ──发布──▶ published
                         │                      │
                         └─撤回 withdraw         └─任一 rejected──▶ rejected
发布：publish（可指定 scheduled_for 定时切换）/ rollback 回到上一版 / recall 撤回生效版本
```

- 版本内容在提交复核时冻结（SHA-256 摘要 `content_hash`），之后不可修改；
- 批准链按 `required_approvers` 顺序逐级复核，每步记录复核人与当时内容哈希；
- 发布事件只增不改；同一策略任意时刻至多一个 `effective` 版本
  （进程锁 + `BEGIN IMMEDIATE` + 数据库部分唯一索引三重保证，跨进程并发发布
  恰好一个成功，其余确定性返回 409）；
- 定时切换由后台循环执行，宕机期间到点的切换会在重启恢复时补发；
- 运行快照、信号与订单留痕均不可变，`/orders/{order_id}/trace` 可从订单
  还原当时的版本、参数、风险设置、批准依据并校验哈希完整性。

### 主要接口（前缀 `/api/strategies`）

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /drafts` | 创建草稿 |
| `PATCH /versions/{id}` / `DELETE /versions/{id}` | 修改/删除草稿 |
| `POST /versions/{id}/submit` | 提交复核（冻结内容） |
| `POST /versions/{id}/reviews` | 登记一步复核（按链顺序、指定复核人） |
| `POST /versions/{id}/withdraw` | 撤回复核 |
| `POST /versions/{id}/publish` | 发布（可带 `scheduled_for` 定时切换） |
| `POST /publications/{id}/cancel` | 取消待生效的定时发布 |
| `POST /{name}/rollback` / `POST /{name}/recall` | 回滚 / 撤回 |
| `POST /scheduled/activate` | 立即处理所有到点的定时切换 |
| `GET /versions` / `GET /versions/{id}` | 版本与批准链历史 |
| `GET /publications` / `GET /{name}/effective` | 发布事件流 / 当前生效版本 |
| `POST /api/trading/runs/start` | 启动任务并固定策略与风险设置 |
| `POST /api/trading/runs/stop` / `GET /api/trading/runs/pinned` | 任务管理 |
| `GET /runs/{run_id}` | 运行快照（含批准依据） |
| `GET /runs/{run_id}/signals` / `/orders` | 运行信号与订单留痕 |
| `GET /orders/{order_id}/trace` | 从订单还原批准依据并校验完整性 |
