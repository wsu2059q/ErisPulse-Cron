# ErisPulse-Cron

ErisPulse 定时任务调度模块，为其他模块提供统一的定时任务 API。支持一次性定时、间隔循环、Cron 表达式，回调传参，SQLite 持久化（重启不丢任务）。

## 特性

- **三种定时类型**: 一次性 (`once`)、间隔循环 (`interval`)、Cron 表达式 (`cron`)
- **回调传参**: 创建时传入 `callback_data`，触发时原样返回，方便识别任务来源
- **持久化**: 所有任务存储在 SQLite，重启后自动恢复
- **错过策略**: 支持立即触发 / 跳过 / 重新调度
- **任务管理**: 暂停、恢复、取消、手动触发、清理过期任务

## 安装

```bash
epsdk install Cron
```

> 依赖 ErisPulse >= 2.4.4

## 快速开始

```python
from ErisPulse import sdk

# 1. 注册回调处理器
@sdk.Cron.on_trigger
async def handle_trigger(info):
    data = info["callback_data"]
    print(f"任务触发: {info['task_id']}, 数据: {data}")

# 2. 创建定时任务
task_id = sdk.Cron.once(
    delay=60,
    callback_data={"type": "reminder", "msg": "该喝水了"},
)
```

## API 参考

通过 `sdk.Cron` 访问所有接口。

### 创建任务

#### `once()` — 一次性定时

```python
task_id = sdk.Cron.once(
    delay=600,                                    # 延迟秒数
    # trigger_at=1712345678.0,                    # 或指定绝对时间戳（二选一）
    callback_data={"order_id": "123"},            # 回调时原样返回
    label="订单超时提醒",                           # 可选标签
    source="MyModule",                            # 创建者模块名
    missed_policy="fire_immediately",             # 错过策略
)
# 返回: task_id (str)
```

#### `interval()` — 间隔循环

```python
task_id = sdk.Cron.interval(
    interval_seconds=300,                         # 间隔秒数
    callback_data={"monitor": "server-1"},
    delay=60,                                     # 首次延迟（可选，默认立即开始）
    max_runs=100,                                 # 最大触发次数，0=无限
    label="健康检查",
    source="MyModule",
)
```

#### `cron()` — Cron 表达式

```python
task_id = sdk.Cron.cron(
    expression="0 8 * * 1-5",                     # 标准 5 段 cron
    callback_data={"type": "daily_report"},
    timezone="Asia/Shanghai",                     # 时区
    max_runs=0,
    label="工作日报",
    source="MyModule",
)
```

常用 Cron 表达式：

| 表达式 | 说明 |
|--------|------|
| `*/5 * * * *` | 每 5 分钟 |
| `0 8 * * *` | 每天早 8 点 |
| `30 9 * * 1-5` | 工作日 9:30 |
| `0 0 1 * *` | 每月 1 号 |

### 回调

#### `on_trigger(handler)` — 注册回调

```python
@sdk.Cron.on_trigger
async def my_handler(info):
    data = info["callback_data"]
    # info 结构:
    # {
    #     "task_id": "a1b2c3...",
    #     "task_type": "once",           # once / interval / cron
    #     "callback_data": {...},        # 创建时传入的数据
    #     "label": "订单超时",
    #     "source": "MyModule",
    #     "run_count": 3,                # 当前第几次触发
    #     "max_runs": 0,
    #     "created_at": 1712345678.0,
    #     "last_run": 1712345978.0,
    #     "trigger_time": 1712346000.0,  # 本次触发时间
    # }
```

支持注册多个 handler，全部都会被调用。单个 handler 异常不影响其他 handler。

#### `off_trigger(handler)` — 取消回调

```python
sdk.Cron.off_trigger(my_handler)
```

### 管理任务

```python
# 取消
sdk.Cron.cancel(task_id)

# 暂停
sdk.Cron.pause(task_id)

# 恢复（继续按原计划执行）
sdk.Cron.resume(task_id)

# 恢复并重新计算下次触发时间
sdk.Cron.resume(task_id, reschedule=True)

# 手动立即触发（不影响原计划）
await sdk.Cron.trigger_now(task_id)

# 查看单个任务
task = sdk.Cron.get_task(task_id)

# 列出任务（支持过滤）
tasks = sdk.Cron.list_tasks()
tasks = sdk.Cron.list_tasks(source="MyModule")
tasks = sdk.Cron.list_tasks(status="pending")
tasks = sdk.Cron.list_tasks(task_type="cron")

# 删除任务记录
sdk.Cron.delete_task(task_id)

# 清理 7 天前的已完成/已取消任务
sdk.Cron.cleanup()
sdk.Cron.cleanup(max_age_seconds=86400 * 30)
```

### 错过策略 (missed_policy)

模块重启后，对于错过触发时间的任务：

| 策略 | 行为 |
|------|------|
| `fire_immediately` | 立即触发（默认） |
| `skip` | 跳过本次，等下次 |
| `reschedule` | 从当前时间重新计算下次触发 |

## 完整示例

```python
from ErisPulse import sdk
from ErisPulse.Core.Bases import BaseModule

class OrderModule(BaseModule):
    async def on_load(self, event):
        @sdk.Cron.on_trigger
        async def on_cron_trigger(info):
            data = info["callback_data"]

            if data.get("type") == "order_timeout":
                order_id = data["order_id"]
                await self.cancel_order(order_id)

            elif data.get("type") == "daily_summary":
                await self.send_daily_summary()

    async def create_order(self, order_id):
        # 创建订单，30 分钟后检查是否支付
        sdk.Cron.once(
            delay=1800,
            callback_data={"type": "order_timeout", "order_id": order_id},
            label=f"订单#{order_id}超时",
            source="OrderModule",
            missed_policy="fire_immediately",
        )

    async def start_daily_report(self):
        # 每天早 8 点生成报表
        sdk.Cron.cron(
            expression="0 8 * * *",
            callback_data={"type": "daily_summary"},
            timezone="Asia/Shanghai",
            label="每日报表",
            source="OrderModule",
        )
```

## 数据存储

任务数据存储在 SQLite 表 `cron_tasks` 中（通过 `sdk.storage` 管理），字段包括：

| 字段 | 类型 | 说明 |
|------|------|------|
| id | TEXT | 任务唯一 ID |
| type | TEXT | `once` / `interval` / `cron` |
| status | TEXT | `pending` / `paused` / `completed` / `cancelled` |
| trigger_at | REAL | 一次性任务的触发时间戳 |
| interval_seconds | REAL | 间隔秒数 |
| cron_expr | TEXT | Cron 表达式 |
| cron_timezone | TEXT | 时区，默认 `Asia/Shanghai` |
| callback_data | TEXT | JSON 序列化的回调数据 |
| label | TEXT | 标签 |
| source | TEXT | 创建者模块名 |
| next_run | REAL | 下次触发时间 |
| run_count | INTEGER | 已触发次数 |
| max_runs | INTEGER | 最大次数，0=无限 |
| missed_policy | TEXT | 错过策略 |

## License

MIT
