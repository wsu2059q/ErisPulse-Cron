import asyncio
import time

from ErisPulse import sdk
from ErisPulse.Core.Event import command


async def main():
    is_init = await sdk.init()
    if not is_init:
        sdk.logger.error("SDK 初始化失败")
        return

    cron = sdk.Cron

    trigger_log: list[str] = []

    @cron.on_trigger
    async def on_cron_trigger(info):
        data = info["callback_data"]
        label = info.get("label") or info["task_id"][:8]
        msg = (f"[触发] {label} | "
               f"type={info['task_type']} | "
               f"run={info['run_count']} | "
               f"data={data}")
        trigger_log.append(msg)
        sdk.logger.info(msg)
        try:
            for adapter_name in sdk.adapter.platforms:
                adapter = sdk.adapter.get(adapter_name)
                await adapter.Send.To("user", "U1001").Text(msg)
        except Exception:
            pass

    # ========== 测试命令 ==========

    @command("cron.once", help="测试一次性定时: /cron.once <秒数>")
    async def cmd_once(event):
        args = event.get_command_args()
        seconds = float(args[0]) if args else 5
        task_id = cron.once(
            delay=seconds,
            callback_data={"test": "once", "created_by": event.get_user_id()},
            label=f"一次性({seconds}s)",
            source="TestScript",
        )
        await event.reply(f"已创建一次性任务\nID: {task_id}\n将在 {seconds} 秒后触发")

    @command("cron.interval", help="测试间隔循环: /cron.interval <间隔秒> [次数]")
    async def cmd_interval(event):
        args = event.get_command_args()
        seconds = float(args[0]) if args else 5
        max_runs = int(args[1]) if len(args) > 1 else 3
        task_id = cron.interval(
            interval_seconds=seconds,
            callback_data={"test": "interval", "interval": seconds},
            max_runs=max_runs,
            label=f"间隔({seconds}s x{max_runs})",
            source="TestScript",
        )
        await event.reply(f"已创建间隔任务\nID: {task_id}\n间隔 {seconds}s, 最多 {max_runs} 次")

    @command("cron.cron", help="测试Cron表达式: /cron.cron <表达式>")
    async def cmd_cron(event):
        args = event.get_command_args()
        expr = args[0] if args else "*/1 * * * *"
        task_id = cron.cron(
            expression=expr,
            callback_data={"test": "cron", "expr": expr},
            max_runs=3,
            label=f"Cron({expr})",
            source="TestScript",
        )
        await event.reply(f"已创建 Cron 任务\nID: {task_id}\n表达式: {expr}\n最多触发 3 次")

    @command("cron.list", help="列出所有任务")
    async def cmd_list(event):
        tasks = cron.list_tasks()
        if not tasks:
            await event.reply("当前没有任务")
            return
        lines = [f"共 {len(tasks)} 个任务:\n"]
        for t in tasks:
            status_emoji = {"pending": "🟢", "paused": "⏸️", "completed": "✅", "cancelled": "❌"}.get(t["status"], "?")
            remaining = max(0, t.get("next_run", 0) - time.time())
            if t["status"] != "pending":
                remaining_str = "-"
            else:
                remaining_str = f"{remaining:.0f}s"
            lines.append(
                f"{status_emoji} {t['id'][:8]} | {t['type']:8s} | "
                f"{t['status']:9s} | next={remaining_str} | "
                f"run={t.get('run_count', 0)}/{t.get('max_runs', 0) or '∞'} | "
                f"{t.get('label', '')}"
            )
        await event.reply("\n".join(lines))

    @command("cron.cancel", help="取消任务: /cron.cancel <task_id>")
    async def cmd_cancel(event):
        args = event.get_command_args()
        if not args:
            await event.reply("用法: /cron.cancel <task_id(前8位即可)>")
            return
        target = args[0]
        tasks = cron.list_tasks()
        matched = [t for t in tasks if t["id"].startswith(target)]
        if not matched:
            await event.reply(f"未找到任务: {target}")
            return
        results = []
        for t in matched:
            ok = cron.cancel(t["id"])
            results.append(f"{'✅' if ok else '❌'} {t['id'][:8]}")
        await event.reply("\n".join(results))

    @command("cron.pause", help="暂停任务: /cron.pause <task_id>")
    async def cmd_pause(event):
        args = event.get_command_args()
        if not args:
            await event.reply("用法: /cron.pause <task_id(前8位即可)>")
            return
        target = args[0]
        tasks = cron.list_tasks()
        matched = [t for t in tasks if t["id"].startswith(target)]
        if not matched:
            await event.reply(f"未找到任务: {target}")
            return
        results = []
        for t in matched:
            ok = cron.pause(t["id"])
            results.append(f"{'⏸️' if ok else '❌'} {t['id'][:8]}")
        await event.reply("\n".join(results))

    @command("cron.resume", help="恢复任务: /cron.resume <task_id>")
    async def cmd_resume(event):
        args = event.get_command_args()
        if not args:
            await event.reply("用法: /cron.resume <task_id(前8位即可)>")
            return
        target = args[0]
        tasks = cron.list_tasks(status="paused")
        matched = [t for t in tasks if t["id"].startswith(target)]
        if not matched:
            await event.reply(f"未找到暂停的任务: {target}")
            return
        results = []
        for t in matched:
            ok = cron.resume(t["id"])
            results.append(f"{'▶️' if ok else '❌'} {t['id'][:8]}")
        await event.reply("\n".join(results))

    @command("cron.fire", help="手动触发: /cron.fire <task_id>")
    async def cmd_fire(event):
        args = event.get_command_args()
        if not args:
            await event.reply("用法: /cron.fire <task_id(前8位即可)>")
            return
        target = args[0]
        tasks = cron.list_tasks()
        matched = [t for t in tasks if t["id"].startswith(target) and t["status"] in ("pending", "paused")]
        if not matched:
            await event.reply(f"未找到可触发的任务: {target}")
            return
        result = await cron.trigger_now(matched[0]["id"])
        if result:
            await event.reply(f"已手动触发: {matched[0]['id'][:8]}\nData: {result['callback_data']}")
        else:
            await event.reply("触发失败")

    @command("cron.log", help="查看最近触发日志")
    async def cmd_log(event):
        if not trigger_log:
            await event.reply("暂无触发记录")
            return
        recent = trigger_log[-10:]
        await event.reply(f"最近 {len(recent)} 条触发记录:\n" + "\n".join(recent))

    @command("cron.clean", help="清理已完成/已取消的任务")
    async def cmd_clean(event):
        count = cron.cleanup()
        await event.reply(f"已清理 {count} 条过期记录")

    @command("cron.runall", help="一键运行所有自动化测试")
    async def cmd_runall(event):
        results = []
        results.append("===== 自动化测试 =====\n")

        # Test 1: once
        try:
            tid = cron.once(
                delay=3,
                callback_data={"auto_test": 1, "name": "once_3s"},
                label="自动测试-once",
                source="AutoTest",
            )
            results.append(f"[1/7] once(delay=3) id={tid[:8]}")
        except Exception as e:
            results.append(f"[1/7] once() {e}")

        # Test 2: once with trigger_at
        try:
            tid = cron.once(
                trigger_at=time.time() + 5,
                callback_data={"auto_test": 2, "name": "once_trigger_at"},
                label="自动测试-once(trigger_at)",
                source="AutoTest",
            )
            results.append(f"[2/7] once(trigger_at=now+5) id={tid[:8]}")
        except Exception as e:
            results.append(f"[2/7] once(trigger_at) {e}")

        # Test 3: interval
        try:
            tid = cron.interval(
                interval_seconds=4,
                callback_data={"auto_test": 3, "name": "interval_4s"},
                max_runs=2,
                label="自动测试-interval",
                source="AutoTest",
            )
            results.append(f"[3/7] interval(4s, max=2) id={tid[:8]}")
        except Exception as e:
            results.append(f"[3/7] interval() {e}")

        # Test 4: interval with delay
        try:
            tid = cron.interval(
                interval_seconds=6,
                delay=2,
                callback_data={"auto_test": 4, "name": "interval_delay"},
                max_runs=2,
                label="自动测试-interval(delay)",
                source="AutoTest",
            )
            results.append(f"[4/7] interval(6s, delay=2) id={tid[:8]}")
        except Exception as e:
            results.append(f"[4/7] interval(delay) {e}")

        # Test 5: cron expression
        try:
            tid = cron.cron(
                expression="*/1 * * * *",
                callback_data={"auto_test": 5, "name": "cron_every_min"},
                max_runs=1,
                label="自动测试-cron(每分钟)",
                source="AutoTest",
            )
            results.append(f"[5/7] cron(*/1 * * * *) id={tid[:8]}")
        except Exception as e:
            results.append(f"[5/7] cron() {e}")

        # Test 6: get_task + pause + resume + cancel
        try:
            tid = cron.interval(
                interval_seconds=999,
                callback_data={"auto_test": 6},
                label="自动测试-lifecycle",
                source="AutoTest",
            )
            task = cron.get_task(tid)
            assert task is not None, "get_task returned None"
            assert task["status"] == "pending"

            ok = cron.pause(tid)
            assert ok, "pause failed"
            task = cron.get_task(tid)
            assert task["status"] == "paused"

            ok = cron.resume(tid)
            assert ok, "resume failed"
            task = cron.get_task(tid)
            assert task["status"] == "pending"

            ok = cron.cancel(tid)
            assert ok, "cancel failed"
            task = cron.get_task(tid)
            assert task["status"] == "cancelled"

            results.append(f"[6/7] lifecycle(pause/resume/cancel)")
        except AssertionError as e:
            results.append(f"[6/7] lifecycle {e}")
        except Exception as e:
            results.append(f"[6/7] lifecycle {e}")

        # Test 7: list_tasks filter
        try:
            all_tasks = cron.list_tasks()
            auto_tasks = cron.list_tasks(source="AutoTest")
            pending = cron.list_tasks(status="pending")
            cancelled = cron.list_tasks(status="cancelled")
            results.append(
                f"[7/7] list_tasks "
                f"total={len(all_tasks)} auto={len(auto_tasks)} "
                f"pending={len(pending)} cancelled={len(cancelled)}"
            )
        except Exception as e:
            results.append(f"[7/7] list_tasks {e}")

        results.append("\n===== 测试完成 =====")
        results.append("等待任务触发中... (使用 /cron.log 查看触发记录)")
        await event.reply("\n".join(results))

    await sdk.adapter.startup()
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
