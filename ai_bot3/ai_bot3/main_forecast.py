import asyncio
import logging
import signal
import sys

from core.config_loader import load_config
from core.portfolio3_3_fixed import PortfolioPredictor
from core.scheduler import Scheduler, Job

logging.basicConfig(
    level="INFO",
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def amain():
    cfg = load_config("config.yml")
    portfolio = PortfolioPredictor(cfg)
    # 启动单消费者
    portfolio.start_worker()

    sched = Scheduler()

    # 仅预测：按 forecast.schedule 把任务入队（防抖合并）
    modes = cfg["forecast"]["schedule"]
    for mode, cron_expression in modes.items():
        async def task(m=mode, cron=cron_expression):
            logging.info(f"⏰ 触发预测：{m} - {cron}")
            portfolio.enqueue_predict(m)
        sched.add(Job(name=f"predict:{mode}", cron=cron_expression, coro_fn=task))

    # 优雅退出
    if sys.platform != "win32":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, sched.stop)

    await sched.run_forever()

    # 退出前清理
    await portfolio.close()

if __name__ == "__main__":
    asyncio.run(amain())
