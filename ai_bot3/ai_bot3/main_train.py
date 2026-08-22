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
    portfolio.start_worker()

    sched = Scheduler()

    # 仅训练：按 schedule 把训练任务入队（队列串行，自动合并重复模式）
    modes = cfg["schedule"]
    for mode, cron_expression in modes.items():
        async def task(m=mode, cron=cron_expression):
            logging.info(f"⏰ 触发训练：{m} - {cron}")
            portfolio.enqueue_train(m)
        sched.add(Job(name=f"train:{mode}", cron=cron_expression, coro_fn=task))

    if sys.platform != "win32":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, sched.stop)

    await sched.run_forever()
    await portfolio.close()

if __name__ == "__main__":
    asyncio.run(amain())
