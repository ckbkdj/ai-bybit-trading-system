import asyncio
import datetime as dt
import logging
from typing import Callable

from croniter import croniter

log = logging.getLogger("Scheduler")

class Job:
    def __init__(self, name:str, cron:str, coro_fn:Callable[[], "Awaitable[None]"]):
        self.name, self.cron, self.coro_fn = name, cron, coro_fn
        self._next = None     # 下次触发时间

    def calc_next(self, now=None):
        now = now or dt.datetime.now(dt.timezone.utc)
        self._next = croniter(self.cron, now).get_next(dt.datetime)

    @property
    def due_in(self):
        return max(0, (self._next - dt.datetime.now(dt.timezone.utc)).total_seconds())

class Scheduler:
    """极简 cron 轮询器"""
    def __init__(self):
        self.jobs:list[Job] = []
        self._stop = asyncio.Event()

    def add(self, job:Job):
        job.calc_next()
        self.jobs.append(job)

    def stop(self):
        self._stop.set()

    async def run_forever(self):
        while not self._stop.is_set():
            if not self.jobs:
                await asyncio.sleep(60); continue
            # 找到最近的任务
            self.jobs.sort(key=lambda j: j._next)
            j = self.jobs[0]
            wait = j.due_in
            # await asyncio.wait([self._stop.wait()], timeout=wait)
            stop_task = asyncio.create_task(self._stop.wait())
            await asyncio.wait([stop_task], timeout=wait)
            if self._stop.is_set(): break
            # 到点执行
            asyncio.create_task(self._exec_job(j))
            j.calc_next()  # 重新排期

    async def _exec_job(self, job:Job):
        try:
            log.info("⏰ Run job %s", job.name)
            await job.coro_fn()
        except Exception as e:
            log.exception("Job %s error: %s", job.name, e)
