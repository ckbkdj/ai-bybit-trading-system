import asyncio
import random
import sys
import time
import logging
import aiohttp
from aiohttp import TCPConnector, ClientTimeout
from .rate_limiter import AsyncRateLimiter

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class HTTPClient:
    """改进的HTTP客户端，解决长时间运行后网络请求失效的问题"""

    def __init__(self, proxies: dict | None = None, global_interval: float = 0.1):
        self.global_limiter = AsyncRateLimiter(global_interval)
        self.log = logging.getLogger("HTTP")
        self.proxy = (proxies or {}).get("http")

        # 会话生命周期管理
        self._session = None
        self._session_created_at = 0
        self._session_max_age = 3600  # 1小时后强制重建会话
        self._max_requests_per_session = 1000  # 每个会话最多处理1000个请求
        self._request_count = 0
        self._session_lock = asyncio.Lock()

        # 连接池配置
        self._connector_config = {
            "limit": 20,  # 全局并发连接上限
            "limit_per_host": 8,  # 每主机上限（降低以避免过载）
            "ttl_dns_cache": 600,  # DNS缓存时间延长到10分钟
            "use_dns_cache": True,
            "enable_cleanup_closed": True,
            "keepalive_timeout": 30,  # Keep-alive超时时间
            "force_close": False,  # 避免强制关闭连接
        }

        # 超时配置 - 更加保守
        self._timeout_config = {
            "total": 30,  # 总超时时间增加
            "connect": 10,  # 连接超时时间增加
            "sock_read": 20,  # 读取超时时间增加
            "sock_connect": 10
        }

        # 健康检查相关
        self._last_health_check = 0
        self._health_check_interval = 300  # 5分钟检查一次
        self._consecutive_failures = 0
        self._max_consecutive_failures = 5
        self._create_session()

    def _create_session(self):
        """创建新的HTTP会话"""
        try:
            connector = TCPConnector(
                limit=20,
                limit_per_host=8,
                ttl_dns_cache=300,
                enable_cleanup_closed=True,
                force_close=True  # ✅ 强制销毁旧连接
            )
            timeout = ClientTimeout(total=30, connect=10, sock_read=20)

            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                trust_env=True,
                connector_owner=True,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
            )
        except Exception as e:
            self.log.error(f"创建HTTP会话失败: {e}")
    async def _reset_all(self):
        self.log.warning("⚠️ 开始彻底重建 HTTPClient，包括 connector 和 session")
        try:
            if self._session and not self._session.closed:
                await self._session.close()
                self.log.info("🧹 原 HTTP 会话已关闭")
            await asyncio.sleep(0.2)  # 给 loop 时间清理连接

            connector = TCPConnector(
                limit=20,
                limit_per_host=8,
                ttl_dns_cache=300,
                enable_cleanup_closed=True,
                force_close=True  # ✅ 强制销毁旧连接
            )
            timeout = ClientTimeout(total=30, connect=10, sock_read=20)

            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                trust_env=True,
                connector_owner=True,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
            )

            self._session_created_at = time.time()
            self._request_count = 0
            self._consecutive_failures = 0
            self.log.info("✅ HTTPClient 重建完成，新的会话已创建")
        except Exception as e:
            self.log.error(f"❌ HTTPClient 重建失败: {e}", exc_info=True)

    async def _get_session(self) -> aiohttp.ClientSession:
        """获取或创建HTTP会话，包含更强健的重建逻辑"""
        async with self._session_lock:
            current_time = time.time()

            force_rebuild_reason = None

            if self._session is None:
                force_rebuild_reason = "session 不存在"
            elif getattr(self._session, 'closed', True):
                force_rebuild_reason = "session 已关闭"
            elif not getattr(self._session, '_connector', None):
                force_rebuild_reason = "session 连接器已丢失"
            elif current_time - self._session_created_at > self._session_max_age:
                force_rebuild_reason = "session 生命周期超时"
            elif self._request_count >= self._max_requests_per_session:
                force_rebuild_reason = "达到最大请求数"
            elif self._consecutive_failures >= self._max_consecutive_failures:
                force_rebuild_reason = f"连续失败数达 {self._max_consecutive_failures}"
            if force_rebuild_reason:
                self.log.warning(f"将重建HTTP会话，原因: {force_rebuild_reason}")
                await self._close_session()
                await self._reset_all()
                self._consecutive_failures = 0

            return self._session

    async def _close_session(self):
        """安全关闭当前会话"""
        if self._session and not self._session.closed:
            try:
                await self._session.close()
                # 等待连接器完全关闭
                await asyncio.sleep(0.1)
                self.log.debug("HTTP会话已关闭")
            except Exception as e:
                self.log.warning(f"关闭HTTP会话时出错: {e}")
        self._session = None

    async def _health_check(self):
        """定期健康检查"""
        current_time = time.time()
        if current_time - self._last_health_check < self._health_check_interval:
            return

        self._last_health_check = current_time

        try:
            if self._session and not self._session.closed:
                # 检查连接器状态
                connector = self._session.connector
                if hasattr(connector, '_conns'):
                    active_conns = sum(len(conns) for conns in connector._conns.values())
                    self.log.debug(f"活跃连接数: {active_conns}")

                    # 如果活跃连接过多，考虑重建会话
                    if active_conns > self._connector_config["limit"] * 0.8:
                        self.log.warning("活跃连接数过多，标记会话需要重建")
                        self._consecutive_failures = self._max_consecutive_failures

        except Exception as e:
            self.log.warning(f"健康检查失败: {e}")

    async def get(self, url: str, params=None, *, retry: int = 3, limiter=None, **kwargs):
        """改进的GET请求方法"""
        lim = limiter or self.global_limiter
        await lim.wait()

        original_retry = retry
        backoff_base = 0.5

        for attempt in range(retry):
            session = None
            try:
                # 执行健康检查
                await self._health_check()

                # 获取会话
                session = await self._get_session()
                self._request_count += 1

                # 合并请求参数
                request_kwargs = {
                    'params': params,
                    'proxy': self.proxy,
                    **kwargs
                }

                # 执行请求
                async with session.get(url, **request_kwargs) as response:
                    # 检查响应状态
                    if response.status == 429:  # Rate limited
                        retry_after = int(response.headers.get('Retry-After', 5))
                        self.log.warning(f"遇到限流，等待 {retry_after} 秒")
                        await asyncio.sleep(retry_after)
                        continue

                    response.raise_for_status()

                    # 重置失败计数
                    self._consecutive_failures = 0

                    # 尝试解析JSON
                    try:
                        return await response.json()
                    except aiohttp.ContentTypeError:
                        # 如果不是JSON，返回文本
                        return await response.text()

            except (
                    aiohttp.ClientConnectionError,
                    aiohttp.ServerDisconnectedError,
                    aiohttp.ClientConnectorError,
                    aiohttp.ClientProxyConnectionError
            ) as e:
                self._consecutive_failures += 1
                self.log.warning(f"连接错误 (尝试 {attempt + 1}/{retry}): {e}")

                # 连接错误时强制重建会话
                if attempt < retry - 1:
                    await self._close_session()
                    backoff = backoff_base * (2 ** attempt) + random.random()
                    await asyncio.sleep(backoff)
                else:
                    self.log.error(f"GET {url} 连接失败，已达到最大重试次数")
                    return None

            except asyncio.TimeoutError as e:
                self._consecutive_failures += 1
                self.log.warning(f"请求超时 (尝试 {attempt + 1}/{retry}): {url}")

                if attempt < retry - 1:
                    backoff = backoff_base * (2 ** attempt) + random.random()
                    await asyncio.sleep(backoff)
                elif attempt == retry - 1:
                    backoff = backoff_base * (2 ** attempt) + random.random()
                    await asyncio.sleep(backoff)
                    await self._reset_all()
                else:
                    self.log.error(f"GET {url} 超时失败，已达到最大重试次数")
                    await self._close_session()
                    await self._reset_all()
                    return None

            except aiohttp.ClientResponseError as e:
                if e.status == 451:
                    # Binance restricted-location: non-retryable in this deployment.
                    # Return a sentinel so callers can use neutral/local fallback without noisy ERROR logs.
                    self.log.warning(f"客户端错误 451 restricted location: {e.message}")
                    return {"_http_status": 451, "_error": "restricted_location"}

                if e.status in (500, 502, 503, 504):  # 服务器错误，可以重试
                    self._consecutive_failures += 1
                    self.log.warning(f"服务器错误 {e.status} (尝试 {attempt + 1}/{retry})")

                    if attempt < retry - 1:
                        backoff = backoff_base * (2 ** attempt) + random.random()
                        await asyncio.sleep(backoff)
                        continue

                # 客户端错误，不重试
                self.log.error(f"客户端错误 {e.status}: {e.message}")
                return None

            except Exception as e:
                self._consecutive_failures += 1
                self.log.error(f"未预期的错误 (尝试 {attempt + 1}/{retry}): {e}")

                if attempt < retry - 1:
                    backoff = backoff_base * (2 ** attempt) + random.random()
                    await asyncio.sleep(backoff)
                else:
                    return None

        return None

    async def post(self, url: str, data=None, json=None, *, retry: int = 3, limiter=None, **kwargs):
        """POST请求方法"""
        lim = limiter or self.global_limiter
        await lim.wait()

        for attempt in range(retry):
            try:
                await self._health_check()
                session = await self._get_session()
                self._request_count += 1

                request_kwargs = {
                    'data': data,
                    'json': json,
                    'proxy': self.proxy,
                    **kwargs
                }

                async with session.post(url, **request_kwargs) as response:
                    if response.status == 429:
                        retry_after = int(response.headers.get('Retry-After', 5))
                        await asyncio.sleep(retry_after)
                        continue

                    response.raise_for_status()
                    self._consecutive_failures = 0

                    try:
                        return await response.json()
                    except aiohttp.ContentTypeError:
                        return await response.text()

            except Exception as e:
                self._consecutive_failures += 1
                self.log.error(f"POST请求失败 (尝试 {attempt + 1}/{retry}): {e}")

                if attempt < retry - 1:
                    await self._close_session()
                    backoff = 0.5 * (2 ** attempt) + random.random()
                    await asyncio.sleep(backoff)
                else:
                    return None

        return None

    async def aclose(self):
        """关闭HTTP客户端"""
        try:
            await self._close_session()
            self.log.info("HTTP客户端已关闭")
        except Exception as e:
            self.log.error(f"关闭HTTP客户端时出错: {e}")

    def get_stats(self) -> dict:
        """获取客户端统计信息"""
        return {
            "request_count": self._request_count,
            "consecutive_failures": self._consecutive_failures,
            "session_age": time.time() - self._session_created_at if self._session else 0,
            "session_active": self._session is not None and not self._session.closed
        }

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()