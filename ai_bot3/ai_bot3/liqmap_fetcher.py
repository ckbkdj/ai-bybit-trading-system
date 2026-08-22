"""Coinglass 反向接口数据采集基线（run_v3.sh 主入口之一）。

本模块是项目数据栈的中心：所有需要从 Coinglass 拉取的真实预测指标都通过
这里完成 token 生成 / 加密 / 解密 / 落盘。它支撑：

1. 爆仓图（liqMap）原始抓取——保留旧路径 ``data/{BASE}.json``。
2. 通用解密 GET 帮手 ``fetch_decrypted``，复用 ``cache-ts-v2`` / AES / OTP /
   ``scrypt.js`` 流程。
3. 辅助预测指标，全部按基币（如 ``BTC``）参数请求，落盘到
   ``data/coinglass_metrics/`` 下：

   - ``{base}_open_interest.json`` – 全网持仓量及变化
   - ``{base}_open_interest_exchange.json`` – 各交易所持仓
   - ``{base}_open_interest_statistics.json`` – 统计快照
   - ``{base}_funding_rate.json`` – 资金费率正负 / 强弱
   - ``{base}_funding_rate_avg.json`` – 资金费率均值
   - ``{base}_long_short_ratio.json`` – 多空 / 顶级账户多空
   - ``{base}_volume_24h.json`` – 24H 成交量
   - ``{base}_volume_category.json`` – 合约市场分类快照
   - ``{base}_liquidation_history.json`` – 历史爆仓
   - ``{base}_liquidation_today.json`` – 今日多空爆仓与人数
   - ``{base}_liquidation_exchange.json`` – 各交易所爆仓
   - ``{base}_liquidation_coin.json`` – 各币种爆仓
4. 上下文新闻 / 事件 wrappers：``financial_calendar.json``、
   ``whale_alert.json``、``fear_greed_index.json``、``news_context.json``、
   ``events.json``。如果远端不可用，写入 ``status=stale/unavailable/error``
   并在保留旧数据基础上通过 ``send_util.send_error_warning`` 发飞书告警
   （带冷却 / 去重）。

5. 调度：

   - 爆仓图按固定 ``interval_seconds``（默认 300 秒）轮询，币种间随机间隔。
   - 辅助指标按 30~45 分钟随机周期刷新。
   - 鲸鱼大额转账独立按 ~30 分钟刷新并稳定去重。
   - BTC 60 秒窗口内波动 ≥ 0.8% 立即触发 ``refresh_event_signal(force=True)``。

6. 鲁棒性：任何 endpoint 失败仅记录日志，不会清空历史文件，不会让消费者
   （``core/sentiment.py`` / ``core/data_fetch.py``）崩溃；
   未验证的接口（如 ``/api/stock/news``、``/api/articles``）一律不放进
   生产默认 endpoint 列表。
"""

from __future__ import annotations

import base64
import json
import logging
import os
import random
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyotp
import requests
import execjs
from Crypto.Cipher import AES

from utils.send_util import  send_error_warning_with_cooldown


logging.basicConfig(
    level="INFO",
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


# ---------------------------------------------------------------------------
# 路径常量
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
METRICS_DIR = DATA_DIR / "coinglass_metrics"
EVENTS_FILE = METRICS_DIR / "events.json"
WHALE_SEEN_FILE = METRICS_DIR / "whale_alert_seen.json"
NEWS_CONTEXT_FILE = METRICS_DIR / "news_context.json"
FINANCIAL_CALENDAR_FILE = METRICS_DIR / "financial_calendar.json"
WHALE_ALERT_FILE = METRICS_DIR / "whale_alert.json"
FEAR_GREED_FILE = METRICS_DIR / "fear_greed_index.json"

DATA_DIR.mkdir(parents=True, exist_ok=True)
METRICS_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# 工具：时间戳 ISO 格式
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def _atomic_dump(path: Path, payload: Dict[str, Any]) -> None:
    """原子写入 JSON：先 ``.tmp`` 再 ``replace``。"""
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        tmp.replace(path)
    except Exception as exc:  # pragma: no cover
        logging.warning(f"原子写入 {path.name} 失败: {exc}")
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass


def _safe_load_json(path: Path) -> Optional[Dict[str, Any]]:
    """安全读取 JSON；失败返回 None，不抛异常。"""
    try:
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        return {"data": data}
    except Exception:
        return None


# ---------------------------------------------------------------------------
# 上下文数据归一化（独立函数，便于单元测试）
# ---------------------------------------------------------------------------

def _parse_numeric(value: Any) -> Optional[float]:
    """解析 Coinglass 可能带百分号/逗号/空串的数值字段。"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text or text in ("-", "--"):
        return None
    if text.endswith("%"):
        text = text[:-1]
    try:
        return float(text)
    except Exception:
        return None


def _calendar_sentiment_score(raw: Dict[str, Any]) -> float:
    """财经事件方向打分：实际值 - 预测值，再按预测值绝对值归一化到 [-1, 1]。"""
    try:
        actual = _parse_numeric(raw.get("actual") or raw.get("pubValue"))
        forecast = _parse_numeric(raw.get("forecast") or raw.get("estimateValue"))
        if actual is None or forecast is None:
            return 0.0
        denom = abs(forecast) + 1e-9
        return max(-1.0, min(1.0, (actual - forecast) / denom))
    except Exception:
        return 0.0


def normalize_financial_calendar(raw_list: Any, source: str = "coinglass_calendar") -> List[Dict[str, Any]]:
    """把 Coinglass 财经日历原始 list 归一化为统一字段。

    输出每条记录字段包括：
    - generated_at / source / type / title / country / currency / importance
    - event_time / actual / forecast / previous / sentiment_score / impact_tags
    """
    items: List[Dict[str, Any]] = []
    if not isinstance(raw_list, list):
        return items
    for raw in raw_list[:64]:
        if not isinstance(raw, dict):
            continue
        title = raw.get("title") or raw.get("event") or raw.get("calendarName") or ""
        actual = raw.get("actual") if raw.get("actual") is not None else raw.get("pubValue")
        forecast = raw.get("forecast") if raw.get("forecast") is not None else raw.get("estimateValue")
        previous = raw.get("previous") if raw.get("previous") is not None else raw.get("preValue")
        event_time = raw.get("time") or raw.get("date") or raw.get("pubTimestamp") or raw.get("pubTime") or ""
        items.append({
            "generated_at": _now_iso(),
            "source": source,
            "type": raw.get("type") or "calendar",
            "title": str(title)[:200],
            "country": raw.get("country") or raw.get("region") or raw.get("countryName") or raw.get("countryCode") or "",
            "currency": raw.get("currency") or "",
            "importance": raw.get("importance") or raw.get("star") or 0,
            "event_time": event_time,
            "actual": actual,
            "forecast": forecast,
            "previous": previous,
            "sentiment_score": _calendar_sentiment_score(raw),
            "impact_tags": raw.get("impactTags") or raw.get("dataEffectCode") or raw.get("dataEffect") or [],
        })
    return items


def _whale_remark(raw: Dict[str, Any]) -> Dict[str, Any]:
    """解析 marketHistory 里 remark JSON，失败返回空 dict。"""
    remark = raw.get("remark")
    if isinstance(remark, dict):
        return remark
    if isinstance(remark, str) and remark.strip():
        try:
            parsed = json.loads(remark)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _is_exchange_name(value: str) -> bool:
    value = (value or "").lower()
    if not value or value in ("unknown", "unknown wallet"):
        return False
    exchange_keys = (
        "exchange", "binance", "okx", "bybit", "huobi", "htx", "kraken",
        "kucoin", "bitget", "coinbase", "bitfinex", "gate", "mexc",
        "crypto.com", "upbit", "bitstamp", "gemini", "bitflyer", "deribit",
    )
    return any(k in value for k in exchange_keys)


def _whale_direction_hint(raw: Dict[str, Any]) -> str:
    """从大额转账原始记录推断方向：流入交易所 / 流出交易所 / 交易所互转 / 钱包间。"""
    remark = _whale_remark(raw)
    to_addr = str(raw.get("to") or raw.get("toAddr") or remark.get("to") or "")
    from_addr = str(raw.get("from") or raw.get("fromAddr") or remark.get("from") or "")
    from_is_ex = _is_exchange_name(from_addr)
    to_is_ex = _is_exchange_name(to_addr)
    if to_is_ex and not from_is_ex:
        return "inflow_to_exchange"
    if from_is_ex and not to_is_ex:
        return "outflow_from_exchange"
    if from_is_ex and to_is_ex:
        return "exchange_to_exchange"
    return "wallet_transfer"


def _whale_sentiment_score(raw: Dict[str, Any]) -> float:
    """流入交易所偏空 (-0.6)；流出交易所偏多 (+0.6)；交易所互转/未知中性 0。"""
    hint = _whale_direction_hint(raw)
    if hint == "inflow_to_exchange":
        return -0.6
    if hint == "outflow_from_exchange":
        return 0.6
    return 0.0


def _stable_whale_id(raw: Dict[str, Any]) -> str:
    """生成稳定去重 ID：优先 txId / hash；否则用 time+symbol+amount 拼接。"""
    remark = _whale_remark(raw)
    tx_id = (
        raw.get("txId")
        or raw.get("tx_id")
        or raw.get("hash")
        or remark.get("hash")
        or raw.get("id")
        or f"{raw.get('time') or raw.get('createTime') or raw.get('date','')}-{raw.get('symbol','')}-{raw.get('amount') or raw.get('volUsd','')}"
    )
    return str(tx_id)[:120]


def normalize_whale_alert(
    raw_list: Any,
    *,
    seen: Optional[Dict[str, int]] = None,
    include_seen: bool = False,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """归一化鲸鱼大额转账并按 ``seen`` 去重。

    返回 ``(items, new_seen)``：默认 ``items`` 仅包含未在 ``seen`` 中出现过的记录；
    ``new_seen`` 是这一批新增条目的去重键映射，调用方可合并到全局 seen 文件。

    ``include_seen=True`` 用于持久化真实接口快照：即使交易已见过，也保留到
    ``items``，避免把“接口真实返回但没有新增交易”误判成 endpoint 不可用。
    """
    items: List[Dict[str, Any]] = []
    new_seen: Dict[str, int] = {}
    if not isinstance(raw_list, list):
        return items, new_seen
    seen = seen or {}
    flattened: List[Dict[str, Any]] = []
    for raw in raw_list[:64]:
        if not isinstance(raw, dict):
            continue
        # /api/marketHistory 返回形如 {date, list:[...]} 的分组结构
        if isinstance(raw.get("list"), list):
            for child in raw.get("list") or []:
                if isinstance(child, dict):
                    merged = dict(child)
                    merged.setdefault("groupDate", raw.get("date"))
                    flattened.append(merged)
        else:
            flattened.append(raw)

    emitted: set = set()
    for raw in flattened[:64]:
        if not isinstance(raw, dict):
            continue
        remark = _whale_remark(raw)
        tx_id = _stable_whale_id(raw)
        if not tx_id or tx_id in emitted:
            continue
        is_seen = tx_id in seen
        if is_seen and not include_seen:
            continue
        items.append({
            "tx_id": tx_id,
            "time": raw.get("time") or raw.get("createTime") or raw.get("date") or raw.get("groupDate") or int(time.time()),
            "symbol": raw.get("symbol") or raw.get("coin") or "",
            "amount_usd": float(raw.get("amountUsd") or raw.get("usdValue") or raw.get("volUsd") or 0.0),
            "price": raw.get("price"),
            "from": raw.get("from") or raw.get("fromAddr") or remark.get("from") or "",
            "to": raw.get("to") or raw.get("toAddr") or remark.get("to") or "",
            "tx_url": remark.get("web") or "",
            "direction_hint": _whale_direction_hint(raw),
            "sentiment_score": _whale_sentiment_score(raw),
            "generated_at": _now_iso(),
        })
        emitted.add(tx_id)
        if not is_seen:
            new_seen[tx_id] = int(time.time())
    return items, new_seen


def normalize_fear_greed(payload: Any) -> Optional[Dict[str, Any]]:
    """归一化恐惧贪婪指数：value(0-100) -> sentiment_score(-1,1)。"""
    if not isinstance(payload, dict):
        return None
    try:
        if payload.get("value") is not None:
            value = float(payload.get("value"))
            timestamp = payload.get("timestamp") or int(time.time())
        elif payload.get("price") is not None:
            value = float(payload.get("price"))
            timestamp = int(time.time())
        elif isinstance(payload.get("valueList"), list) and payload.get("valueList"):
            value = float(payload["valueList"][-1])
            times = payload.get("timeList") if isinstance(payload.get("timeList"), list) else []
            timestamp = times[-1] if times else int(time.time())
        else:
            return None
    except Exception:
        return None
    if value <= 24:
        classification = "extreme fear"
    elif value <= 44:
        classification = "fear"
    elif value <= 55:
        classification = "neutral"
    elif value <= 75:
        classification = "greed"
    else:
        classification = "extreme greed"
    return {
        "value": value,
        "classification": payload.get("classification") or classification,
        "timestamp": timestamp,
        # 极度恐惧 = -1，极度贪婪 = +1
        "sentiment_score": max(-1.0, min(1.0, (value - 50.0) / 50.0)),
        "generated_at": _now_iso(),
    }


# ---------------------------------------------------------------------------
# Coinglass 反向抓取器
# ---------------------------------------------------------------------------

class CoinglassFetcher:
    """Coinglass 反向接口采集器。

    要点：
    -----
    * 爆仓图流程 ``fetch_liqmap`` 保持旧签名，``data/{BASE}.json`` 仍兼容。
    * 非爆仓图接口仅保留经过 真实解密 / 抓取验证的 endpoint。
    * 新闻 / 财经日历 / 鲸鱼大额转账 / 恐惧贪婪指数等接口，如果远端无法
      验证，wrapper 状态会写为 ``unavailable`` / ``stale`` / ``error``，
      并复用 ``send_util.send_error_warning_with_cooldown`` 发飞书告警。
    * 一律不引入 Finnhub / NewsData / Reddit / TextBlob 旧链路。
    """

    # 已经过真实解密验证的 endpoint。每个 key 是一个逻辑指标名，value 是
    # 主路径或 fallback 路径列表。
    DEFAULT_ENDPOINTS: Dict[str, Any] = {
        "liqmap":                    "/api/index/5/liqMap",
        # ---- 持仓量 ----
        "open_interest":             "/api/openInterest/info",
        "open_interest_exchange":    "/api/openInterest/ex/info",
        "open_interest_statistics":  "/api/openInterest/statistics",
        # ---- 资金费率 ----
        "funding_rate":              "/api/fundingRate/coin/detail",
        "funding_rate_avg":          "/api/fundingRate/avg",
        # ---- 多空比 / 顶级账户 ----
        "long_short_ratio":          "/api/openInterest/oiVolRadio",
        # ---- 24H 成交量 / 市场快照 ----
        "volume_24h":                "/api/home/v2/coinMarkets",
        "volume_category":           "/api/futures/market/category?full=true",
        # ---- 爆仓数据 ----
        "liquidation_history":       "/api/futures/liquidation/chart",
        "liquidation_today":         "/api/futures/liquidation/today",
        "liquidation_exchange":      "/api/futures/liquidation/ex/info",
        "liquidation_coin":          "/api/coin/liquidation",
        # ---- 新闻 / 事件接口：只保留当前实测解密通过的 endpoint ----
        "news":                      [],
        "financial_calendar":        [
            "/api/economic/calendar/data",
            "/api/economic/calendar/event",
            "/api/economic/calendar/activities",
        ],
        "whale_alert":               ["/api/marketHistory"],
        "fear_greed_index":          [
            "/api/index/cgri",
            "/api/index/cgri/performance",
        ],
    }

    # 记录哪些指标参数走全名（如 liqmap 用 Binance_BTCUSDT），其它一律走基币
    LIQMAP_STYLE_METRICS = ("liqmap",)

    # 不需要按 symbol 维度抓取（即只抓全市场快照）的指标
    GLOBAL_METRICS = ("volume_category",)

    # 上下文聚合相关：哪些归类为新闻 / 上下文（用于 news_context 聚合）
    NEWS_CONTEXT_METRICS = (
        "financial_calendar",
        "whale_alert",
        "fear_greed_index",
    )

    def __init__(
        self,
        secret: str,
        aes_key: str,
        symbol_list: List[str],
        interval: str = "1",
        limit: str = "1500",
        proxy: Optional[str] = None,
        endpoints: Optional[Dict[str, Any]] = None,
        metrics_interval_range: Tuple[int, int] = (30 * 60, 45 * 60),
        whale_interval_range: Tuple[int, int] = (28 * 60, 32 * 60),
        sudden_move_threshold: float = 0.008,
        sudden_move_window_sec: int = 60,
        data_path: Optional[str] = None,
        alert_cooldown_sec: int = 1800,
    ):
        self.secret = secret
        self.aes_key = aes_key
        self.symbol_list = list(symbol_list)
        self.interval = interval
        self.limit = limit
        self.proxy = proxy
        self.data_path = data_path or str(DATA_DIR)
        self.metrics_path = str(METRICS_DIR)
        os.makedirs(self.metrics_path, exist_ok=True)

        # endpoints：浅合并默认 + 调用方覆盖
        self.endpoints: Dict[str, Any] = dict(self.DEFAULT_ENDPOINTS)
        if endpoints:
            self.endpoints.update(endpoints)

        lo, hi = metrics_interval_range
        self.metrics_interval_min = int(min(lo, hi))
        self.metrics_interval_max = int(max(lo, hi))

        wl, wh = whale_interval_range
        self.whale_interval_min = int(min(wl, wh))
        self.whale_interval_max = int(max(wl, wh))

        self.sudden_move_threshold = float(sudden_move_threshold)
        self.sudden_move_window_sec = int(sudden_move_window_sec)
        self.alert_cooldown_sec = int(alert_cooldown_sec)

        self._last_btc_price: Optional[float] = None
        self._last_btc_seen_at: float = 0.0
        self._news_lock = threading.Lock()
        self._metrics_lock = threading.Lock()
        self._whale_lock = threading.Lock()

        # ---- 鲸鱼去重持久化加载 ----
        self._whale_seen: Dict[str, int] = {}
        self._load_whale_seen()

        # ---- 旧逆向流程的 headers ----
        current_timestamp = time.time()
        time_difference = random.randint(10, 30)
        past_timestamp = current_timestamp - time_difference
        logging.info(f"当前时间戳: {int(current_timestamp)}")
        logging.info(f"倒退{time_difference}秒后的时间戳: {int(past_timestamp)}")
        self.headers = {
            'accept': 'application/json',
            'accept-language': 'zh-CN,zh;q=0.9',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'cache-ts-v2': str(int(past_timestamp)),
            'dnt': '1',
            'encryption': 'true',
            'language': 'zh',
            'obe': 's_909c3c676d4c43b6938bdd393f2e3709',
            'origin': 'https://www.coinglass.com',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.coinglass.com/',
            'sec-ch-ua': '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36'
            ),
        }

        with open(PROJECT_ROOT / "scrypt.js", "r", encoding="utf-8") as f:
            self.scrypt_js = execjs.compile(f.read())

    # ----------------------------------------------------------------- 工具
    def generate_token(self) -> str:
        """生成每次请求的加密 token（保留旧逆向逻辑）。"""
        timestamp = int(time.time())
        self.headers['cache-ts-v2'] = str(timestamp)
        totp = pyotp.TOTP(self.secret, interval=30)
        otp = totp.at(timestamp)
        plaintext = f"{timestamp},{otp}".encode()

        pad_len = 16 - len(plaintext) % 16
        padded = plaintext + bytes([pad_len] * pad_len)
        cipher = AES.new(bytes.fromhex(self.aes_key), AES.MODE_ECB)
        encrypted = cipher.encrypt(padded)
        return base64.b64encode(encrypted).decode()

    def _proxies(self) -> Optional[Dict[str, str]]:
        return {"http": self.proxy, "https": self.proxy} if self.proxy else None

    def _decrypt(self, encrypt_data: str, response_headers: Dict[str, str], url_path: str) -> Any:
        user = response_headers.get("user")
        v = response_headers.get("v")
        if v == '0':
            pw = self.headers['cache-ts-v2']
        elif v == '2':
            pw = response_headers.get("time")
        elif v == '55':
            pw = '170b070da9654622'
        elif v == '66':
            pw = 'd6537d845a964081'
        elif v == '77':
            pw = '863f08689c97435b'
        else:
            pw = url_path
        e = self.scrypt_js.call("getE", user, pw)
        return self.scrypt_js.call("Yt", encrypt_data, e)

    # ----------------------------------------------------------- 通用 GET
    def fetch_decrypted(
        self,
        url_path: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = 10,
    ) -> Optional[Any]:
        """对 ``url_path`` 走 token + 解密流程。失败返回 ``None``，永不抛异常。

        Coinglass 的 capi 偶发 TLS EOF / 连接复位；这类网络瞬断需要重试，
        否则单次 SSL 错误会被误判为 endpoint 不可用并写 unavailable。
        """
        last_exc: Optional[Exception] = None
        for attempt in range(3):
            token = self.generate_token()
            merged_params: Dict[str, Any] = dict(params or {})
            merged_params['data'] = token
            try:
                response = requests.get(
                    f'https://capi.coinglass.com{url_path}',
                    headers=self.headers,
                    params=merged_params,
                    proxies=self._proxies(),
                    timeout=timeout,
                )
                response.raise_for_status()
                payload = response.json()
                encrypt_data = payload.get('data') if isinstance(payload, dict) else None
                if not encrypt_data:
                    logging.debug(f"[{url_path}] 空 / 不支持负载，跳过")
                    return None
                decrypted = self._decrypt(encrypt_data, response.headers, url_path)
                if isinstance(decrypted, str):
                    try:
                        decrypted = json.loads(decrypted)
                    except Exception:
                        pass
                return decrypted
            except requests.exceptions.RequestException as exc:  # 网络 / TLS / HTTP 路径
                last_exc = exc
                if attempt < 2:
                    delay = 0.6 * (attempt + 1) + random.random() * 0.4
                    logging.info(
                        f"[{url_path}] 网络抓取失败，{delay:.1f}s 后重试 "
                        f"({attempt + 1}/3): {exc}"
                    )
                    time.sleep(delay)
                    continue
                logging.info(f"[{url_path}] 抓取失败: {exc}")
                return None
            except Exception as exc:  # pragma: no cover - 解密 / JSON 等非网络路径
                last_exc = exc
                logging.info(f"[{url_path}] 抓取失败: {exc}")
                return None
        if last_exc is not None:
            logging.info(f"[{url_path}] 抓取失败: {last_exc}")
        return None

    # ----------------------------------------------------------- 爆仓图
    def fetch_liqmap(self, symbol: str, url: Optional[str] = None) -> bool:
        url_path = url or self.endpoints.get("liqmap", "/api/index/5/liqMap")
        params = {
            'merge': 'true',
            'symbol': symbol,                # 这里保持 Binance_BTCUSDT 风格
            'interval': self.interval,
            'limit': self.limit,
        }
        decrypted = self.fetch_decrypted(url_path, params=params, timeout=10)
        if decrypted is None:
            logging.info(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 获取 {symbol} liqmap 失败"
            )
            try:
                send_error_warning_with_cooldown(
                    f"liqmap:{symbol}",
                    f"{symbol} 爆仓图抓取失败",
                    cooldown_seconds=self.alert_cooldown_sec,
                )
            except Exception:
                pass
            return False

        try:
            base = symbol.split('_')[-1].replace('USDT', '')
            full_path = os.path.join(self.data_path, f"{base}.json")
            with open(full_path, 'w', encoding='utf-8') as f:
                json.dump(decrypted, f, ensure_ascii=False, indent=2)
            logging.info(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {base}.json 爆仓图已更新 ✅"
            )
            return True
        except Exception as exc:
            logging.warning(f"持久化 {symbol} 爆仓图失败: {exc}")
            return False

    # ------------------------------------------------------- 辅助指标抓取
    def _resolve_endpoint(self, metric_name: str) -> List[str]:
        spec = self.endpoints.get(metric_name)
        if not spec:
            return []
        if isinstance(spec, str):
            return [spec]
        if isinstance(spec, (list, tuple)):
            return [str(p) for p in spec if p]
        return []

    def _persist_wrapper(
        self,
        path: Path,
        *,
        source: str,
        metric: str,
        status: str,
        data: Any = None,
        items: Optional[List[Dict[str, Any]]] = None,
        error: Optional[str] = None,
        synthetic: bool = False,
        keep_old_on_failure: bool = True,
    ) -> Dict[str, Any]:
        """以统一 wrapper 落盘任意上下文 / 指标 JSON。

        wrapper 字段：
        - generated_at: ISO 时间字符串
        - ts: 时间戳秒
        - source: 数据来源（接口路径或 ``synthetic`` / ``local``）
        - metric: 指标名
        - status: ``ok`` / ``stale`` / ``unavailable`` / ``error``
        - data: 原始或归一化负载
        - items: 列表型条目（如新闻、鲸鱼）
        - error: 失败原因（可选）
        - synthetic: 是否是本地合成
        """
        wrapper = {
            "generated_at": _now_iso(),
            "ts": int(time.time()),
            "source": source,
            "metric": metric,
            "status": status,
            "data": data,
            "items": items or [],
            "error": error,
            "synthetic": bool(synthetic),
        }

        # 状态非 ok 时，尽量保留旧数据；只更新 status / generated_at
        if status != "ok" and keep_old_on_failure:
            existing = _safe_load_json(path)
            if existing:
                # 合并：保留旧 data / items，仅刷新状态与时间
                wrapper["data"] = existing.get("data", wrapper["data"])
                wrapper["items"] = existing.get("items", wrapper["items"])
                wrapper["previous_generated_at"] = existing.get("generated_at")
                wrapper["status"] = "stale" if existing.get("status") == "ok" else status
        _atomic_dump(path, wrapper)

        # 状态非 ok 时，触发飞书冷却告警
        if status in ("unavailable", "error", "stale"):
            try:
                send_error_warning_with_cooldown(
                    f"coinglass:{metric}:{status}",
                    (
                        f"【Coinglass数据源不可用】\n"
                        f"数据源: {metric}\n"
                        f"状态: {status}\n"
                        f"时间: {wrapper['generated_at']}\n"
                        f"错误: {error or 'endpoint unavailable'}\n"
                        f"影响: 已保留旧数据/降级，不会中断本地预测。"
                    ),
                    cooldown_seconds=self.alert_cooldown_sec,
                )
            except Exception:
                pass
        return wrapper

    def _persist_metric(self, base: str, metric_name: str, payload: Any) -> Dict[str, Any]:
        """落盘单币种指标 wrapper。"""
        path = Path(self.metrics_path) / f"{base}_{metric_name}.json"
        # 取第一个候选 endpoint 作为来源标识；若不存在用兜底字符串
        endpoints = self._resolve_endpoint(metric_name)
        source = endpoints[0] if endpoints else f"coinglass:{metric_name}"
        return self._persist_wrapper(
            path,
            source=source,
            metric=metric_name,
            status="ok",
            data=payload,
            items=[],
        )

    def fetch_metric(self, symbol: str, metric_name: str) -> bool:
        """对单币种 ``metric_name`` 尝试每个候选 endpoint，命中即落盘。"""
        if metric_name == "liqmap":
            return self.fetch_liqmap(symbol)

        paths = self._resolve_endpoint(metric_name)
        if not paths:
            return False

        # 真实接口验证：辅助指标必须传基币 ``BTC``，不是 ``Binance_BTCUSDT``
        base = symbol.split('_')[-1].replace('USDT', '').replace('USD', '').upper()
        params: Dict[str, Any] = {"symbol": base}
        if metric_name in self.GLOBAL_METRICS:
            params = {}
        for path in paths:
            payload = self.fetch_decrypted(path, params=params, timeout=8)
            if payload is None:
                continue
            self._persist_metric(base, metric_name, payload)
            logging.info(f"[{base}] 指标 {metric_name} 已通过 {path} 更新")
            return True

        # 本币种失败，写一个 unavailable wrapper（保留旧数据）
        path = Path(self.metrics_path) / f"{base}_{metric_name}.json"
        self._persist_wrapper(
            path,
            source=paths[0],
            metric=metric_name,
            status="unavailable",
            data=None,
            error="endpoint unavailable",
        )
        return False

    # ----------------------------------------------------- 新闻 / 事件聚合
    def _liqmap_imbalance(self, base: str) -> Optional[Dict[str, float]]:
        """从本地爆仓图 JSON 计算粗略的多空压力 imbalance。"""
        try:
            path = Path(self.data_path) / f"{base}.json"
            if not path.exists():
                return None
            with path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            if isinstance(payload, str):
                payload = json.loads(payload)
            if not isinstance(payload, dict):
                return None
            last_price = float(payload.get("lastPrice") or 0.0)
            liq = payload.get("liqMapV2") or {}
            if last_price <= 0 or not liq:
                return None
            long_below = 0.0
            short_above = 0.0
            for k, entries in liq.items():
                try:
                    p = float(k)
                except Exception:
                    continue
                for entry in entries or ():
                    if not isinstance(entry, (list, tuple)) or len(entry) < 2:
                        continue
                    try:
                        h = float(entry[1])
                    except Exception:
                        continue
                    if p < last_price:
                        long_below += h
                    elif p > last_price:
                        short_above += h
            denom = long_below + short_above
            imbalance = 0.0 if denom <= 0 else (short_above - long_below) / denom
            return {
                "last_price": last_price,
                "long_below": long_below,
                "short_above": short_above,
                "imbalance": max(-1.0, min(1.0, imbalance)),
            }
        except Exception as exc:
            logging.debug(f"_liqmap_imbalance({base}) 失败: {exc}")
            return None

    def _save_events(self, events: List[Dict[str, Any]], synthetic: bool) -> None:
        """以统一 wrapper 落盘 events.json。"""
        self._persist_wrapper(
            EVENTS_FILE,
            source="synthetic_liqmap" if synthetic else "coinglass_news",
            metric="events",
            status="ok",
            items=events[-32:],
            synthetic=synthetic,
        )

    def refresh_event_signal(self, force: bool = False) -> None:
        """拉取新闻 / 事件并归一化。

        当前 Coinglass 新闻接口未通过真实解密验证，因此默认走本地合成
        fallback：基于 BTC 爆仓图 imbalance 派生一个事件分。即便如此，
        ``events.json`` 始终会更新，确保下游 ``core/sentiment.py`` 仍可读。
        """
        with self._news_lock:
            news_paths = self._resolve_endpoint("news")
            collected: List[Dict[str, Any]] = []
            for path in news_paths:
                payload = self.fetch_decrypted(path, params={"language": "en"}, timeout=6)
                if not payload:
                    continue
                if isinstance(payload, dict):
                    items = (
                        payload.get("list")
                        or payload.get("items")
                        or payload.get("data")
                        or []
                    )
                elif isinstance(payload, list):
                    items = payload
                else:
                    items = []
                for raw in items[:24]:
                    if not isinstance(raw, dict):
                        continue
                    title = (
                        raw.get("title")
                        or raw.get("titleZh")
                        or raw.get("name")
                        or ""
                    )
                    body = raw.get("desc") or raw.get("content") or ""
                    ts = raw.get("createTime") or raw.get("ts") or int(time.time())
                    score = self._score_text(f"{title} {body}")
                    if score == 0.0 and not title:
                        continue
                    collected.append({
                        "ts": int(ts) if isinstance(ts, (int, float)) else int(time.time()),
                        "title": str(title)[:200],
                        "score": float(score),
                        "source": path,
                    })
                if collected:
                    break

            if collected:
                self._save_events(collected, synthetic=False)
                logging.info(f"events 已通过 Coinglass 新闻刷新 ({len(collected)} 条)")
                return

            # ---- 本地合成 fallback：基于 BTC 爆仓图 imbalance ----
            btc_metrics = self._liqmap_imbalance("BTC")
            synthetic_event = {
                "ts": int(time.time()),
                "title": "synthetic:btc_liqmap_imbalance",
                "score": float((btc_metrics or {}).get("imbalance", 0.0)),
                "source": "liqmap_local",
                "forced": bool(force),
            }
            self._save_events([synthetic_event], synthetic=True)
            logging.info("events 已通过本地 BTC 爆仓图合成 fallback 刷新")

    # ---- 简易词典极性打分（不引入第三方 NLP） ----
    _POS = (
        "bull", "rally", "surge", "soar", "gain", "rise", "rises",
        "approve", "approved", "long", "buying", "breakout", "support",
        "upgrade", "partnership", "etf", "inflow", "inflows",
    )
    _NEG = (
        "bear", "drop", "plunge", "crash", "fall", "falls", "hack",
        "exploit", "outflow", "outflows", "ban", "lawsuit", "regulator",
        "short", "selling", "downgrade", "delist", "liquidation",
    )

    @classmethod
    def _score_text(cls, text: str) -> float:
        if not text:
            return 0.0
        t = text.lower()
        pos = sum(1 for w in cls._POS if w in t)
        neg = sum(1 for w in cls._NEG if w in t)
        if pos == 0 and neg == 0:
            return 0.0
        denom = pos + neg
        return (pos - neg) / float(denom)

    # ----------------------------------- 财经日历 / 鲸鱼 / 恐惧贪婪
    def refresh_financial_calendar(self) -> None:
        """刷新财经事件、央行动态；仅使用已实测可解密的 Coinglass endpoint。"""
        paths = self._resolve_endpoint("financial_calendar")
        items: List[Dict[str, Any]] = []
        ok = False
        last_error: Optional[str] = None
        for path in paths:
            payload = self.fetch_decrypted(path, params={}, timeout=8)
            if not payload:
                last_error = "decrypt failed"
                continue
            raw_list = payload.get("list") if isinstance(payload, dict) else payload
            if not isinstance(raw_list, list):
                continue
            normalized = normalize_financial_calendar(raw_list, source=path)
            if normalized:
                items = normalized
                ok = True
                break
        if ok:
            self._persist_wrapper(
                FINANCIAL_CALENDAR_FILE,
                source="coinglass_calendar",
                metric="financial_calendar",
                status="ok",
                items=items,
            )
        else:
            self._persist_wrapper(
                FINANCIAL_CALENDAR_FILE,
                source="coinglass_calendar",
                metric="financial_calendar",
                status="unavailable",
                error=last_error or "endpoint not verified",
            )

    @staticmethod
    def _calendar_sentiment(raw: Dict[str, Any]) -> float:
        """极简财经事件方向打分，仅供权重融合参考。委托给模块级辅助函数。"""
        return _calendar_sentiment_score(raw)

    def _load_whale_seen(self) -> None:
        try:
            data = _safe_load_json(WHALE_SEEN_FILE) or {}
            seen = data.get("seen") if isinstance(data, dict) else None
            if isinstance(seen, dict):
                # 仅保留最近 1500 条
                self._whale_seen = dict(list(seen.items())[-1500:])
        except Exception:
            self._whale_seen = {}

    def _save_whale_seen(self) -> None:
        try:
            _atomic_dump(WHALE_SEEN_FILE, {
                "generated_at": _now_iso(),
                "ts": int(time.time()),
                "seen": dict(list(self._whale_seen.items())[-1500:]),
            })
        except Exception:
            pass

    def refresh_whale_alert(self) -> None:
        """刷新大额转账。未实测通过 endpoint 时静默跳过，不伪造链接、不发不可用告警。"""
        paths = self._resolve_endpoint("whale_alert")
        if not paths:
            logging.debug("whale_alert endpoint 未验证，跳过刷新")
            return
        items: List[Dict[str, Any]] = []
        ok = False
        last_error: Optional[str] = None
        new_seen: Dict[str, int] = {}
        with self._whale_lock:
            for path in paths:
                params = {"dataType": 5, "pageNum": 1, "pageSize": 50}
                payload = self.fetch_decrypted(path, params=params, timeout=8)
                if not payload:
                    last_error = "decrypt failed"
                    continue
                raw_list = payload.get("list") if isinstance(payload, dict) else payload
                if not isinstance(raw_list, list):
                    continue
                # 委托给模块级归一化函数。持久化真实接口快照时 include_seen=True，
                # 避免“接口真实返回但没有新增交易”被误判为 endpoint 不可用。
                batch_items, batch_seen = normalize_whale_alert(
                    raw_list,
                    seen=self._whale_seen,
                    include_seen=True,
                )
                if batch_items:
                    items = batch_items
                    new_seen = batch_seen
                    ok = True
                    break
            if ok:
                self._whale_seen.update(new_seen)
                self._save_whale_seen()
                self._persist_wrapper(
                    WHALE_ALERT_FILE,
                    source="coinglass_whale_alert",
                    metric="whale_alert",
                    status="ok",
                    items=items,
                )
            else:
                # whale_alert 没有已实测可用 endpoint：静默降级，保留旧数据，
                # 绝不调用 _persist_wrapper(status=unavailable/error/stale)，
                # 以避免反复触发 send_error_warning_with_cooldown 的飞书告警。
                # JSON 中仍以 status="skipped" + synthetic=True 明确标注本次未抓取。
                existing = _safe_load_json(WHALE_ALERT_FILE) or {}
                wrapper = {
                    "generated_at": _now_iso(),
                    "ts": int(time.time()),
                    "source": "coinglass_whale_alert",
                    "metric": "whale_alert",
                    "status": "skipped",
                    "data": existing.get("data"),
                    "items": existing.get("items") or [],
                    "error": None,
                    "synthetic": True,
                    "note": "whale_alert endpoint not verified; degrade quietly, keep old data",
                    "skip_reason": last_error or "endpoint not verified",
                    "previous_generated_at": existing.get("generated_at"),
                    "previous_status": existing.get("status"),
                }
                _atomic_dump(WHALE_ALERT_FILE, wrapper)
                logging.info(
                    "whale_alert 静默降级（status=skipped），保留旧数据，未发送不可用告警"
                )

    @staticmethod
    def _whale_direction_hint(raw: Dict[str, Any]) -> str:
        return _whale_direction_hint(raw)

    @staticmethod
    def _whale_sentiment(raw: Dict[str, Any]) -> float:
        """流入交易所偏空，流出交易所偏多；其它为中性。委托给模块级函数。"""
        return _whale_sentiment_score(raw)

    def refresh_fear_greed(self) -> None:
        """刷新恐惧贪婪指数；仅使用已实测可解密的 Coinglass endpoint。"""
        paths = self._resolve_endpoint("fear_greed_index")
        ok = False
        last_error: Optional[str] = None
        data: Optional[Dict[str, Any]] = None
        for path in paths:
            payload = self.fetch_decrypted(path, params={}, timeout=8)
            if not payload:
                last_error = "decrypt failed"
                continue
            data = normalize_fear_greed(payload)
            if data is None:
                last_error = "fear_greed payload invalid"
                continue
            ok = True
            break
        if ok:
            self._persist_wrapper(
                FEAR_GREED_FILE,
                source="coinglass_fear_greed",
                metric="fear_greed_index",
                status="ok",
                data=data,
            )
        else:
            self._persist_wrapper(
                FEAR_GREED_FILE,
                source="coinglass_fear_greed",
                metric="fear_greed_index",
                status="unavailable",
                error=last_error or "endpoint not verified",
            )

    def refresh_news_context(self) -> None:
        """合并 events / 财经日历 / 鲸鱼 / 恐惧贪婪 形成 news_context.json。"""
        events_w = _safe_load_json(EVENTS_FILE) or {}
        fc_w = _safe_load_json(FINANCIAL_CALENDAR_FILE) or {}
        wh_w = _safe_load_json(WHALE_ALERT_FILE) or {}
        fg_w = _safe_load_json(FEAR_GREED_FILE) or {}

        def _items_score(wrapper: Dict[str, Any]) -> Tuple[float, int]:
            items = wrapper.get("items") or []
            if not items:
                return 0.0, 0
            total = 0.0
            cnt = 0
            for it in items:
                try:
                    s = float(it.get("sentiment_score") or it.get("score") or 0.0)
                except Exception:
                    continue
                total += s
                cnt += 1
            if cnt == 0:
                return 0.0, 0
            return total / cnt, cnt

        events_score, events_cnt = _items_score(events_w)
        fc_score, fc_cnt = _items_score(fc_w)
        wh_score, wh_cnt = _items_score(wh_w)
        fg_score = 0.0
        try:
            fg_score = float((fg_w.get("data") or {}).get("sentiment_score") or 0.0)
        except Exception:
            pass

        # 为每个目标币种单独计算（鲸鱼按 symbol 命中加权）
        per_symbol: Dict[str, Dict[str, Any]] = {}
        for sym in self.symbol_list:
            base = sym.split("_")[-1].replace("USDT", "").replace("USD", "").upper()
            sym_score = 0.0
            sym_count = 0
            for it in (wh_w.get("items") or []):
                if str(it.get("symbol") or "").upper().startswith(base):
                    sym_score += float(it.get("sentiment_score") or 0.0)
                    sym_count += 1
            sym_avg = sym_score / sym_count if sym_count else 0.0
            per_symbol[base] = {
                "whale_sentiment_avg": sym_avg,
                "whale_count": sym_count,
            }

        importance = 0.0
        for it in (fc_w.get("items") or []):
            try:
                importance = max(importance, float(it.get("importance") or 0.0) / 3.0)
            except Exception:
                continue

        news_context_score = 0.40 * events_score + 0.25 * fc_score + 0.20 * wh_score + 0.15 * fg_score
        news_context_score = max(-1.0, min(1.0, news_context_score))

        wrapper = {
            "generated_at": _now_iso(),
            "ts": int(time.time()),
            "scores": {
                "events_score": events_score,
                "events_count": events_cnt,
                "financial_calendar_score": fc_score,
                "macro_event_importance": importance,
                "whale_alert_score": wh_score,
                "whale_transfer_count": wh_cnt,
                "whale_net_flow_score": wh_score,
                "fear_greed_score": fg_score,
                "news_context_score": news_context_score,
                "final_news_score": news_context_score,
            },
            "anchor_weights": {
                "macro_calendar_weight": 0.20 if fc_cnt else 0.05,
                "whale_flow_weight": 0.20 if wh_cnt else 0.05,
                "fear_greed_weight": 0.10 if fg_w.get("status") == "ok" else 0.03,
                "liquidation_map_weight": 0.30,
                "btc_anchor_weight": 0.20,
            },
            "per_symbol": per_symbol,
            "source_times": {
                "events": events_w.get("generated_at"),
                "financial_calendar": fc_w.get("generated_at"),
                "whale_alert": wh_w.get("generated_at"),
                "fear_greed_index": fg_w.get("generated_at"),
            },
            "source_status": {
                "events": events_w.get("status"),
                "financial_calendar": fc_w.get("status"),
                "whale_alert": wh_w.get("status"),
                "fear_greed_index": fg_w.get("status"),
            },
        }
        _atomic_dump(NEWS_CONTEXT_FILE, wrapper)

    # ------------------------------------------- 突发 BTC 行情触发新闻刷新
    def _check_sudden_btc_move(self) -> bool:
        snap = self._liqmap_imbalance("BTC")
        if not snap:
            return False
        price = snap["last_price"]
        now = time.time()
        force = False
        if (
            self._last_btc_price
            and self._last_btc_price > 0
            and (now - self._last_btc_seen_at) <= self.sudden_move_window_sec
        ):
            change = abs(price - self._last_btc_price) / self._last_btc_price
            if change >= self.sudden_move_threshold:
                force = True
                logging.warning(
                    f"BTC 突发行情: {self._last_btc_price} -> {price} ({change:.2%})"
                )
        self._last_btc_price = price
        self._last_btc_seen_at = now
        return force

    # --------------------------------------------------------- 调度循环
    def _aux_metric_names(self) -> List[str]:
        excluded = {"liqmap", "news"} | set(self.NEWS_CONTEXT_METRICS)
        return [name for name in self.endpoints if name not in excluded]

    def _liqmap_loop(self, interval_seconds: int) -> None:
        url = self.endpoints.get("liqmap", "/api/index/5/liqMap")
        while True:
            start_time = time.time()
            for symbol in self.symbol_list:
                self.fetch_liqmap(symbol, url)
                delay = random.randint(3, 8)
                logging.info(f"等待 {delay} 秒后继续抓取下一个币种...")
                time.sleep(delay)

            try:
                if self._check_sudden_btc_move():
                    self.refresh_event_signal(force=True)
                    self.refresh_news_context()
            except Exception as exc:
                logging.debug(f"BTC 突发行情检查失败: {exc}")

            elapsed = time.time() - start_time
            remaining = max(1, interval_seconds - int(elapsed) if interval_seconds > elapsed else interval_seconds)
            logging.info(f"等待 {remaining} 秒进入下一轮爆仓图抓取...")
            time.sleep(remaining)

    def _aux_metrics_loop(self) -> None:
        time.sleep(random.randint(30, 90))
        while True:
            metric_names = self._aux_metric_names()
            with self._metrics_lock:
                for symbol in self.symbol_list:
                    for metric in metric_names:
                        if metric in self.GLOBAL_METRICS and symbol != self.symbol_list[0]:
                            # 全市场快照只跑一次
                            continue
                        try:
                            self.fetch_metric(symbol, metric)
                        except Exception as exc:
                            logging.debug(f"fetch_metric({symbol},{metric}) 失败: {exc}")
                        time.sleep(random.uniform(0.6, 1.6))

                # ---- 上下文：财经日历 + 恐惧贪婪 + events + news_context ----
                try:
                    self.refresh_financial_calendar()
                except Exception as exc:
                    logging.debug(f"refresh_financial_calendar 失败: {exc}")
                try:
                    self.refresh_fear_greed()
                except Exception as exc:
                    logging.debug(f"refresh_fear_greed 失败: {exc}")
                try:
                    self.refresh_event_signal(force=False)
                except Exception as exc:
                    logging.debug(f"refresh_event_signal 失败: {exc}")
                try:
                    self.refresh_news_context()
                except Exception as exc:
                    logging.debug(f"refresh_news_context 失败: {exc}")

            sleep_for = random.randint(self.metrics_interval_min, self.metrics_interval_max)
            logging.info(f"[aux] 下次辅助指标刷新: {sleep_for}s")
            time.sleep(sleep_for)

    def _whale_loop(self) -> None:
        """鲸鱼大额转账独立循环，约 30 分钟一轮，稳定去重。"""
        time.sleep(random.randint(60, 180))
        while True:
            try:
                self.refresh_whale_alert()
                self.refresh_news_context()
            except Exception as exc:
                logging.debug(f"refresh_whale_alert 失败: {exc}")
            sleep_for = random.randint(self.whale_interval_min, self.whale_interval_max)
            logging.info(f"[whale] 下次鲸鱼大额转账刷新: {sleep_for}s")
            time.sleep(sleep_for)

    def start_auto_update(self, interval_seconds: int = 300) -> None:
        """启动 爆仓图 + 辅助指标 + 鲸鱼 三个后台守护线程。"""
        threading.Thread(
            target=self._liqmap_loop,
            args=(interval_seconds,),
            daemon=True,
            name="coinglass-liqmap",
        ).start()
        threading.Thread(
            target=self._aux_metrics_loop,
            daemon=True,
            name="coinglass-aux",
        ).start()
        threading.Thread(
            target=self._whale_loop,
            daemon=True,
            name="coinglass-whale",
        ).start()


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    symbols = [
        'Binance_BTCUSDT',
        'Binance_ETHUSDT',
        'Binance_XRPUSDT',
        'Binance_SOLUSDT',
        'Binance_1000PEPEUSDT',
        # 'Binance_PNUTUSDT',
        # 'Binance_TRXUSDT',
    ]

    fetcher = CoinglassFetcher(
        secret=os.environ.get("COINGLASS_TOTP_SECRET", ""),
        aes_key=os.environ.get("COINGLASS_AES_KEY", ""),
        symbol_list=symbols,
        interval='1',
        limit='1500',
    )

    fetcher.start_auto_update(interval_seconds=300)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info('程序已手动停止。')
