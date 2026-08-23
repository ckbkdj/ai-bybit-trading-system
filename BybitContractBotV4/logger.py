import logging
import re
import threading
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path


_SECRET_PATTERNS = (
    re.compile(r"(?i)(authorization\s*[:=]\s*bearer\s+)[^\s,;]+"),
    re.compile(r"(?i)((?:api[_-]?key|secret(?:_key)?|signature|sign)\s*[:=]\s*)[^\s,;]+"),
    re.compile(r"(?i)(X-BAPI-SIGN\s*[:=]\s*)[^\s,;]+"),
)


def redact_secrets(value):
    text = str(value)
    for pattern in _SECRET_PATTERNS:
        text = pattern.sub(r"\1<redacted>", text)
    return text


class SecretRedactionFilter(logging.Filter):
    def filter(self, record):
        record.msg = redact_secrets(record.getMessage())
        record.args = ()
        return True


class DuplicateRateLimitFilter(logging.Filter):
    """Bound identical log storms while preserving a suppression count."""

    def __init__(self, window_seconds: float = 60, burst: int = 20, max_keys: int = 4096):
        super().__init__()
        self.window_seconds = max(1.0, float(window_seconds))
        self.burst = max(1, int(burst))
        self.max_keys = max(128, int(max_keys))
        self._lock = threading.Lock()
        self._records: dict[tuple[int, str], tuple[float, int, int]] = {}

    def filter(self, record):
        now = time.monotonic()
        key = (record.levelno, record.getMessage())
        with self._lock:
            if key not in self._records and len(self._records) >= self.max_keys:
                oldest = min(self._records, key=lambda item: self._records[item][0])
                self._records.pop(oldest, None)
            started, emitted, suppressed = self._records.get(key, (now, 0, 0))
            if now - started >= self.window_seconds:
                if suppressed:
                    record.msg = f"{record.getMessage()} [suppressed {suppressed} repeats]"
                    record.args = ()
                self._records[key] = (now, 1, 0)
                return True
            if emitted < self.burst:
                self._records[key] = (started, emitted + 1, suppressed)
                return True
            self._records[key] = (started, emitted, suppressed + 1)
            return False

def setup_logger(log_file_path, level=logging.INFO):
    log_path = Path(log_file_path).resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger()
    for existing_handler in logger.handlers:
        existing_handler.addFilter(SecretRedactionFilter())
    if any(getattr(handler, "_quant_execution_handler", False) for handler in logger.handlers):
        return logger

    # 25 MiB x (current + 14 backups) bounds a runaway service below 375 MiB.
    handler = RotatingFileHandler(
        log_path,
        maxBytes=25 * 1024 * 1024,
        backupCount=14,
        encoding="utf-8",
    )
    handler._quant_execution_handler = True

    # 配置基本日志格式
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    handler.addFilter(SecretRedactionFilter())
    handler.addFilter(DuplicateRateLimitFilter())

    # 设置日志级别
    logger.setLevel(level)

    # 添加文件处理器到 logger
    logger.addHandler(handler)

    # 如果是在开发环境，还可以添加控制台输出
    if level == logging.DEBUG:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # 关闭日志的传播，防止与root logger重复记录
    logger.propagate = False

    return logger

# 使用示例：
log_file_path = Path(__file__).resolve().parent / "logs" / "trading_service.log"
logger = setup_logger(log_file_path, level=logging.INFO)

# 记录日志
# logger.info("This is a test log message.")
# logger.warning("This is a test warning message.")
