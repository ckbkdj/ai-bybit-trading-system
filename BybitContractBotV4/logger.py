import logging
from logging.handlers import TimedRotatingFileHandler

def setup_logger(log_file_path, level=logging.INFO):
    # 获取 logger 实例，如果已经配置过就直接返回
    logger = logging.getLogger()
    if logger.hasHandlers():
        return logger

    # 设置日志文件每天切换一次，保留 3 天备份文件
    handler = TimedRotatingFileHandler(log_file_path, when="midnight", interval=1, backupCount=3, encoding='utf-8')

    # 配置基本日志格式
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

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
log_file_path = './log.txt'
logger = setup_logger(log_file_path, level=logging.INFO)

# 记录日志
# logger.info("This is a test log message.")
# logger.warning("This is a test warning message.")