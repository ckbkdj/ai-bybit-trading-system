import sqlite3
from datetime import datetime, timedelta

import pytz

from logger import logger

DB_FILE = "price_changes.db"

# 初始化上海时区
shanghai_tz = pytz.timezone('Asia/Shanghai')

# 初始化数据库，创建表格
def initialize_db():
    """初始化数据库和表格"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS price_changes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME NOT NULL,
                        price REAL NOT NULL)''')

    conn.commit()
    conn.close()


# 插入价格变动记录
def insert_price_change(price):
    """插入价格变动并记录带毫秒的时间戳"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 获取当前时间并将其转换为上海时间
    utc_now = datetime.now(pytz.utc)  # 获取 UTC 时间
    shanghai_time = utc_now.astimezone(shanghai_tz)  # 转换为上海时间

    # 插入价格记录和当前上海时间戳
    cursor.execute('''
            INSERT INTO price_changes (price, timestamp) 
            VALUES (?, ?)
        ''', (price, shanghai_time.strftime('%Y-%m-%d %H:%M:%S.%f')))
    conn.commit()
    conn.close()


def get_earliest_date_difference():
    """获取最早记录距离今天多少天"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 获取数据库中最早的数据（时间戳最小的那条记录）
    cursor.execute('''
        SELECT timestamp FROM price_changes
        ORDER BY timestamp ASC
        LIMIT 1
    ''')
    earliest_data = cursor.fetchone()

    conn.close()

    if earliest_data:
        # 获取最早数据的日期（去除时间部分，只保留日期）
        earliest_date = datetime.strptime(earliest_data[0], "%Y-%m-%d %H:%M:%S.%f").date()

        # 获取今天的日期
        today = datetime.today().date()

        # 计算日期差值
        date_difference = (today - earliest_date).days

        return date_difference
    else:
        return None  # 如果数据库没有数据，返回None

        
def get_previous_data_of_day(date_str):
    """获取某天最早数据的前一条数据"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 获取当天最早的数据
    cursor.execute('''
        SELECT price, timestamp FROM price_changes
        WHERE timestamp >= ? || ' 00:00:00'
        ORDER BY timestamp ASC
        LIMIT 1
    ''', (date_str,))
    first_data = cursor.fetchone()

    if first_data:
        # 获取最早数据的前一条数据
        cursor.execute('''
            SELECT price, timestamp FROM price_changes
            WHERE timestamp < ?
            ORDER BY timestamp DESC
            LIMIT 1
        ''', (first_data[1],))
        previous_data = cursor.fetchone()
        conn.close()
        return previous_data

    conn.close()
    return None


def get_day_price_difference(date_str):
    """获取某一天的最早数据和最晚数据的差值"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 获取当天最早的数据
    cursor.execute('''
        SELECT price, timestamp FROM price_changes
        WHERE timestamp >= ? || ' 00:00:00'
        ORDER BY timestamp ASC
        LIMIT 1
    ''', (date_str,))
    first_data = cursor.fetchone()

    # 获取当天最晚的数据
    cursor.execute('''
        SELECT price, timestamp FROM price_changes
        WHERE timestamp >= ? || ' 00:00:00' 
        AND timestamp <= ? || ' 23:59:59'
        ORDER BY timestamp DESC
        LIMIT 1
    ''', (date_str, date_str))
    last_data = cursor.fetchone()

    conn.close()

    if first_data and last_data:
        # 获取当天最早数据的前一条数据
        previous_data = get_previous_data_of_day(date_str)

        if previous_data:
            # 计算最早数据前一条记录与最晚数据之间的差值
            price_diff = last_data[0] - previous_data[0]
        else:
            # 如果没有前一条数据，则计算当天最早数据与最晚数据之间的差值
            price_diff = last_data[0] - first_data[0]

        return price_diff
    else:
        return None


def get_last_seven_days_data():
    """获取最近七天的数据（包含今天），并计算每日的价格差值"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 获取当前时间和七天前的日期
    today = datetime.now()
    seven_days_ago = today - timedelta(days=7)

    # 转换为字符串格式 (例如 2024-12-24)
    today_str = today.strftime('%Y-%m-%d')
    seven_days_ago_str = seven_days_ago.strftime('%Y-%m-%d')

    # 查询最近七天的日期（按日期升序排列）
    cursor.execute('''
        SELECT DISTINCT strftime('%Y-%m-%d', timestamp) 
        FROM price_changes
        WHERE timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp ASC
    ''', (seven_days_ago_str + ' 00:00:00', today_str + ' 23:59:59'))

    # 获取所有日期
    records = cursor.fetchall()
    conn.close()

    # 格式化数据为所需的形式
    values = []
    for record in records:
        date_str = record[0]  # 日期部分 (yyyy-mm-dd)
        price_diff = get_day_price_difference(date_str)  # 获取当天价格差值

        if price_diff is not None:
            values.append({
                "type": "$",  # Assuming all data are price values
                "day": date_str,  # 日期
                "value": price_diff  # 价格差值
            })

    return {"values": values}

# 获取最新的价格
def get_latest_price():
    """获取最新价格"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT price FROM price_changes
        ORDER BY timestamp DESC
        LIMIT 1
    ''')
    latest_price = cursor.fetchone()
    conn.close()

    return latest_price[0] if latest_price else 0.0


# 获取最早时间戳的价格
def get_earliest_price():
    """获取最早时间戳的价格"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT price, timestamp FROM price_changes
        ORDER BY timestamp ASC
        LIMIT 1
    ''')
    earliest_data = cursor.fetchone()
    conn.close()

    if earliest_data:
        return earliest_data[0]  # 返回价格
    else:
        return 0.0  # 如果没有数据，返回0.0


# 获取某个日期的最初数据
def get_first_data_of_day(date_str):
    """根据日期获取当天00:00后的第一条数据"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT price, timestamp FROM price_changes
        WHERE timestamp >= ? || ' 00:00:00'
        ORDER BY timestamp ASC
        LIMIT 1
    ''', (date_str,))
    first_data = cursor.fetchone()
    conn.close()

    return first_data


# 获取某个日期的最后数据
def get_last_data_of_day(date_str):
    """获取某个日期最后的价格数据"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT price, timestamp FROM price_changes
        WHERE timestamp >= ? || ' 00:00:00'
        AND timestamp < ? || ' 23:59:59'
        ORDER BY timestamp DESC
        LIMIT 1
    ''', (date_str, date_str))
    last_data = cursor.fetchone()
    conn.close()

    return last_data


# 获取昨天的最后数据
def get_last_data_of_yesterday():
    """获取昨天最后的价格数据"""
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')

    return get_last_data_of_day(yesterday_str)


# 获取上月的最后数据
def get_last_data_of_last_month():
    """获取上月的最后数据"""
    first_day_this_month = datetime.now().replace(day=1)
    last_day_last_month = first_day_this_month - timedelta(days=1)
    last_month_str = last_day_last_month.strftime('%Y-%m-%d')

    return get_last_data_of_day(last_month_str)


# 获取本月的最初数据
def get_first_data_of_this_month():
    """获取本月最初的数据"""
    first_day_this_month = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    first_month_str = first_day_this_month.strftime('%Y-%m-%d')

    return get_first_data_of_day(first_month_str)


# 获取本年的最初数据
def get_first_data_of_this_year():
    """获取本年最初的数据"""
    first_day_this_year = datetime.now().replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    first_year_str = first_day_this_year.strftime('%Y-%m-%d')

    return get_first_data_of_day(first_year_str)


# 获取收益
def get_profit():
    """自动查询本次收益和其他收益情况"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 获取最新的两条记录：最新价格和上次价格
    cursor.execute('''
        SELECT price, timestamp FROM price_changes
        ORDER BY timestamp DESC
        LIMIT 2
    ''')
    last_two_records = cursor.fetchall()
    this_profit_percentage = 0
    this_profit = 0
    daily_profit_percentage = 0
    daily_profit = 0
    monthly_profit_percentage = 0
    monthly_profit = 0
    yearly_profit_percentage = 0
    yearly_profit = 0
    if len(last_two_records) == 2:
        current_price = last_two_records[0][0]
        previous_price = last_two_records[1][0]
        this_profit = current_price - previous_price
        this_profit_percentage = (this_profit / previous_price) * 100
        this_profit_percentage = f'{this_profit_percentage:.2f}%'
        logger.error(f"本次收益百分比: {this_profit_percentage}")
        logger.error(f"本次收益: {this_profit:.3f}")
    else:
        logger.error("无法计算本次收益，缺少足够的历史数据。")
        conn.close()  # 如果没有足够的数据，及时关闭连接

    # 计算当日收益：
    # 找今天的最初数据，找到后向前找一条数据
    day_start_data = get_first_data_of_day(datetime.now().strftime('%Y-%m-%d'))

    if day_start_data:
        # 找到当天最初数据后，向前查找一条数据
        cursor.execute('''
            SELECT price, timestamp FROM price_changes
            WHERE timestamp < ?
            ORDER BY timestamp DESC
            LIMIT 1
        ''', (day_start_data[1],))
        previous_data = cursor.fetchone()

        if previous_data:
            base_price = previous_data[0]
        else:
            base_price = day_start_data[0]

        # 计算当天的收益
        daily_profit = current_price - base_price
        daily_profit_percentage = (daily_profit / base_price) * 100
        daily_profit_percentage = f'{daily_profit_percentage:.2f}%'
        logger.error(f"当日收益百分比: {daily_profit_percentage}")
        logger.error(f"当日收益: {daily_profit:.3f}")

    # 计算当月收益：
    # 获取本月最初数据，找到后向前查找一条数据
    month_start_data = get_first_data_of_this_month()

    if month_start_data:
        # 找到本月最初数据后，向前查找一条数据
        cursor.execute('''
            SELECT price, timestamp FROM price_changes
            WHERE timestamp < ?
            ORDER BY timestamp DESC
            LIMIT 1
        ''', (month_start_data[1],))
        previous_data = cursor.fetchone()

        if previous_data:
            base_price = previous_data[0]
        else:
            base_price = month_start_data[0]

        # 计算当月的收益
        monthly_profit = current_price - base_price
        monthly_profit_percentage = (monthly_profit / base_price) * 100
        monthly_profit_percentage = f'{monthly_profit_percentage:.2f}%'
        logger.error(f"当月收益百分比: {monthly_profit_percentage}")
        logger.error(f"当月收益: {monthly_profit:.3f}")

    # 计算当年收益：
    # 获取本年最初数据，找到后向前查找一条数据
    year_start_data = get_first_data_of_this_year()

    if year_start_data:
        # 找到本年最初数据后，向前查找一条数据
        cursor.execute('''
            SELECT price, timestamp FROM price_changes
            WHERE timestamp < ?
            ORDER BY timestamp DESC
            LIMIT 1
        ''', (year_start_data[1],))
        previous_data = cursor.fetchone()

        if previous_data:
            base_price = previous_data[0]
        else:
            base_price = year_start_data[0]

        # 计算当年的收益
        yearly_profit = current_price - base_price
        yearly_profit_percentage = (yearly_profit / base_price) * 100
        yearly_profit_percentage = f'{yearly_profit_percentage:.2f}%'
        logger.error(f"当年收益百分比: {yearly_profit_percentage}")
        logger.error(f"当年收益: {yearly_profit:.3f}")

    conn.close()  # 关闭连接

    day = get_earliest_date_difference()
    data = get_last_seven_days_data()
    return this_profit_percentage, this_profit, daily_profit_percentage, daily_profit, \
           monthly_profit_percentage, monthly_profit, yearly_profit_percentage, yearly_profit,\
            day,data



# 主程序逻辑
if __name__ == "__main__":
    # # 初始化数据库
    # initialize_db()

    # # 插入价格记录（初始价格为1500）
    # print("插入价格 1500.0")
    # insert_price_change(1600.0)

    # # 获取当前价格并计算收益情况（此时应该是没有收益）
    # print("首次运行时：")
    # get_profit()

    # # 插入第二个价格记录（价格变动为1600）
    # print("插入价格 1600.0")
    # insert_price_change(1700.0)

    # # 获取当前价格并计算收益情况（此时应该能计算出收益）
    # print("\n第二次运行时：")
    # get_profit()

    # # 插入第三个价格记录（价格变动为1700）
    # print("插入价格 1700.0")
    # insert_price_change(1800.0)

    # # 获取当前价格并计算收益情况（此时应该能计算出收益）
    # print("\n第三次运行时：")
    # get_profit()
    earliest_price = get_earliest_price()
    print(f"数据库中最早的价格是: {earliest_price}")