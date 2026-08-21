import sys

import pandas as pd
import requests
import configparser
import os
import tempfile
import threading
import time
import json
import traceback

from earnings import initialize_db, insert_price_change, get_profit
from logger import logger
from bybit import LazyBybitClient, build_bybit_client
from prediction_client import PredictionClient, PredictionUnavailable
from runtime_config import TradingMode, TradingSettings
from decimal import Decimal, ROUND_HALF_UP, getcontext
from datetime import datetime, timedelta
from pathlib import Path

SETTINGS = TradingSettings.load(Path(__file__).resolve().parent)
BYBIT = LazyBybitClient(lambda: build_bybit_client(SETTINGS))
PREDICTION_CLIENT = PredictionClient(SETTINGS)
_INI_WRITE_LOCK = threading.Lock()
# ordertype = 'limit'
ordertype = 'market'


def get_ini_data(file_path, section=None, key=None):
    # 获取配置数据
    config = configparser.ConfigParser()
    config.read(file_path)
    if section and key:
        return config.get(section, key)

    # 遍历所有节
    data = {}
    for section in config.sections():
        data[section] = {}

        # 遍历节中的键值对
        for key, value in config.items(section):
            data[section][key] = value
    return data


def update_ini_data(file_path, section, key, value):
    """Serialize INI updates and atomically promote the complete new file."""
    target = Path(file_path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    with _INI_WRITE_LOCK:
        config = configparser.ConfigParser()
        config.read(target, encoding='utf-8')
        if not config.has_section(section):
            config.add_section(section)
        config.set(section, key, str(value))

        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f'.{target.name}.',
            suffix='.tmp',
            dir=str(target.parent),
        )
        try:
            with os.fdopen(descriptor, 'w', encoding='utf-8') as configfile:
                config.write(configfile)
                configfile.flush()
                os.fsync(configfile.fileno())
            os.replace(temporary_name, target)
        finally:
            if os.path.exists(temporary_name):
                os.unlink(temporary_name)


def get_ohlcv_data(exchange, symbol, timeframe='15m', limit=15):
    # 获取K线数据
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)

    # 提取最高价和最低价
    high_prices = [candle[2] for candle in ohlcv]  # 最高价位于索引2
    low_prices = [candle[3] for candle in ohlcv]  # 最低价位于索引3

    # 返回最高价 & 最低价
    return max(high_prices), min(low_prices)


def calculate_sma(df, period=44):
    df['average_price'] = (df['high'] + df['low'] + df['close']) / 3
    df['EMA'] = df['average_price'].ewm(span=period, adjust=False).mean()
    df['EMA180'] = df['average_price'].ewm(span=180, adjust=False).mean()
    return df


def fetch_ohlcv_macd(exchange, symbol, timeframe='1m', limit=1000):
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    return ohlcv


def calculate_latest_entry_price(latest_price, before_avgPrice, avgPrice, positionValue, before_positionValue):
    """
    计算最新加仓价格

    :param before_avgPrice: 上一次仓位的平均入场价格（USDT）
    :param avgPrice: 当前仓位的平均入场价格（USDT）
    :param positionValue: 当前持有的总仓位价值（USDT）
    :param before_positionValue: 上一次持有的总仓位价值（USDT）
    :return: 新加仓价格（USDT）
    """
    # 计算最新加仓价格
    if positionValue == before_positionValue:
        return latest_price

    new_entry_price = ((avgPrice * positionValue) - (before_avgPrice * before_positionValue)) / (
            positionValue - before_positionValue)

    return new_entry_price


def compare_significant_digits2(number1, number2, n=3):
    # 将数字转换为 Decimal 类型来保证精度
    num1 = Decimal(str(number1))
    num2 = Decimal(str(number2))

    # 计算两个数字的差值
    abs_diff = abs(num1 - num2)

    # 获取数字的字符串表示（保留原始精度）
    str_num1 = str(num1).lstrip('0')  # 去除前导零
    str_num2 = str(num2).lstrip('0')  # 去除前导零

    # 找到小数点的位置
    def get_effective_length(num_str):
        if '.' in num_str:
            # 分成整数部分和小数部分
            integer_part, decimal_part = num_str.split('.')
            return len(integer_part), len(decimal_part)
        else:
            return len(num_str), 0  # 没有小数部分

    int_len1, dec_len1 = get_effective_length(str_num1)
    int_len2, dec_len2 = get_effective_length(str_num2)

    # 比较有效位数：如果整数部分或小数部分不同，则认为它们的有效位数不对齐
    if int_len1 != int_len2 or dec_len1 != dec_len2:
        return False

    # 如果差值大到无法满足有效位的条件（例如大于 10^-n），直接返回 False
    if abs_diff >= Decimal(10) ** -n * 2:
        return False
    else:
        return True


def compare_significant_digits(number1, number2, n=3):
    # 将数字转换为 Decimal 类型来保证精度
    num1 = Decimal(str(number1))
    num2 = Decimal(str(number2))

    # 将数字转换为科学计数法，保留 n 位有效数字
    def to_scientific_notation(num, n):
        # 使用科学计数法，保留 n 位有效数字
        scientific_notation = f"{num:.{n}e}"

        # 如果没有 'e'，说明是一个普通数字，没有指数部分
        if 'e' not in scientific_notation:
            # 添加指数部分，假设为 0
            scientific_notation += 'e0'

        # 分离出有效数字和指数部分
        base, exponent = scientific_notation.split('e')

        # 如果科学计数法中的 base 部分有小数点，移除它
        base = base.replace('.', '')[:n]

        # 返回有效数字和指数部分
        return base, int(exponent)

    # 获取两个数字的科学计数法表示
    base1, exponent1 = to_scientific_notation(num1, n)
    base2, exponent2 = to_scientific_notation(num2, n)
    if n < 4:
        b2 = compare_significant_digits2(number1, number2, n + 1)
    else:
        b2 = True
    # 比较有效数字和指数部分
    return base1 == base2 and exponent1 == exponent2 and b2


# 计算 Bollinger Bands
def calculate_bollinger_bands(df, df_1m, symbol, length=20,
                              multiplier=2,
                              multiplier_doge=2.05,
                              multiplier_xrp=2,
                              multiplier_ada=1.92,
                              multiplier_pepe=1.88,
                              multiplier_super=1.88,  # 趋势缩窄
                              multiplier_pepe_add_b=1.78,
                              multiplier_pepe_add_b2=1.68,
                              multiplier_pepe_add_b3=1.98,
                              multiplier_pepe_add_s=1.98,
                              multiplier_add=1.88,
                              ):
    # df['MA'] = df['close'].ewm(span=length, adjust=False).mean()
    # df['StdDev'] = df['close'].ewm(span=length, adjust=False).std()
    df['MA'] = df['close'].rolling(window=length).mean()
    df['StdDev'] = df['close'].rolling(window=length).std()
    df_1m['MA'] = df_1m['close'].rolling(window=length).mean()
    df_1m['StdDev'] = df_1m['close'].rolling(window=length).std()
    df_1m['UpperBand'] = df_1m['MA'] + (multiplier * df_1m['StdDev'])
    df_1m['LowerBand'] = df_1m['MA'] - (multiplier_doge * df_1m['StdDev'])
    if symbol == "DOGEUSDT":
        df['UpperBand'] = df['MA'] + (multiplier_doge * df['StdDev'])
        df['LowerBand'] = df['MA'] - (multiplier_doge * df['StdDev'])
        df['UpperBand_add'] = df['MA'] + (multiplier_add * df['StdDev'])
        df['LowerBand_add'] = df['MA'] - (multiplier_add * df['StdDev'])
        multiplier_super = 1.78
    elif symbol == "XRPUSDT":
        df['UpperBand'] = df['MA'] + (multiplier_xrp * df['StdDev'])
        df['LowerBand'] = df['MA'] - (multiplier_xrp * df['StdDev'])
        df['UpperBand_add'] = df['MA'] + (multiplier_add * df['StdDev'])
        df['LowerBand_add'] = df['MA'] - (multiplier_add * df['StdDev'])
    elif symbol == "ADAUSDT":
        df['UpperBand'] = df['MA'] + (multiplier_ada * df['StdDev'])
        df['LowerBand'] = df['MA'] - (multiplier_ada * df['StdDev'])
        df['UpperBand_add'] = df['MA'] + (multiplier_add * df['StdDev'])
        df['LowerBand_add'] = df['MA'] - (multiplier_add * df['StdDev'])
    elif symbol == "1000PEPEUSDT":
        df['UpperBand'] = df['MA'] + (multiplier_pepe * df['StdDev'])
        df['LowerBand'] = df['MA'] - (multiplier_pepe * df['StdDev'])
        df['UpperBand_add'] = df['MA'] + (multiplier_pepe_add_s * df['StdDev'])
        df['LowerBand_add2'] = df['MA'] - (multiplier_pepe_add_b2 * df['StdDev'])
        df['LowerBand_add3'] = df['MA'] - (multiplier_pepe_add_b3 * df['StdDev'])
        df['LowerBand_add'] = df['MA'] - (multiplier_pepe_add_b * df['StdDev'])

    df['UpperBand_super'] = df['MA'] + (multiplier_super * df['StdDev'])
    df['LowerBand_super'] = df['MA'] - (multiplier_super * df['StdDev'])

    # 计算布林带下轨到中线的百分比
    df['Percentage_Lower'] = ((df['MA'] - df['LowerBand']) / df['LowerBand']) * 100
    # 计算布林带上轨到中线的百分比
    df['Percentage_Upper'] = ((df['UpperBand'] - df['MA']) / df['UpperBand']) * 100
    return df


def check_conditions(row, df, trade_type, timeframe):
    """
    检查满足条件的函数，区分买入和卖出逻辑。
    :param row: 当前行数据
    :param df: DataFrame
    :param trade_type: "buy" 或 "sell"，用于判断买入或卖出逻辑
    :return: 是否满足条件
    """
    # 获取当前行及前面5行
    relevant_rows = df.iloc[max(0, row.name - 4):row.name + 1]
    if row.name < 4:
        return False
    # TODO DOGE
    if trade_type == "buy":
        # 买入逻辑：(close - open) / open >= 0.0049
        if timeframe == '15m':
            condition_met = ((relevant_rows['open'] - relevant_rows['close']) / relevant_rows['open'] >= 0.01)  # &
        elif timeframe == '5m':
            condition_met = ((relevant_rows['open'] - relevant_rows['close']) / relevant_rows['open'] >= 0.0025)  # &
        elif timeframe == '3m':
            condition_met = ((relevant_rows['open'] - relevant_rows['close']) / relevant_rows['open'] >= 0.0019)  # &
        #    (relevant_rows['close'] < relevant_rows['LowerBand']))
        # else:
        #     condition_met = np.zeros(len(df), dtype=bool)  # 如果没有匹配的条件，返回全 False
    else:
        # 卖出逻辑：(open - close) / close >= 0.0049
        if timeframe == '15m':
            condition_met = ((relevant_rows['close'] - relevant_rows['open']) / relevant_rows['open'] >= 0.01)  # &
        elif timeframe == '5m':
            condition_met = ((relevant_rows['close'] - relevant_rows['open']) / relevant_rows['open'] >= 0.0025)  # &
        elif timeframe == '3m':
            condition_met = ((relevant_rows['close'] - relevant_rows['open']) / relevant_rows['open'] >= 0.0019)  # &
        #   (relevant_rows['close'] > relevant_rows['UpperBand']))
        # else:
        #     condition_met = np.zeros(len(df), dtype=bool)  # 如果没有匹配的条件，返回全 False

    # 如果有任意一行满足条件，就返回 True
    return condition_met.any()


# 生成做多和做空信号
def generate_signals_(symbol, df, df_1m, timeframe):
    # ma200 倒数第二位
    df['ema_1_previous'] = df['EMA'].shift(2)
    # ma200 倒数第六位
    df['ema_4_previous'] = df['EMA'].shift(6)

    # ma180 倒数第二位
    df['ema_1_previous_180'] = df['EMA180'].shift(2)
    # ma180 倒数第六位
    df['ema_4_previous_180'] = df['EMA180'].shift(6)
    # 查看 5根蜡烛 做多做空最低波动多少才能下单(逆势做多做空条件）
    df['buy_condition_met'] = df.apply(lambda row: check_conditions(row, df, "buy", timeframe), axis=1)
    df['sell_condition_met'] = df.apply(lambda row: check_conditions(row, df, "sell", timeframe), axis=1)
    # 顺势做多判断
    df['emab'] = df['ema_4_previous'] < df['ema_1_previous']
    # 顺势做空判断
    df['emas180'] = df['ema_4_previous_180'] > df['ema_1_previous_180']
    ema_b_condition = df['emab'].fillna(False)
    ema_s_condition = df['emas180'].fillna(False)

    df['amplitude_percentage'] = ((df['close'] - df['open']) / df['open']) * 100
    df['is_amplitude_valid_buy'] = df['amplitude_percentage'].abs() >= 0.09
    df['is_amplitude_valid_sell'] = df['amplitude_percentage'].abs() >= 0.08
    # 1分钟快线决定开单走向
    df['signal_mid_sell'] = ((df_1m['close'] > df_1m['MA']) & (df_1m['open'] > df_1m['MA'])) | (
            df['close'].apply(lambda x: Decimal(x) * Decimal(1.001)) < df['MA'])
    df['signal_mid_buy'] = (df_1m['close'] < df_1m['MA']) & (df_1m['open'] < df_1m['MA']) | (
            df['close'].apply(lambda x: Decimal(x) * Decimal(0.999)) > df['MA'])
    # df['sell_signal_mid'] = (df_1m['close'] > df_1m['MA'])
    df['1_up'] = (df_1m['close'] > df_1m['open'])
    # TODO 最新 DOGE
    if symbol == "DOGEUSDT":
        b = 0.0099
        s = 0.0089
        pl = 0.0042
        pu = 0.0038
        n = 3
    elif symbol == "XRPUSDT":
        b = 0.012
        s = 0.01
        pl = 0.0048
        pu = 0.0042
        n = 3
    elif symbol == "ADAUSDT":
        b = 0.012
        s = 0.01
        pl = 0.0042
        pu = 0.0038
        n = 3
    elif symbol == "1000PEPEUSDT":
        b = 0.012
        s = 0.01
        pl = 0.0061
        pu = 0.0051
        n = 4
    if (df['Percentage_Lower'].iloc[-1] > b):
        df['buy_signal'] = (
                ((df['close'] < df['LowerBand_super']) |
                 ((df.apply(lambda row: compare_significant_digits(row['close'], row['LowerBand_super'], n),
                            axis=1)) & (
                          df['close'] > df['open']))
                 ) &
                (ema_b_condition | (df['buy_condition_met'])))
        if symbol == "1000PEPEUSDT":
            df['buy_signal_add'] = (((df['close'] < df['LowerBand_add'])
                                     | ((df['close'] < df['open']) & (
                        df.apply(lambda row: compare_significant_digits(row['close'], row['LowerBand_add'], n),
                                 axis=1)))
                                     ) | ((df['close'] < df['LowerBand_add2'])
                                          | ((df['close'] < df['open']) & (
                        df.apply(lambda row: compare_significant_digits(row['close'], row['LowerBand_add2'], n),
                                 axis=1)))
                                          ) | ((df['close'] < df['LowerBand_add3'])
                                               | ((df['close'] < df['open']) & (
                        df.apply(lambda row: compare_significant_digits(row['close'], row['LowerBand_add3'], n),
                                 axis=1)))
                                               ) | (
                                            ((df['close'] < df['LowerBand']) |
                                             ((df.apply(
                                                 lambda row: compare_significant_digits(row['close'], row['LowerBand'],
                                                                                        n),
                                                 axis=1)) & (df['close'] > df['open']))
                                             ) &
                                            (ema_b_condition | (
                                                    df['buy_condition_met'] & (df['Percentage_Lower'] > pl))))
                                    )
        else:
            df['buy_signal_add'] = (((df['close'] < df['LowerBand_add'])
                                     | ((df['close'] < df['open']) & (
                        df.apply(lambda row: compare_significant_digits(row['close'], row['LowerBand_add'], n),
                                 axis=1)))
                                     # & (ema_b_condition | df['buy_condition_met'])
                                     )  # & (df['close'] < df['open']) #& (Decimal(df['low']/abs(df['close'] - df['open'])) < Decimal('0.25')) # 买入信号 布林带 针行禁下单
                                    | (
                                            ((df['close'] < df['LowerBand']) |
                                             ((df.apply(
                                                 lambda row: compare_significant_digits(row['close'], row['LowerBand'],
                                                                                        n),
                                                 axis=1)) & (df['close'] > df['open']))
                                             ) &
                                            (ema_b_condition | (
                                                    df['buy_condition_met'] & (df['Percentage_Lower'] > pl))))
                                    # & (df['close'] < df['open']) #& (Decimal(df['low']/abs(df['close'] - df['open'])) < Decimal('0.25')) # 买入信号 布林带 针行禁下单
                                    )
    else:
        df['buy_signal'] = ((df['close'] < df['LowerBand']) |
                            ((df.apply(lambda row: compare_significant_digits(row['close'], row['LowerBand'], n),
                                       axis=1)) & (
                                     df['close'] > df['open']))
                            )
        if symbol == "1000PEPEUSDT":
            df['buy_signal_add'] = (((df['close'] < df['LowerBand_add'])
                                     | ((df['close'] < df['open']) & (
                        df.apply(lambda row: compare_significant_digits(row['close'], row['LowerBand_add'], n),
                                 axis=1)))
                                     ) | ((df['close'] < df['LowerBand_add3'])
                                          | ((df['close'] < df['open']) & (
                        df.apply(lambda row: compare_significant_digits(row['close'], row['LowerBand_add3'], n),
                                 axis=1))))
                                    )
        else:
            df['buy_signal_add'] = ((df['close'] < df['LowerBand_add'])
                                    | ((df['close'] < df['open']) & (
                        df.apply(lambda row: compare_significant_digits(row['close'], row['LowerBand_add'], n),
                                 axis=1)))
                                    # & (ema_b_condition | df['buy_condition_met'])
                                    )  # & (df['close'] < df['open']) #& (Decimal(df['low']/abs(df['close'] - df['open'])) < Decimal('0.25')) # 买入信号 布林带 针行禁下单

    if (df['Percentage_Upper'].iloc[-1] > s):
        df['sell_signal'] = (
                ((df['close'] > df['UpperBand_super']) |
                 ((df.apply(lambda row: compare_significant_digits(row['close'], row['UpperBand_super'], n),
                            axis=1)) & (df['close'] < df['open']))
                 ) &
                (ema_s_condition | (df['sell_condition_met'])))  # & (df['close'] > df['open'])
        df['sell_signal_add'] = (((df['close'] > df['UpperBand_super']) |
                                  ((df.apply(
                                      lambda row: compare_significant_digits(row['close'], row['UpperBand_super'], n),
                                      axis=1)) & (
                                           df['close'] < df['open']))
                                  ) | ((df['close'] > df['UpperBand_add'])
                                       | ((df['close'] > df['open']) & (
                    df.apply(lambda row: compare_significant_digits(row['close'], row['UpperBand_add'], n), axis=1)))
                                       # & (ema_s_condition | df['sell_condition_met']))#& (df['close'] > df['open']
                                       ))
    else:
        df['sell_signal'] = (
                ((df['close'] > df['UpperBand']) |
                 ((df.apply(lambda row: compare_significant_digits(row['close'], row['UpperBand'], n), axis=1)) & (
                         df['close'] < df['open']))
                 ) &
                (ema_s_condition | (
                        df['sell_condition_met'] & (df['Percentage_Upper'] >= pu))))  # & (df['close'] > df['open'])
        df['sell_signal_add'] = ((df['close'] > df['UpperBand_add'])
                                 | ((df['close'] > df['open']) & (
                    df.apply(lambda row: compare_significant_digits(row['close'], row['UpperBand_add'], n), axis=1)))
                                 # & (ema_s_condition | df['sell_condition_met']))#& (df['close'] > df['open']
                                 )
    # 加仓满足基本条件即可

    df['buy_signal_l'] = ((df['close'] > df[
        'open'])  # #& (Decimal(df['low']/abs(df['close'] - df['open'])) < Decimal('0.25')) # 买入信号 布林带 针行禁下单
                          & ((~df.apply(lambda row: compare_significant_digits(row['close'], row['LowerBand']),
                                        axis=1)) |
                             ((((df['close'] - df['open']) / df['open'] * 100) > 0.15) & (
                                 df.apply(lambda row: compare_significant_digits(row['close'], row['LowerBand']),
                                          axis=1)))))  # 当前价格和下轨有效数前三位不一致
    df['sell_signal_l'] = ((df['close'] < df[
        'open'])  # & (Decimal(df['high']/abs(df['close'] - df['open'])) < Decimal('0.25'))# sell信号 布林带 针行禁下单
                           & ((~df.apply(lambda row: compare_significant_digits(row['close'], row['UpperBand']),
                                         axis=1)) |
                              ((((df['open'] - df['close']) / df['open'] * 100) > 0.15) & (
                                  df.apply(lambda row: compare_significant_digits(row['close'], row['UpperBand']),
                                           axis=1)))))  # 当前价格和上轨有效数前三位不一致

    df['buy_signal_l_new'] = ((df['close'] > df[
        'open'])  # #& (Decimal(df['low']/abs(df['close'] - df['open'])) < Decimal('0.25')) # 买入信号 布林带 针行禁下单
                              & ((~df.apply(lambda row: compare_significant_digits(row['close'], row['LowerBand']),
                                            axis=1)) |
                                 ((((df['close'] - df['open']) / df['open'] * 100) > 0.13) & (
                                     df.apply(lambda row: compare_significant_digits(row['close'], row['LowerBand']),
                                              axis=1)))))  # 当前价格和下轨有效数前三位不一致
    df['sell_signal_l_new'] = ((df['close'] < df[
        'open'])  # & (Decimal(df['high']/abs(df['close'] - df['open'])) < Decimal('0.25'))# sell信号 布林带 针行禁下单
                               & ((~df.apply(lambda row: compare_significant_digits(row['close'], row['UpperBand']),
                                             axis=1)) |
                                  ((((df['open'] - df['close']) / df['open'] * 100) > 0.13) & (
                                      df.apply(lambda row: compare_significant_digits(row['close'], row['UpperBand']),
                                               axis=1)))))  # 当前价格和上轨有效数前三位不一致

    # df['buy_signal_l'] = (df['close'] > df['open']) & (Decimal(df['low']/abs(df['close'] - df['open'])) < Decimal('2')) & (Decimal(df['high']/abs(df['close'] - df['open'])) < Decimal('2')) # 买入信号 布林带 针行禁下单
    # df['sell_signal_l'] = (df['close'] < df['open']) & (Decimal(df['low']/abs(df['close'] - df['open'])) < Decimal('2')) & (Decimal(df['high']/abs(df['close'] - df['open'])) < Decimal('2'))  #& (Decimal(df['high']/abs(df['close'] - df['open'])) < Decimal('0.25'))# sell信号 布林带 针行禁下单

    return df


# def check_extreme_conditions(candle_progress, wr14, wr20):
#     """
#     检查极端条件。如果蜡烛进度超过90%，并且WR14和WR20是极端值（0或-100），
#     则返回True，表示存在极端情况。
#     """
#     return (candle_progress >= 90) and ((wr14 == 0 or wr14 == -100) and (wr20 == 0 or wr20 == -100))
#
def calculate_candle_progress(latest, current_time):
    """
    计算蜡烛进度百分比。蜡烛持续时间为1分钟，通过蜡烛的开始时间和当前时间计算进度。
    """
    candle_start_time = latest['timestamp']
    candle_duration = timedelta(minutes=1)
    time_elapsed = current_time - candle_start_time
    progress_percentage = (time_elapsed.total_seconds() / candle_duration.total_seconds()) * 100
    return progress_percentage


#
# def check_latest_candle_signals(latest, candle_progress):
#     """
#     检查最新一根蜡烛是否满足买卖信号条件，并且考虑蜡烛进度和极端情况。
#     """
#     # 计算当前最新一根蜡烛的极端条件
#     extreme_condition = check_extreme_conditions(candle_progress, latest['WR14'], latest['WR20'])
#
#     # 根据最新蜡烛和极端条件，判断是否发出买卖信号
#     sell_signal = latest['sell_signal3'] and not extreme_condition
#     buy_signal = latest['buy_signal3'] and not extreme_condition
#
#     return sell_signal, buy_signal
# ------------------------------------------------------------------------------------------
def get_exchange_time_in_beijing(exchange):
    server_time_ms = exchange.fetch_time()
    # 转换为 UTC 时间
    # server_time_utc = datetime.fromtimestamp(server_time_ms / 1000, tz=timezone.utc)
    server_time_utc = datetime.fromtimestamp(server_time_ms / 1000)
    # 转换为北京时间 (UTC+8)
    # beijing_time = server_time_utc + timedelta(hours=8)
    return server_time_utc


# 初始化一个变量来记录最后一次发送通知的时间
last_send_time = 0


def should_send_notification():
    global last_send_time
    current_time = time.time()  # 获取当前时间（单位：秒）

    # 检查距离上次发送时间是否超过5分钟（300秒）
    if current_time - last_send_time >= 300:
        last_send_time = current_time  # 更新最后发送时间
        return True
    else:
        return False


def get_xrp_scalping_data(api_url=None, symbol="XRPUSDT", mode="scalping"):
    """Compatibility wrapper around the versioned, fail-closed prediction client."""
    try:
        return PREDICTION_CLIENT.fetch(symbol, mode, api_url=api_url)
    except PredictionUnavailable as e:
        logger.error(f"{symbol}/{mode} prediction rejected: {e}")
        return None
    except Exception as e:
        logger.error(f"{symbol}/{mode} prediction request failed: {e}")
        return None


def get_price(exchange, usdt, total_usdt, free_usdt, unrealisedPnl, curRealisedPnl, file_path, symbol, baselimit=96,
              timeframe='15m',
              oldpricetime=0):
    # 开关
    switch = get_ini_data(file_path, symbol, 'switch')
    if switch != 'on':
        return
    # 仓位
    symbol_positions = BYBIT.get_open_positions(symbol)
    predicted_data = get_xrp_scalping_data(symbol=symbol, mode="scalping")
    if not predicted_data:
        logger.error('predicted_data is empty')
        return
    # 获取更新时间戳
    remainingtime = Decimal(get_ini_data(file_path, symbol, 'remainingtime'))
    leverage = int(get_ini_data(file_path, symbol, 'leverage'))
    # 获取配置原最高最低价格
    oldbaselowprice = Decimal(get_ini_data(file_path, symbol, 'basemin'))
    oldbasehightprice = Decimal(get_ini_data(file_path, symbol, 'basemax'))
    # 获取配置中的老价格线
    oldbaselowline = Decimal(get_ini_data(file_path, symbol, 'lowline'))
    oldbasehightline = Decimal(get_ini_data(file_path, symbol, 'hightline'))
    # -------------- 修改指定时间段内最高价 & 最低价 --------------
    # 获取15秒内蜡烛数据
    ten_max, ten_mix = get_ohlcv_data(exchange, symbol, '1s', 15)
    trend = predicted_data['trend']
    logger.info(f'{symbol}  -- 15S最高价 --- {ten_max} 15S最低价 --- {ten_mix}  trend: {trend}')
    # 获取配置价格
    oldpricemix = Decimal(get_ini_data(file_path, symbol, 'oldpricemix'))
    oldpricemax = Decimal(get_ini_data(file_path, symbol, 'oldpricemax'))
    # 获取当前时间
    # now = datetime.now()
    now = get_exchange_time_in_beijing(exchange)
    # 获取90S后的时间戳
    later = now + timedelta(seconds=90)
    # 将datetime对象转换为时间戳
    tentimesleep = later.timestamp()
    if ten_max > oldpricemax or oldpricemax == 0:
        update_ini_data(file_path, symbol, 'oldpricemax', str(ten_max))
        update_ini_data(file_path, symbol,
                        'oldpricemaxtime', str(tentimesleep))
        logger.info(f'{symbol}  -- 更新最高价')
    if ten_mix < oldpricemix or oldpricemix == 0:
        update_ini_data(file_path, symbol, 'oldpricemix', str(ten_mix))
        update_ini_data(file_path, symbol,
                        'oldpricemixtime', str(tentimesleep))
        logger.info(f'{symbol}  -- 更新最低价')
    # ------------------------------------
    # current_time = datetime.now()
    # # 指定时间后的时间戳
    # timestamp = current_time.timestamp()
    # timeframe_now = float(get_ini_data(file_path, symbol, 'timeframe_now'))
    # if timestamp <= timeframe_now:
    #     timeframe = get_ini_data(file_path, symbol, 'timeframe_new')

    ohlcv = fetch_ohlcv_macd(exchange, symbol, timeframe, limit=220)
    ohlcv_1m = fetch_ohlcv_macd(exchange, symbol, "1m", limit=220)
    # if True:
    #     print(f'ohlcv_1m {ohlcv_1m}')
    #     return
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df_1m = pd.DataFrame(ohlcv_1m, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    # 获取市价 & 更新配置市价
    ticker = exchange.fetch_ticker(symbol)
    last_price = Decimal(str(ticker['last']))
    df = df.tail(220).copy()
    df_1m = df_1m.tail(220).copy()
    df = calculate_sma(df, 200)
    df = calculate_bollinger_bands(df, df_1m, symbol)
    df.reset_index(drop=True, inplace=True)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')  # 确保时间戳为datetime类型
    df = generate_signals_(symbol, df, df_1m, timeframe)
    latest = df.iloc[-1]  # 获取最新一行
    latest2 = df.iloc[-2]  # 获取最新一行
    timestampstart = latest['timestamp']
    logger.info(f'{symbol}  -- timestampstart {timestampstart}')
    # if True:
    #     print(f'ohlcv_1m {ohlcv_1m}')
    #     return
    # 获取有效时间
    nowtimesleep = Decimal(time.time())
    oldpricemixtime = Decimal(get_ini_data(
        file_path, symbol, 'oldpricemixtime'))
    oldpricemaxtime = Decimal(get_ini_data(
        file_path, symbol, 'oldpricemaxtime'))
    if nowtimesleep > oldpricemixtime:
        logger.info(f'{symbol}  -- 最低价超出有效时间，价格重置 配置时间戳 {oldpricemixtime}')
        update_ini_data(file_path, symbol, 'oldpricemix', str(last_price))
        # update_ini_data(file_path, symbol, 'oldpricemixtime', str(nowtimesleep))
    if nowtimesleep > oldpricemaxtime:
        logger.info(f'{symbol}  -- 最高价超出有效时间，价格重置 配置时间戳 {oldpricemaxtime}')
        update_ini_data(file_path, symbol, 'oldpricemax', str(last_price))
        # update_ini_data(file_path, symbol, 'oldpricemaxtime', str(nowtimesleep))

    if oldpricetime == 0:
        old_price = get_ini_data(file_path, symbol, 'nowprice')
        update_ini_data(file_path, symbol, 'oldprice', old_price)

    update_ini_data(file_path, symbol, 'nowprice', str(last_price))
    if len(symbol_positions) <= 0:
        update_ini_data(file_path, symbol, 'lock_profits', 'off')
    lock_profit = get_ini_data(file_path, symbol, 'lock_profits')
    # symbol_rsi, buy_threshold, sell_threshold, signals = get_latest_rsi(exchange, symbol)
    update_ini_data(file_path, symbol, 'lock_profits', 'off')
    # 如果有仓位，判断是否可以锁盈利
    if symbol_positions:
        for o in symbol_positions:
            lock_profits(exchange, symbol_positions, df, timeframe, lock_profit, latest, latest2, o, leverage,
                         './setting_v4.ini',
                         last_price, usdt)
    if lock_profit == 'on':
        return
    if timeframe == '15m':
        later = latest['timestamp'] + timedelta(seconds=14 * 60 + 52)  # 5
        notdo_later = latest['timestamp'] + timedelta(seconds=15 * 60 + 1)
        cancel_later = latest['timestamp'] + timedelta(seconds=5 * 60)
    elif timeframe == '5m':
        later = latest['timestamp'] + timedelta(seconds=4 * 60 + 52)  # 5
        notdo_later = latest['timestamp'] + timedelta(seconds=5 * 60 + 1)
        cancel_later = latest['timestamp'] + timedelta(seconds=2 * 60 + 30)
    elif timeframe == '3m':
        later = latest['timestamp'] + timedelta(seconds=2 * 60 + 52)  # 5
        notdo_later = latest['timestamp'] + timedelta(seconds=3 * 60 + 1)
        cancel_later = latest['timestamp'] + timedelta(seconds=60 + 30)
    # TODO 两种下单模式，
    # 1. 普通下单
    # 2. 达到下单要求，找最优下单。
    timestampstart = latest['timestamp'].timestamp()
    later = later.timestamp()
    now = now.timestamp()
    cancel_later = cancel_later.timestamp()
    # 时间戳不满做的情况下不能下单
    if now < later:
        logger.info(f'{symbol}  -- 时间戳不满足 {timestampstart} notdo_later {notdo_later} later {later} now {now}')
        # 如果有仓位，比较更新最新下单时间
        if symbol_positions:
            for data in symbol_positions:
                symbol = data['info']['symbol']
                side = data['info']['side'].lower()
                # markprice = Decimal(data['markPrice'])
                size = data['info']['size']
                avgprice = data['info']['avgPrice']
                createdTime_now = data['info']['createdTime']
                if side == 'buy':
                    createdTime_now_old = get_ini_data(file_path, symbol, 'createdtime_now_buy')
                    size_old = get_ini_data(file_path, symbol, 'size_buy')
                    p = 3
                    # 破历史，重置更新时间
                    update_ini_data(file_path, symbol, 'createdtime_now_buy', createdTime_now)
                    update_ini_data(file_path, symbol, 'size_buy', size)
                    current_time = datetime.now()
                    # 指定时间后的时间戳
                    timestamp = current_time.timestamp()
                    update_ini_data(file_path, symbol, 'long_last_time', str(timestamp))
                else:
                    createdTime_now_old = get_ini_data(file_path, symbol, 'createdtime_now_sell')
                    size_old = get_ini_data(file_path, symbol, 'size_sell')
                    p = 4
                    # 破历史，重置更新时间
                    update_ini_data(file_path, symbol, 'createdtime_now_sell', createdTime_now)
                    update_ini_data(file_path, symbol, 'size_sell', size)
                    current_time = datetime.now()
                    # 指定时间后的时间戳
                    timestamp = current_time.timestamp()
                    update_ini_data(file_path, symbol, 'short_last_time', str(timestamp))
                if createdTime_now != createdTime_now_old or size != size_old:
                    logger.info(f"size: {size},size_old: {size_old}")
                    if size > size_old:
                        lark_send(
                            symbol,
                            "",
                            p, avgprice, avgprice)
                    else:
                        # p = 7
                        lark_send(
                            symbol,
                            "",
                            7, size, size)
        # 如果两边相等，播报盈利情况
        if total_usdt == free_usdt:
            allMoney = float(get_ini_data(file_path, "MINE", 'usdt'))
            if allMoney != total_usdt:
                initialize_db()
                insert_price_change(total_usdt)
                update_ini_data(file_path, "MINE", 'usdt', str(total_usdt))
                this_profit_percentage, this_profit, daily_profit_percentage, daily_profit, \
                    monthly_profit_percentage, monthly_profit, yearly_profit_percentage, yearly_profit, day, data = get_profit()
                lark_send(
                    symbol,
                    "",
                    5, once_earnings=str(this_profit_percentage), once_usdt=str(this_profit),
                    day_earnings=str(daily_profit_percentage),
                    day_usdt=str(daily_profit), month_earnings=str(monthly_profit_percentage),
                    month_usdt=str(monthly_profit),
                    year_earnings=str(yearly_profit_percentage), year_usdt=str(yearly_profit),
                    total_money=str(total_usdt), day=day, data=data)
        # 如果不相等，播报下单情况（TODO 未完工--减仓提示）
        if total_usdt != free_usdt:
            logger.info(
                f'{symbol}  -- free_usdt {free_usdt}  loss_money {unrealisedPnl} curRealisedPnl {curRealisedPnl} total_usdt {total_usdt}')
            if Decimal(1 - free_usdt / total_usdt) > Decimal(0.09):
                loss_money = abs(unrealisedPnl + curRealisedPnl)
                # loss_percentage = Decimal(1 - free_usdt / total_usdt) * 100
                # 2025年06月17日 09:52:05修改 认为上面写的不对
                loss_percentage = Decimal(loss_money / total_usdt) * 100
                if should_send_notification():
                    lark_send(
                        symbol,
                        "",
                        6,
                        loss_percentage=str(loss_percentage),
                        loss_money=str(loss_money),
                        total_money=str(total_usdt))
        return
    # 检查订单是否成交未成交删除此波段不做
    fetch = fetch_open_order(symbol, latest, latest2, cancel_later, timeframe, now, file_path)
    if fetch:
        logger.info(f'{symbol}  -- 有未完成的订单 {fetch}')
        return
    else:
        # 重新获取仓位-毫秒级获取
        symbol_positions = BYBIT.get_open_positions(symbol)
    logger.info(f'{symbol}  -- 蜡烛最后8秒钟进场 {timestampstart} later {later} now {now}')
    # 获取最新 & 更新最高最低
    base = get_ohlcv_data(exchange, symbol, timeframe, baselimit)
    update_ini_data(file_path, symbol, 'basemax', str(base[0]))
    update_ini_data(file_path, symbol, 'basemin', str(base[1]))
    # 计算最新的价格差值
    basepricelow = Decimal(str(base[1]))
    basepricehigh = Decimal(str(base[0]))
    limit = basepricehigh - basepricelow

    # 判断是否有仓位
    if not symbol_positions:
        # 做单重置returnrate 浮动最高回报率 , 且在无仓时才更新
        update_ini_data(file_path, symbol, 'buyreturnrate', '0')
        update_ini_data(file_path, symbol, 'sellreturnrate', '0')
        update_ini_data(file_path, symbol, 'buyposition', "0")
        update_ini_data(file_path, symbol, 'sellposition', '0')
        update_ini_data(file_path, symbol, 'buycount', '0')
        update_ini_data(file_path, symbol, 'sellcount', '0')
        timestamp = str(latest['timestamp'].timestamp())
        update_ini_data(file_path, symbol, 'selltime', timestamp)
        update_ini_data(file_path, symbol, 'buytime', timestamp)
        update_ini_data(file_path, symbol,
                        f'3counts_takeprofit_buy', 'off')
        update_ini_data(file_path, symbol,
                        f'3counts_takeprofit_sell', 'off')

        buy_count = 0
        sell_count = 0
        # 是否可以做空
        if latest2.get(
                'sell_signal') and not latest.get('sell_signal') and latest.get('sell_signal_l_new') and latest.get(
            'is_amplitude_valid_sell') and predicted_data['trend'] == 'down':
            # TODO 与上面条件不一样2025年02月05日 17:31:07增加
            nowtimesleep = Decimal(time.time())
            long_last_time = Decimal(get_ini_data(file_path, symbol, 'long_last_time'))

            if nowtimesleep - long_last_time > 60 * 5 * 3:
                last_price = go_short(exchange, file_path, last_price, latest, latest2, leverage, limit, sell_count,
                                      symbol, usdt, True)
        elif latest2.get(
                'buy_signal') and not latest.get('buy_signal') and latest.get('buy_signal_l_new') and latest.get(
            'is_amplitude_valid_buy') and predicted_data['trend'] == 'up':
            # if symbol == "XRPUSDT":
            #     if latest.get('signal_mid_buy'):
            #         last_price = go_long(exchange, file_path, last_price, latest, latest2, leverage, limit, buy_count,
            #                              symbol, usdt, True)
            #     else:
            #         last_price = go_short(exchange, file_path, last_price, latest, latest2, leverage, limit, sell_count,
            #                               symbol, usdt)
            # else:
            # TODO 与上面条件不一样2025年02月05日 17:31:07增加
            nowtimesleep = Decimal(time.time())
            short_last_time = Decimal(get_ini_data(file_path, symbol, 'short_last_time'))
            if nowtimesleep - short_last_time > 60 * 5 * 3:
                last_price = go_long(exchange, file_path, last_price, latest, latest2, leverage, limit, buy_count,
                                     symbol, usdt, True)
    # 已持仓单向
    elif len(symbol_positions) == 1:
        order = symbol_positions[0]
        # positionValue = Decimal(order['info']['positionValue'])
        # leverage_now = Decimal(order['info']['leverage'])
        # usdt_ = Decimal(int(positionValue / leverage_now))
        # if usdt < usdt_:
        #     usdt = usdt_
        side = order['info']['side'].lower()
        if order['info']['side'].lower() == 'buy':
            update_ini_data(file_path, symbol, 'sellreturnrate', '0')
            update_ini_data(file_path, symbol, 'sellposition', '0')
            update_ini_data(file_path, symbol, 'sellcount', '0')
            timestamp = str(latest['timestamp'].timestamp())
            update_ini_data(file_path, symbol, 'selltime', timestamp)
            update_ini_data(file_path, symbol,
                            f'3counts_takeprofit_sell', 'off')
            sell_count = 0
            if Decimal(time.time()) > oldpricemaxtime:
                ticker = exchange.fetch_ticker(symbol)
                last_price = Decimal(str(ticker['last']))
                if latest2.get('sell_signal') and not latest.get('sell_signal') and latest.get('sell_signal_l') and \
                        predicted_data['trend'] == 'down':
                    BYBIT.create_order(
                        symbol, 'sell', last_price, None, None, usdt, leverage, ordertype)
                    timestamp = str(latest['timestamp'].timestamp())
                    update_ini_data(file_path, symbol, 'selltime', timestamp)
                    rbb = sell_count + 1
                    update_ini_data(file_path, symbol, 'sellcount', str(rbb))
                    update_ini_data(file_path, symbol, 'oldpricemix', '0')
                    update_ini_data(file_path, symbol, 'sellfirst', 'true')
                    timestamp = latest['open']
                    update_ini_data(file_path, symbol,
                                    'selladdopen', str(timestamp))
                    if latest['high'] > latest2['high']:
                        maxhigh = latest['high']
                    else:
                        maxhigh = latest2['high']
                    update_ini_data(file_path, symbol, 'selllatestmax', str(maxhigh))
                    logger.info(
                        f'selllatestmax {symbol} 方向：{side} selllatestmax：{maxhigh}')
                    logger.info(
                        f'{symbol} 可可以做空 做空价格大概 {last_price}')
        elif order['info']['side'].lower() == 'sell':
            update_ini_data(file_path, symbol, 'buyreturnrate', '0')
            update_ini_data(file_path, symbol, 'buyposition', "0")
            update_ini_data(file_path, symbol, 'buycount', '0')
            timestamp = str(latest['timestamp'].timestamp())
            update_ini_data(file_path, symbol, 'buytime', timestamp)
            update_ini_data(file_path, symbol,
                            f'3counts_takeprofit_buy', 'off')
            buy_count = 0
            if Decimal(time.time()) > oldpricemixtime:
                ticker = exchange.fetch_ticker(symbol)
                last_price = Decimal(str(ticker['last']))
                if latest2.get('buy_signal') and not latest.get('buy_signal') and latest.get('buy_signal_l') and \
                        predicted_data['trend'] == 'up':
                    # if latest2.get('buy_signal_lock') and (last_close2 + last_low2) / 2 or ((
                    #         latest3.get('buy_signal_lockl') and (
                    #         last_close2 + last_high2) / 2 > last_price)) < last_price and rate >= 16:
                    # if latest3.get('buy_signal3') and ((last_high2 + last_low2) / 2 < last_price) and rate >= 20:
                    BYBIT.create_order(
                        symbol, 'buy', last_price, None, None, usdt, leverage, ordertype)
                    timestamp = str(latest['timestamp'].timestamp())
                    update_ini_data(file_path, symbol, 'buytime', timestamp)
                    rbb = buy_count + 1
                    update_ini_data(file_path, symbol, 'buycount', str(rbb))
                    update_ini_data(file_path, symbol, 'oldpricemax', '0')
                    update_ini_data(file_path, symbol, 'buyfirst', 'true')
                    if latest['low'] < latest2['low']:
                        maxlow = latest['low']
                    else:
                        maxlow = latest2['low']
                    update_ini_data(file_path, symbol, 'buylatestmax', str(maxlow))
                    logger.info(
                        f'selllatestmax {symbol} 方向：buy buylatestmax：{maxlow}')
                    current_time = datetime.now()
                    # 指定时间后的时间戳
                    timestamp = current_time.timestamp()
                    update_ini_data(file_path, symbol,
                                    'buyaddopen', str(timestamp))
                    logger.info(
                        f'{symbol} 可可以做多  做多价格 {last_price} ')

    # 与配置中价格线比较
    if oldbasehightline != 0 and oldbaselowline != 0:
        if last_price >= oldbasehightline:
            lark_send(
                symbol,
                f'{symbol} -------> 历史最高价 {oldbasehightprice}  历史最低价 {oldbaselowprice} 做多线 {oldbaselowline} 做空线 {oldbasehightline} 当前价格：{last_price} ',
                0)
            # 破历史，重置更新时间
            update_ini_data(file_path, symbol, 'remainingtime', '0')
        elif last_price <= oldbaselowline:
            lark_send(
                symbol,
                f'{symbol} -------> 历史最高价 {oldbasehightprice}  历史最低价 {oldbaselowprice} 做多线 {oldbaselowline} 做空线 {oldbasehightline} 当前价格：{last_price} ')
            # 破历史，重置更新时间
            update_ini_data(file_path, symbol, 'remainingtime', '0')

    # 计算价格系数
    lowradius = Decimal(get_ini_data(file_path, symbol, 'lowradius'))
    hightradius = Decimal(get_ini_data(file_path, symbol, 'hightradius'))
    if limit >= 0:
        # 做空线
        hightline = basepricehigh + ((limit / 10) * hightradius)
        # 做多线
        lowline = basepricelow - ((limit / 10) * lowradius)

        # 防止系数过大保护
        if lowline > (basepricelow + ((limit / 10) * 3)):
            lowline = basepricelow + ((limit / 10) * 3)
        if hightline < (basepricehigh - ((limit / 10) * 3)):
            hightline = basepricehigh - ((limit / 10) * 3)

    buyreturnrate = Decimal(get_ini_data(file_path, symbol, 'buyreturnrate'))
    sellreturnrate = Decimal(get_ini_data(file_path, symbol, 'sellreturnrate'))
    # 如果当前时间大于配置时间，则更新
    if time.time() > remainingtime or buyreturnrate > 0 or sellreturnrate > 0:
        # if hightline != oldbasehightline or lowline != oldbaselowline:
        # 限价调整。
        if time.time() > remainingtime:
            update_ini_data(file_path, symbol, 'hightline', str(hightline))
            update_ini_data(file_path, symbol, 'lowline', str(lowline))
            # 获取更新时间(分钟)
            updatetime = int(get_ini_data(file_path, symbol, 'updatetime'))
            # 获取当前时间
            current_time = datetime.now()
            # 添加 配置updatetime 分钟
            new_time = current_time + timedelta(minutes=updatetime)
            # 指定时间后的时间戳
            timestamp = new_time.timestamp()
            update_ini_data(file_path, symbol, 'remainingtime', str(timestamp))
    # 开始准备
    middle_price = (lowline + hightline) / 2
    # 开单种子
    hightlimit = Decimal(get_ini_data(file_path, symbol, 'hightlimit'))
    lowlimit = Decimal(get_ini_data(file_path, symbol, 'lowlimit'))
    hight_price = middle_price + (limit / 10) * hightlimit
    low_price = middle_price - (limit / 10) * lowlimit
    logger.info(
        f'{symbol} ---- 当前中间价 --- {middle_price} -- 做多价格 {low_price} --- 做空价格 {hight_price} ---  limit:{limit}')


def go_short(exchange, file_path, last_price, latest, latest2, leverage, limit, sell_count, symbol, usdt, go_10=False):
    # 满足下跌趋势，不回头直接做空
    if latest.get('1_up'):
        BYBIT.create_order(
            symbol, 'sell', last_price * Decimal(1.00034), None, None, usdt, leverage)
    elif Decimal(abs(latest['open'] - latest['close']) / latest['open']) > 0.0092:
        # BYBIT.create_order(
        #     symbol, 'sell', last_price, None, None, usdt, leverage)
        BYBIT.create_order(
            # symbol, 'sell', Decimal(latest['open']) * Decimal(1.0055), None, None, usdt, leverage)
            symbol, 'sell', Decimal(latest['open']) * Decimal(0.9968), None, None, usdt, leverage)
    elif Decimal(abs(latest['open'] - latest['close']) / latest['open']) > 0.0066:
        BYBIT.create_order(
            # symbol, 'sell', Decimal(latest['open']) * Decimal(1.0025), None, None, usdt, leverage)
            symbol, 'sell', Decimal(latest['open']) * Decimal(0.99685), None, None, usdt, leverage)
    elif Decimal(abs(latest['open'] - latest['close']) / latest['open']) >= 0.0038:
        BYBIT.create_order(
            # symbol, 'sell', Decimal(latest['open']) * Decimal(1.0035), None, None, usdt, leverage)
            symbol, 'sell', Decimal(latest['open']) * Decimal(0.9975), None, None, usdt, leverage)
    elif Decimal(abs(latest2['open'] - latest2['close']) / latest2['open']) < 0.0006:
        BYBIT.create_order(
            symbol, 'sell', last_price, None, None, usdt, leverage, ordertype)
    else:
        # BYBIT.create_order(
        #     symbol, 'sell', last_price, None, None, usdt, leverage)
        if go_10:
            BYBIT.create_order(
                symbol, 'sell', last_price * Decimal(1.001), None, None, usdt, leverage)
            # timestamp = str(latest['timestamp'].timestamp())
        else:
            BYBIT.create_order(
                symbol, 'sell', last_price * Decimal(1.00034), None, None, usdt, leverage)
    timestamp = str(latest['timestamp'].timestamp())
    update_ini_data(file_path, symbol, 'selltime', timestamp)
    rbc = sell_count + 1
    update_ini_data(file_path, symbol, 'sellcount', str(rbc))
    if latest['high'] > latest2['high']:
        maxhigh = latest['high']
    else:
        maxhigh = latest2['high']
    update_ini_data(file_path, symbol, 'selllatestmax', str(maxhigh))
    logger.info(
        f'selllatestmax {symbol} 方向：sell selllatestmax：{maxhigh}')
    ticker = exchange.fetch_ticker(symbol)
    last_price = Decimal(str(ticker['last']))
    # 指定时间后的时间戳
    timestamp = latest['open']
    update_ini_data(file_path, symbol,
                    'selladdopen', str(timestamp))
    logger.info(
        f'{symbol} 无仓位 可可以做空 {last_price} limit:{limit} 做空价格 {last_price}')
    time.sleep(3)  # 暂停 2 秒
    return last_price


def go_long(exchange, file_path, last_price, latest, latest2, leverage, limit, buy_count, symbol, usdt, go_10=False):
    # 满足上涨趋势，不回头直接做多
    if not latest.get('1_up'):
        BYBIT.create_order(
            symbol, 'buy', Decimal(last_price) * Decimal(0.99966), None, None, usdt, leverage)
    elif Decimal(abs(latest['open'] - latest['close']) / latest['open']) > 0.0092:
        # BYBIT.create_order(
        #     symbol, 'buy', last_price, None, None, usdt, leverage, ordertype)
        BYBIT.create_order(
            symbol, 'buy', Decimal(latest['close']) * Decimal(0.9968), None, None, usdt, leverage)
    elif Decimal(abs(latest['open'] - latest['close']) / latest['open']) > 0.0066:
        BYBIT.create_order(
            symbol, 'buy', Decimal(latest['close']) * Decimal(0.9985), None, None, usdt, leverage)
    elif Decimal(abs(latest['open'] - latest['close']) / latest['open']) >= 0.0038:
        BYBIT.create_order(
            symbol, 'buy', Decimal(latest['close']) * Decimal(0.9975), None, None, usdt, leverage)
    elif Decimal(abs(latest2['open'] - latest2['close']) / latest2['open']) < 0.0006:
        # BYBIT.create_order(
        #     symbol, 'buy', Decimal(latest['close']) * Decimal(0.9965), None, None, usdt, leverage)
        BYBIT.create_order(
            symbol, 'buy', last_price, None, None, usdt, leverage, ordertype)
    else:
        # BYBIT.create_order(
        #     symbol, 'buy', last_price, None, None, usdt, leverage, ordertype)
        if go_10:
            BYBIT.create_order(
                symbol, 'buy', Decimal(last_price) * Decimal(0.999), None, None, usdt, leverage)
        else:
            BYBIT.create_order(
                symbol, 'buy', Decimal(last_price) * Decimal(0.99966), None, None, usdt, leverage)
    # BYBIT.create_order(
    #     symbol, 'sell', last_price, None, None, usdt, leverage, ordertype)
    timestamp = str(latest['timestamp'].timestamp())
    # update_ini_data(file_path, symbol, 'selltime', timestamp)
    # rbc = sell_count + 1
    # update_ini_data(file_path, symbol, 'sellcount', str(rbc))
    update_ini_data(file_path, symbol, 'buytime', timestamp)
    rbb = buy_count + 1
    update_ini_data(file_path, symbol, 'buycount', str(rbb))
    if latest['low'] < latest2['low']:
        maxlow = latest['low']
    else:
        maxlow = latest2['low']
    update_ini_data(file_path, symbol, 'buylatestmax', str(maxlow))
    logger.info(
        f'selllatestmax {symbol} 方向：buy buylatestmax：{maxlow}')
    ticker = exchange.fetch_ticker(symbol)
    last_price = Decimal(str(ticker['last']))
    current_time = datetime.now()
    # 指定时间后的时间戳
    timestamp = current_time.timestamp()
    update_ini_data(file_path, symbol,
                    'buyaddopen', str(timestamp))
    # update_ini_data(file_path, symbol,
    #                 'selladdopen', str(timestamp))
    logger.info(
        f'{symbol} 无仓位 可可以做多 做多价格 {last_price} limit:{limit} ')
    time.sleep(3)  # 暂停 2 秒
    return last_price


def fetch_open_order(symbol, latest, latest2, cancel_later, timeframe, now: float, file_path='./setting_v4.ini'):
    orders = BYBIT.get_open_orders(symbol)
    if orders and len(orders) > 0:
        selltime = float(get_ini_data(file_path, symbol, 'selltime'))
        buytime = float(get_ini_data(file_path, symbol, 'buytime'))
        if timeframe == '15m':
            later = timedelta(seconds=15 * 60 * 2 - 12)  # 5
            # five_candles_time = timestampstart-timedelta(seconds=15 * 60*5+10)
        elif timeframe == '5m':
            later = timedelta(seconds=5 * 60 * 2 - 12)  # 5
        elif timeframe == '3m':
            later = timedelta(seconds=3 * 60 * 2 - 12)  # 5
        for order in orders:
            reduceOnly = order['info']['reduceOnly']
            logger.info(
                f'{symbol} reduceOnly {reduceOnly} ')
            if now < cancel_later:
                buy_signal = latest.get('buy_signal')
                sell_signal = latest.get('sell_signal')
            else:
                buy_signal = latest2.get('buy_signal')
                sell_signal = latest2.get('sell_signal')
            if order['info']['side'].lower() == 'buy':
                if now - buytime >= later.total_seconds() and order['info'][
                    'orderType'].lower() == 'limit' and not reduceOnly or buy_signal:
                    BYBIT.cancel_order(order['info']['orderId'], symbol)
                elif order['info']['orderType'].lower() == 'limit' and not reduceOnly:
                    return True
            else:
                if now - selltime >= later.total_seconds() and order['info'][
                    'orderType'].lower() == 'limit' and not reduceOnly or sell_signal:
                    BYBIT.cancel_order(order['info']['orderId'], symbol)
                elif order['info']['orderType'].lower() == 'limit' and not reduceOnly:
                    return True
        return False


def lock_profits(exchange, symbol_positions, df, timeframe, lock_profit, latest, latest2, data, leverage,
                 file_path='./setting_v4.ini',
                 last_price=None,
                 usdt=1):
    symbol = data['info']['symbol']
    side = data['info']['side'].lower()
    markprice = Decimal(data['markPrice'])
    size = abs(Decimal(data['info']['size']))
    avgprice = Decimal(data['info']['avgPrice'])
    takeprofit = data['info']['takeProfit']
    takeProfit = None
    try:
        takeProfit = Decimal(takeprofit)
    except Exception as e:
        # logger.info(f'未达到调单标准，无需调整限价 - {e}')
        logger.info(f'崩溃了 takeProfit - - {traceback.format_exc()}')
        pass
    stop_loss_price = data['info']['stopLoss']
    lens = len(symbol_positions)
    # buylast = 0
    # buysecond = 0
    # selllast = 0
    # sellsecond = 0
    # 趋势判断
    threshold = False
    if side == 'buy':
        buylast = Decimal(get_ini_data(file_path, symbol, 'buylast'))
        buysecond = Decimal(get_ini_data(file_path, symbol, 'buysecond'))
        # if last buy prices is not equal to avgprice then update it
        if buylast != avgprice:
            update_ini_data(file_path, symbol, 'buysecond', str(buylast))
            update_ini_data(file_path, symbol, 'buylast', str(avgprice))
            buysecond = buylast
            buylast = avgprice
        if buylast - buysecond > 0 > buysecond:
            threshold = True

    else:
        selllast = Decimal(get_ini_data(file_path, symbol, 'selllast'))
        sellsecond = Decimal(get_ini_data(file_path, symbol, 'sellsecond'))
        # if last sell prices is not equal to avgprice then update it
        if selllast != avgprice:
            update_ini_data(file_path, symbol, 'sellsecond', str(selllast))
            update_ini_data(file_path, symbol, 'selllast', str(avgprice))
            sellsecond = selllast
            selllast = avgprice
        if selllast - sellsecond < 0 < sellsecond:
            threshold = True

    buy_threshold = False

    # createdTime_0 = 0
    # createdTime_1 = 0
    # if lens > 1:
    #     createdTime_0 = int(symbol_positions[0]['info']['createdTime'])
    #     createdTime_1 = int(symbol_positions[1]['info']['createdTime'])
    createdTime_now = int(data['info']['createdTime'])
    current_time_millis = int(time.time() * 1000)
    dif_time = current_time_millis - createdTime_now
    # 负盈利 获取配置参数
    buyposition = Decimal(get_ini_data(file_path, symbol, 'buyposition'))
    sellposition = Decimal(get_ini_data(file_path, symbol, 'sellposition'))

    # 负盈利 获取配置参数
    pullbuyposition = Decimal(get_ini_data(
        file_path, symbol, 'pullbuyposition'))
    pullsellposition = Decimal(get_ini_data(
        file_path, symbol, 'pullsellposition'))
    ini_returnrate = 0
    if side == 'buy':
        returnrate = ((markprice - avgprice) / avgprice * leverage) * 100
        ini_returnrate = Decimal(get_ini_data(
            file_path, symbol, 'buyreturnrate'))

        # 买单最小止损
        buy_limit = Decimal('5') * (buyposition + Decimal('1'))
        mix = Decimal('45') - buy_limit

    elif side == 'sell':
        returnrate = ((avgprice - markprice) / avgprice * leverage) * 100
        ini_returnrate = Decimal(get_ini_data(
            file_path, symbol, 'sellreturnrate'))

        # 卖单最小拉止损
        sell_limit = Decimal('5') * (sellposition + Decimal('1'))
        mix = Decimal('45') - sell_limit

    logger.info(
        f'{side} --- {symbol} returnrate {returnrate:.4f} lens {lens} dif_time {dif_time} mix ---> {mix}')

    # oldpricemixtime = Decimal(get_ini_data(
    #     file_path, symbol, 'oldpricemixtime'))
    # oldpricemaxtime = Decimal(get_ini_data(
    #     file_path, symbol, 'oldpricemaxtime'))
    # now_time = Decimal(time.time())
    positionincreasefactor = Decimal(get_ini_data(
        file_path, symbol, 'positionincreasefactor'))

    profitpullposition = Decimal(get_ini_data(
        file_path, symbol, 'profitpullposition'))

    plus_position = positionincreasefactor
    plus_times = 5
    pull_times = 2
    # 盈利加仓
    if returnrate > profitpullposition:
        if pullbuyposition < pull_times and side == 'buy' and (
                latest2.get('buy_signal')):
            usdt *= (pullbuyposition + 1) / 2
            logger.info(
                f'{symbol} 盈利开始加仓了 加仓金额 {usdt}')
            BYBIT.create_order(
                symbol, 'buy', last_price, None, None, usdt, leverage, ordertype)
            update_ini_data(file_path, symbol, 'buyreturnrate', '0')
            pullbuyposition += 1
            update_ini_data(file_path, symbol,
                            'pullbuyposition', str(pullbuyposition))
            ini_returnrate = 0
        elif pullsellposition < pull_times and side == 'sell' and (
                latest2.get('sell_signal')):
            usdt *= (pullsellposition + 1) / 2
            logger.info(f'{symbol} 盈利开始加仓了 加仓金额 {usdt}')
            BYBIT.create_order(
                symbol, 'sell', last_price, None, None, usdt, leverage, ordertype)
            update_ini_data(file_path, symbol, 'sellreturnrate', '0')
            pullsellposition += 1
            update_ini_data(file_path, symbol,
                            'pullsellposition', str(pullsellposition))
            ini_returnrate = 0
    buy_count = int(get_ini_data(file_path, symbol, 'buycount'))
    sell_count = int(get_ini_data(file_path, symbol, 'sellcount'))
    # buyaddopen = Decimal(get_ini_data(file_path, symbol, 'buyaddopen'))
    # selladdopen = Decimal(get_ini_data(file_path, symbol, 'selladdopen'))

    avgPrice = avgprice
    positionValue = Decimal(size)
    if side == 'buy' and buy_count > 1:
        before_avgPrice = Decimal(get_ini_data(file_path, symbol, 'buybeforeavgprice'))
        before_positionValue = Decimal(get_ini_data(file_path, symbol, 'buybeforepositionvalue'))
        latest_price = Decimal(get_ini_data(file_path, symbol, 'buylatestprice'))
    elif side == 'sell' and sell_count > 1:
        before_avgPrice = Decimal(get_ini_data(file_path, symbol, 'sellbeforeavgprice'))
        before_positionValue = Decimal(get_ini_data(file_path, symbol, 'sellbeforepositionvalue'))
        latest_price = Decimal(get_ini_data(file_path, symbol, 'selllatestprice'))
    else:
        before_avgPrice = avgPrice
        before_positionValue = positionValue
        latest_price = avgPrice

    latest_price = Decimal(calculate_latest_entry_price(latest_price, before_avgPrice, avgPrice, positionValue,
                                                        before_positionValue))
    # 指定时间后的时间戳
    # current_time = datetime.now()
    # timestamp = float(current_time.timestamp())
    ticker = exchange.fetch_ticker(symbol)
    last_price = Decimal(str(ticker['last']))
    last_open = Decimal(latest['open'])
    selltime = float(get_ini_data(file_path, symbol, 'selltime'))
    buytime = float(get_ini_data(file_path, symbol, 'buytime'))
    lastdo_time = 0
    if side == 'buy':
        returnrate_new = ((last_price - latest_price) / latest_price * leverage) * 100
        returnrate_open = ((last_open - latest_price) / latest_price * leverage) * 100
        lastdo_time = buytime
    else:
        returnrate_new = ((latest_price - last_price) / latest_price * leverage) * 100
        returnrate_open = ((latest_price - last_open) / latest_price * leverage) * 100
        lastdo_time = selltime

    logger.info(
        f'{symbol} side {side} 负盈利 回报率: {returnrate_new}  avgprice: {avgprice} latest_price: {latest_price} buy_count: {buy_count}  sell_count: {sell_count}  markprice: {markprice} size {size}')
    last_time = latest['timestamp'].timestamp()

    # 负盈利加仓 - 补仓
    nowt = get_exchange_time_in_beijing(exchange)
    timestampstart = latest['timestamp']
    if timeframe == '15m':
        later = latest['timestamp'] + timedelta(seconds=14 * 60 + 52)  # 5
        notdo_later = timestampstart + timedelta(seconds=15 * 60 + 1)
        # five_candles_time = timestampstart-timedelta(seconds=15 * 60*5+10)
    elif timeframe == '5m':
        later = timestampstart + timedelta(seconds=4 * 60 + 50)  # 5
        notdo_later = timestampstart + timedelta(seconds=5 * 60 + 1)
    elif timeframe == '3m':
        later = timestampstart + timedelta(seconds=2 * 60 + 50)  # 5
        notdo_later = timestampstart + timedelta(seconds=3 * 60 + 1)
        # five_candles_time = timestampstart-timedelta(seconds=5 * 60 * 5+10)
    later = later.timestamp()
    notdo_later = notdo_later.timestamp()
    # candles_time = five_candles_time.timestamp()
    now = nowt.timestamp()
    lowprice = Decimal(get_ini_data(file_path, symbol, 'buylatestmax'))
    highprice = Decimal(get_ini_data(file_path, symbol, 'selllatestmax'))
    add_position = False
    add_position_lock = False
    lock_returnrate = 0
    if lock_profit == 'on':
        if notdo_later > now > later:
            # if buyposition > plus_times:
            if (side == 'buy' and last_time > buytime and
                    ((latest2.get('buy_signal') and not latest.get('buy_signal') and latest.get('buy_signal_l')) or
                     (latest2.get('sell_signal') and not latest.get('sell_signal') and latest.get(
                         'sell_signal_l')) and returnrate > 0)):
                add_position = True

            elif (side == 'sell' and last_time > selltime and
                  ((latest2.get('buy_signal') and not latest.get('buy_signal') and latest.get(
                      'buy_signal_l') and returnrate > 0) or
                   (latest2.get('sell_signal') and not latest.get('sell_signal') and latest.get('sell_signal_l')))):
                add_position = True
            else:
                lock_returnrate = abs(Decimal(get_ini_data(file_path, symbol, 'lock_returnrate')))
                if returnrate > lock_returnrate:
                    add_position_lock = True
                else:
                    return
        else:
            return
    else:
        if returnrate_new < plus_position and returnrate_open < plus_position and (notdo_later > now > later):
            # if buyposition > plus_times:
            if side == 'buy' and last_time > buytime and latest2.get('buy_signal_add') and not latest.get(
                    'buy_signal_add') and latest.get('buy_signal_l') and buy_count < 3:
                # usdt *= buyposition + 1
                # todo 修改加仓金额
                onePositionValue = usdt * leverage / last_price
                rateCount = positionValue / onePositionValue
                if rateCount <= 0.5:
                    # usdt = positionValue/onePositionValue*usdt+0.05
                    usdt = (onePositionValue - positionValue + 5) * last_price / leverage
                    buy_count = 0
                    buyposition = 0
                elif rateCount <= 1:
                    # usdt = positionValue/onePositionValue*usdt+0.6
                    usdt = (positionValue + 5) * last_price / leverage
                    buy_count = 1
                    buyposition = 1
                else:
                    usdt *= buyposition + 1
                logger.info(
                    f'{symbol} buy 准备开始加仓了 加仓金额 {usdt} ')
                BYBIT.create_order(
                    symbol, 'buy', last_price, None, None, usdt, leverage, ordertype)
                timestamp = str(latest['timestamp'].timestamp())
                update_ini_data(file_path, symbol, 'buytime', timestamp)

                rbb = buy_count + 1
                update_ini_data(file_path, symbol, 'buycount', str(rbb))
                update_ini_data(file_path, symbol, 'buyreturnrate', '0')
                update_ini_data(file_path, symbol, 'buybeforeavgprice', str(avgPrice))
                update_ini_data(file_path, symbol, 'buybeforepositionvalue', str(positionValue))
                update_ini_data(file_path, symbol, 'buylatestprice', str(latest_price))
                if latest['low'] < latest2['low']:
                    maxlow = latest['low']
                else:
                    maxlow = latest2['low']
                update_ini_data(file_path, symbol, 'buylatestmax', str(maxlow))
                logger.info(
                    f'selllatestmax {symbol} 方向：{side} buylatestmax：{maxlow}')
                buyposition += 1
                update_ini_data(file_path, symbol,
                                'buyposition', str(buyposition))
                # 指定时间后的时间戳
                timestamp = latest['open']
                update_ini_data(file_path, symbol,
                                'buyaddopen', str(timestamp))
                # ini_returnrate = 0
                if BYBIT.edit_positions(symbol, side, 0, 0):
                    logger.info(
                        f'加仓调整调整订单 {symbol} 方向：{side} 止损价格：{stop_loss_price} 止盈价格:{takeprofit}')
                    return
            elif side == 'sell' and last_time > selltime and latest2.get('sell_signal_add') and not latest.get(
                    'sell_signal_add') and latest.get('sell_signal_l') and sell_count < 3:
                # usdt *= sellposition +
                # todo 修改加仓金额
                onePositionValue = usdt * leverage / last_price
                rateCount = positionValue / onePositionValue
                if rateCount <= 0.5:
                    # usdt = (1-positionValue / onePositionValue) * usdt + 0.05
                    usdt = (onePositionValue - positionValue + 5) * last_price / leverage
                    # sell_count = 1
                    # sellposition = -1
                    # TODO 修改加仓策略
                    sell_count = 0
                    sellposition = 0
                elif rateCount <= 1:
                    # usdt = positionValue / onePositionValue * usdt + 0.8
                    usdt = (positionValue + 5) * last_price / leverage
                    # sell_count = 2
                    # sellposition = 1
                    # TODO 修改加仓策略
                    sell_count = 1
                    sellposition = 1
                else:
                    usdt *= sellposition + 1
                logger.info(
                    f'{symbol} sell 准备开始加仓了 加仓金额 {usdt} ')
                BYBIT.create_order(
                    symbol, 'sell', last_price, None, None, usdt, leverage, ordertype)
                # TODO 未来实现 加仓逻辑 利益最大化
                # BYBIT.create_order(
                #     symbol, 'sell', last_price * Decimal(1.001), None, None, usdt, leverage)
                # BYBIT.create_order(
                #     symbol, 'buy', Decimal(last_price) * Decimal(0.999), None, None, usdt, leverage)
                timestamp = str(latest['timestamp'].timestamp())
                update_ini_data(file_path, symbol, 'selltime', timestamp)
                rbc = sell_count + 1
                update_ini_data(file_path, symbol, 'sellcount', str(rbc))
                update_ini_data(file_path, symbol, 'sellreturnrate', '0')
                update_ini_data(file_path, symbol, 'sellbeforeavgprice', str(avgPrice))
                update_ini_data(file_path, symbol, 'sellbeforepositionvalue', str(positionValue))
                update_ini_data(file_path, symbol, 'selllatestprice', str(latest_price))
                if latest['high'] > latest2['high']:
                    maxhigh = Decimal(latest['high'])
                else:
                    maxhigh = Decimal(latest2['high'])
                update_ini_data(file_path, symbol, 'selllatestmax', str(maxhigh))
                logger.info(
                    f'selllatestmax {symbol} 方向：{side} selllatestmax：{maxhigh}')
                sellposition += 1
                update_ini_data(file_path, symbol,
                                'sellposition', str(sellposition))
                timestamp = latest['open']
                update_ini_data(file_path, symbol,
                                'selladdopen', str(timestamp))
                # ini_returnrate = 0
                if BYBIT.edit_positions(symbol, side, 0, 0):
                    logger.info(
                        f'加仓调整调整订单 {symbol} 方向：{side} 止损价格：{stop_loss_price} 止盈价格:{takeprofit}')
                    return
    stop_loss_percent = None
    bl = returnrate > Decimal('20')
    logger.info(f'returnrate - {returnrate} takeprofit -  {takeprofit}  mix - {mix} bl - {bl}')

    later_down = nowt - timedelta(seconds=5 * 60 * 16)
    later_down = later_down.timestamp()
    if lock_profit == 'on' and add_position is True:
        stop_loss_percent = returnrate - int(5 * leverage / 100)
    elif lock_profit == 'on' and add_position is False:
        if add_position_lock is True:
            stop_loss_percent = lock_returnrate + 5
        else:
            return
    # elif returnrate > Decimal('120') * Decimal(leverage / 100):
    #     stop_loss_percent = returnrate - int(15 * leverage / 100)
    # elif returnrate > Decimal('100') * Decimal(leverage / 100):
    #     stop_loss_percent = returnrate - int(50 * leverage / 100)
    # elif returnrate > Decimal('60') * Decimal(leverage / 100):
    #     stop_loss_percent = returnrate - int(15 * leverage / 100)
    # elif returnrate > Decimal('53') * Decimal(leverage / 100):
    #     stop_loss_percent = returnrate - int(7 * leverage / 100)
    # elif returnrate > Decimal('38') * Decimal(leverage / 100):
    #     # stop_loss_percent = returnrate - int(10*leverage/100)
    #     stop_loss_percent = returnrate / 2
    # elif returnrate > Decimal('26') * Decimal(leverage / 100) and timeframe == '5m':
    #     stop_loss_percent = int(16 * leverage / 100)
    if returnrate > Decimal('120') * Decimal(leverage / 100):
        stop_loss_percent = returnrate - 15
    elif returnrate > Decimal('100') * Decimal(leverage / 100):
        stop_loss_percent = returnrate - 10
    elif returnrate > Decimal('80') * Decimal(leverage / 100):
        stop_loss_percent = returnrate - 15
    elif returnrate > Decimal('60') * Decimal(leverage / 100):
        stop_loss_percent = returnrate - Decimal(10) * Decimal(leverage / 100)
    elif returnrate > Decimal('50') * Decimal(leverage / 100):
        stop_loss_percent = returnrate - Decimal(8) * Decimal(leverage / 100)
    elif returnrate > Decimal('40') * Decimal(leverage / 100):
        #     stop_loss_percent = returnrate - 5
        # elif returnrate > mix:
        stop_loss_percent = int(14 * leverage / 100)
    new_stop_loss_price = None
    try:
        if stop_loss_percent:
            if side == 'buy':
                new_stop_loss_price = avgprice * \
                                      (Decimal(1) + Decimal(stop_loss_percent) / (Decimal(100) * Decimal(leverage)))
            elif side == 'sell':
                new_stop_loss_price = avgprice * \
                                      (Decimal(1) - Decimal(stop_loss_percent) / (Decimal(100) * Decimal(leverage)))
    except Exception as e:
        # logger.info(f'未达到调单标准，无需调整限价 - {e}')
        logger.info(f'崩溃了 takeprofit - - {traceback.format_exc()}')
        # return
        logger.info(
            f'symbol - {symbol} 崩溃了 takeprofit -  {takeprofit}  new_stop_loss_price - {new_stop_loss_price} avgprice - {avgprice} stop_loss_percent - {stop_loss_percent}')
        pass
    if returnrate > ini_returnrate or new_stop_loss_price:
        if side == 'buy':
            if new_stop_loss_price and stop_loss_price:
                if Decimal(new_stop_loss_price) > Decimal(stop_loss_price):
                    update_ini_data(file_path, symbol,
                                    'buyreturnrate', str(returnrate))
                    stop_loss_price = new_stop_loss_price
                    takeprofit = 0
                else:
                    return
            elif new_stop_loss_price and not stop_loss_price:
                update_ini_data(file_path, symbol,
                                'sellreturnrate', str(returnrate))
                stop_loss_price = new_stop_loss_price
                logger.info(f'symbol - {symbol}  进来了 takeprofit -  {takeprofit}  stop_loss_price {stop_loss_price}')
                takeprofit = 0
        elif side == 'sell':
            if new_stop_loss_price and stop_loss_price:
                if Decimal(new_stop_loss_price) < Decimal(stop_loss_price):
                    update_ini_data(file_path, symbol,
                                    'sellreturnrate', str(returnrate))
                    stop_loss_price = new_stop_loss_price
                    takeprofit = 0
                else:
                    return
            elif new_stop_loss_price and not stop_loss_price:
                update_ini_data(file_path, symbol,
                                'sellreturnrate', str(returnrate))
                stop_loss_price = new_stop_loss_price
                takeprofit = 0
    logger.info(f'symbol - {symbol}  takeprofit -  {takeprofit}  stop_loss_price {stop_loss_price}')
    # 无止盈价 & 止损价，防插针太快未盈利，设置100止盈
    if symbol == "DOGEUSDT":
        s = 0.0089
    elif symbol == "XRPUSDT":
        s = 0.0079
    elif symbol == "ADAUSDT":
        s = 0.0079
    elif symbol == "1000PEPEUSDT":
        s = 0.0069
    else:
        s = 0.01
    can_edit_positions = True
    if not takeprofit and not stop_loss_price:
        if side == 'buy':
            stop_loss_price = 0
            bpl = df['Percentage_Lower'].iloc[-1] > s
            if not bpl:
                if symbol == "ADAUSDT":
                    takeprofit = Decimal(avgprice) * \
                                 (1 + Decimal(13.8) / (100 * Decimal(leverage)))
                elif symbol == "DOGEUSDT" or symbol == "XRPUSDT":
                    takeprofit = Decimal(avgprice) * \
                                 (1 + Decimal(16.5) / (100 * Decimal(leverage)))
                elif symbol == "1000PEPEUSDT":
                    takeprofit = Decimal(avgprice) * \
                                 (1 + Decimal(10.8) / (100 * Decimal(leverage)))
            else:
                takeprofit = Decimal(avgprice) * \
                             (1 + 100 / (100 * Decimal(leverage)))
        elif side == 'sell':
            bpl = df['Percentage_Upper'].iloc[-1] > s
            if not bpl:
                if symbol == "ADAUSDT":
                    # if symbol == "ETHUSDT" or symbol == "BTCUSDT":
                    takeprofit = Decimal(avgprice) * \
                                 (1 - Decimal(13.8) / (100 * Decimal(leverage)))
                elif symbol == "DOGEUSDT" or symbol == "XRPUSDT":
                    # if symbol == "ETHUSDT" or symbol == "BTCUSDT":
                    takeprofit = Decimal(avgprice) * \
                                 (1 - Decimal(16.5) / (100 * Decimal(leverage)))
                elif symbol == "1000PEPEUSDT":
                    # if symbol == "ETHUSDT" or symbol == "BTCUSDT":
                    takeprofit = Decimal(avgprice) * \
                                 (1 - Decimal(10.8) / (100 * Decimal(leverage)))
            else:
                takeprofit = Decimal(avgprice) * \
                             (1 - 100 / (100 * Decimal(leverage)))
    # 2025年06月17日 09:32:48 删除认为 跟下面重复
    # elif side == 'buy' and returnrate < 0 and lastdo_time < later_down:
    #     takeprofit = Decimal(avgprice) * \
    #                  (1 + Decimal(9.5) / (100 * Decimal(leverage)))# TODO 8 -> 9.5
    # elif side == 'sell' and returnrate < 0 and lastdo_time < later_down:
    #     takeprofit = Decimal(avgprice) * \
    #                  (1 - Decimal(9.5) / (100 * Decimal(leverage)))# TODO 8 -> 9.5
    elif buy_count > 2 and side == 'buy' and returnrate < 0:
        takeprofit = Decimal(avgprice) * \
                     (1 + Decimal(10) / (100 * Decimal(leverage)))
        stop_loss_price = 0
    elif sell_count > 2 and side == 'sell' and returnrate < 0:
        takeprofit = Decimal(avgprice) * \
                     (1 - Decimal(10) / (100 * Decimal(leverage)))
        stop_loss_price = 0
    elif not stop_loss_price:
        logger.info(f'symbol - {symbol}  stop_loss_price return')
        can_edit_positions = False

    # TODO 未完成：第三次加仓时间
    # TODO 未完成：止盈如果遇到资金费率是100%的时候做空止盈是 30%
    # TODO 优化
    if can_edit_positions:
        if returnrate < -Decimal('65') and buy_count > 2:
            update_ini_data(file_path, symbol,
                            f'3counts_takeprofit_buy', 'on')
        elif returnrate < -Decimal('65') and sell_count > 2:
            update_ini_data(file_path, symbol,
                            f'3counts_takeprofit_sell', 'on')
        if side == 'buy':
            if returnrate < 0 and lastdo_time < later_down and str(
                    get_ini_data(file_path, symbol, f'3counts_takeprofit_{side}')) == 'on':
                new_takeprofit = Decimal(avgprice) * \
                                 (1 + Decimal(55) / (100 * Decimal(leverage)))  #
                logger.info(f'takeProfit: {takeProfit}  new_takeprofit-> {new_takeprofit}')
                if lens > 1:
                    new_takeprofit = Decimal(avgprice) * \
                                     (1 + Decimal(15) / (100 * Decimal(leverage)))  #
                if takeProfit and takeProfit > new_takeprofit:
                    takeprofit = takeProfit
                    # TODO 如果手动修改了止盈就不要设置stop_loss_price
                    stop_loss_price = 0
                elif not takeProfit:
                    logger.info(f'symbol - {symbol}  takeProfit return')
                    return
                else:
                    takeprofit = new_takeprofit
            elif returnrate < 0 and lastdo_time < later_down:
                takeprofit = Decimal(avgprice) * \
                             (1 + Decimal(9.5) / (100 * Decimal(leverage)))  # TODO 9 -> 9.5
            elif returnrate < 0 and buy_count == 2:
                if symbol == "DOGEUSDT" or symbol == "1000PEPEUSDT":
                    takeprofit = Decimal(avgprice) * \
                                 (1 + Decimal(18.2) / (100 * Decimal(leverage)))
                else:
                    takeprofit = Decimal(avgprice) * \
                                 (1 + Decimal(22) / (100 * Decimal(leverage)))
            elif returnrate < 0 and buy_count > 2:
                if symbol == "DOGEUSDT" or symbol == "1000PEPEUSDT":
                    takeprofit = Decimal(avgprice) * \
                                 (1 + Decimal(10) / (100 * Decimal(leverage)))
                else:
                    takeprofit = Decimal(avgprice) * \
                                 (1 + Decimal(66) / (100 * Decimal(leverage)))  # TODO 15 -> 66 2026年01月20日 10:20:30 贪心算法
                    #takeprofit = Decimal(avgprice) * \
                    #         (1 + Decimal(15) / (100 * Decimal(leverage)))
        elif side == 'sell':
            if returnrate < 0 and lastdo_time < later_down and str(
                    get_ini_data(file_path, symbol, f'3counts_takeprofit_{side}')) == 'on':
                new_takeprofit = Decimal(avgprice) * \
                                 (1 - Decimal(55) / (100 * Decimal(leverage)))  #
                logger.info(f'takeProfit: {takeProfit}  new_takeprofit-> {new_takeprofit}')
                # 如果双仓，那么降低要求
                if lens > 1:
                    new_takeprofit = Decimal(avgprice) * \
                                     (1 - Decimal(15) / (100 * Decimal(leverage)))  #
                if takeProfit and takeProfit < new_takeprofit:
                    takeprofit = takeProfit
                    # TODO 如果手动修改了止盈就不要设置stop_loss_price
                    stop_loss_price = 0
                elif not takeProfit:
                    logger.info(f'symbol - {symbol}  takeProfit2 return')
                    return
                else:
                    takeprofit = new_takeprofit
            elif returnrate < 0 and lastdo_time < later_down:
                takeprofit = Decimal(avgprice) * \
                             (1 - Decimal(9.5) / (100 * Decimal(leverage)))  # TODO 9 -> 9.5
            elif returnrate < 0 and sell_count == 2:
                if symbol == "DOGEUSDT" or symbol == "1000PEPEUSDT":
                    takeprofit = Decimal(avgprice) * \
                                 (1 - Decimal(18.2) / (100 * Decimal(leverage)))
                else:
                    takeprofit = Decimal(avgprice) * \
                                 (1 - Decimal(22) / (100 * Decimal(leverage)))
            elif returnrate < 0 and sell_count > 2:
                if symbol == "DOGEUSDT" or symbol == "1000PEPEUSDT":
                    takeprofit = Decimal(avgprice) * \
                                 (1 - Decimal(10) / (100 * Decimal(leverage)))
                else:
                    takeprofit = Decimal(avgprice) * \
                                (1 - Decimal(66) / (100 * Decimal(leverage)))  # TODO 2026年01月20日 10:20:56 贪心算法
                    #takeprofit = Decimal(avgprice) * \
                    #        (1 - Decimal(15) / (100 * Decimal(leverage)))


        logger.info(
            f'最终回报率 {symbol} 方向：{side} 回报率：{returnrate} init回报率：{ini_returnrate} 止损价格：{stop_loss_price} 止盈价格:{takeprofit}')

        if BYBIT.edit_positions(symbol, side, stop_loss_price, takeprofit):
            logger.info(
                f'调整订单 {symbol} 方向：{side} 止损价格：{stop_loss_price} 止盈价格:{takeprofit}')

    if side == 'buy':
        bpl = df['Percentage_Lower'].iloc[-1] > s
        k = "takeprofit_limit_b"
        tl = get_ini_data(file_path, symbol, k)
        if symbol == "ADAUSDT":
            takeprofit_limit = Decimal(avgprice) * \
                               (1 + Decimal(12.8) / (100 * Decimal(leverage)))
        elif symbol == "DOGEUSDT" or symbol == "XRPUSDT":
            # if symbol == "ETHUSDT" or symbol == "BTCUSDT":
            takeprofit_limit = Decimal(avgprice) * \
                               (1 + Decimal(15.2) / (100 * Decimal(leverage)))
        elif symbol == "1000PEPEUSDT":
            takeprofit_limit = Decimal(avgprice) * \
                               (1 + Decimal(9.8) / (100 * Decimal(leverage)))
        # takeprofit_limit1 = Decimal(avgprice) * \
        #                     (1 + Decimal(15) / (100 * Decimal(leverage)))
        # takeprofit_limit2 = Decimal(avgprice) * \
        #                     (1 + Decimal(21) / (100 * Decimal(leverage)))
        # takeprofit_limit3 = Decimal(avgprice) * \
        #                     (1 + Decimal(28) / (100 * Decimal(leverage)))
        # takeprofit_limit4 = Decimal(avgprice) * \
        #                     (1 + Decimal(32) / (100 * Decimal(leverage)))

        takeprofit_limit = Decimal(avgprice) * \
                           (1 + Decimal(32) / (100 * Decimal(leverage)))
        takeprofit_limit1 = Decimal(avgprice) * \
                            (1 + Decimal(41) / (100 * Decimal(leverage)))
        takeprofit_limit2 = Decimal(avgprice) * \
                            (1 + Decimal(58) / (100 * Decimal(leverage)))
        takeprofit_limit3 = Decimal(avgprice) * \
                            (1 + Decimal(80) / (100 * Decimal(leverage)))
        takeprofit_limit4 = Decimal(avgprice) * \
                            (1 + Decimal(100) / (100 * Decimal(leverage)))
        # takeprofit_limit5 = Decimal(avgprice) * \
        #              (1 + Decimal(52) / (100 * Decimal(leverage)))
    else:
        k = "takeprofit_limit_s"
        tl = get_ini_data(file_path, symbol, k)

        if symbol == "ADAUSDT":
            takeprofit_limit = Decimal(avgprice) * \
                               (1 - Decimal(12.5) / (100 * Decimal(leverage)))
        elif symbol == "DOGEUSDT" or symbol == "XRPUSDT":
            # if symbol == "ETHUSDT" or symbol == "BTCUSDT":
            takeprofit_limit = Decimal(avgprice) * \
                               (1 - Decimal(15.2) / (100 * Decimal(leverage)))
        elif symbol == "1000PEPEUSDT":
            takeprofit_limit = Decimal(avgprice) * \
                               (1 - Decimal(9.8) / (100 * Decimal(leverage)))
        bpl = df['Percentage_Upper'].iloc[-1] > s
        # takeprofit_limit1 = Decimal(avgprice) * \
        #                     (1 - 15 / (100 * Decimal(leverage)))
        # takeprofit_limit2 = Decimal(avgprice) * \
        #                     (1 - 21 / (100 * Decimal(leverage)))
        # takeprofit_limit3 = Decimal(avgprice) * \
        #                     (1 - 28 / (100 * Decimal(leverage)))
        # takeprofit_limit4 = Decimal(avgprice) * \
        #                     (1 - 32 / (100 * Decimal(leverage)))
        # takeprofit_limit5 = Decimal(avgprice) * \
        #                     (1 - 52 / (100 * Decimal(leverage)))
        takeprofit_limit = Decimal(avgprice) * \
                           (1 - Decimal(32) / (100 * Decimal(leverage)))
        takeprofit_limit1 = Decimal(avgprice) * \
                            (1 - Decimal(41) / (100 * Decimal(leverage)))
        takeprofit_limit2 = Decimal(avgprice) * \
                            (1 - Decimal(58) / (100 * Decimal(leverage)))
        takeprofit_limit3 = Decimal(avgprice) * \
                            (1 - Decimal(80) / (100 * Decimal(leverage)))
        takeprofit_limit4 = Decimal(avgprice) * \
                            (1 - Decimal(100) / (100 * Decimal(leverage)))
    is_exists = BYBIT.check_any_limit_order_exists(symbol, side)
    logger.info(f'symbol - {symbol}  is_exists {is_exists}')
    if (not is_exists or tl != str(avgprice)) and (
            size > 500 or (symbol == "XRPUSDT" or symbol == "ADAUSDT" and size > 50)):
        logger.info(f'symbol - {symbol}  is_exists {is_exists} 进来了')
        if not bpl:
            BYBIT.create_limit_liquidation_order(symbol, side, size, float(takeprofit_limit))
            # logger.info('无需调整订单')
        else:
            if side == 'buy' and buy_count >= 2:
                amount = size * Decimal(0.8)
                BYBIT.create_limit_liquidation_order(symbol, side, amount, float(takeprofit_limit1))
                BYBIT.create_limit_liquidation_order(symbol, side, size - amount, float(takeprofit_limit2))
            elif side == 'sell' and sell_count >= 2:
                amount = size * Decimal(0.8)
                BYBIT.create_limit_liquidation_order(symbol, side, amount, float(takeprofit_limit1))
                BYBIT.create_limit_liquidation_order(symbol, side, size - amount, float(takeprofit_limit2))
            else:
                amount = size / Decimal(6.0)
                amount2 = size / Decimal(5.0)
                amount3 = size / Decimal(4.0)
                BYBIT.create_limit_liquidation_order(symbol, side, amount, float(takeprofit_limit1))
                BYBIT.create_limit_liquidation_order(symbol, side, amount2, float(takeprofit_limit2))
                BYBIT.create_limit_liquidation_order(symbol, side, amount2, float(takeprofit_limit3))
                BYBIT.create_limit_liquidation_order(symbol, side, amount3, float(takeprofit_limit4))

        # BYBIT.create_limit_liquidation_order(symbol,side,size/6,float(takeprofit_limit5))
        update_ini_data(file_path, symbol, k, str(avgprice))
    else:
        logger.info('无需调整订单')
    # 如果 takeprofit 小于 takeprofit_limit 那么就创建一个止盈订单
    try:
        if side == 'buy':
            if Decimal(takeprofit) < takeprofit_limit:
                BYBIT.create_limit_liquidation_order(symbol, side, size, float(takeprofit * Decimal(0.99995)))
        else:
            if Decimal(takeprofit) > takeprofit_limit:
                BYBIT.create_limit_liquidation_order(symbol, side, size, float(takeprofit * Decimal(1.00006)))
    except Exception as e:
        if Decimal(takeprofit) == 0:
            if side == 'buy':
                takeprofit_limit = Decimal(avgprice) * \
                                   (1 + Decimal(55) / (100 * Decimal(leverage)))
            else:
                takeprofit = Decimal(avgprice) * \
                             (1 - Decimal(55) / (100 * Decimal(leverage)))
        else:
            takeprofit = Decimal(takeprofit)
        logger.info(f'无需调整订单 takeprofit - {takeprofit} side - {side} size - {size}')
        if side == 'buy':
            BYBIT.create_limit_liquidation_order(symbol, side, size, float(takeprofit * Decimal(0.99995)))
        else:
            BYBIT.create_limit_liquidation_order(symbol, side, size, float(takeprofit * Decimal(1.00006)))


def get_data(exchange, file_path):
    inidata = get_ini_data(file_path)

    balance = BYBIT.get_balances()
    total_usdt = balance['USDT']['total']
    free_usdt = balance['USDT']['free']
    # logger.info(f'balance-{balance}')
    if free_usdt is None:
        free_usdt = total_usdt - (float(balance['info']['result']['list'][0]['coin'][0]['totalPositionIM']))
    unrealisedPnl = float(balance['info']['result']['list'][0]['coin'][0]['unrealisedPnl'])
    # curRealisedPnl = float(balance['info']['result']['list'][0]['coin'][0]['curRealisedPnl'])
    try:
        curRealisedPnl = balance['info']['result']['list'][0]['coin'][0].get('curRealisedPnl', 0)
    except (KeyError, IndexError, TypeError):
        curRealisedPnl = 0
    # result = Decimal(total_usdt * 0.03)
    # result = Decimal(total_usdt * 0.0066)
    # 单个
    result = Decimal(total_usdt * 0.006)
    usdt = result.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    # for t in range(10):
    for d in inidata:
        logger.info(f'------------- {d} -------------')
        if d == 'MINE':
            continue
        get_price(exchange, usdt, total_usdt, free_usdt, unrealisedPnl, curRealisedPnl, file_path, d, int(
            inidata[d]['baselimit']), inidata[d]['timeframe'], 0)
        logger.info(f'-------------------------------')


def lark_send(symbol, text="", plus=1, zuoduo='', zuokong='',
              once_earnings='', once_usdt='', day_earnings='', day_usdt='', month_earnings='', month_usdt='',
              year_earnings='', year_usdt='', total_money='',
              loss_percentage='', loss_money='', day='', data='',
              larkwebhook=None):
    larkwebhook = larkwebhook or SETTINGS.lark_webhook_url
    if not larkwebhook:
        logger.debug("LARK_WEBHOOK_URL is empty; notification skipped")
        return False
    headers = {'Content-Type': 'application/json'}
    tt = "Mine"
    if plus == 2:
        payload = {
            'msg_type': 'interactive',
            'card': {
                'type': 'template',
                'data': {
                    'template_id': 'AAqC3ca3kgjgl',
                    "template_variable": {
                        'btitle': f'{tt} - {symbol}',
                        'zuoduo': zuoduo,
                        'zuokong': zuokong,
                        'url': f'https://www.bybit.com/trade/usdt/{symbol}',
                    }
                }
            }
        }
    elif plus == 6:
        payload = {
            'msg_type': 'interactive',
            'card': {
                'type': 'template',
                'data': {
                    'template_id': 'AAqSqDsaqoF0N',
                    "template_variable": {
                        'btitle': f'{tt} - {symbol}',
                        'loss_percentage': loss_percentage,
                        'loss_money': loss_money,
                        'total_money': total_money,
                        'url': f'https://www.bybit.com/trade/usdt/{symbol}',
                    }
                }
            }
        }
    elif plus == 5:
        v_datasets = {
            "type": "line",
            "title": {
                "text": "7日收益曲线"
            },
            "data": data,
            "xField": [
                "day",
                "type"
            ],
            "yField": "value",
            "seriesField": "type",
            "legends": {
                "visible": True,
                "orient": "bottom"
            }
        }
        payload = {
            'msg_type': 'interactive',
            'card': {
                'type': 'template',
                'data': {
                    'template_id': 'AAqSqOWoNgWiw',
                    "template_variable": {
                        'btitle': f'{tt}',
                        'once_earnings': once_earnings,
                        'once_usdt': once_usdt,
                        'day_earnings': day_earnings,
                        'day_usdt': day_usdt,
                        'month_earnings': month_earnings,
                        'month_usdt': month_usdt,
                        'year_earnings': year_earnings,
                        'year_usdt': year_usdt,
                        'total_money': total_money,
                        'v_datasets': v_datasets,
                        'days_operation': day,
                        'url': f'https://www.bybit.com/trade/usdt/{symbol}',
                    }
                }
            }
        }
    elif plus > 2:
        if plus == 3:
            template_id = 'AAqS6ajhl4zxo'
        elif plus == 4:
            template_id = 'AAqS6orLUGnfo'
        # elif plus == 7:
        else:
            template_id = 'AAqFxFCMuKNiB'
        payload = {
            'msg_type': 'interactive',
            'card': {
                'type': 'template',
                'data': {
                    'template_id': template_id,
                    "template_variable": {
                        'btitle': f'{tt} -{symbol}',
                        'zuoduo': zuoduo,
                        'zuokong': zuokong,
                        'url': f'https://www.bybit.com/trade/usdt/{symbol}',
                    }
                }
            }
        }
    else:
        if plus == 1:
            template_id = 'AAqC3rflQvjIb'
        # elif plus == 0:
        else:
            template_id = 'AAqC3uFe9KIx4'

        payload = {
            'msg_type': 'interactive',
            'card': {
                'type': 'template',
                'data': {
                    'template_id': template_id,
                    "template_variable": {
                        'text': f'{tt} - {text}',
                        'url': f'https://www.bybit.com/trade/usdt/{symbol}',
                        'symbol': symbol
                    },
                }
            }
        }

    try:
        response = requests.post(
            larkwebhook,
            headers=headers,
            data=json.dumps(payload),
            timeout=SETTINGS.prediction_timeout_seconds,
        )
        response.raise_for_status()
        return True
    except requests.RequestException as e:
        logger.error(f"Lark notification failed: {e}")
        return False


def set_tracking_exit(file_path):
    # 如果仓位无止损，则添加官方追踪出场，反之，则取消追踪出场
    position = BYBIT.get_all_open_positions()
    for r in position:
        symbol = r['info']['symbol']
        leverage = r['leverage']
        side = r['info']['side'].lower()
        markprice = r['markPrice']
        avgprice = r['entryPrice']
        stoplossprice = r['stopLossPrice']
        trailingStop = float(r['info']['trailingStop'])
        # print(stoplossprice,'---------<<')
        count = 0
        if side == 'buy':
            count = int(get_ini_data(file_path, symbol, 'buycount'))
        elif side == 'sell':
            count = int(get_ini_data(file_path, symbol, 'sellcount'))
        if count < 2:
            return
        target_return_percent = 28
        # print('当前方向',side)
        # print('当前币对',symbol)
        # print('杠杆倍数',leverage)
        if not stoplossprice and not trailingStop:
            if side == 'buy':
                returnrate = ((markprice - avgprice) / avgprice * leverage) * 100
                target_price = avgprice * (1 + (target_return_percent / 100) / leverage)
                target_split = target_price - (avgprice * (1 + (10 / 100) / leverage))
                # print(symbol,'-',side,'-',returnrate,target_price,target_split)
            else:
                returnrate = ((avgprice - markprice) / avgprice * leverage) * 100
                target_price = avgprice * (1 - (target_return_percent / 100) / leverage)
                target_split = (avgprice * (1 - (10 / 100) / leverage)) - target_price
            # print('------->>>',returnrate,target_price,target_split)
            # print('当前盈利',returnrate)
            # print('29% 盈利价格',target_price)
            # print('价差',target_split)
            if returnrate > 28:
                target_price = None
            BYBIT.set_trailing_stop(symbol, side, target_split, target_price, 'LastPrice')
            logger.info('监测到无止损订单，设置追踪出场', symbol, side, target_price)
        elif stoplossprice and trailingStop:
            logger.info(f'当前止损 {stoplossprice},币对 {symbol},方向 {side}')
            BYBIT.set_trailing_stop(symbol, side, 0)


def main():
    import ccxt

    logger.warning("Trading runtime mode: %s", SETTINGS.mode.value)
    if SETTINGS.mode is TradingMode.SHADOW:
        logger.warning("SHADOW mode is active: all authenticated order operations stay in memory")
    exchange = ccxt.bybit({
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'},
    })
    if SETTINGS.mode is TradingMode.TESTNET:
        exchange.set_sandbox_mode(True)
    runtime_ini = str(SETTINGS.root / 'setting_v4.ini')
    # get_data(exchange, './setting.ini')
    while True:
        try:
            time.sleep(3)
            get_data(exchange, runtime_ini)
            set_tracking_exit(runtime_ini)
        except Exception as e:
            logger.error(
                f" ------------------------ error ------------------------ ")
            logger.error(e)
            logger.error(traceback.format_exc())

            exc_type, exc_value, exc_traceback = sys.exc_info()
            logger.error("Exception type: %s" % exc_type)
            logger.error("Exception value: %s" % exc_value)
            for line in traceback.format_exception(exc_type, exc_value, exc_traceback):
                logger.error(line)
            logger.error(
                " ------------------------ ----- ------------------------ ")
            time.sleep(5)


if __name__ == '__main__':
    main()
    # lark_send(
    #     "测试",
    #     "",
    #     7, "1","1")
