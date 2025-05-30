#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
股票股息自动计算工具

功能：
1. 从数据库获取股票基本信息、分红数据和股价数据
2. 计算8年、5年、3年、1年的平均股息及相关指标
3. 分别计算基于不复权价和前复权价的两套指标
4. 将计算结果保存到数据库中
"""

import sqlite3
import pandas as pd
import numpy as np
import os
import logging
import time
from datetime import datetime
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dividend_calc.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 从环境变量获取配置
SOURCE_DB_PATH = os.getenv('SOURCE_DB_PATH', 'stock_data.db')
TARGET_DB_PATH = os.getenv('TARGET_DB_PATH', 'stock_index_data.db')
BALL_CODE_TABLE = os.getenv('BALL_CODE_TABLE', 'ball_code_data')
BONUS_DETAILS_TABLE = os.getenv('BONUS_DETAILS_TABLE', 'akshare_bonus_details')
DAILY_QUOTES_TABLE = os.getenv('DAILY_QUOTES_TABLE', 'daily_quotes')
DAILY_QUOTES_TABLE_QFQ = os.getenv('DAILY_QUOTES_TABLE_QFQ', 'daily_quotes_qfq')
DIVIDEND_RATIO_TABLE = 'stock_dividend_ratio'  # 目标表名

# 计算周期（年）
PERIODS = [8, 5, 3, 1]

# 理论交易日数（每年约252个交易日）
THEORETICAL_TRADING_DAYS_PER_YEAR = 252

# 缺失数据阈值（如果缺失超过10%的数据，则标记为错误）
MISSING_DATA_THRESHOLD = 0.1

def connect_db(db_path):
    """
    连接到SQLite数据库
    
    Args:
        db_path: 数据库文件路径
        
    Returns:
        sqlite3.Connection: 数据库连接对象，连接失败则返回None
    """
    try:
        conn = sqlite3.connect(db_path)
        logging.info(f"成功连接到数据库: {db_path}")
        return conn
    except sqlite3.Error as e:
        logging.error(f"连接数据库 {db_path} 失败: {e}")
        return None

def create_dividend_ratio_table(conn):
    """
    创建股息率表（如果不存在）
    
    Args:
        conn: 数据库连接对象
        
    Returns:
        bool: 成功返回True，失败返回False
    """
    if conn is None:
        logging.error("数据库连接无效，无法创建股息率表")
        return False
    
    try:
        cursor = conn.cursor()
        
        # 创建表结构
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {DIVIDEND_RATIO_TABLE} (
            -- 核心标识符
            ts_code TEXT PRIMARY KEY NOT NULL,
            symbol TEXT,
            list_date TEXT,
            listing_years REAL, -- (脚本运行日期 - list_date).days / 365.25

            -- 8年期指标 (N=8)
            status_n8 TEXT,
            avg_price_n8_unadj REAL,
            avg_dividend_n8 REAL,
            dividend_yield_n8_unadj REAL,
            avg_price_n8_fadj REAL,
            dividend_yield_n8_fadj REAL,
            dividend_pattern_n8 TEXT,
            dividend_years_count_n8 INTEGER,
            consecutive_dividend_years_n8 INTEGER,

            -- 5年期指标 (N=5)
            status_n5 TEXT,
            avg_price_n5_unadj REAL,
            avg_dividend_n5 REAL,
            dividend_yield_n5_unadj REAL,
            avg_price_n5_fadj REAL,
            dividend_yield_n5_fadj REAL,
            dividend_pattern_n5 TEXT,
            dividend_years_count_n5 INTEGER,
            consecutive_dividend_years_n5 INTEGER,

            -- 3年期指标 (N=3)
            status_n3 TEXT,
            avg_price_n3_unadj REAL,
            avg_dividend_n3 REAL,
            dividend_yield_n3_unadj REAL,
            avg_price_n3_fadj REAL,
            dividend_yield_n3_fadj REAL,
            dividend_pattern_n3 TEXT,
            dividend_years_count_n3 INTEGER,
            consecutive_dividend_years_n3 INTEGER,

            -- 1年期指标 (N=1)
            status_n1 TEXT,
            avg_price_n1_unadj REAL,
            avg_dividend_n1 REAL,
            dividend_yield_n1_unadj REAL,
            avg_price_n1_fadj REAL,
            dividend_yield_n1_fadj REAL,
            dividend_pattern_n1 TEXT,
            dividend_years_count_n1 INTEGER,
            consecutive_dividend_years_n1 INTEGER,

            -- 现价股息率相关
            latest_close_unadj REAL,
            latest_close_fadj REAL,
            latest_close_date TEXT, -- 每只股票自己的最新交易日期
            current_yield_unadj REAL,
            current_yield_fadj REAL,

            -- 更新信息
            update_time TEXT -- 脚本更新该记录的时间戳
        )
        ''')
        
        # 创建索引
        cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{DIVIDEND_RATIO_TABLE}_ts_code ON {DIVIDEND_RATIO_TABLE}(ts_code)')
        
        logging.info(f"股息率表 '{DIVIDEND_RATIO_TABLE}' 检查/创建完毕")
        return True
    except sqlite3.Error as e:
        logging.error(f"创建股息率表时出错: {e}")
        return False

def get_stock_list(conn):
    """
    从数据库获取股票列表及基本信息
    
    Args:
        conn: 数据库连接对象
        
    Returns:
        pd.DataFrame: 包含股票代码、符号和上市日期的DataFrame
    """
    try:
        query = f"SELECT ts_code, symbol, list_date FROM {BALL_CODE_TABLE}"
        df = pd.read_sql_query(query, conn)
        logging.info(f"成功获取 {len(df)} 只股票的基本信息")
        return df
    except Exception as e:
        logging.error(f"获取股票列表时出错: {e}")
        return pd.DataFrame()

def get_dividend_data_for_stock(conn, symbol):
    """
    从数据库获取指定股票的分红数据

    Args:
        conn: 数据库连接对象
        symbol: 股票代码 (e.g., '000001')

    Returns:
        pd.DataFrame: 包含指定股票分红数据的DataFrame
    """
    try:
        query = f"SELECT symbol, ex_dividend_date, cash_dividend_ratio FROM {BONUS_DETAILS_TABLE} WHERE symbol = ?"
        df = pd.read_sql_query(query, conn, params=(symbol,))

        # 过滤掉ex_dividend_date为NULL的记录
        null_count = df['ex_dividend_date'].isnull().sum()
        if null_count > 0:
            logging.warning(f"股票 {symbol}: 发现 {null_count} 条分红记录的除权除息日为NULL，这些记录将被忽略")
            df = df.dropna(subset=['ex_dividend_date'])

        logging.debug(f"成功获取股票 {symbol} 的 {len(df)} 条分红记录")
        return df
    except Exception as e:
        logging.error(f"获取股票 {symbol} 的分红数据时出错: {e}")
        return pd.DataFrame()

def get_price_data_for_stock(conn, table_name, ts_code):
    """
    从数据库获取指定股票的股价数据

    Args:
        conn: 数据库连接对象
        table_name: 表名（不复权或前复权）
        ts_code: 股票代码 (e.g., '000001.SZ')

    Returns:
        pd.DataFrame: 包含指定股票股价数据的DataFrame
    """
    try:
        query = f"SELECT ts_code, trade_date, close FROM {table_name} WHERE ts_code = ?"
        df = pd.read_sql_query(query, conn, params=(ts_code,))
        logging.debug(f"成功从 {table_name} 获取股票 {ts_code} 的 {len(df)} 条股价记录")
        return df
    except Exception as e:
        logging.error(f"获取股票 {ts_code} 的股价数据时出错 (表: {table_name}): {e}")
        return pd.DataFrame()

def calculate_annual_dividends(dividend_df):
    """
    计算每只股票每年的总分红
    
    Args:
        dividend_df: 包含分红数据的DataFrame
        
    Returns:
        pd.DataFrame: 包含每只股票每年总分红的DataFrame
    """
    try:
        # 确保ex_dividend_date是字符串类型
        dividend_df['ex_dividend_date'] = dividend_df['ex_dividend_date'].astype(str)
        
        # 提取年份
        dividend_df['year'] = dividend_df['ex_dividend_date'].str[:4].astype(int)
        
        # 将cash_dividend_ratio中的NaN或None替换为0
        dividend_df['cash_dividend_ratio'] = dividend_df['cash_dividend_ratio'].fillna(0)
        
        # 按股票代码和年份分组，计算每年的总分红
        annual_dividends = dividend_df.groupby(['symbol', 'year'])['cash_dividend_ratio'].sum().reset_index()
        
        # 计算每股分红（除以10，因为cash_dividend_ratio是每10股分红）
        annual_dividends['dividend_per_share'] = annual_dividends['cash_dividend_ratio'] / 10
        
        logging.info(f"成功计算 {len(annual_dividends)} 条年度分红记录")
        return annual_dividends
    except Exception as e:
        logging.error(f"计算年度分红时出错: {e}")
        return pd.DataFrame()

def check_listing_time(list_date, start_year):
    """
    检查股票上市时间是否满足计算条件
    
    Args:
        list_date: 上市日期（YYYYMMDD格式）
        start_year: 计算周期的起始年份
        
    Returns:
        bool: 如果上市时间满足条件返回True，否则返回False
    """
    try:
        # 确保list_date是字符串类型
        list_date = str(list_date)
        
        # 如果list_date格式不正确，返回False
        if len(list_date) < 8:
            return False
        
        # 提取上市年份
        list_year = int(list_date[:4])
        
        # 检查上市年份是否早于或等于计算周期的起始年份
        return list_year <= start_year
    except Exception as e:
        logging.warning(f"检查上市时间时出错: {e}")
        return False

def check_price_data_completeness(price_df, ts_code, start_year, end_year, n_years):
    """
    检查股价数据的完整性
    
    Args:
        price_df: 包含股价数据的DataFrame
        ts_code: 股票代码
        start_year: 计算周期的起始年份
        end_year: 计算周期的结束年份
        n_years: 计算周期（年）
        
    Returns:
        bool: 如果数据完整性满足条件返回True，否则返回False
    """
    try:
        # 过滤出指定股票代码和日期范围的数据
        start_date = f"{start_year}0101"
        end_date = f"{end_year}1231"
        filtered_df = price_df[(price_df['ts_code'] == ts_code) & 
                              (price_df['trade_date'] >= start_date) & 
                              (price_df['trade_date'] <= end_date)]
        
        # 计算实际交易日数
        actual_days = len(filtered_df)
        
        # 计算理论交易日数
        theoretical_days = n_years * THEORETICAL_TRADING_DAYS_PER_YEAR
        
        # 计算缺失比例
        if theoretical_days == 0:
            return False
        
        missing_ratio = (theoretical_days - actual_days) / theoretical_days
        
        # 如果缺失比例超过阈值，返回False
        return missing_ratio < MISSING_DATA_THRESHOLD
    except Exception as e:
        logging.warning(f"检查股价数据完整性时出错 (股票: {ts_code}): {e}")
        return False

def calculate_average_price(price_df, ts_code, start_year, end_year):
    """
    计算指定时间范围内的平均股价
    
    Args:
        price_df: 包含股价数据的DataFrame
        ts_code: 股票代码
        start_year: 计算周期的起始年份
        end_year: 计算周期的结束年份
        
    Returns:
        float: 平均股价，如果没有有效数据则返回None
    """
    try:
        # 过滤出指定股票代码和日期范围的数据
        start_date = f"{start_year}0101"
        end_date = f"{end_year}1231"
        filtered_df = price_df[(price_df['ts_code'] == ts_code) & 
                              (price_df['trade_date'] >= start_date) & 
                              (price_df['trade_date'] <= end_date)]
        
        # 过滤出close > 0的记录
        valid_df = filtered_df[filtered_df['close'] > 0]
        
        # 如果没有有效数据，返回None
        if valid_df.empty:
            return None
        
        # 计算平均股价
        avg_price = valid_df['close'].mean()
        
        return avg_price
    except Exception as e:
        logging.warning(f"计算平均股价时出错 (股票: {ts_code}): {e}")
        return None

def calculate_average_dividend(annual_dividends, symbol, start_year, end_year):
    """
    计算指定时间范围内的平均分红
    
    Args:
        annual_dividends: 包含年度分红数据的DataFrame
        symbol: 股票代码（不带后缀）
        start_year: 计算周期的起始年份
        end_year: 计算周期的结束年份
        
    Returns:
        tuple: (平均分红, 分红模式字符串, 分红年数, 连续分红年数)
    """
    try:
        # 创建包含所有年份的序列
        years = list(range(start_year, end_year + 1))
        
        # 初始化分红数据字典，默认为0
        dividend_dict = {year: 0 for year in years}
        
        # 填充实际分红数据
        for _, row in annual_dividends[(annual_dividends['symbol'] == symbol) & 
                                     (annual_dividends['year'] >= start_year) & 
                                     (annual_dividends['year'] <= end_year)].iterrows():
            dividend_dict[row['year']] = row['dividend_per_share']
        
        # 提取分红值列表（按年份排序）
        dividend_values = [dividend_dict[year] for year in sorted(dividend_dict.keys())]
        
        # 计算平均分红
        avg_dividend = sum(dividend_values) / len(dividend_values) if dividend_values else 0
        
        # 生成分红模式字符串（O表示有分红，X表示无分红）
        dividend_pattern = ''.join(['O' if val > 0 else 'X' for val in dividend_values])
        
        # 计算分红年数
        dividend_years_count = dividend_pattern.count('O')
        
        # 计算连续分红年数（从最右边开始）
        consecutive_count = 0
        for char in reversed(dividend_pattern):
            if char == 'O':
                consecutive_count += 1
            else:
                break
        
        return avg_dividend, dividend_pattern, dividend_years_count, consecutive_count
    except Exception as e:
        logging.warning(f"计算平均分红时出错 (股票: {symbol}): {e}")
        return 0, 'X' * (end_year - start_year + 1), 0, 0

def calculate_dividend_yield(avg_dividend, avg_price):
    """
    计算股息率
    
    Args:
        avg_dividend: 平均分红
        avg_price: 平均股价
        
    Returns:
        float: 股息率，如果平均股价无效则返回None
    """
    if avg_price is None or avg_price <= 0:
        return None
    
    return avg_dividend / avg_price

def calculate_current_yield(avg_dividend_n1, latest_close):
    """
    计算现价股息率
    
    Args:
        avg_dividend_n1: 1年平均分红
        latest_close: 最新收盘价
        
    Returns:
        float: 现价股息率，如果最新收盘价无效则返回None
    """
    if avg_dividend_n1 is None or latest_close is None or latest_close <= 0:
        return None
    
    return avg_dividend_n1 / latest_close

def get_latest_close_price(price_df, ts_code):
    """
    获取最新收盘价和日期
    
    Args:
        price_df: 包含股价数据的DataFrame
        ts_code: 股票代码
        
    Returns:
        tuple: (最新收盘价, 最新日期)，如果没有数据则返回(None, None)
    """
    try:
        # 过滤出指定股票代码的数据
        filtered_df = price_df[price_df['ts_code'] == ts_code]
        
        # 如果没有数据，返回None
        if filtered_df.empty:
            return None, None
        
        # 找出最新日期
        latest_date = filtered_df['trade_date'].max()
        
        # 获取最新日期的收盘价
        latest_close = filtered_df[filtered_df['trade_date'] == latest_date]['close'].iloc[0]
        
        return latest_close, latest_date
    except Exception as e:
        logging.warning(f"获取最新收盘价时出错 (股票: {ts_code}): {e}")
        return None, None

def calculate_listing_years(list_date):
    """
    计算上市年限
    
    Args:
        list_date: 上市日期（YYYYMMDD格式）
        
    Returns:
        float: 上市年限，如果上市日期无效则返回None
    """
    try:
        # 确保list_date是字符串类型
        list_date = str(list_date)
        
        # 如果list_date格式不正确，返回None
        if len(list_date) < 8:
            return None
        
        # 转换为datetime对象
        list_datetime = datetime.strptime(list_date, '%Y%m%d')
        
        # 计算上市至今的天数
        days = (datetime.now() - list_datetime).days
        
        # 转换为年
        years = days / 365.25
        
        return years
    except Exception as e:
        logging.warning(f"计算上市年限时出错 (上市日期: {list_date}): {e}")
        return None

def calculate_stock_dividend_ratios(source_conn, stock_df):
    """
    计算所有股票的股息率指标

    Args:
        source_conn: 源数据库连接对象
        stock_df: 包含股票基本信息的DataFrame

    Returns:
        list: 包含所有股票计算结果的字典列表
    """
    # 获取当前年份
    current_year = datetime.now().year

    # 结果列表
    results = []

    # 处理每只股票
    total_stocks = len(stock_df)
    for idx, stock in stock_df.iterrows():
        ts_code = stock['ts_code']
        symbol = stock['symbol']
        list_date = stock['list_date']

        logging.info(f"处理股票 [{idx+1}/{total_stocks}]: {ts_code}")

        # 获取该股票的分红数据
        dividend_df_stock = get_dividend_data_for_stock(source_conn, symbol)
        if dividend_df_stock.empty:
            logging.warning(f"股票 {ts_code} 没有有效的分红数据，跳过计算")
            # 即使没有分红数据，也记录基本信息和状态
            result_no_dividend = {
                'ts_code': ts_code,
                'symbol': symbol,
                'list_date': list_date,
                'listing_years': calculate_listing_years(list_date),
                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            for n_period in PERIODS:
                result_no_dividend[f'status_n{n_period}'] = 'ERR_NO_DIVIDEND_DATA'
            results.append(result_no_dividend)
            continue
        
        annual_dividends = calculate_annual_dividends(dividend_df_stock)

        # 获取该股票的不复权和前复权价格数据
        price_unadj_df_stock = get_price_data_for_stock(source_conn, DAILY_QUOTES_TABLE, ts_code)
        price_fadj_df_stock = get_price_data_for_stock(source_conn, DAILY_QUOTES_TABLE_QFQ, ts_code)

        if price_unadj_df_stock.empty or price_fadj_df_stock.empty:
            logging.warning(f"股票 {ts_code} 缺少价格数据，跳过计算")
            # 记录基本信息和状态
            result_no_price = {
                'ts_code': ts_code,
                'symbol': symbol,
                'list_date': list_date,
                'listing_years': calculate_listing_years(list_date),
                'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            for n_period in PERIODS:
                 result_no_price[f'status_n{n_period}'] = 'ERR_NO_PRICE_DATA'
            results.append(result_no_price)
            continue

        # 初始化结果字典
        result = {
            'ts_code': ts_code,
            'symbol': symbol,
            'list_date': list_date,
            'listing_years': calculate_listing_years(list_date),
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        # 获取最新收盘价和日期
        latest_close_unadj, latest_close_date_unadj = get_latest_close_price(price_unadj_df_stock, ts_code)
        latest_close_fadj, latest_close_date_fadj = get_latest_close_price(price_fadj_df_stock, ts_code)

        result['latest_close_unadj'] = latest_close_unadj
        result['latest_close_fadj'] = latest_close_fadj
        result['latest_close_date'] = latest_close_date_unadj

        avg_dividend_n1 = None

        for n in PERIODS:
            end_year = current_year - 1
            start_year = end_year - n + 1
            status = 'OK'

            if not check_listing_time(list_date, start_year):
                status = 'ERR_LISTING_TIME'
            elif not check_price_data_completeness(price_unadj_df_stock, ts_code, start_year, end_year, n):
                status = 'ERR_PRICE_DATA'
            
            result[f'status_n{n}'] = status

            if status != 'OK':
                continue

            avg_price_unadj = calculate_average_price(price_unadj_df_stock, ts_code, start_year, end_year)
            avg_price_fadj = calculate_average_price(price_fadj_df_stock, ts_code, start_year, end_year)
            
            avg_dividend, dividend_pattern, dividend_years_count, consecutive_dividend_years = \
                calculate_average_dividend(annual_dividends, symbol, start_year, end_year)
            
            dividend_yield_unadj = calculate_dividend_yield(avg_dividend, avg_price_unadj)
            dividend_yield_fadj = calculate_dividend_yield(avg_dividend, avg_price_fadj)

            if n == 1:
                avg_dividend_n1 = avg_dividend

            result[f'avg_price_n{n}_unadj'] = avg_price_unadj
            result[f'avg_price_n{n}_fadj'] = avg_price_fadj
            result[f'avg_dividend_n{n}'] = avg_dividend
            result[f'dividend_yield_n{n}_unadj'] = dividend_yield_unadj
            result[f'dividend_yield_n{n}_fadj'] = dividend_yield_fadj
            result[f'dividend_pattern_n{n}'] = dividend_pattern
            result[f'dividend_years_count_n{n}'] = dividend_years_count
            result[f'consecutive_dividend_years_n{n}'] = consecutive_dividend_years

        result['current_yield_unadj'] = calculate_current_yield(avg_dividend_n1, latest_close_unadj)
        result['current_yield_fadj'] = calculate_current_yield(avg_dividend_n1, latest_close_fadj)

        results.append(result)

    return results

def save_results_to_db(conn, results):
    """
    将计算结果保存到数据库
    
    Args:
        conn: 数据库连接对象
        results: 包含计算结果的字典列表
        
    Returns:
        int: 成功保存的记录数
    """
    if conn is None or not results:
        logging.error("数据库连接无效或没有结果数据，无法保存")
        return 0
    
    try:
        cursor = conn.cursor()
        saved_count = 0
        
        for result in results:
            # 构建列名和占位符
            columns = ', '.join(result.keys())
            placeholders = ', '.join(['?' for _ in result.keys()])
            
            # 构建SQL语句
            sql = f"INSERT OR REPLACE INTO {DIVIDEND_RATIO_TABLE} ({columns}) VALUES ({placeholders})"
            
            # 执行SQL
            cursor.execute(sql, list(result.values()))
            saved_count += 1
        
        # 提交事务
        conn.commit()
        
        logging.info(f"成功保存 {saved_count} 条记录到 {DIVIDEND_RATIO_TABLE} 表")
        return saved_count
    except sqlite3.Error as e:
        logging.error(f"保存结果到数据库时出错: {e}")
        conn.rollback()
        return 0

def main():
    """
    主函数
    """
    logging.info("=== 股票股息自动计算工具 ===")
    start_time = time.time()
    
    # 连接源数据库
    source_conn = connect_db(SOURCE_DB_PATH)
    if source_conn is None:
        return
    
    # 连接目标数据库
    target_conn = connect_db(TARGET_DB_PATH)
    if target_conn is None:
        source_conn.close()
        return
    
    # 创建股息率表
    if not create_dividend_ratio_table(target_conn):
        source_conn.close()
        target_conn.close()
        return
    
    # 获取股票列表
    stock_df = get_stock_list(source_conn)
    if stock_df.empty:
        logging.error("无法获取股票列表，程序退出")
        source_conn.close()
        target_conn.close()
        return
    
    # 计算股息率指标
    results = calculate_stock_dividend_ratios(source_conn, stock_df)
    
    # 保存结果到数据库
    saved_count = save_results_to_db(target_conn, results)
    
    # 关闭数据库连接
    source_conn.close()
    target_conn.close()
    
    # 计算总耗时
    elapsed_time = time.time() - start_time
    logging.info(f"处理完成，共计算 {len(results)} 只股票的股息率指标，成功保存 {saved_count} 条记录")
    logging.info(f"总耗时: {elapsed_time:.2f} 秒 ({elapsed_time/60:.2f} 分钟)")

if __name__ == "__main__":
    main()