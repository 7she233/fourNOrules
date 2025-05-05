"""获取bonus信息
1. 通过stock_data.db中的表ball_code_data获取stock_code
2. 通过api获取bonus信息，剔除ashare_ex_dividend_date为None的数据
3. 存入stock_data.db中的表dividend_history
4. 通过llm提取bonus信息，得到具体的bonus信息

数据更新规则：
1. 每天更新一次
2. 根据ts_code和ex_divident_date进行数据更新

数据使用规则
plan_status = "实施方案"，参与计算
"""

"""数据库表结构
CREATE TABLE IF NOT EXISTS dividend_history (
    ts_code TEXT NOT NULL,                      -- 股票代码 (需要您在存储时加入，API响应本身不包含)
    dividend_year TEXT,                         -- 分红年度/期间描述 (来自API的 'dividend_year')
    ex_dividend_date TEXT NOT NULL,             -- 除权除息日 (YYYYMMDD格式, 从 'ashare_ex_dividend_date' 转换)
    equity_date TEXT,                           -- 股权登记日 (YYYYMMDD格式, 从 'equity_date' 转换)
    plan_explain TEXT,                          -- 分红方案说明原文 (来自API的 'plan_explain')
    cash_div_pre_tax REAL DEFAULT 0,            -- 每10股税前现金分红(元) (从 'plan_explain' 解析)
    stock_div REAL DEFAULT 0,                   -- 每10股转增股数 (从 'plan_explain' 解析)
    plan_status TEXT,                           -- 方案状态 ('预案', '实施方案', 从 'plan_explain' 解析)
    is_cancelled INTEGER DEFAULT 0,             -- 是否取消 (1表示是, 0表示否, 基于 'cancle_dividend_date' 判断)
    parse_status INTEGER DEFAULT 0,             -- LLM解析状态 (0:未解析, 1:已解析成功, -1:解析失败)
    api_update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 数据入库时间
    PRIMARY KEY (ts_code, ex_dividend_date)     -- 联合主键，确保同一股票同一除权日只有一条记录
);
"""

import pysnowball as ball
import os
import sqlite3
import pandas as pd
import time
import logging
import random
from datetime import datetime
from dotenv import load_dotenv
from collections import deque
import threading
import concurrent.futures

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("stock_bonus.log"),
        logging.StreamHandler()
    ]
)

# 获取环境变量
SNOWBALL_TOKEN = os.getenv('SNOWBALL_TOKEN')
DB_PATH = os.getenv('DB_PATH', 'stock_data.db')
BALL_CODE_TABLE = os.getenv('BALL_CODE_TABLE', 'ball_code_data')
DIVIDEND_HISTORY_TABLE = os.getenv('DIVIDEND_HISTORY_TABLE', 'dividend_history')

# API调用限制配置
API_MINUTE_LIMIT = int(os.getenv('API_MINUTE_LIMIT', 30))  # 每分钟API调用次数限制
API_CALL_INTERVAL = float(os.getenv('API_CALL_INTERVAL', 0.5))  # 基础API调用间隔（秒）
MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))  # 最大重试次数
BONUS_SIZE = int(os.getenv('BONUS_SIZE', 25))  # 获取分红记录的数量
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 1))  # 并行处理的最大线程数
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 10))  # 每批处理的股票数量
SKIP_PARSED = os.getenv('SKIP_PARSED', 'True').lower() in ('true', '1', 't')  # 是否跳过已解析的记录

# 用于速率限制和并发控制的全局变量
api_call_timestamps = deque()
rate_limit_lock = threading.Lock()
concurrency_semaphore = threading.Semaphore(MAX_WORKERS)  # 控制并发数量


def connect_db(db_path):
    """连接到SQLite数据库"""
    conn = sqlite3.connect(db_path)
    logging.info(f"成功连接到数据库: {db_path}")
    return conn


def create_table_if_not_exists(conn):
    """创建dividend_history表（如果不存在）"""
    cursor = conn.cursor()
    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {DIVIDEND_HISTORY_TABLE} (
        ts_code TEXT NOT NULL,
        dividend_year TEXT,
        ex_dividend_date TEXT NOT NULL,
        equity_date TEXT,
        plan_explain TEXT,
        cash_div_pre_tax REAL DEFAULT 0,
        stock_div REAL DEFAULT 0,
        plan_status TEXT,
        is_cancelled INTEGER DEFAULT 0,
        parse_status INTEGER DEFAULT 0,
        api_update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (ts_code, ex_dividend_date)
    )
    ''')
    conn.commit()
    logging.info(f"已确保{DIVIDEND_HISTORY_TABLE}表存在")
    
    # 检查表中是否已存在parse_status字段，如果不存在则添加
    try:
        cursor.execute(f"PRAGMA table_info({DIVIDEND_HISTORY_TABLE})")
        columns = [column[1] for column in cursor.fetchall()]
        if 'parse_status' not in columns:
            logging.info(f"向{DIVIDEND_HISTORY_TABLE}表添加parse_status字段")
            cursor.execute(f"ALTER TABLE {DIVIDEND_HISTORY_TABLE} ADD COLUMN parse_status INTEGER DEFAULT 0")
            conn.commit()
    except Exception as e:
        logging.error(f"检查或添加parse_status字段时出错: {e}")



def get_stock_codes_from_db():
    """从数据库获取股票代码信息"""
    try:
        conn = connect_db(DB_PATH)
        query = f"SELECT ts_code, ball_code FROM {BALL_CODE_TABLE}"
        df = pd.read_sql_query(query, conn)
        conn.close()
        logging.info(f"成功从数据库获取{len(df)}只股票的代码信息")
        return df
    except Exception as e:
        logging.error(f"获取股票代码时出错: {e}")
        return pd.DataFrame()


def exponential_backoff(retry_count):
    """计算指数退避等待时间
    
    Args:
        retry_count (int): 当前重试次数
        
    Returns:
        float: 应等待的秒数
    """
    # 基础等待时间为API_CALL_INTERVAL，每次重试翻倍，并添加一些随机性
    wait_time = API_CALL_INTERVAL * (2 ** retry_count) * (0.5 + random.random())
    # 设置上限，避免等待时间过长
    return min(wait_time, 60.0)


def check_rate_limit():
    """检查并等待以符合API每分钟调用限制

    Returns:
        tuple: (bool, float) - 是否可以继续调用, 需要等待的时间(秒)
    """
    with rate_limit_lock:
        now = time.time()
        # 移除一分钟之前的旧时间戳
        while api_call_timestamps and api_call_timestamps[0] < now - 60:
            api_call_timestamps.popleft()

        if len(api_call_timestamps) >= API_MINUTE_LIMIT:
            wait_time = 60 - (now - api_call_timestamps[0]) + 0.1  # 加一点缓冲
            logging.info(f"[速率限制] 已达到 {API_MINUTE_LIMIT} 次/分钟限制，需等待 {wait_time:.2f} 秒...")
            return False, wait_time
        else:
            return True, 0


def get_bonus_data(ball_code, size=None, retries=0):
    """获取股票分红数据

    Args:
        ball_code (str): 雪球格式的股票代码
        size (int, optional): 获取的记录数量. 默认为None，将使用环境变量BONUS_SIZE.
        retries (int, optional): 当前重试次数. 默认为0.

    Returns:
        dict: 分红数据
    """
    # 如果未指定size，则使用环境变量中的BONUS_SIZE
    if size is None:
        size = BONUS_SIZE
        
    if retries >= MAX_RETRIES:
        logging.error(f"获取{ball_code}的分红数据失败，已达到最大重试次数")
        return None

    thread_id = threading.get_ident()
    
    # 使用信号量控制并发数量
    with concurrency_semaphore:
        try:
            # 检查API调用限制
            can_proceed, wait_time = check_rate_limit()
            if not can_proceed:
                logging.info(f"[线程 {thread_id}] 等待API速率限制，将在 {wait_time:.2f}秒 后继续")
                time.sleep(wait_time)

            # 记录API调用时间
            with rate_limit_lock:
                api_call_timestamps.append(time.time())

            # 调用API获取分红数据
            start_time = time.time()
            bonus_data = ball.bonus(ball_code, size=size)
            api_time = time.time() - start_time
            
            logging.debug(f"[线程 {thread_id}] API响应时间: {api_time:.2f}秒")
            time.sleep(API_CALL_INTERVAL)  # 基础间隔，避免过快调用
            
            return bonus_data
        except Exception as e:
            logging.warning(f"[线程 {thread_id}] 获取{ball_code}的分红数据时出错: {e}，尝试重试({retries+1}/{MAX_RETRIES})")
            backoff_time = exponential_backoff(retries)  # 使用指数退避函数
            logging.info(f"[线程 {thread_id}] 将在 {backoff_time:.2f}秒 后重试...")
            time.sleep(backoff_time)
            return get_bonus_data(ball_code, size, retries + 1)


def format_date(date_str):
    """将API返回的日期格式化为YYYYMMDD格式

    Args:
        date_str (str): 原始日期字符串，可能为None

    Returns:
        str: 格式化后的日期，如果输入为None则返回None
    """
    if not date_str:
        return None
    
    try:
        # 尝试解析日期并格式化
        # 检查输入类型，兼容字符串和整数时间戳（毫秒）
        if isinstance(date_str, int):
            try:
                # 假设是毫秒时间戳，转换为秒
                timestamp_sec = date_str / 1000
                date_obj = datetime.fromtimestamp(timestamp_sec)
                return date_obj.strftime('%Y%m%d')
            except Exception as e:
                logging.warning(f"整数时间戳格式化失败: {date_str}, 错误: {e}")
                return str(date_str) # 返回原始整数的字符串形式
        elif isinstance(date_str, str):
            try:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                return date_obj.strftime('%Y%m%d')
            except ValueError:
                 # 尝试解析另一种常见格式，如果需要
                 try:
                     date_obj = datetime.strptime(date_str, '%Y/%m/%d')
                     return date_obj.strftime('%Y%m%d')
                 except ValueError as e:
                     logging.warning(f"日期字符串格式化失败: {date_str}, 错误: {e}")
                     return date_str # 返回原始字符串
        else:
            logging.warning(f"未知的日期格式: {date_str}, 类型: {type(date_str)}")
            return None # 或返回原始值 str(date_str)

    except Exception as e:
        logging.warning(f"日期格式化过程中发生意外错误: {date_str}, 错误: {e}")
        return None # 或返回原始值 str(date_str)


def process_bonus_data(bonus_data, ts_code):
    """处理分红数据，转换为数据库表格式

    Args:
        bonus_data (dict): API返回的分红数据
        ts_code (str): 股票代码（tushare格式）

    Returns:
        list: 处理后的数据记录列表
    """
    if not bonus_data or 'data' not in bonus_data or 'items' not in bonus_data['data']:
        logging.warning(f"股票{ts_code}没有有效的分红数据")
        return []

    records = []
    for item in bonus_data['data']['items']:
        # 跳过没有除权除息日的记录
        if not item.get('ashare_ex_dividend_date'):
            continue

        # 格式化日期
        ex_dividend_date = format_date(item.get('ashare_ex_dividend_date'))
        equity_date = format_date(item.get('equity_date'))

        # 检查是否取消分红
        is_cancelled = 1 if item.get('cancle_dividend_date') else 0

        record = {
            'ts_code': ts_code,
            'dividend_year': item.get('dividend_year'),
            'ex_dividend_date': ex_dividend_date,
            'equity_date': equity_date,
            'plan_explain': item.get('plan_explain'),
            'cash_div_pre_tax': 0,  # 默认值，后续由LLM解析
            'stock_div': 0,         # 默认值，后续由LLM解析
            'plan_status': '',      # 默认值，后续由LLM解析
            'is_cancelled': is_cancelled
        }
        records.append(record)

    return records


def save_to_database(records, conn):
    """将处理后的记录保存到数据库

    Args:
        records (list): 处理后的数据记录列表
        conn (sqlite3.Connection): 数据库连接

    Returns:
        int: 成功插入或更新的记录数
    """
    if not records:
        return 0

    cursor = conn.cursor()
    count = 0
    skipped = 0

    for record in records:
        try:
            # 如果配置为跳过已解析的记录，则先检查该记录是否已存在且已成功解析
            if SKIP_PARSED:
                cursor.execute(f'''
                SELECT parse_status FROM {DIVIDEND_HISTORY_TABLE}
                WHERE ts_code = ? AND ex_dividend_date = ?
                ''', (record['ts_code'], record['ex_dividend_date']))
                result = cursor.fetchone()
                
                # 如果记录存在且已成功解析(parse_status=1)，则跳过更新
                if result and result[0] == 1:
                    skipped += 1
                    continue
            
            # 使用INSERT OR REPLACE语句处理数据更新，保留parse_status字段
            # 如果是新记录，parse_status将使用默认值0
            # 如果是更新记录，需要保留原有的parse_status值
            cursor.execute(f'''
            INSERT OR REPLACE INTO {DIVIDEND_HISTORY_TABLE} 
            (ts_code, dividend_year, ex_dividend_date, equity_date, plan_explain, 
             cash_div_pre_tax, stock_div, plan_status, is_cancelled, parse_status, api_update_time)
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?,
                COALESCE((SELECT parse_status FROM {DIVIDEND_HISTORY_TABLE} 
                          WHERE ts_code = ? AND ex_dividend_date = ?), 0),
                CURRENT_TIMESTAMP
            )
            ''', (
                record['ts_code'],
                record['dividend_year'],
                record['ex_dividend_date'],
                record['equity_date'],
                record['plan_explain'],
                record['cash_div_pre_tax'],
                record['stock_div'],
                record['plan_status'],
                record['is_cancelled'],
                record['ts_code'],
                record['ex_dividend_date']
            ))
            count += 1
        except Exception as e:
            logging.error(f"保存记录时出错: {e}, 记录: {record}")

    conn.commit()
    if skipped > 0:
        logging.info(f"跳过了{skipped}条已成功解析的记录")
    return count


def process_stock(stock_row):
    """处理单只股票的分红数据

    Args:
        stock_row (pandas.Series): 包含ts_code和ball_code的行

    Returns:
        int: 成功处理的记录数
    """
    ts_code = stock_row['ts_code']
    ball_code = stock_row['ball_code']
    conn = None # 初始化 conn 为 None
    thread_id = threading.get_ident()
    logging.info(f"[线程 {thread_id}] 正在处理股票: {ts_code} (雪球代码: {ball_code})")

    try:
        # 在线程内部创建数据库连接
        conn = connect_db(DB_PATH)

        # 获取分红数据
        bonus_data = get_bonus_data(ball_code)
        if not bonus_data:
            logging.warning(f"[线程 {thread_id}] 无法获取股票{ts_code}的分红数据")
            return 0

        # 处理数据
        records = process_bonus_data(bonus_data, ts_code)
        if not records:
            logging.info(f"[线程 {thread_id}] 股票{ts_code}没有有效的分红记录")
            return 0

        # 保存到数据库
        count = save_to_database(records, conn)
        logging.info(f"[线程 {thread_id}] 股票{ts_code}成功保存{count}条分红记录")

        return count
    except Exception as e:
        logging.error(f"[线程 {thread_id}] 处理股票 {ts_code} 时发生错误: {e}")
        return 0 # 返回0表示处理失败
    finally:
        # 确保数据库连接在使用后被关闭
        if conn:
            conn.close()
            logging.debug(f"[线程 {thread_id}] 关闭数据库连接")


def process_batch(batch_df, batch_num, total_batches):
    """处理一批股票的分红数据

    Args:
        batch_df (pandas.DataFrame): 包含ts_code和ball_code的DataFrame
        batch_num (int): 当前批次编号
        total_batches (int): 总批次数

    Returns:
        tuple: (处理的股票数, 保存的记录数)
    """
    batch_processed = 0
    batch_records = 0

    # 创建线程池
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 提交所有任务到线程池，不再传递 conn
        future_to_stock = {executor.submit(process_stock, row): row for _, row in batch_df.iterrows()}

        # 处理完成的任务结果
        for future in concurrent.futures.as_completed(future_to_stock):
            stock = future_to_stock[future]
            try:
                records_count = future.result()
                if records_count > 0:
                    batch_processed += 1
                    batch_records += records_count
                # 如果 result 返回 0 但没有异常，也算处理了一个股票（即使没有记录保存）
                elif records_count == 0 and not future.exception():
                    batch_processed += 1
            except Exception as exc:
                ts_code = stock['ts_code']
                logging.error(f'处理股票 {ts_code} 时生成异常: {exc}')
            # 可以在这里添加更详细的进度报告，例如每完成N个任务

    logging.info(f"批次 {batch_num}/{total_batches} 完成: 处理了 {batch_processed} 只股票, 保存了 {batch_records} 条记录")
    return batch_processed, batch_records


def main():
    """主函数，执行整个流程"""
    logging.info("开始获取股票分红数据...")
    start_total_time = time.time()

    # 确保数据库和表存在
    try:
        conn_main = connect_db(DB_PATH)
        create_table_if_not_exists(conn_main)
        conn_main.close() # 主线程不再需要保持连接
    except Exception as e:
        logging.error(f"初始化数据库连接或创建表失败: {e}")
        return

    # 获取所有股票代码
    stock_codes_df = get_stock_codes_from_db()
    if stock_codes_df.empty:
        logging.error("未能获取到股票代码，程序终止")
        return

    total_stocks = len(stock_codes_df)
    logging.info(f"总共需要处理 {total_stocks} 只股票")

    total_processed_stocks = 0
    total_saved_records = 0

    # 分批处理
    num_batches = (total_stocks + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(num_batches):
        start_index = i * BATCH_SIZE
        end_index = min((i + 1) * BATCH_SIZE, total_stocks)
        batch_df = stock_codes_df.iloc[start_index:end_index]
        batch_num = i + 1

        logging.info(f"--- 开始处理批次 {batch_num}/{num_batches} ({len(batch_df)}只股票) ---")
        start_batch_time = time.time()

        # 处理当前批次，不再传递 conn
        processed, saved = process_batch(batch_df, batch_num, num_batches)

        total_processed_stocks += processed
        total_saved_records += saved

        end_batch_time = time.time()
        logging.info(f"--- 批次 {batch_num}/{num_batches} 处理完毕，耗时: {end_batch_time - start_batch_time:.2f} 秒 ---")

        # 可以在批次之间添加短暂休眠，进一步降低API压力
        # time.sleep(1)

    end_total_time = time.time()
    logging.info("所有股票分红数据处理完成")
    logging.info(f"总计处理股票数: {total_processed_stocks}/{total_stocks}")
    logging.info(f"总计保存记录数: {total_saved_records}")
    logging.info(f"总耗时: {end_total_time - start_total_time:.2f} 秒")

if __name__ == "__main__":
    # 检查必要的环境变量
    if not SNOWBALL_TOKEN:
        logging.error("错误: 环境变量 SNOWBALL_TOKEN 未设置!")
    else:
        # 设置雪球token
        ball.set_token(f'xq_a_token={SNOWBALL_TOKEN}')
        main()