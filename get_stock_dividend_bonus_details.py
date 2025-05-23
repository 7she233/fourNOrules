"""
获取股票分红配送详情 (来自东方财富) 并存储到数据库。
执行时间1h左右
功能：
1. 从数据库 ball_code_data 表获取股票代码列表 (ts_code, 作为 symbol 使用)。
2. 使用 akshare.stock_fhps_detail_em 接口获取每个股票的分红配送历史数据。
3. 支持并发获取数据，并进行适当的速率控制和错误处理。
4. 将获取到的数据存储到 stock_data.db 数据库的 akshare_bonus_details 表中。
5. 配置项通过 .env 文件管理。
"""
import akshare as ak
import sqlite3
import pandas as pd
from datetime import datetime
import time
import os
import random
from dotenv import load_dotenv
import concurrent.futures
import threading
from collections import deque
import logging

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 加载 .env 文件中的环境变量
load_dotenv()

# ------------------- 配置区 (从 .env 文件加载) -------------------
DB_PATH = os.getenv('DB_PATH', 'stock_data.db')
BONUS_DETAILS_TABLE = os.getenv('BONUS_DETAILS_TABLE', 'akshare_bonus_details')
BALL_CODE_TABLE = os.getenv('BALL_CODE_TABLE', 'ball_code_data')

# API调用与并发配置
API_CALL_INTERVAL = float(os.getenv('AKSHARE_API_CALL_INTERVAL', 0.3)) # 基础API调用间隔（秒），增加间隔以降低被封风险
MAX_RETRIES = int(os.getenv('AKSHARE_MAX_RETRIES', 3))                 # 最大重试次数
BASE_BACKOFF_TIME = float(os.getenv('AKSHARE_BASE_BACKOFF_TIME', 2.0)) # 基础退避时间（秒）
MAX_BACKOFF_TIME = float(os.getenv('AKSHARE_MAX_BACKOFF_TIME', 60.0))  # 最大退避时间（秒）
FAILED_SYMBOLS_LOG = os.getenv('AKSHARE_FAILED_SYMBOLS_LOG', 'akshare_failed_symbols.log') # 失败代码日志文件
MAX_CONCURRENT_WORKERS = int(os.getenv('AKSHARE_MAX_CONCURRENT_WORKERS', 3)) # 最大并发工作线程数，保守设置

# --- 锁和信号量 ---
log_lock = threading.Lock()
db_lock = threading.Lock()
concurrency_semaphore = threading.Semaphore(MAX_CONCURRENT_WORKERS)

# ------------------- 列名映射 -------------------
# Akshare 返回的列名到我们数据库表列名的映射
COLUMN_MAPPING = {
    'symbol': 'symbol', # 我们会自己添加这一列
    '报告期': 'report_date',
    '业绩披露日期': 'disclosure_date',
    '送转股份-送转总比例': 'bonus_transfer_total_ratio',
    '送转股份-送股比例': 'bonus_share_ratio',
    '送转股份-转股比例': 'transfer_share_ratio',
    '现金分红-现金分红比例': 'cash_dividend_ratio',
    '现金分红-现金分红比例描述': 'cash_dividend_desc',
    '现金分红-股息率': 'dividend_yield_ratio',
    '每股收益': 'eps',
    '每股净资产': 'bps',
    '每股公积金': 'retained_earnings_ps',
    '每股未分配利润': 'undistributed_profit_ps',
    '净利润同比增长': 'net_profit_growth_rate',
    '总股本': 'total_shares',
    '预案公告日': 'plan_announcement_date',
    '股权登记日': 'equity_registration_date',
    '除权除息日': 'ex_dividend_date',
    '方案进度': 'plan_progress',
    '最新公告日期': 'latest_announcement_date'
}

def connect_db(db_path):
    """连接到 SQLite 数据库"""
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False, timeout=10) # 增加timeout
        logging.info(f"成功连接到数据库: {db_path}")
        return conn
    except sqlite3.Error as e:
        logging.error(f"连接数据库 {db_path} 失败: {e}")
        return None

def create_bonus_details_table(conn, table_name):
    """如果分红配送详情表不存在，则创建该表"""
    if conn is None:
        logging.error(f"数据库连接无效，无法创建表 '{table_name}'")
        return False
    try:
        with db_lock: # 保护 DDL 操作
            with conn:
                cursor = conn.cursor()
                cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    symbol TEXT NOT NULL,
                    report_date TEXT NOT NULL,
                    disclosure_date TEXT,
                    bonus_transfer_total_ratio REAL,
                    bonus_share_ratio REAL,
                    transfer_share_ratio REAL,
                    cash_dividend_ratio REAL,
                    cash_dividend_desc TEXT,
                    dividend_yield_ratio REAL,
                    eps REAL,
                    bps REAL,
                    retained_earnings_ps REAL,
                    undistributed_profit_ps REAL,
                    net_profit_growth_rate REAL,
                    total_shares INTEGER,
                    plan_announcement_date TEXT, 
                    equity_registration_date TEXT,
                    ex_dividend_date TEXT, 
                    plan_progress TEXT,
                    latest_announcement_date TEXT,
                    PRIMARY KEY (symbol, report_date)
                )
                """)
                # 考虑为经常查询的列添加索引，例如 symbol, ex_dividend_date
                cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_symbol ON {table_name}(symbol)')
                cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_report_date ON {table_name}(report_date)')
        logging.info(f"数据表 '{table_name}' 检查/创建完毕")
        return True
    except sqlite3.Error as e:
        logging.error(f"创建或检查表 '{table_name}' 时出错: {e}")
        return False

def get_stock_codes_from_db(conn, stock_code_table_name):
    """从数据库 ball_code_data 表获取股票代码列表 (ts_code)，并转换为 akshare symbol 格式"""
    if conn is None:
        logging.error("数据库连接无效，无法获取股票代码。")
        return []
    symbols = []
    try:
        with conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT DISTINCT ts_code FROM {stock_code_table_name}")
            ts_codes = [row[0] for row in cursor.fetchall()]
            for ts_code in ts_codes:
                if '.' in ts_code:
                    symbols.append(ts_code.split('.')[0]) # 例如 300073.SZ -> 300073
                else:
                    symbols.append(ts_code) # 如果已经是纯数字代码
            logging.info(f"从 '{stock_code_table_name}' 表成功获取 {len(symbols)} 个唯一股票代码 (已转换为symbol格式)")
            return symbols
    except sqlite3.Error as e:
        logging.error(f"从数据库表 '{stock_code_table_name}' 获取股票代码时出错: {e}")
        return []

def exponential_backoff(retry_count):
    """计算指数退避等待时间"""
    wait_time = min(MAX_BACKOFF_TIME, BASE_BACKOFF_TIME * (2 ** retry_count))
    jitter = wait_time * 0.2 * (random.random() - 0.5) * 2
    return max(0, wait_time + jitter)

def log_failed_symbol(symbol, error_msg):
    """记录失败的股票代码到日志文件 (线程安全)"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] 代码失败: {symbol} | 错误: {error_msg}\n"
    try:
        with log_lock:
            with open(FAILED_SYMBOLS_LOG, 'a', encoding='utf-8') as f:
                f.write(log_entry)
        logging.warning(f"已记录失败代码 {symbol} 到 {FAILED_SYMBOLS_LOG}")
    except IOError as e:
        logging.error(f"写入失败日志文件 {FAILED_SYMBOLS_LOG} 时出错: {e}")

def fetch_dividend_bonus_for_symbol(symbol_code):
    """获取单个股票代码的分红配送详情 (线程池任务)"""
    thread_id = threading.get_ident()
    logging.info(f"[线程 {thread_id}] 开始处理代码: {symbol_code}")
    retry_count = 0

    with concurrency_semaphore:
        while retry_count <= MAX_RETRIES:
            try:
                if retry_count > 0:
                    logging.info(f"[线程 {thread_id}][{symbol_code}] 第 {retry_count} 次重试获取数据...")
                
                time.sleep(API_CALL_INTERVAL * (1 + random.uniform(-0.1, 0.1))) # 增加随机性的小幅抖动
                
                start_api_time = time.time()
                df = ak.stock_fhps_detail_em(symbol=symbol_code)
                api_fetch_time = time.time() - start_api_time

                if df is not None and not df.empty:
                    logging.info(f"[线程 {thread_id}][{symbol_code}] ✓ 成功获取 {len(df)} 条分红配送数据 (API耗时 {api_fetch_time:.2f}s)。")
                    df['symbol'] = symbol_code # 添加 symbol 列
                    df.rename(columns=COLUMN_MAPPING, inplace=True) # 重命名列以匹配数据库
                    
                    # 确保所有目标列都存在，若不存在则填充None
                    for target_col in COLUMN_MAPPING.values():
                        if target_col not in df.columns:
                            df[target_col] = None
                    return df
                elif df is not None and df.empty:
                    logging.info(f"[线程 {thread_id}][{symbol_code}] ✓ 查询成功，但该股票无分红配送数据 (API耗时 {api_fetch_time:.2f}s)。")
                    return pd.DataFrame() # 返回空DataFrame，表示无数据但成功
                else: # df is None
                    error_msg = "Akshare API 返回 None"
                    logging.warning(f"[线程 {thread_id}][{symbol_code}] ⚠ {error_msg} (API耗时 {api_fetch_time:.2f}s)")
                    # (处理逻辑与Exception一致，会进入重试)
                    raise ValueError(error_msg) # 触发重试逻辑

            except Exception as e:
                error_msg = str(e)
                logging.warning(f"[线程 {thread_id}][{symbol_code}] ✗ 获取数据时发生异常: {error_msg}")
                retry_count += 1
                if retry_count <= MAX_RETRIES:
                    backoff = exponential_backoff(retry_count)
                    logging.info(f"[线程 {thread_id}][{symbol_code}] 将在 {backoff:.2f}秒 后重试 ({retry_count}/{MAX_RETRIES})...")
                    time.sleep(backoff)
                else:
                    logging.error(f"[线程 {thread_id}][{symbol_code}] ✗ 达到最大重试次数 {MAX_RETRIES}，放弃此代码。错误: {error_msg}")
                    log_failed_symbol(symbol_code, error_msg)
                    return None # 明确返回 None 表示最终失败
        
        # 如果循环结束仍未成功 (理论上应该在循环内返回或记录失败)
        logging.error(f"[线程 {thread_id}][{symbol_code}] ✗ 未知原因导致获取失败 (超出重试次数后)")
        log_failed_symbol(symbol_code, "未知原因导致获取失败 (超出重试次数后)")
        return None

def save_data_to_db(conn, df_list, table_name):
    """将 DataFrame 列表中的数据保存到 SQLite 数据库 (线程安全, 使用 INSERT OR REPLACE)"""
    if conn is None or not df_list:
        logging.info(f"无数据或数据库连接无效，不执行保存到 '{table_name}'。")
        return 0

    total_rows_affected = 0
    combined_df = pd.concat(df_list, ignore_index=True)
    if combined_df.empty:
        logging.info(f"合并后的DataFrame为空，不执行保存到 '{table_name}'。")
        return 0

    try:
        with db_lock:
            with conn:
                cursor = conn.cursor()
                # 获取表结构中的列名以确保顺序和存在性
                cursor.execute(f"PRAGMA table_info({table_name})")
                table_columns_info = cursor.fetchall()
                db_table_columns = [info[1] for info in table_columns_info]

                # 筛选DataFrame，只保留表中存在的列，并按表顺序排列
                df_to_save = pd.DataFrame(columns=db_table_columns)
                for col in db_table_columns:
                    if col in combined_df.columns:
                        df_to_save[col] = combined_df[col]
                    else:
                        df_to_save[col] = None # 如果DataFrame缺少某列，则填充None
                
                # 数据类型转换和清洗
                date_cols_to_format = [
                    'report_date', 'disclosure_date', 'plan_announcement_date',
                    'equity_registration_date', 'ex_dividend_date', 'latest_announcement_date'
                ]
                for col in date_cols_to_format:
                    if col in df_to_save.columns:
                        df_to_save[col] = pd.to_datetime(df_to_save[col], errors='coerce').dt.strftime('%Y-%m-%d')
                        # 对于主键中的日期列，如果转换后为NaT (变成None字符串)，SQLite会将其视为NULL
                        # 这对于 PRIMARY KEY (symbol, ex_dividend_date, plan_announcement_date) 是可接受的
                        # 因为SQLite允许多个NULL值组合作为主键的一部分，只要组合是唯一的
                        # 但最好确保关键日期如 ex_dividend_date, plan_announcement_date 有效
                        df_to_save[col] = df_to_save[col].replace({pd.NaT: None, 'NaT': None})
                
                if 'total_shares' in df_to_save.columns:
                    df_to_save['total_shares'] = pd.to_numeric(df_to_save['total_shares'], errors='coerce').fillna(0).astype('int64')
                
                # 准备数据进行 executemany
                tuples_to_insert = [tuple(x) for x in df_to_save.to_numpy()]
                if not tuples_to_insert:
                    logging.info("没有有效数据行可供插入。")
                    return 0
                
                cols_placeholder = ','.join(db_table_columns)
                values_placeholder = ','.join(['?'] * len(db_table_columns))
                sql = f"INSERT OR REPLACE INTO {table_name} ({cols_placeholder}) VALUES ({values_placeholder})"
                
                cursor.executemany(sql, tuples_to_insert)
                total_rows_affected = cursor.rowcount if cursor.rowcount != -1 else len(tuples_to_insert) # rowcount behavior varies
                conn.commit() # 确保提交
                logging.info(f"成功向 '{table_name}' 插入/替换 {total_rows_affected} 行数据。")

    except sqlite3.Error as e:
        logging.error(f"保存数据到表 '{table_name}' 时发生数据库错误: {e} (类型: {type(e)})")
        # conn.rollback() # with conn 会自动处理回滚
        return -1 # 表示错误
    except Exception as e:
        logging.error(f"保存数据到表 '{table_name}' 时发生未知错误: {e}")
        return -1
    return total_rows_affected

def process_symbols_concurrently(symbols_to_fetch):
    """并发获取所有指定股票代码的分红配送数据"""
    all_data_dfs = []
    failed_symbols_count = 0
    successful_symbols_count = 0
    total_symbols = len(symbols_to_fetch)

    logging.info(f"开始并发处理 {total_symbols} 个股票代码...")
    fetch_start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_WORKERS) as executor:
        future_to_symbol = {executor.submit(fetch_dividend_bonus_for_symbol, symbol): symbol for symbol in symbols_to_fetch}
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_symbol)):
            symbol = future_to_symbol[future]
            try:
                data_df = future.result()
                if data_df is not None and not data_df.empty:
                    all_data_dfs.append(data_df)
                    successful_symbols_count += 1
                elif data_df is not None and data_df.empty: # 无数据也算成功获取
                    successful_symbols_count += 1
                else: # data_df is None (获取失败)
                    failed_symbols_count += 1
            except Exception as exc:
                logging.error(f"处理股票 {symbol} 的future结果时产生异常: {exc}")
                log_failed_symbol(symbol, str(exc))
                failed_symbols_count += 1
            
            if (i + 1) % 50 == 0 or (i + 1) == total_symbols: # 每处理50个或最后一个时打印进度
                logging.info(f"进度: {i+1}/{total_symbols} | 成功: {successful_symbols_count} | 失败: {failed_symbols_count}")

    fetch_elapsed_time = time.time() - fetch_start_time
    logging.info(f"所有股票数据获取完成，耗时: {fetch_elapsed_time:.2f} 秒。成功: {successful_symbols_count}, 失败: {failed_symbols_count}")
    
    if not all_data_dfs:
        logging.info("未获取到任何有效的分红配送数据。")
        return pd.DataFrame() # 返回空DataFrame
    
    # return pd.concat(all_data_dfs, ignore_index=True) # 改为返回列表，由save_data_to_db合并
    return all_data_dfs

def main():
    """主函数"""
    logging.info(f"=== Akshare股票分红配送详情获取工具 (表: {BONUS_DETAILS_TABLE}) ===")
    start_overall_time = time.time()

    # 1. 连接数据库
    conn = connect_db(DB_PATH)
    if conn is None:
        return

    # 2. 创建目标数据表 (如果不存在)
    if not create_bonus_details_table(conn, BONUS_DETAILS_TABLE):
        conn.close()
        return

    # 3. 从 ball_code_data 表获取股票代码 (已转换为 symbol 格式)
    symbols_to_fetch = get_stock_codes_from_db(conn, BALL_CODE_TABLE)
    if not symbols_to_fetch:
        logging.info("数据库中没有找到需要处理的股票代码，程序退出。")
        conn.close()
        return
    
    logging.info(f"将从Akshare获取 {len(symbols_to_fetch)} 个股票的分红配送数据。")

    # 4. 并发获取数据
    # 分批处理，每批 N 个代码，批次间可以有等待，进一步降低请求频率
    BATCH_SIZE = int(os.getenv('AKSHARE_BATCH_SIZE', 100)) # 每批处理100个代码
    WAIT_SECONDS_BETWEEN_BATCHES = int(os.getenv('AKSHARE_WAIT_BETWEEN_BATCHES', 5)) # 每批处理后等待秒数
    
    num_batches = (len(symbols_to_fetch) + BATCH_SIZE - 1) // BATCH_SIZE
    logging.info(f"将股票代码分为 {num_batches} 批进行处理，每批最多 {BATCH_SIZE} 个。批间等待 {WAIT_SECONDS_BETWEEN_BATCHES} 秒。")
    
    all_fetched_data_dfs = [] # 存储所有批次获取到的DataFrame列表
    total_successful_symbols_overall = 0
    total_failed_symbols_overall = 0

    for i in range(num_batches):
        batch_start_index = i * BATCH_SIZE
        batch_end_index = min((i + 1) * BATCH_SIZE, len(symbols_to_fetch))
        current_batch_symbols = symbols_to_fetch[batch_start_index:batch_end_index]

        logging.info(f"--- 开始处理批次 {i+1}/{num_batches} ({len(current_batch_symbols)} 个代码) ---")
        
        batch_data_dfs = process_symbols_concurrently(current_batch_symbols)
        
        if batch_data_dfs: # process_symbols_concurrently 返回的是 list of dfs
            all_fetched_data_dfs.extend(batch_data_dfs)
            # 统计该批次的成功失败 (注意: process_symbols_concurrently 内部已打印)
            # 这里可以根据返回的df列表长度和None的数量来估算，但更准确的是依赖其内部日志
        
        logging.info(f"批次 {i+1}/{num_batches} 数据获取阶段完成。")

        if i < num_batches - 1:
            logging.info(f"等待 {WAIT_SECONDS_BETWEEN_BATCHES} 秒后开始下一批... ({datetime.now().strftime('%H:%M:%S')})")
            time.sleep(WAIT_SECONDS_BETWEEN_BATCHES)

    # 5. 保存数据到数据库
    if all_fetched_data_dfs:
        logging.info(f"总共获取到 {len(all_fetched_data_dfs)} 个DataFrame，准备合并并保存到数据库 '{BONUS_DETAILS_TABLE}'...")
        save_start_time = time.time()
        rows_affected = save_data_to_db(conn, all_fetched_data_dfs, BONUS_DETAILS_TABLE)
        save_elapsed_time = time.time() - save_start_time
        if rows_affected >= 0:
            logging.info(f"数据成功保存到 '{BONUS_DETAILS_TABLE}'，共影响 {rows_affected} 行，耗时: {save_elapsed_time:.2f} 秒。")
        else:
            logging.error("数据保存过程中发生错误。")
    else:
        logging.info("未获取到任何可保存的分红配送数据。")

    # 总结
    overall_elapsed_time = time.time() - start_overall_time
    logging.info(f"--- 任务完成 ---")
    logging.info(f"处理股票总数: {len(symbols_to_fetch)}")
    # 成功和失败的统计在 process_symbols_concurrently 中更细致，这里可以不重复
    logging.info(f"总耗时: {overall_elapsed_time:.2f} 秒")

    failed_log_exists = os.path.exists(FAILED_SYMBOLS_LOG) and os.path.getsize(FAILED_SYMBOLS_LOG) > 0
    if failed_log_exists:
        logging.warning(f"注意：有失败记录，请检查日志文件 {FAILED_SYMBOLS_LOG}")
    else:
        logging.info("所有股票代码处理完毕，无失败记录 (或日志文件为空)。")

    conn.close()
    logging.info("数据库连接已关闭。")

if __name__ == '__main__':
    # 检查 .env 文件是否存在，如果不存在则提示
    if not os.path.exists('.env'):
        logging.warning("警告：.env 文件不存在。将使用默认配置。")
        logging.warning("请确保 DB_PATH 等配置正确，或创建 .env 文件。")
        sample_env_content = """# 数据库文件路径
DB_PATH=stock_data.db

# Akshare 相关表名
BONUS_DETAILS_TABLE=akshare_bonus_details
BALL_CODE_TABLE=ball_code_data

# API 调用与并发配置 (根据需要调整)
AKSHARE_API_CALL_INTERVAL=0.3
AKSHARE_MAX_RETRIES=3
AKSHARE_BASE_BACKOFF_TIME=2.0
AKSHARE_MAX_BACKOFF_TIME=60.0
AKSHARE_FAILED_SYMBOLS_LOG=akshare_failed_symbols.log
AKSHARE_MAX_CONCURRENT_WORKERS=3 # 初始建议值，可根据网络和API情况调整
AKSHARE_BATCH_SIZE=100
AKSHARE_WAIT_BETWEEN_BATCHES=5
"""
        logging.info("\n示例 .env 文件内容：")
        print(sample_env_content) # 直接打印到控制台
    main()