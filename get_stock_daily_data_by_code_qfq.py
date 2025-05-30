"""
1. 通过指定的 stock_code 获取股票的前复权日线数据。
2. 从数据库 ball_code_data 表获取 stock_code 列表。
3. 从 .env 配置文件获取 start_date 和 end_date。
4. 逐个处理 stock_code，获取指定日期范围内的数据。
5. 将获取到的数据保存到数据库中 (表名由 DAILY_QUOTES_TABLE_QFQ 环境变量指定)。
"""
import tushare as ts
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import random
import argparse # argparse 导入了但未使用，可以考虑移除或按需使用
from dotenv import load_dotenv
import concurrent.futures
import threading
from collections import deque

# 加载 .env 文件中的环境变量
load_dotenv()

# ------------------- 配置区 (从 .env 文件加载) -------------------
TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN', 'YOUR_TUSHARE_TOKEN')
DB_PATH = os.getenv('DB_PATH', 'stock_data.db')
# 使用 _QFQ 后缀的表名环境变量，确保数据存入正确的表
DAILY_QUOTES_TABLE = os.getenv('DAILY_QUOTES_TABLE_QFQ', 'daily_quotes_qfq')
BALL_CODE_TABLE = os.getenv('BALL_CODE_TABLE', 'ball_code_data')

# 日期设置逻辑 (优先使用 .env, 其次使用默认值)
_start_date_env = os.getenv('START_DATE')
DEFAULT_START_DATE = (datetime.now()).strftime('%Y%m%d') # 默认到当前日期
START_DATE = _start_date_env if _start_date_env else DEFAULT_START_DATE

_end_date_env = os.getenv('END_DATE')
DEFAULT_END_DATE = (datetime.now()).strftime('%Y%m%d') # 默认到当前日期
END_DATE = _end_date_env if _end_date_env else DEFAULT_END_DATE

# API调用配置
API_CALL_INTERVAL = float(os.getenv('API_CALL_INTERVAL', 0.45)) # 基础API调用间隔（秒）
MAX_RETRIES = int(os.getenv('MAX_RETRIES', 1))                 # 最大重试次数
BASE_BACKOFF_TIME = float(os.getenv('BASE_BACKOFF_TIME', 1.0)) # 基础退避时间（秒）
MAX_BACKOFF_TIME = float(os.getenv('MAX_BACKOFF_TIME', 60.0))  # 最大退避时间（秒）
FAILED_CODES_LOG = os.getenv('FAILED_CODES_LOG_QFQ', 'failed_codes_qfq.log') # 失败代码日志文件 (使用不同名称避免冲突)
MAX_CONCURRENT_WORKERS = int(os.getenv('MAX_CONCURRENT_WORKERS', 10)) # 最大并发工作线程数
API_MINUTE_LIMIT = int(os.getenv('API_MINUTE_LIMIT', 199)) # 每分钟API调用次数限制 (Tushare pro_bar 接口通常有更严格的限制，例如每分钟200-300次，需查阅最新文档)

# --- 速率限制器和锁全局变量 ---
api_call_timestamps = deque()
rate_limit_lock = threading.Lock()
concurrency_semaphore = threading.Semaphore(MAX_CONCURRENT_WORKERS)
log_lock = threading.Lock() # 用于保护日志文件写入
db_lock = threading.Lock() # 用于保护数据库写入操作
# ------------------------------------------------------------------

def check_rate_limit():
    """检查并等待以符合API每分钟调用限制

    Returns:
        tuple: (bool, float) 第一个元素为 True 表示可以继续调用, False 表示需要等待，第二个元素为需要等待的时间（秒）
    """
    with rate_limit_lock:
        now = time.time()
        # 移除一分钟之前的旧时间戳
        while api_call_timestamps and api_call_timestamps[0] < now - 60:
            api_call_timestamps.popleft()

        if len(api_call_timestamps) >= API_MINUTE_LIMIT:
            wait_time = 60 - (now - api_call_timestamps[0]) + 0.1 # 加一点缓冲
            print(f"[速率限制] 已达到 {API_MINUTE_LIMIT} 次/分钟限制，需等待 {wait_time:.2f} 秒...")
            return False, wait_time
        else:
            return True, 0

def record_api_call():
    """记录一次API调用时间戳"""
    with rate_limit_lock:
        api_call_timestamps.append(time.time())

def initialize_tushare():
    """初始化 Tushare Pro API"""
    if not TUSHARE_TOKEN or TUSHARE_TOKEN == 'YOUR_TUSHARE_TOKEN':
        print("错误：请在 .env 文件中设置有效的 TUSHARE_TOKEN！")
        return None
    ts.set_token(TUSHARE_TOKEN)
    pro = ts.pro_api()
    # print(f"Tushare pro 实例: {pro}, 类型: {type(pro)}") # 调试：打印 pro 实例和类型
    print("Tushare API 初始化成功")
    return pro

def connect_db(db_path):
    """连接到 SQLite 数据库"""
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        print(f"成功连接到数据库: {db_path} (允许多线程访问)")
        return conn
    except sqlite3.Error as e:
        print(f"连接数据库失败: {e}")
        return None

def create_table_if_not_exists(conn, table_name):
    """如果日线行情表不存在，则创建表并添加必要的索引

    Args:
        conn: 数据库连接对象
        table_name (str): 要创建的表名

    Returns:
        bool: 成功返回 True, 失败返回 False
    """
    if conn is None:
        print(f"错误：数据库连接无效，无法创建表 '{table_name}'")
        return False
    try:
        with conn:
            cursor = conn.cursor()
            # pro_bar 返回的字段与 daily 基本一致，此表结构通用
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                ts_code TEXT,
                trade_date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                pre_close REAL,
                `change` REAL,  -- 'change' 是 SQL 关键字，最好用反引号括起来
                pct_chg REAL,
                vol REAL,
                amount REAL,
                PRIMARY KEY (ts_code, trade_date)
            )
            """)
            cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_ts_code ON {table_name}(ts_code)')
            cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date)')
        print(f"数据表 '{table_name}' 检查/创建完毕")
        return True
    except sqlite3.Error as e:
        print(f"创建或检查表 '{table_name}' 时出错: {e}")
        return False

def exponential_backoff(retry_count):
    """计算指数退避等待时间"""
    wait_time = min(MAX_BACKOFF_TIME, BASE_BACKOFF_TIME * (2 ** retry_count))
    jitter = wait_time * 0.2 * (random.random() - 0.5) * 2 # +/- 20% jitter
    return max(0, wait_time + jitter)

def log_failed_code(ts_code, start_date, end_date, error_msg):
    """记录失败的股票代码到日志文件 (线程安全)

    Args:
        ts_code (str): 单个股票代码
        start_date (str): 开始日期
        end_date (str): 结束日期
        error_msg (str): 错误信息
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] 代码失败: {ts_code} | 日期范围: {start_date}-{end_date} | 错误: {error_msg}\n"
    try:
        with log_lock:
            with open(FAILED_CODES_LOG, 'a', encoding='utf-8') as f:
                f.write(log_entry)
        print(f"[线程 {threading.get_ident()}] 已记录失败代码 {ts_code} 到 {FAILED_CODES_LOG}")
    except IOError as e:
        print(f"[线程 {threading.get_ident()}] 写入失败日志文件 {FAILED_CODES_LOG} 时出错: {e}")

def fetch_stock_data_for_code(pro, ts_code, start_date, end_date):
    """获取单个股票代码在指定日期范围内的前复权日线行情数据，带重试和速率限制

    Args:
        pro: Tushare API 实例
        ts_code (str): 股票代码
        start_date (str): 开始日期 YYYYMMDD
        end_date (str): 结束日期 YYYYMMDD

    Returns:
        pd.DataFrame or None: 成功返回数据DataFrame，失败返回None
    """
    thread_id = threading.get_ident()
    print(f"[线程 {thread_id}] 开始处理代码 (前复权): {ts_code} ({start_date}-{end_date})")
    retry_count = 0

    with concurrency_semaphore:
        while retry_count <= MAX_RETRIES:
            can_call, wait_needed = check_rate_limit()
            while not can_call:
                time.sleep(wait_needed)
                can_call, wait_needed = check_rate_limit()

            try:
                if retry_count > 0:
                    print(f"[线程 {thread_id}][{ts_code}] 第 {retry_count} 次重试获取数据...")

                start_time = time.time()
                record_api_call()
                # print(f"[线程 {thread_id}][{ts_code}] 调用 pro.pro_bar 前，pro 实例: {pro}, 类型: {type(pro)}") # 调试
                # --- API 调用修改为 ts.pro_bar 获取前复权数据 (直接调用，不使用 pro_api 参数) ---
                df = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_date, end_date=end_date)
                # ------------------------------------------------------
                api_time = time.time() - start_time

                time.sleep(API_CALL_INTERVAL)

                if df is not None and not df.empty:
                    print(f"[线程 {thread_id}][{ts_code}] ✓ 成功获取 {len(df)} 条前复权数据 (耗时 {api_time:.2f}s)")
                    return df
                elif df is not None and df.empty:
                    print(f"[线程 {thread_id}][{ts_code}] ✓ 查询成功，但日期范围内无前复权数据 (耗时 {api_time:.2f}s)")
                    return pd.DataFrame()
                else:
                    print(f"[线程 {thread_id}][{ts_code}] ⚠ Tushare API (pro_bar) 返回 None (耗时 {api_time:.2f}s)")
                    retry_count += 1
                    if retry_count <= MAX_RETRIES:
                        backoff = exponential_backoff(retry_count)
                        print(f"[线程 {thread_id}][{ts_code}] 将在 {backoff:.2f}秒 后重试...")
                        time.sleep(backoff)
                    else:
                        error_msg = "达到最大重试次数后 Tushare API (pro_bar) 仍返回 None"
                        print(f"[线程 {thread_id}][{ts_code}] ✗ {error_msg}")
                        log_failed_code(ts_code, start_date, end_date, error_msg)
                        return None

            except Exception as e:
                error_msg = str(e)
                print(f"[线程 {thread_id}][{ts_code}] ✗ 获取前复权数据时发生异常: {error_msg}")
                retry_count += 1
                if retry_count <= MAX_RETRIES:
                    backoff = exponential_backoff(retry_count)
                    print(f"[线程 {thread_id}][{ts_code}] 将在 {backoff:.2f}秒 后重试...")
                    time.sleep(backoff)
                else:
                    print(f"[线程 {thread_id}][{ts_code}] ✗ 达到最大重试次数 {MAX_RETRIES}，放弃此代码")
                    log_failed_code(ts_code, start_date, end_date, error_msg)
                    return None

    log_failed_code(ts_code, start_date, end_date, "未知原因导致获取失败 (超出重试范围)")
    return None

def save_data_to_db(conn, df, table_name):
    """将 DataFrame 数据批量保存到 SQLite 数据库 (线程安全)

    Args:
        conn: 数据库连接对象
        df (pd.DataFrame): 包含待保存数据的 DataFrame
        table_name (str): 目标数据库表名

    Returns:
        int: 成功保存的记录数, 错误则返回 -1
    """
    if df is None or df.empty:
        return 0

    affected_rows = 0
    try:
        with db_lock:
            with conn:
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA table_info({table_name})")
                db_columns_info = cursor.fetchall()
                db_columns = [info[1] for info in db_columns_info]

                # 筛选DataFrame中与数据库表匹配的列，并按数据库列顺序排列
                # pro_bar 返回的列名与 daily 基本一致，但最好做一次对齐
                df_to_save = pd.DataFrame()
                missing_cols_in_df = []
                extra_cols_in_df = list(df.columns)

                for col in db_columns:
                    if col in df.columns:
                        df_to_save[col] = df[col]
                        if col in extra_cols_in_df:
                            extra_cols_in_df.remove(col)
                    else:
                        missing_cols_in_df.append(col)
                
                if missing_cols_in_df:
                    print(f"警告: DataFrame中缺少数据库表 '{table_name}' 的列: {missing_cols_in_df}。这些列将不会被填充。")
                if extra_cols_in_df:
                    print(f"警告: DataFrame中包含数据库表 '{table_name}' 未定义的额外列: {extra_cols_in_df}。这些列将被忽略。")

                if df_to_save.empty and not df.empty:
                    print(f"错误: DataFrame列与表 '{table_name}' 列完全不匹配，无法保存。")
                    return -1
                elif df_to_save.empty and df.empty:
                     return 0 # DataFrame 本身为空

                columns_str = ', '.join([f'`{col}`' for col in df_to_save.columns]) # 使用反引号处理可能的关键字列名
                placeholders = ', '.join(['?'] * len(df_to_save.columns))
                sql = f"INSERT OR REPLACE INTO {table_name} ({columns_str}) VALUES ({placeholders})"

                data_to_insert = [tuple(x) for x in df_to_save.to_numpy()]
                cursor.executemany(sql, data_to_insert)
                affected_rows = cursor.rowcount
        return affected_rows

    except sqlite3.OperationalError as e:
        thread_id = threading.get_ident()
        if 'database is locked' in str(e):
            print(f"[线程 {thread_id}] 保存数据到表 '{table_name}' 时遇到数据库锁定错误: {e}。")
        else:
            print(f"[线程 {thread_id}] 保存数据到表 '{table_name}' 时发生数据库操作错误: {e}")
        return -1
    except sqlite3.Error as e:
        thread_id = threading.get_ident()
        print(f"[线程 {thread_id}] 保存数据到表 '{table_name}' 时发生 SQLite 错误: {e}")
        return -1
    except Exception as e:
        thread_id = threading.get_ident()
        print(f"[线程 {thread_id}] 保存数据时发生未知错误: {e}")
        return -1

def get_stock_codes_from_db(conn, table_name):
    """从数据库指定表获取股票代码列表"""
    if conn is None:
        print("错误：数据库连接无效，无法获取股票代码")
        return []
    try:
        with conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT DISTINCT ts_code FROM {table_name}")
            codes = [row[0] for row in cursor.fetchall()]
            print(f"从 '{table_name}' 表成功获取 {len(codes)} 个唯一股票代码")
            return codes
    except sqlite3.Error as e:
        print(f"从数据库表 '{table_name}' 获取股票代码时出错: {e}")
        return []

def get_existing_data_count(conn, table_name):
    """获取数据库表中已有的记录数"""
    if conn is None:
        print("错误：数据库连接无效，无法获取记录数")
        return 0
    try:
        with conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            return count
    except sqlite3.Error as e:
        print(f"获取表 '{table_name}' 记录数时出错: {e}")
        return 0

def process_stock_data_concurrently(pro, codes, start_date, end_date):
    """使用线程池并发获取多个股票代码的日线数据

    Args:
        pro: Tushare API 实例
        codes (list): 股票代码列表
        start_date (str): 开始日期 YYYYMMDD
        end_date (str): 结束日期 YYYYMMDD

    Returns:
        pd.DataFrame or None: 合并后的数据DataFrame，或None
    """
    all_data = []
    failed_count = 0
    success_count = 0
    total_codes = len(codes)

    print(f"\n开始并发获取前复权日线数据，共 {total_codes} 支股票，最大并发数: {MAX_CONCURRENT_WORKERS}")
    print(f"日期范围: {start_date} 至 {end_date}")
    print(f"API 限制: {API_MINUTE_LIMIT} 次/分钟")

    start_overall_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_WORKERS) as executor:
        future_to_code = {executor.submit(fetch_stock_data_for_code, pro, code, start_date, end_date): code for code in codes}

        processed_count = 0
        for future in concurrent.futures.as_completed(future_to_code):
            code = future_to_code[future]
            processed_count += 1
            progress = (processed_count / total_codes) * 100
            elapsed = time.time() - start_overall_time
            print(f"[进度 {progress:.1f}%] {processed_count}/{total_codes} | 耗时 {elapsed:.1f}s", end='\r')

            try:
                result_df = future.result()
                if result_df is not None and not result_df.empty:
                    all_data.append(result_df)
                    success_count += 1
                elif result_df is None:
                    failed_count += 1
            except Exception as exc:
                print(f'\n[错误] 代码 {code} 在并发执行中生成异常: {exc}')
                log_failed_code(code, start_date, end_date, f"并发执行异常: {exc}")
                failed_count += 1

    print("\n并发获取完成。" + " " * 20)
    end_overall_time = time.time()
    print(f"并发获取总耗时: {end_overall_time - start_overall_time:.2f} 秒")

    if all_data:
        print(f"成功获取了 {success_count} 支股票的前复权数据，准备合并...")
        try:
            merged_df = pd.concat(all_data, ignore_index=True)
            print(f"数据合并完成，共 {len(merged_df)} 条记录")
            return merged_df
        except Exception as merge_exc:
            print(f"\n[错误] 合并数据时发生异常: {merge_exc}")
            return None
    else:
        print(f"未能成功获取任何股票的前复权数据 (成功: {success_count}, 失败: {failed_count})")
        return None

def main():
    """主函数"""
    print("=== 股票前复权日线数据并发获取工具 ===\n")

    if not TUSHARE_TOKEN or TUSHARE_TOKEN == 'YOUR_TUSHARE_TOKEN':
        print("错误：请在 .env 文件中设置有效的 TUSHARE_TOKEN！")
        return

    pro = initialize_tushare()
    if pro is None: return
    conn = connect_db(DB_PATH)
    if conn is None: return

    if not create_table_if_not_exists(conn, DAILY_QUOTES_TABLE):
        conn.close()
        return

    # existing_count = get_existing_data_count(conn, DAILY_QUOTES_TABLE)
    # print(f"数据库 '{DAILY_QUOTES_TABLE}' 中已有 {existing_count} 条前复权记录")

    codes_to_fetch = get_stock_codes_from_db(conn, BALL_CODE_TABLE)
    if not codes_to_fetch:
        print(f"数据库 '{BALL_CODE_TABLE}' 中没有找到需要处理的股票代码，程序退出。")
        conn.close()
        return

    print(f"将处理从 '{BALL_CODE_TABLE}' 获取的 {len(codes_to_fetch)} 支股票。")

    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 200)) # 每批处理的代码数量
    WAIT_SECONDS_BETWEEN_BATCHES = int(os.getenv('WAIT_SECONDS_BETWEEN_BATCHES', 3)) # 每批处理后等待秒数
    total_saved_count = 0
    overall_start_time = time.time()

    num_batches = (len(codes_to_fetch) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\n将股票代码分为 {num_batches} 批进行处理，每批最多 {BATCH_SIZE} 个。")

    for i in range(num_batches):
        batch_start_index = i * BATCH_SIZE
        batch_end_index = min((i + 1) * BATCH_SIZE, len(codes_to_fetch))
        current_batch_codes = codes_to_fetch[batch_start_index:batch_end_index]

        print(f"\n--- 开始处理批次 {i+1}/{num_batches} ({len(current_batch_codes)} 个代码) ---")
        batch_daily_data = process_stock_data_concurrently(pro, current_batch_codes, START_DATE, END_DATE)

        if batch_daily_data is not None and not batch_daily_data.empty:
            print(f"获取到 {len(batch_daily_data)} 条前复权数据，准备保存到数据库 '{DAILY_QUOTES_TABLE}'...")
            saved_count_batch = save_data_to_db(conn, batch_daily_data, DAILY_QUOTES_TABLE)
            if saved_count_batch > 0:
                print(f"批次 {i+1}: 成功保存 {saved_count_batch} 条记录到 '{DAILY_QUOTES_TABLE}'")
                total_saved_count += saved_count_batch
            elif saved_count_batch == 0:
                print(f"批次 {i+1}: 没有新的记录被保存 (可能数据已存在或为空)。")
            else:
                print(f"批次 {i+1}: 保存数据到 '{DAILY_QUOTES_TABLE}' 失败。")
        else:
            print(f"批次 {i+1}: 未能获取到数据或数据为空，不进行保存。")
        
        if i < num_batches - 1:
            print(f"批次 {i+1} 处理完毕，等待 {WAIT_SECONDS_BETWEEN_BATCHES} 秒后开始下一批..."
            )
            time.sleep(WAIT_SECONDS_BETWEEN_BATCHES)

    overall_end_time = time.time()
    print(f"\n--- 所有批次处理完毕 ---")
    print(f"总耗时: {overall_end_time - overall_start_time:.2f} 秒")
    print(f"总共成功保存 {total_saved_count} 条前复权记录到 '{DAILY_QUOTES_TABLE}'")

    # final_existing_count = get_existing_data_count(conn, DAILY_QUOTES_TABLE)
    # print(f"数据库 '{DAILY_QUOTES_TABLE}' 中最终共有 {final_existing_count} 条记录")
    
    # 检查是否有记录失败
    failed_log_exists = os.path.exists(FAILED_CODES_LOG) and os.path.getsize(FAILED_CODES_LOG) > 0
    if failed_log_exists:
        print(f"注意：有失败记录，请检查日志文件 {FAILED_CODES_LOG}")
    print("-----------------")

    conn.close()
    print("数据库连接已关闭。")

if __name__ == '__main__':
    main()
