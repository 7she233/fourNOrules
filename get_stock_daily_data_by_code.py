"""
1. 通过指定的 stock_code 获取股票的日线数据。
2. 从数据库 ball_code_data 表获取 stock_code 列表。
3. 从 .env 配置文件获取 start_date 和 end_date。
4. 逐个处理 stock_code，获取指定日期范围内的数据。
5. 将获取到的数据保存到数据库中。
"""
import tushare as ts
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import random
import argparse
from dotenv import load_dotenv
import concurrent.futures
import threading
from collections import deque

# 加载 .env 文件中的环境变量
load_dotenv()

# ------------------- 配置区 (从 .env 文件加载) -------------------
TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN', 'YOUR_TUSHARE_TOKEN')
DB_PATH = os.getenv('DB_PATH', 'stock_data.db')
DAILY_QUOTES_TABLE = os.getenv('DAILY_QUOTES_TABLE', 'daily_quotes') # 使用与 get_stock_daily.py 一致的表名
BALL_CODE_TABLE = os.getenv('BALL_CODE_TABLE', 'ball_code_data') # 新增：从env获取ball_code表名

# 日期设置逻辑 (优先使用 .env, 其次使用默认值)
_start_date_env = os.getenv('START_DATE')
DEFAULT_START_DATE = (datetime.now() - timedelta(days=3)).strftime('%Y%m%d') # 默认到当前-3天
START_DATE = _start_date_env if _start_date_env else DEFAULT_START_DATE

_end_date_env = os.getenv('END_DATE')
DEFAULT_END_DATE = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d') # 默认到当前-1天
END_DATE = _end_date_env if _end_date_env else DEFAULT_END_DATE

# API调用配置
API_CALL_INTERVAL = float(os.getenv('API_CALL_INTERVAL', 0.15)) # 基础API调用间隔（秒）- 可以适当调低，由速率限制器控制
MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))                 # 最大重试次数
BASE_BACKOFF_TIME = float(os.getenv('BASE_BACKOFF_TIME', 1.0)) # 基础退避时间（秒）
MAX_BACKOFF_TIME = float(os.getenv('MAX_BACKOFF_TIME', 60.0))  # 最大退避时间（秒）
FAILED_CODES_LOG = os.getenv('FAILED_CODES_LOG', 'failed_codes.log') # 失败代码日志文件
MAX_CONCURRENT_WORKERS = int(os.getenv('MAX_CONCURRENT_WORKERS', 10)) # 最大并发工作线程数
API_MINUTE_LIMIT = int(os.getenv('API_MINUTE_LIMIT', 500)) # 每分钟API调用次数限制

# --- 速率限制器和锁全局变量 ---
api_call_timestamps = deque()
rate_limit_lock = threading.Lock()
concurrency_semaphore = threading.Semaphore(MAX_CONCURRENT_WORKERS)
log_lock = threading.Lock() # 用于保护日志文件写入
db_lock = threading.Lock() # 新增：用于保护数据库写入操作
# ------------------------------------------------------------------

def check_rate_limit():
    """检查并等待以符合API每分钟调用限制

    Returns:
        bool: True 表示可以继续调用, False 表示需要等待
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
    print("Tushare API 初始化成功")
    return pro

def connect_db(db_path):
    """连接到 SQLite 数据库"""
    try:
        # 使用 check_same_thread=False 允许多线程共享连接，但需自行保证线程安全
        # 对于简单的读写分离或独立事务，通常是安全的
        conn = sqlite3.connect(db_path, check_same_thread=False)
        print(f"成功连接到数据库: {db_path} (允许多线程访问)")
        return conn
    except sqlite3.Error as e:
        print(f"连接数据库失败: {e}")
        return None

def create_table_if_not_exists(conn, table_name):
    """如果日线行情表不存在，则创建表并添加必要的索引"""
    if conn is None:
        print(f"错误：数据库连接无效，无法创建表 '{table_name}'")
        return False
    try:
        # 使用独立的 cursor，避免多线程冲突
        with conn:
            cursor = conn.cursor()
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                ts_code TEXT,
                trade_date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                pre_close REAL,
                change REAL,
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
        # 不需要 rollback，因为 with conn 会自动处理
        return False

def exponential_backoff(retry_count):
    """计算指数退避等待时间"""
    wait_time = min(MAX_BACKOFF_TIME, BASE_BACKOFF_TIME * (2 ** retry_count))
    jitter = wait_time * 0.2 * (random.random() - 0.5) * 2 # +/- 20% jitter
    return max(0, wait_time + jitter) # Ensure wait time is non-negative

def log_failed_code(ts_code, start_date, end_date, error_msg):
    """记录失败的股票代码到日志文件 (线程安全)

    Args:
        ts_code: 单个股票代码
        start_date: 开始日期
        end_date: 结束日期
        error_msg: 错误信息
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] 代码失败: {ts_code} | 日期范围: {start_date}-{end_date} | 错误: {error_msg}\n"
    try:
        with log_lock: # 使用锁保护文件写入
            with open(FAILED_CODES_LOG, 'a', encoding='utf-8') as f:
                f.write(log_entry)
        print(f"[线程 {threading.get_ident()}] 已记录失败代码 {ts_code} 到 {FAILED_CODES_LOG}")
    except IOError as e:
        print(f"[线程 {threading.get_ident()}] 写入失败日志文件 {FAILED_CODES_LOG} 时出错: {e}")

def fetch_stock_data_for_code(pro, ts_code, start_date, end_date):
    """获取单个股票代码在指定日期范围内的日线行情数据，带重试和速率限制 (设计为线程池任务)

    Args:
        pro: Tushare API 实例
        ts_code (str): 股票代码
        start_date (str): 开始日期 YYYYMMDD
        end_date (str): 结束日期 YYYYMMDD

    Returns:
        pd.DataFrame or None: 成功返回数据DataFrame，失败返回None
    """
    thread_id = threading.get_ident()
    print(f"[线程 {thread_id}] 开始处理代码: {ts_code} ({start_date}-{end_date})")
    retry_count = 0

    with concurrency_semaphore: # 控制并发数量
        while retry_count <= MAX_RETRIES:
            # 检查每分钟速率限制
            can_call, wait_needed = check_rate_limit()
            while not can_call:
                time.sleep(wait_needed)
                can_call, wait_needed = check_rate_limit()

            try:
                if retry_count > 0:
                    print(f"[线程 {thread_id}][{ts_code}] 第 {retry_count} 次重试获取数据...")

                start_time = time.time()
                # --- API 调用 --- #
                record_api_call() # 在调用前记录
                df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
                # --------------- #
                api_time = time.time() - start_time

                # API调用后的基础间隔 (可以移除或保留一个非常小的值，主要靠速率限制器)
                time.sleep(API_CALL_INTERVAL)

                if df is not None and not df.empty:
                    print(f"[线程 {thread_id}][{ts_code}] ✓ 成功获取 {len(df)} 条数据 (耗时 {api_time:.2f}s)")
                    return df
                elif df is not None and df.empty:
                    print(f"[线程 {thread_id}][{ts_code}] ✓ 查询成功，但日期范围内无数据 (耗时 {api_time:.2f}s)")
                    return pd.DataFrame() # 返回空DataFrame表示无数据
                else:
                    print(f"[线程 {thread_id}][{ts_code}] ⚠ Tushare API 返回 None (耗时 {api_time:.2f}s)")
                    retry_count += 1
                    if retry_count <= MAX_RETRIES:
                        backoff = exponential_backoff(retry_count)
                        print(f"[线程 {thread_id}][{ts_code}] 将在 {backoff:.2f}秒 后重试...")
                        time.sleep(backoff)
                    else:
                        error_msg = "达到最大重试次数后 Tushare API 仍返回 None"
                        print(f"[线程 {thread_id}][{ts_code}] ✗ {error_msg}")
                        log_failed_code(ts_code, start_date, end_date, error_msg)
                        return None # 明确返回 None 表示失败

            except Exception as e:
                error_msg = str(e)
                print(f"[线程 {thread_id}][{ts_code}] ✗ 获取数据时发生异常: {error_msg}")
                retry_count += 1
                if retry_count <= MAX_RETRIES:
                    backoff = exponential_backoff(retry_count)
                    print(f"[线程 {thread_id}][{ts_code}] 将在 {backoff:.2f}秒 后重试...")
                    time.sleep(backoff)
                else:
                    print(f"[线程 {thread_id}][{ts_code}] ✗ 达到最大重试次数 {MAX_RETRIES}，放弃此代码")
                    log_failed_code(ts_code, start_date, end_date, error_msg)
                    return None # 明确返回 None 表示失败

    # 如果循环结束仍未成功 (理论上应该在循环内返回或记录失败)
    log_failed_code(ts_code, start_date, end_date, "未知原因导致获取失败")
    return None

def save_data_to_db(conn, df, table_name): # 修改：添加 conn 参数
    """将 DataFrame 数据批量保存到 SQLite 数据库，使用 INSERT OR REPLACE 逻辑 (线程安全)。
       每次调用此函数时都会创建一个新的数据库连接。

    Args:
        df (pd.DataFrame): 包含待保存数据的 DataFrame。
        table_name (str): 目标数据库表名。

    Returns:
        int: 成功保存的记录数 (受影响的行数)，如果发生错误则返回 -1。
    """
    if df is None or df.empty:
        return 0

    affected_rows = 0
    try:
        # 使用 db_lock 确保数据库写入操作的线程安全
        with db_lock:
            # 使用 with conn 确保事务性
            with conn:
                cursor = conn.cursor()
                # 获取目标表列名
                cursor.execute(f"PRAGMA table_info({table_name})")
                db_columns = [info[1] for info in cursor.fetchall()]

                # 检查并调整 DataFrame 列
                if list(df.columns) != db_columns:
                    try:
                        df = df[db_columns]
                    except KeyError as ke:
                        print(f"错误：DataFrame 中缺少数据库表 '{table_name}' 所需的列: {ke}")
                        return -1

                # 构建 SQL 语句
                columns_str = ', '.join(db_columns)
                placeholders = ', '.join(['?'] * len(db_columns))
                sql = f"INSERT OR REPLACE INTO {table_name} ({columns_str}) VALUES ({placeholders})"

                # 准备数据
                data_to_insert = [tuple(x) for x in df.to_numpy()]

                # 批量插入
                cursor.executemany(sql, data_to_insert)
                affected_rows = cursor.rowcount

        return affected_rows

    except sqlite3.OperationalError as e:
        thread_id = threading.get_ident()
        # 特别处理 database is locked 错误
        if 'database is locked' in str(e):
            print(f"[线程 {thread_id}] 保存数据到表 '{table_name}' 时遇到数据库锁定错误: {e}。可能是并发写入冲突或连接未及时释放。")
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
    finally:
        pass # 连接由主函数管理，此处不再关闭

def get_stock_codes_from_db(conn, table_name):
    """从数据库 ball_code_data 表获取 ST_CODE 代码列表"""
    if conn is None:
        print("错误：数据库连接无效，无法获取股票代码")
        return []
    try:
        with conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT DISTINCT ts_code FROM {table_name}") # 假设代码列名为 ts_code
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
    """使用线程池并发获取多个股票代码的日线数据，并返回合并后的DataFrame

    Args:
        pro: Tushare API 实例
        codes (list): 股票代码列表
        start_date (str): 开始日期 YYYYMMDD
        end_date (str): 结束日期 YYYYMMDD

    Returns:
        pd.DataFrame or None: 合并后的数据DataFrame，如果全部失败或无数据则返回None
    """
    all_data = []
    failed_count = 0
    success_count = 0
    total_codes = len(codes)

    print(f"\n开始并发获取日线数据，共 {total_codes} 支股票，最大并发数: {MAX_CONCURRENT_WORKERS}")
    print(f"日期范围: {start_date} 至 {end_date}")
    print(f"API 限制: {API_MINUTE_LIMIT} 次/分钟")

    start_overall_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_WORKERS) as executor:
        # 创建 future 字典，方便追踪
        future_to_code = {executor.submit(fetch_stock_data_for_code, pro, code, start_date, end_date): code for code in codes}

        processed_count = 0
        for future in concurrent.futures.as_completed(future_to_code):
            code = future_to_code[future]
            processed_count += 1
            progress = (processed_count / total_codes) * 100
            elapsed = time.time() - start_overall_time
            print(f"[进度 {progress:.1f}%] {processed_count}/{total_codes} | 耗时 {elapsed:.1f}s", end='\r') # 实时进度

            try:
                result_df = future.result()
                if result_df is not None and not result_df.empty:
                    all_data.append(result_df)
                    success_count += 1
                elif result_df is None: # fetch_stock_data_for_code 返回 None 表示失败
                    failed_count += 1
                # 空 DataFrame (result_df is not None and result_df.empty) 表示查询成功但无数据，不计入失败也不添加
            except Exception as exc:
                print(f'\n[错误] 代码 {code} 在并发执行中生成异常: {exc}')
                log_failed_code(code, start_date, end_date, f"并发执行异常: {exc}")
                failed_count += 1

    print("\n并发获取完成。" + " " * 20) # 清除进度条残留
    end_overall_time = time.time()
    print(f"并发获取总耗时: {end_overall_time - start_overall_time:.2f} 秒")

    if all_data:
        print(f"成功获取了 {success_count} 支股票的数据，准备合并...")
        try:
            merged_df = pd.concat(all_data, ignore_index=True)
            print(f"数据合并完成，共 {len(merged_df)} 条记录")
            return merged_df
        except Exception as merge_exc:
            print(f"\n[错误] 合并数据时发生异常: {merge_exc}")
            return None # 合并失败也返回 None
    else:
        print(f"未能成功获取任何股票的数据 (成功: {success_count}, 失败: {failed_count})")
        return None # 明确返回 None

# --- 主程序逻辑 ---
def main():
    """主函数"""
    print("=== 股票日线数据并发获取工具 ===\n")

    if not TUSHARE_TOKEN or TUSHARE_TOKEN == 'YOUR_TUSHARE_TOKEN':
        print("错误：请在 .env 文件中设置有效的 TUSHARE_TOKEN！")
        return

    # 初始化API和数据库连接
    pro = initialize_tushare()
    if pro is None:
        return
    conn = connect_db(DB_PATH)
    if conn is None:
        return

    # 确保日线数据表存在
    if not create_table_if_not_exists(conn, DAILY_QUOTES_TABLE):
        conn.close()
        return

    # 获取数据库中已有的数据记录数
    existing_count = get_existing_data_count(conn, DAILY_QUOTES_TABLE)
    print(f"数据库 '{DAILY_QUOTES_TABLE}' 中已有 {existing_count} 条记录")

    # 从数据库 ball_code_data 表获取股票代码
    codes_to_fetch = get_stock_codes_from_db(conn, BALL_CODE_TABLE)

    if not codes_to_fetch:
        print("数据库中没有找到需要处理的股票代码，程序退出。")
        conn.close()
        return

    print(f"将处理从数据库获取的 {len(codes_to_fetch)} 支股票。")

    # --- 分批处理逻辑 --- #
    BATCH_SIZE = 200 # 每批处理100个代码
    WAIT_SECONDS_BETWEEN_BATCHES = 3 # 每批处理后等待60秒
    total_saved_count = 0
    total_fetch_time = 0.0
    total_save_time = 0.0
    overall_start_time = time.time()

    num_batches = (len(codes_to_fetch) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\n将股票代码分为 {num_batches} 批进行处理，每批最多 {BATCH_SIZE} 个。")

    for i in range(num_batches):
        batch_start_index = i * BATCH_SIZE
        batch_end_index = min((i + 1) * BATCH_SIZE, len(codes_to_fetch))
        current_batch_codes = codes_to_fetch[batch_start_index:batch_end_index]

        print(f"\n--- 开始处理批次 {i+1}/{num_batches} ({len(current_batch_codes)} 个代码) ---")

        # 并发获取当前批次数据
        batch_fetch_start_time = time.time()
        batch_daily_data = process_stock_data_concurrently(pro, current_batch_codes, START_DATE, END_DATE)
        batch_fetch_elapsed = time.time() - batch_fetch_start_time
        total_fetch_time += batch_fetch_elapsed
        print(f"批次 {i+1} 数据获取完成，耗时: {batch_fetch_elapsed:.2f}秒")

        batch_saved_count = 0
        batch_save_elapsed = 0.0

        # 保存当前批次数据
        if batch_daily_data is not None and not batch_daily_data.empty:
            print(f"获取到 {len(batch_daily_data)} 条数据，准备保存到数据库 '{DAILY_QUOTES_TABLE}'...")
            required_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close']
            missing_columns = [col for col in required_columns if col not in batch_daily_data.columns]
            if missing_columns:
                print(f"警告：批次 {i+1} 数据缺少必要列: {missing_columns}，无法保存")
            else:
                batch_save_start_time = time.time()
                # 调用修改后的 save_data_to_db，传递共享的 conn
                batch_saved_count = save_data_to_db(conn, batch_daily_data, DAILY_QUOTES_TABLE)
                batch_save_elapsed = time.time() - batch_save_start_time
                total_save_time += batch_save_elapsed
                if batch_saved_count >= 0:
                    print(f"批次 {i+1} 数据保存完成，共影响 {batch_saved_count} 行，耗时: {batch_save_elapsed:.2f}秒")
                    total_saved_count += batch_saved_count
                else:
                    print(f"批次 {i+1} 数据保存过程中发生错误")
        else:
            print(f"批次 {i+1} 未获取到有效数据或所有获取均失败，跳过数据库保存步骤")

        # 如果不是最后一批，则等待指定时间
        if i < num_batches - 1:
            print(f"批次 {i+1} 处理完毕，等待 {WAIT_SECONDS_BETWEEN_BATCHES} 秒后开始下一批... ({datetime.now().strftime('%H:%M:%S')})")
            time.sleep(WAIT_SECONDS_BETWEEN_BATCHES)
        else:
            print(f"所有批次处理完毕。")

    # --- 任务总结 --- #
    overall_elapsed_time = time.time() - overall_start_time
    updated_count = get_existing_data_count(conn, DAILY_QUOTES_TABLE)

    print(f"\n--- 整体任务总结 ---")
    print(f"总批次数: {num_batches}")
    print(f"累计数据获取耗时: {total_fetch_time:.2f}秒")
    print(f"累计数据保存耗时: {total_save_time:.2f}秒")
    print(f"总耗时 (含等待时间): {overall_elapsed_time:.2f}秒")
    print(f"数据库 '{DAILY_QUOTES_TABLE}' 现有 {updated_count} 条记录")
    print(f"本次操作累计影响 {total_saved_count} 行 (可能包含更新和插入)")
    # 注意：new_records 的计算可能不准确，因为 save_data_to_db 使用 INSERT OR REPLACE
    # print(f"本次新增/更新 {new_records} 条记录") # 可以注释掉或保留，但需理解其局限性

    failed_log_exists = os.path.exists(FAILED_CODES_LOG) and os.path.getsize(FAILED_CODES_LOG) > 0
    if failed_log_exists:
        print(f"注意：有失败记录，请检查日志文件 {FAILED_CODES_LOG}")
    print("-----------------")

    conn.close()
    print("数据库连接已关闭。")

if __name__ == '__main__':
    main()