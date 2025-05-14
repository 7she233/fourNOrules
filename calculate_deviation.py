import sqlite3
import datetime
import time
import os
import pandas as pd



# 定义需要计算偏离率的时间周期 (标签: timedelta)，根据PRD调整
# PRD: 8年、5年、3年、1年、6个月
TIME_PERIODS = {
    '6months': datetime.timedelta(days=180), # 6个月
    '1year': datetime.timedelta(days=365),   # 1年
    '3years': datetime.timedelta(days=3*365), # 3年
    '5years': datetime.timedelta(days=5*365), # 5年
    '8years': datetime.timedelta(days=8*365), # 8年
}

# 确保表头顺序与 TIME_PERIODS 一致，方便后续操作，并与PRD列名对应
# PRD列名: deviation_rate_8years, deviation_rate_5years, deviation_rate_3years, deviation_rate_1year, deviation_rate_6months
ORDERED_PERIOD_KEYS = ['8years', '5years', '3years', '1year', '6months']

def get_db_connection(db_path):
    """获取指定路径的数据库连接

    Args:
        db_path (str): 数据库文件的路径

    Returns:
        sqlite3.Connection: 数据库连接对象
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row # 允许通过列名访问数据
    return conn

def create_deviation_table_if_not_exists():
    """如果 deviation_rates 表不存在于目标数据库，则创建该表 (根据PRD)
    目标数据库: stock_index_data.db
    表名: deviation_rates
    """
    conn = get_db_connection(TARGET_DATABASE_PATH)
    cursor = conn.cursor()

    # 根据PRD构建列名
    # deviation_rate_8years, deviation_rate_5years, deviation_rate_3years, deviation_rate_1year, deviation_rate_6months
    deviation_columns = ', '.join([f"deviation_rate_{key} REAL" for key in ORDERED_PERIOD_KEYS])

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS deviation_rates (
        ts_code TEXT PRIMARY KEY,
        {deviation_columns},
        calculation_status TEXT,
        last_attempt_timestamp TEXT,
        last_success_date TEXT, -- YYYYMMDD format as per PRD
        failure_reason TEXT
    );
    """
    # PRD还提到 last_close_price 和 last_date，但它们似乎更适合作为计算过程中的临时变量
    # 或者如果需要在表中存储当时的最新价格和日期，可以加回来。当前脚本逻辑是从源数据动态获取。
    # 暂时不加入 last_close_price 和 last_date 到 deviation_rates 表，以严格匹配PRD的输出表结构。
    # 如果需要，可以后续讨论添加。

    cursor.execute(create_table_sql)
    conn.commit()
    conn.close()
    print(f"Table 'deviation_rates' in '{TARGET_DATABASE_PATH}' checked/created successfully.")

def get_historical_prices(ts_code, calculation_base_date_dt):
    """获取指定股票的历史日交易数据 (根据PRD)
    源数据库: stock_data.db, 表: daily_quotes
    获取从上市到 calculation_base_date_dt (含) 的所有数据，用于后续不同时间窗口的计算。

    Args:
        ts_code (str): 股票代码
        calculation_base_date_dt (datetime.date): 计算基准日 (通常是最新交易日)

    Returns:
        pd.DataFrame: 包含 'trade_date' (datetime.date) 和 'close' (float) 列的DataFrame，按日期升序排列
                      返回空DataFrame如果获取失败或无数据
    """
    conn = get_db_connection(SOURCE_DATABASE_PATH)
    query = """
    SELECT trade_date, close 
    FROM daily_quotes 
    WHERE ts_code = ? AND trade_date <= ?
    ORDER BY trade_date ASC
    """
    try:
        calculation_base_date_str = calculation_base_date_dt.strftime('%Y%m%d') # 假设 daily_quotes.trade_date 是 YYYYMMDD
        df = pd.read_sql_query(query, conn, params=(ts_code, calculation_base_date_str))
        if not df.empty:
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df.dropna(subset=['close'], inplace=True)
        return df
    except Exception as e:
        # print(f"Error fetching historical prices for {ts_code}: {e}") # TODO: Replace with logging
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def calculate_deviation_for_stock(ts_code, all_historical_data_df):
    """计算单只股票在不同时间周期内的偏离率 (根据PRD)

    Args:
        ts_code (str): 股票代码
        all_historical_data_df (pd.DataFrame): 包含该股票所有历史交易数据 ('trade_date', 'close') 的DataFrame，
                                             已按 trade_date 升序排列。

    Returns:
        dict: 包含偏离率计算结果的字典。
              键为 'ts_code', 'deviation_rate_8years', ..., 'calculation_status', 'failure_reason'。
              如果计算失败，偏离率值为 None，status 为 'Failed'。
    """
    results = {'ts_code': ts_code}
    # 初始化所有偏离率字段为 None，状态为 Failed，原因为空
    for key in ORDERED_PERIOD_KEYS:
        results[f'deviation_rate_{key}'] = None
    results['calculation_status'] = 'Failed' # 默认失败
    results['failure_reason'] = ''

    if all_historical_data_df.empty or 'close' not in all_historical_data_df.columns or 'trade_date' not in all_historical_data_df.columns:
        results['failure_reason'] = 'Error: Insufficient historical data (empty or missing columns).'
        return results

    if all_historical_data_df.empty:
        results['failure_reason'] = 'Error: No historical data found after initial fetch.'
        return results
        
    latest_record = all_historical_data_df.iloc[-1]
    current_price = latest_record['close']
    calculation_base_date = latest_record['trade_date'] # 这是 datetime.date 对象

    if pd.isna(current_price):
        results['failure_reason'] = f'Error: Current price is invalid (NaN) on {calculation_base_date}.'
        return results

    calculated_at_least_one_period = False
    failure_reasons_for_periods = []

    for period_key, time_delta_obj in TIME_PERIODS.items(): # time_delta_obj is a timedelta
        period_end_date = calculation_base_date - datetime.timedelta(days=1)
        period_start_date = period_end_date - time_delta_obj + datetime.timedelta(days=1)
        
        period_data_df = all_historical_data_df[
            (all_historical_data_df['trade_date'] >= period_start_date) &
            (all_historical_data_df['trade_date'] <= period_end_date)
        ]

        if period_data_df.empty or period_data_df['close'].isnull().all():
            failure_reasons_for_periods.append(f'{period_key}: No data or all closes NaN in period [{period_start_date} to {period_end_date}].')
            # results[f'deviation_rate_{period_key}'] remains None (initialized)
            continue

        min_price_in_period = period_data_df['close'].min()

        if pd.isna(min_price_in_period) or min_price_in_period <= 0:
            failure_reasons_for_periods.append(f'{period_key}: Min price {min_price_in_period} in period [{period_start_date} to {period_end_date}].')
            # results[f'deviation_rate_{period_key}'] remains None (initialized)
            continue

        deviation = (current_price - min_price_in_period) / min_price_in_period
        results[f'deviation_rate_{period_key}'] = deviation
        calculated_at_least_one_period = True # Mark that at least one period was successfully calculated
    
    # Determine overall calculation status
    if calculated_at_least_one_period:
        results['calculation_status'] = 'Success'
        if failure_reasons_for_periods: # Some periods might have failed but at least one succeeded
            results['failure_reason'] = f"Partially successful. Failures: {'; '.join(failure_reasons_for_periods)}"
        else:
            results['failure_reason'] = '' # All attempted periods were successful
    else:
        # No period was successfully calculated, status remains 'Failed' (default)
        # Ensure all deviation rates are None if they weren't already
        for key_to_null in ORDERED_PERIOD_KEYS:
            results[f'deviation_rate_{key_to_null}'] = None
        
        # Append period-specific failures to any top-level failure reason
        current_failure_reason = results.get('failure_reason', '')
        all_periods_failure_info = f"All periods failed calculation. Details: {'; '.join(failure_reasons_for_periods)} Current price: {current_price} on {calculation_base_date}."
        if current_failure_reason and 'Error: Insufficient historical data' not in current_failure_reason and 'Error: No historical data found' not in current_failure_reason and 'Error: Current price is invalid' not in current_failure_reason:
            results['failure_reason'] = f"{current_failure_reason} {all_periods_failure_info}"
        elif not current_failure_reason:
             results['failure_reason'] = all_periods_failure_info

    return results

def update_deviation_rates(deviation_result, latest_trade_date_for_success):
    """将单条计算得到的偏离率数据更新到目标数据库 (根据PRD)
    目标数据库: stock_index_data.db, 表: deviation_rates
    使用 INSERT OR REPLACE 或 INSERT ON CONFLICT DO UPDATE 逻辑。

    Args:
        deviation_result (dict): 从 calculate_deviation_for_stock 返回的单条结果字典。
        latest_trade_date_for_success (datetime.date): 用于填充 last_success_date 的最新交易日，
                                                     仅当 calculation_status 为 'Success' 时使用。
    """
    if not deviation_result or 'ts_code' not in deviation_result:
        return

    conn = get_db_connection(TARGET_DATABASE_PATH)
    cursor = conn.cursor()

    columns = ['ts_code'] + [f'deviation_rate_{key}' for key in ORDERED_PERIOD_KEYS] + \
              ['calculation_status', 'last_attempt_timestamp', 'last_success_date', 'failure_reason']
    
    placeholders = ', '.join(['?'] * len(columns))
    
    update_set_parts = []
    for col_key in ORDERED_PERIOD_KEYS:
        update_set_parts.append(f"deviation_rate_{col_key} = ?")
    update_set_parts.append("calculation_status = ?")
    update_set_parts.append("last_attempt_timestamp = ?")
    update_set_parts.append("last_success_date = ?")
    update_set_parts.append("failure_reason = ?")
    update_set_clause = ", ".join(update_set_parts)

    sql = f"""
    INSERT INTO deviation_rates ({', '.join(columns)})
    VALUES ({placeholders})
    ON CONFLICT(ts_code) DO UPDATE SET
        {update_set_clause};
    """

    current_timestamp_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    status = deviation_result.get('calculation_status', 'Failed')
    failure_msg = deviation_result.get('failure_reason', '')
    
    last_success_date_str = None
    if status == 'Success' and latest_trade_date_for_success:
        last_success_date_str = latest_trade_date_for_success.strftime('%Y%m%d')

    params_tuple = [deviation_result['ts_code']]
    for key in ORDERED_PERIOD_KEYS:
        params_tuple.append(deviation_result.get(f'deviation_rate_{key}'))
    params_tuple.append(status)
    params_tuple.append(current_timestamp_str)
    params_tuple.append(last_success_date_str)
    params_tuple.append(failure_msg)
    
    # Values for the UPDATE part (these are the same values, just listed again for the SET clause)
    for key in ORDERED_PERIOD_KEYS:
        params_tuple.append(deviation_result.get(f'deviation_rate_{key}'))
    params_tuple.append(status)
    params_tuple.append(current_timestamp_str)
    params_tuple.append(last_success_date_str)
    params_tuple.append(failure_msg)

    try:
        cursor.execute(sql, tuple(params_tuple))
        conn.commit()
    except sqlite3.Error as e:
        # print(f"Database error for {deviation_result['ts_code']}: {e}") # TODO: Replace with logging
        pass
    finally:
        if conn:
            conn.close()

def get_all_stock_codes():
    """获取所有股票代码 (ts_code) 从 ball_code_data 表 (根据PRD)
    源数据库: stock_data.db
    """
    conn = get_db_connection(SOURCE_DATABASE_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT ts_code FROM ball_code_data ORDER BY ts_code")
    stock_codes = [row['ts_code'] for row in cursor.fetchall()]
    conn.close()
    return stock_codes

import os
import pandas as pd
import argparse
import logging
from dotenv import load_dotenv # 新增导入

# 加载 .env 文件中的环境变量
load_dotenv()

# 从环境变量获取配置，提供默认值
SOURCE_DATABASE_PATH = os.getenv('SOURCE_DB_PATH', os.path.join(os.path.dirname(__file__), 'stock_data.db'))
TARGET_DATABASE_PATH = os.getenv('TARGET_DB_PATH', os.path.join(os.path.dirname(__file__), 'stock_index_data.db'))
LOG_LEVEL_STR = os.getenv('LOG_LEVEL', 'INFO').upper()

# 配置日志记录
log_level_map = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}
actual_log_level = log_level_map.get(LOG_LEVEL_STR, logging.INFO)
logging.basicConfig(level=actual_log_level, format='%(asctime)s - %(levelname)s - %(message)s')

# 确保路径是绝对路径，如果它们是相对的
if not os.path.isabs(SOURCE_DATABASE_PATH):
    SOURCE_DATABASE_PATH = os.path.join(os.path.dirname(__file__), SOURCE_DATABASE_PATH)
if not os.path.isabs(TARGET_DATABASE_PATH):
    TARGET_DATABASE_PATH = os.path.join(os.path.dirname(__file__), TARGET_DATABASE_PATH)

# PRD中定义的重试次数
MAX_RETRIES = 2 # 最多额外尝试2次，即总共3次

def process_single_stock(ts_code, attempt_count=0):
    """处理单个股票的偏离率计算，包含重试逻辑 (根据PRD)

    Args:
        ts_code (str): 股票代码
        attempt_count (int): 当前尝试次数 (0-indexed)

    Returns:
        bool: True 如果成功处理 (即使最终计算结果是 'Failed' 状态也算成功处理完毕), False 如果发生意外错误导致无法继续
    """
    logging.info(f"Processing {ts_code}, attempt {attempt_count + 1}/{MAX_RETRIES + 1}")
    try:
        # 获取该股票的全部历史数据，计算基准日为今天
        # PRD: 所有偏离率计算均基于每个ts_code在daily_quotes表中可获得的最新一个交易日
        # 为了简化，我们假设脚本运行时，当天的行情数据已经获取完毕
        # 如果需要更精确的“最新交易日”，可能需要从数据库查询 daily_quotes 的最新 trade_date
        # 这里暂时使用脚本运行的当天日期作为获取历史数据的上限
        today_date = datetime.date.today()
        historical_data_df = get_historical_prices(ts_code, today_date)

        if historical_data_df.empty:
            logging.warning(f"No historical data for {ts_code} up to {today_date}. Marking as failed.")
            # 即使没有数据，也记录一次尝试
            failure_result = {
                'ts_code': ts_code,
                'calculation_status': 'Failed',
                'failure_reason': f'No historical data found up to {today_date}.'
            }
            for key in ORDERED_PERIOD_KEYS:
                failure_result[f'deviation_rate_{key}'] = None
            update_deviation_rates(failure_result, None) # latest_trade_date_for_success is None
            return True # 算作已处理

        latest_trade_date_for_success = historical_data_df['trade_date'].max() if not historical_data_df.empty else None
        
        deviation_result = calculate_deviation_for_stock(ts_code, historical_data_df)
        update_deviation_rates(deviation_result, latest_trade_date_for_success)

        if deviation_result.get('calculation_status') == 'Success':
            logging.info(f"Successfully calculated deviations for {ts_code}.")
        else:
            logging.warning(f"Failed to calculate deviations for {ts_code}. Reason: {deviation_result.get('failure_reason')}")
            # PRD: 单个ts_code计算失败...脚本将立即进行重试，最多额外尝试2次
            if attempt_count < MAX_RETRIES:
                logging.info(f"Retrying for {ts_code}...")
                return process_single_stock(ts_code, attempt_count + 1) # 递归重试
            else:
                logging.error(f"Failed to calculate deviations for {ts_code} after {MAX_RETRIES + 1} attempts. Final reason: {deviation_result.get('failure_reason')}")
                # 最终失败的结果已在 update_deviation_rates 中更新
        return True

    except Exception as e:
        logging.error(f"Unexpected error processing {ts_code} on attempt {attempt_count + 1}: {e}", exc_info=True)
        # 记录一次尝试失败，但不立即重试，除非是 calculate_deviation_for_stock 内部逻辑判断可重试
        # 如果是意外错误，PRD未明确定义是否重试。这里假设意外错误不重试，直接标记为失败。
        error_result = {
            'ts_code': ts_code,
            'calculation_status': 'Failed',
            'failure_reason': f'Unexpected error: {str(e)}'
        }
        for key in ORDERED_PERIOD_KEYS:
            error_result[f'deviation_rate_{key}'] = None
        update_deviation_rates(error_result, None)
        if attempt_count < MAX_RETRIES:
            logging.info(f"Retrying for {ts_code} due to unexpected error...")
            return process_single_stock(ts_code, attempt_count + 1)
        else:
            logging.error(f"Failed to process {ts_code} after {MAX_RETRIES + 1} attempts due to unexpected error.")
        return False # 表示处理中发生不可恢复的错误

def main():
    """主函数，根据命令行参数执行偏离率计算 (根据PRD)"""
    parser = argparse.ArgumentParser(description="Calculate and manage stock deviation rates.")
    parser.add_argument("--codes", type=str, help="Comma-separated list of ts_codes to process (e.g., 000001.SZ,600000.SH). Example: python calculate_deviation.py --codes 000001.SZ,600000.SH")
    parser.add_argument("--watchlist", action="store_true", help="Process ts_codes from the 'my_watchlist' table in stock_index_data.db. Example: python calculate_deviation.py --watchlist")
    parser.add_argument("--retry_failed", action="store_true", help="Retry calculations for ts_codes marked as 'Failed' in 'deviation_rates' table. Example: python calculate_deviation.py --retry_failed")
    # --help or -h is added automatically by argparse

    args = parser.parse_args()

    create_deviation_table_if_not_exists() # 确保目标表存在

    ts_codes_to_process = []

    if args.codes:
        logging.info(f"Processing specific ts_codes: {args.codes}")
        raw_codes = args.codes.split(',')
        # PRD: 对每个ts_code，校验其有效性（例如，是否存在于ball_code_data表...）
        # 这里简化：直接尝试处理，如果get_historical_prices返回空则认为无效或无数据
        # TODO: 可以增加从 ball_code_data 预校验的逻辑
        all_valid_codes_from_db = get_all_stock_codes() # 获取所有有效的ts_code用于校验
        for code in raw_codes:
            clean_code = code.strip()
            if not clean_code:
                continue
            if clean_code in all_valid_codes_from_db:
                ts_codes_to_process.append(clean_code)
            else:
                logging.warning(f"Specified ts_code {clean_code} not found in ball_code_data. Skipping.")
        if not ts_codes_to_process:
            logging.info("No valid ts_codes provided in --codes argument after validation.")
            return

    elif args.watchlist:
        logging.info("Processing ts_codes from watchlist.")
        try:
            conn_index = get_db_connection(TARGET_DATABASE_PATH) # my_watchlist 在 stock_index_data.db
            cursor_index = conn_index.cursor()
            # PRD: 查询stock_index_data.db数据库my_watchlist表，获取所有分组下的所有唯一ts_code列表。
            # 假设 my_watchlist 表结构有 ts_code 列
            cursor_index.execute("SELECT DISTINCT ts_code FROM my_watchlist WHERE ts_code IS NOT NULL AND ts_code != ''")
            watchlist_codes = [row['ts_code'] for row in cursor_index.fetchall()]
            conn_index.close()
            if not watchlist_codes:
                logging.info("Watchlist is empty or contains no valid ts_codes.")
                return
            # TODO: 校验 watchlist_codes 中的 ts_code 是否存在于 ball_code_data
            ts_codes_to_process.extend(watchlist_codes)
        except sqlite3.Error as e:
            logging.error(f"Failed to read from my_watchlist: {e}")
            return

    elif args.retry_failed:
        logging.info("Retrying failed calculations.")
        try:
            conn_target = get_db_connection(TARGET_DATABASE_PATH)
            cursor_target = conn_target.cursor()
            # PRD: 查询deviation_rates表，找出所有calculation_status为 'Failed' 的ts_code列表。
            cursor_target.execute("SELECT ts_code FROM deviation_rates WHERE calculation_status = 'Failed'")
            failed_codes = [row['ts_code'] for row in cursor_target.fetchall()]
            conn_target.close()
            if not failed_codes:
                logging.info("No ts_codes found with 'Failed' status to retry.")
                return
            ts_codes_to_process.extend(failed_codes)
        except sqlite3.Error as e:
            logging.error(f"Failed to read failed ts_codes from deviation_rates: {e}")
            return
    else:
        # 默认行为：全量批量计算 (场景一)
        logging.info("Starting full batch calculation for all ts_codes from ball_code_data.")
        ts_codes_to_process = get_all_stock_codes()
        if not ts_codes_to_process:
            logging.info("No stock codes found in 'ball_code_data' table. Exiting.")
            return

    if not ts_codes_to_process:
        logging.info("No ts_codes to process. Exiting.")
        return

    logging.info(f"Found {len(ts_codes_to_process)} ts_codes to process.")
    
    successful_processing_count = 0
    failed_processing_count = 0

    for i, ts_code in enumerate(ts_codes_to_process):
        logging.info(f"--- Processing {ts_code} ({i+1}/{len(ts_codes_to_process)}) ---")
        if process_single_stock(ts_code):
            successful_processing_count +=1
        else:
            failed_processing_count +=1
        # time.sleep(0.1) # PRD 未要求，但可以考虑避免过于频繁的DB操作或API调用（如果未来有）
    
    logging.info("Deviation calculation process completed.")
    logging.info(f"Summary: Successfully processed {successful_processing_count} ts_codes. Failed to process (due to unexpected errors): {failed_processing_count} ts_codes.")
    # PRD: 脚本退出码: 成功完成所有请求操作时返回0；发生用户输入错误...或执行过程中出现未处理的致命异常时返回非0值
    # 当前实现：如果到这里，都算“成功完成请求操作”，即使某些股票计算失败但被记录。
    # 如果 failed_processing_count > 0，可以考虑返回非0。
    if failed_processing_count > 0:
        exit(1) # 部分处理失败
    else:
        exit(0) # 全部处理成功

if __name__ == '__main__':
    main()