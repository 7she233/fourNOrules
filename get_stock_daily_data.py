"""
1. 获取daily数据
2. 存入db
"""
import tushare as ts
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import random
from dotenv import load_dotenv

# 加载 .env 文件中的环境变量
load_dotenv()

# ------------------- 配置区 (从 .env 文件加载) -------------------
TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN', 'YOUR_TUSHARE_TOKEN')
DB_PATH = os.getenv('DB_PATH', 'stock_data.db')
DAILY_QUOTES_TABLE = os.getenv('TABLE_NAME', 'daily_quotes') # 重命名以区分
BALL_CODE_TABLE = os.getenv('BALL_CODE_TABLE', 'ball_code_data') # 新增：从env获取ball_code表名
# 日期设置逻辑
# 如果环境变量未设置开始日期，则默认为30天前
_start_date_env = os.getenv('START_DATE')
START_DATE = _start_date_env if _start_date_env else (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
# 如果环境变量未设置结束日期，则默认为昨天
_end_date_env = os.getenv('END_DATE')
END_DATE = _end_date_env if _end_date_env else (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
# 确保开始日期不晚于结束日期

try:
    start_dt = datetime.strptime(START_DATE, '%Y%m%d')
    end_dt = datetime.strptime(END_DATE, '%Y%m%d')
    if start_dt > end_dt:
        print(f"警告：开始日期({START_DATE})晚于结束日期({END_DATE})，将使用结束日期作为开始日期")
        START_DATE = END_DATE
except ValueError as e:
    print(f"日期格式错误: {e}，将使用默认日期范围")
    START_DATE = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
    END_DATE = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

# API调用配置
API_CALL_INTERVAL = float(os.getenv('API_CALL_INTERVAL', 0.3))  # 基础API调用间隔（秒）
MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))                 # 最大重试次数
BASE_BACKOFF_TIME = float(os.getenv('BASE_BACKOFF_TIME', 1.0)) # 基础退避时间（秒）
MAX_BACKOFF_TIME = float(os.getenv('MAX_BACKOFF_TIME', 60.0))  # 最大退避时间（秒）
FAILED_BATCH_LOG = os.getenv('FAILED_BATCH_LOG', 'failed_batches.log') # 失败批次日志文件
# ------------------------------------------------------------------

def initialize_tushare():
    """初始化 Tushare Pro API"""
    ts.set_token(TUSHARE_TOKEN)
    pro = ts.pro_api()
    print("Tushare API 初始化成功")
    return pro

def connect_db(db_path):
    """连接到 SQLite 数据库"""
    conn = sqlite3.connect(db_path)
    print(f"成功连接到数据库: {db_path}")
    return conn

def create_table_if_not_exists(conn, table_name):
    """如果日线行情表不存在，则创建表并添加必要的索引"""
    try:
        # 检查数据库连接状态
        if conn is None or not hasattr(conn, 'cursor'):
            print(f"错误：创建表 '{table_name}' 时数据库连接无效或已关闭")
            return False
            
        cursor = conn.cursor()
        
        # 检查表是否已存在
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        table_exists = cursor.fetchone() is not None
        if table_exists:
            print(f"表 '{table_name}' 已存在，检查表结构...")
        else:
            print(f"表 '{table_name}' 不存在，准备创建...")
        
        # 创建表结构，包含必要的索引
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            ts_code TEXT,      -- TS代码
            trade_date TEXT,   -- 交易日期
            open REAL,         -- 开盘价
            high REAL,         -- 最高价
            low REAL,          -- 最低价
            close REAL,        -- 收盘价
            pre_close REAL,    -- 昨收价
            change REAL,       -- 涨跌额
            pct_chg REAL,      -- 涨跌幅
            vol REAL,          -- 成交量 （手）
            amount REAL,       -- 成交额 （千元）
            PRIMARY KEY (ts_code, trade_date) -- 设置联合主键防止重复
        )
        ''')
        
        # 创建索引以优化查询性能
        cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_ts_code ON {table_name}(ts_code)')
        cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_trade_date ON {table_name}(trade_date)')
        cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_close ON {table_name}(close)')
        
        # 提交事务
        try:
            conn.commit()
            print(f"数据表 '{table_name}' 及索引创建完毕")
        except Exception as commit_error:
            print(f"提交创建表事务时出错: {commit_error}")
            conn.rollback()
            return False
        
        # 再次检查表是否存在
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if not cursor.fetchone():
            print(f"错误：表 '{table_name}' 创建失败，未在数据库中找到")
            return False
        
        # 验证表结构
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        if not columns:
            print(f"错误：表 '{table_name}' 没有列定义")
            return False
            
        print(f"表 '{table_name}' 结构验证:")
        for col in columns:
            print(f"  - {col[1]} ({col[2]})")
            
        # 尝试插入一条测试数据并立即删除，确保表可写
        try:
            test_data = ('TEST_CODE', '20000101', 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
            cursor.execute(f"INSERT OR REPLACE INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", test_data)
            cursor.execute(f"DELETE FROM {table_name} WHERE ts_code = 'TEST_CODE' AND trade_date = '20000101'")
            conn.commit()
            print(f"表 '{table_name}' 写入测试成功")
        except Exception as test_error:
            print(f"表 '{table_name}' 写入测试失败: {test_error}")
            conn.rollback()
            # 不返回False，因为表结构可能是正确的，只是有其他约束导致测试写入失败
        
        return True
            
    except Exception as e:
        print(f"创建表 '{table_name}' 时出错: {e}")
        import traceback
        print(f"错误详情: {traceback.format_exc()}")
        try:
            conn.rollback()  # 出错时回滚
        except Exception as rollback_error:
            print(f"回滚事务时出错: {rollback_error}")
        return False
            
    except Exception as e:
        print(f"创建表 '{table_name}' 时出错: {e}")
        conn.rollback()

def log_failed_batch(batch_codes, start_date, end_date, error_msg):
    """记录失败的批次到日志文件，确保格式与解析逻辑一致"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(FAILED_BATCH_LOG, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] 批次失败: {','.join(batch_codes)} | 日期范围: {start_date}-{end_date} | 错误: {error_msg}\n")
    print(f"已记录失败批次到 {FAILED_BATCH_LOG}")
    # 记录日志内容到控制台，方便调试
    print(f"日志内容: [批次失败: {','.join(batch_codes[:3])}{'...' if len(batch_codes) > 3 else ''}] [日期范围: {start_date}-{end_date}]")


def exponential_backoff(retry_count):
    """计算指数退避等待时间"""
    wait_time = min(MAX_BACKOFF_TIME, BASE_BACKOFF_TIME * (2 ** retry_count))
    # 添加随机抖动，避免多个请求同时重试
    jitter = wait_time * 0.2 * (2 * (0.5 - random.random()))
    return wait_time + jitter

def get_stock_daily(pro, codes, start_date, end_date, batch_size=30):
    """批量获取ST_CODE的日线行情数据，使用指数退避重试策略"""
    all_data = []
    failed_batches = []  # 记录所有失败的批次
    
    print(f"\n开始获取日线数据，共 {len(codes)} 支ST_CODE，批次大小: {batch_size}")
    print(f"日期范围: {start_date} 至 {end_date}")
    print(f"最大重试次数: {MAX_RETRIES}, 基础等待时间: {BASE_BACKOFF_TIME}秒")
    
    for i in range(0, len(codes), batch_size):
        batch_codes = codes[i:i + batch_size]
        batch_success = False
        retry_count = 0
        
        # 将多个ST_CODE代码合并为逗号分隔的字符串
        ts_codes = ','.join(batch_codes)
        print(f"\n[批次 {i//batch_size + 1}/{(len(codes)-1)//batch_size + 1}] 正在获取 {len(batch_codes)} 支ST_CODE的日线数据...")
        print(f"  - 请求代码: {ts_codes[:50]}{'...' if len(ts_codes) > 50 else ''}")
        
        while not batch_success and retry_count <= MAX_RETRIES:
            try:
                # 如果是重试，则显示重试信息
                if retry_count > 0:
                    print(f"  - 第 {retry_count} 次重试获取数据...")
                
                # 使用 daily 接口批量获取数据
                start_time = time.time()
                df = pro.daily(ts_code=ts_codes, start_date=start_date, end_date=end_date)
                api_time = time.time() - start_time
                
                # 根据是否重试和API响应时间动态调整等待时间
                wait_time = API_CALL_INTERVAL * (1 + retry_count * 0.5)
                if api_time < 0.1:  # API响应过快，可能是因为限流或其他问题
                    wait_time = wait_time * 2
                
                print(f"  - API响应时间: {api_time:.2f}秒, 等待时间: {wait_time:.2f}秒")
                time.sleep(wait_time)  # 控制API调用频率
                
                if df is not None and not df.empty:
                    print(f"  - ✓ 成功获取 {len(df)} 条数据，涉及代码: {df['ts_code'].nunique()} 个")
                    all_data.append(df)
                    batch_success = True
                else:
                    print(f"  - ⚠ 未获取到数据或返回为空 DataFrame")
                    retry_count += 1
                    if retry_count <= MAX_RETRIES:
                        backoff_time = exponential_backoff(retry_count)
                        print(f"  - 将在 {backoff_time:.2f}秒 后重试...")
                        time.sleep(backoff_time)
                    else:
                        error_msg = "达到最大重试次数后仍未获取到数据"
                        print(f"  - ✗ {error_msg}")
                        log_failed_batch(batch_codes, start_date, end_date, error_msg)
                        failed_batches.append((batch_codes, "空数据"))
            
            except Exception as e:
                error_msg = str(e)
                print(f"  - ✗ 获取数据出错: {error_msg}")
                retry_count += 1
                
                if retry_count <= MAX_RETRIES:
                    backoff_time = exponential_backoff(retry_count)
                    print(f"  - 将在 {backoff_time:.2f}秒 后重试...")
                    time.sleep(backoff_time)
                else:
                    print(f"  - ✗ 达到最大重试次数 {MAX_RETRIES}，放弃此批次")
                    log_failed_batch(batch_codes, start_date, end_date, error_msg)
                    failed_batches.append((batch_codes, error_msg))
    
    # 汇总处理结果
    if all_data:
        total_data = pd.concat(all_data, ignore_index=True)
        print(f"\n数据获取完成，共获取 {len(total_data)} 条数据，涉及 {total_data['ts_code'].nunique()} 支ST_CODE")
        
        if failed_batches:
            print(f"有 {len(failed_batches)} 个批次获取失败，详情请查看 {FAILED_BATCH_LOG}")
        
        return total_data
    else:
        print("\n未能获取任何数据，请检查网络连接和API配置")
        return None

def save_data_to_db(conn, df, table_name, batch_size=1000):
    """将 DataFrame 数据批量保存到 SQLite 数据库"""
    if df is None or df.empty:
        print("没有数据需要保存。")
        return
    
    try:
        # 先检查数据库连接状态
        if conn is None or not hasattr(conn, 'cursor'):
            print("错误：数据库连接无效或已关闭")
            return
            
        cursor = conn.cursor()
        total_rows = len(df)
        
        # 检查表是否存在
        try:
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            if not cursor.fetchone():
                print(f"错误：表 '{table_name}' 不存在，请先创建表")
                return
        except Exception as table_check_error:
            print(f"检查表是否存在时出错: {table_check_error}")
            return
        
        # 检查表结构是否匹配数据列
        try:
            cursor.execute(f"PRAGMA table_info({table_name})")
            table_columns = [col[1] for col in cursor.fetchall()]
            missing_columns = [col for col in df.columns if col not in table_columns]
            if missing_columns:
                print(f"警告：数据包含表中不存在的列: {missing_columns}")
                print(f"表 '{table_name}' 现有列: {table_columns}")
                print(f"数据列: {df.columns.tolist()}")
                # 过滤掉不存在的列
                df = df[[col for col in df.columns if col in table_columns]]
                print(f"已过滤数据，保留列: {df.columns.tolist()}")
        except Exception as column_check_error:
            print(f"检查表结构时出错: {column_check_error}")
        
        # 准备INSERT语句 - 使用INSERT OR REPLACE以更新已有数据
        columns = ','.join(df.columns)
        placeholders = ','.join(['?' for _ in df.columns])
        insert_sql = f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # 添加详细日志
        print(f"SQL语句: {insert_sql}")
        print(f"数据列: {df.columns.tolist()}")
        print(f"数据行数: {total_rows}")
        if total_rows > 0:
            print(f"第一行数据示例: {df.iloc[0].tolist()}")
            # 检查数据类型
            dtypes = df.dtypes
            print(f"数据类型: {dtypes}")
            # 检查是否有空值
            null_counts = df.isnull().sum()
            print(f"空值统计: {null_counts}")
        else:
            print("无数据")
        
        print(f"开始批量保存数据到表 '{table_name}'...")
        
        # 批量处理数据
        inserted_rows = 0
        error_count = 0
        for i in range(0, total_rows, batch_size):
            try:
                batch_df = df.iloc[i:i + batch_size]
                values = [tuple(x) for x in batch_df.values]
                
                # 检查数据值
                if i == 0:  # 只检查第一批次的数据
                    print(f"第一批次数据示例 (前3行): {values[:3] if len(values) >= 3 else values}")
                
                cursor.executemany(insert_sql, values)
                inserted_rows += len(batch_df)
                
                # 定期提交事务
                if i % (batch_size * 10) == 0:
                    conn.commit()
                    print(f"已处理并提交 {i}/{total_rows} 条数据")
            except Exception as batch_error:
                error_count += 1
                print(f"批次处理错误 (行 {i}-{min(i+batch_size, total_rows)}): {batch_error}")
                # 尝试单行插入以找出问题行
                if error_count <= 3:  # 只对前几个错误进行详细诊断
                    for j, row in batch_df.iterrows():
                        try:
                            row_values = tuple(row.values)
                            cursor.execute(insert_sql, row_values)
                            print(f"  - 单行插入成功: 索引 {j}")
                        except Exception as row_error:
                            print(f"  - 单行插入失败: 索引 {j}, 错误: {row_error}")
                            print(f"  - 问题数据: {row.tolist()}")
                # 继续处理下一批次，而不是整个失败
        
        # 最终提交
        try:
            conn.commit()
            print(f"成功将 {inserted_rows}/{total_rows} 条数据保存到表 '{table_name}'")
        except Exception as commit_error:
            print(f"最终提交事务时出错: {commit_error}")
            conn.rollback()
            return
        
        # 验证数据是否成功保存
        try:
            verify_cursor = conn.cursor()
            verify_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            current_count = verify_cursor.fetchone()[0]
            print(f"验证：表 '{table_name}' 当前共有 {current_count} 条记录")
            
            # 检查最近插入的几条记录
            verify_cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
            sample_records = verify_cursor.fetchall()
            print(f"样本记录: {sample_records}")
        except Exception as verify_error:
            print(f"验证数据保存时出错: {verify_error}")
        
    except Exception as e:
        print(f"保存数据到数据库时出错: {e}")
        import traceback
        print(f"错误详情: {traceback.format_exc()}")
        try:
            conn.rollback()  # 出错时回滚
        except Exception as rollback_error:
            print(f"回滚事务时出错: {rollback_error}")


def get_stock_codes_from_db(conn, table_name):
    """从数据库的指定表中获取所有 ts_code"""
    print(f"正在从数据库表 '{table_name}' 获取ST_CODE代码列表...")
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT DISTINCT ts_code FROM {table_name}")
        codes = [row[0] for row in cursor.fetchall()]
        if not codes:
            print(f"在表 '{table_name}' 中未找到任何ST_CODE代码。")
            return []
        print(f"成功从数据库获取到 {len(codes)} 支ST_CODE代码。")
        return codes
    except sqlite3.Error as e:
        print(f"从数据库获取ST_CODE代码时出错: {e}")
        return []


def get_existing_data_count(conn, table_name):
    """获取数据库中已有的数据记录数"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        return count
    except sqlite3.Error as e:
        print(f"获取数据库记录数时出错: {e}")
        return 0

def process_failed_batches():
    """处理之前失败的批次，优化日志解析逻辑并添加详细调试信息"""
    if not os.path.exists(FAILED_BATCH_LOG):
        print(f"未找到失败批次日志文件 {FAILED_BATCH_LOG}，跳过处理")
        return
    
    try:
        # 读取日志文件内容并显示样本
        with open(FAILED_BATCH_LOG, 'r', encoding='utf-8') as f:
            failed_lines = f.readlines()
        
        if not failed_lines:
            print("失败批次日志为空，无需处理")
            return
        
        # 显示日志样本，帮助调试
        print(f"\n发现 {len(failed_lines)} 条失败记录")
        print("\n日志样本(前2条):")
        for i in range(min(2, len(failed_lines))):
            print(f"[{i+1}] {failed_lines[i].strip()}")
        
        print("\n是否尝试重新获取？(y/n)")
        choice = input().strip().lower()
        
        if choice != 'y':
            print("跳过处理失败批次")
            return
        
        # 初始化Tushare和数据库连接
        pro = initialize_tushare()
        conn = connect_db(DB_PATH)
        
        # 解析失败记录并重新获取数据
        success_count = 0
        for i, line in enumerate(failed_lines):
            try:
                print(f"\n正在处理第 {i+1}/{len(failed_lines)} 条记录:")
                print(f"原始日志行: {line.strip()}")
                
                # 解析日志行 - 格式: [时间戳] 批次失败: 代码列表 | 日期范围: 开始日期-结束日期 | 错误: 错误信息
                # 首先提取中括号后的实际内容
                if ']' in line:
                    line_content = line.split(']', 1)[1].strip()
                else:
                    line_content = line.strip()
                
                print(f"提取内容: {line_content}")
                
                # 分割主要部分
                parts = line_content.split('|')
                if len(parts) < 2:
                    print(f"警告: 日志格式不符合预期，无法分割为至少2个部分")
                    continue
                
                # 提取批次代码
                batch_part = parts[0].strip()
                if '批次失败:' not in batch_part:
                    print(f"警告: 未找到'批次失败:'标记")
                    continue
                    
                batch_codes_str = batch_part.split('批次失败:', 1)[1].strip()
                batch_codes = [code.strip() for code in batch_codes_str.split(',')]
                print(f"提取到的代码列表: {batch_codes}")
                print(f"代码数量: {len(batch_codes)}")
                
                # 提取日期范围
                date_part = parts[1].strip()
                if '日期范围:' not in date_part:
                    print(f"警告: 未找到'日期范围:'标记")
                    continue
                    
                date_range_str = date_part.split('日期范围:', 1)[1].strip()
                date_range = date_range_str.split('-')
                if len(date_range) != 2:
                    print(f"警告: 日期范围格式错误: {date_range_str}")
                    continue
                
                start_date, end_date = date_range
                start_date = start_date.strip()
                end_date = end_date.strip()
                print(f"提取到的日期范围: {start_date} 至 {end_date}")
                
                # 验证提取的数据
                if not batch_codes:
                    print("警告: 未提取到有效的代码列表")
                    continue
                    
                if not start_date or not end_date:
                    print("警告: 未提取到有效的日期范围")
                    continue
                
                print(f"\n[重试 {i+1}/{len(failed_lines)}] 正在重新获取失败批次数据...")
                print(f"代码数量: {len(batch_codes)}, 日期范围: {start_date}-{end_date}")
                print(f"第一个代码示例: {batch_codes[0] if batch_codes else '无'}")
                
                # 重新获取数据
                retry_data = get_stock_daily(pro, batch_codes, start_date, end_date)
                if retry_data is not None and not retry_data.empty:
                    # 保存到数据库
                    save_data_to_db(conn, retry_data, DAILY_QUOTES_TABLE)
                    print(f"成功重新获取并保存 {len(retry_data)} 条数据")
                    success_count += 1
                else:
                    print("重试获取数据失败或数据为空")
            
            except Exception as e:
                print(f"处理失败批次时出错: {e}")
                import traceback
                print(f"错误详情: {traceback.format_exc()}")
        
        # 处理完成后，备份并清空失败日志
        if success_count > 0:
            backup_file = f"{FAILED_BATCH_LOG}.{datetime.now().strftime('%Y%m%d%H%M%S')}.bak"
            os.rename(FAILED_BATCH_LOG, backup_file)
            print(f"\n成功处理 {success_count}/{len(failed_lines)} 条失败记录，原日志已备份为 {backup_file}")
        else:
            print(f"\n未能成功处理任何失败记录，原日志保留")
        
        conn.close()
    
    except Exception as e:
        print(f"处理失败批次过程中出错: {e}")
        import traceback
        print(f"错误详情: {traceback.format_exc()}")

def main():
    """主函数"""
    print("=== 股票日线数据获取工具 ===\n")
    
    # 解析命令行参数，支持测试失败批次处理功能
    import sys
    test_failed_batches = False
    if len(sys.argv) > 1 and sys.argv[1] == '--test-failed-batches':
        test_failed_batches = True
        print("\n运行模式: 测试失败批次处理功能")
        process_failed_batches()
        return
    
    print("--- 开始执行 main 函数 ---") # 新增日志
    if not TUSHARE_TOKEN or TUSHARE_TOKEN == 'YOUR_TUSHARE_TOKEN':
        print("错误：请在 .env 文件中设置有效的 TUSHARE_TOKEN！")
        print("或者确保 .env 文件存在于脚本运行目录下。")
        return

    # 初始化API和数据库连接
    pro = initialize_tushare()
    conn = connect_db(DB_PATH)
    
    # 确保日线数据表存在
    create_table_if_not_exists(conn, DAILY_QUOTES_TABLE)

    # 获取数据库中已有的数据记录数
    existing_count = get_existing_data_count(conn, DAILY_QUOTES_TABLE)
    print(f"数据库中已有 {existing_count} 条记录")

    # 检查是否有失败批次需要处理
    if os.path.exists(FAILED_BATCH_LOG) and os.path.getsize(FAILED_BATCH_LOG) > 0:
        print(f"\n检测到失败批次日志文件 {FAILED_BATCH_LOG}")
        print("是否先处理失败批次？(y/n)")
        choice = input().strip().lower()
        if choice == 'y':
            process_failed_batches()

    # 从数据库 ball_code_data 表获取ST_CODE代码
    codes_to_fetch = get_stock_codes_from_db(conn, BALL_CODE_TABLE)
    print(f"--- 从数据库获取到 {len(codes_to_fetch)} 个待处理代码 ---") # 新增日志

    if not codes_to_fetch:
        print("数据库中没有找到ST_CODE代码，程序退出。")
        conn.close()
        return

    print(f"将处理从数据库获取的 {len(codes_to_fetch)} 支ST_CODE。")

    # 批量获取并保存数据
    start_time = time.time()
    daily_data = get_stock_daily(pro, codes_to_fetch, START_DATE, END_DATE)
    
    if daily_data is not None and not daily_data.empty:
        print(f"\n获取到 {len(daily_data)} 条数据，准备保存到数据库...")
        # 检查数据完整性
        required_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close']
        missing_columns = [col for col in required_columns if col not in daily_data.columns]
        if missing_columns:
            print(f"警告：数据缺少必要列: {missing_columns}")
            print(f"当前数据列: {daily_data.columns.tolist()}")
            print("由于缺少必要列，无法保存数据")
        else:
            # 确保表存在
            print("再次确认日线数据表是否存在...")
            create_table_if_not_exists(conn, DAILY_QUOTES_TABLE)
            
            # 保存前先验证数据库连接
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                print("数据库连接正常，开始保存数据...")
                
                # 检查表是否真的存在
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{DAILY_QUOTES_TABLE}'")
                if cursor.fetchone():
                    print(f"确认表 '{DAILY_QUOTES_TABLE}' 存在，继续保存数据")
                    save_data_to_db(conn, daily_data, DAILY_QUOTES_TABLE)
                else:
                    print(f"错误：表 '{DAILY_QUOTES_TABLE}' 不存在，尝试重新创建")
                    create_table_if_not_exists(conn, DAILY_QUOTES_TABLE)
                    save_data_to_db(conn, daily_data, DAILY_QUOTES_TABLE)
            except Exception as db_error:
                print(f"数据库连接异常: {db_error}")
                import traceback
                print(f"错误详情: {traceback.format_exc()}")
                # 尝试重新连接
                print("尝试重新连接数据库...")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = connect_db(DB_PATH)
                create_table_if_not_exists(conn, DAILY_QUOTES_TABLE)
                save_data_to_db(conn, daily_data, DAILY_QUOTES_TABLE)
    else:
        print("未获取到数据，跳过保存步骤")
    
    # 获取更新后的数据记录数
    updated_count = get_existing_data_count(conn, DAILY_QUOTES_TABLE)
    new_records = updated_count - existing_count
    elapsed_time = time.time() - start_time
    
    print(f"\n数据更新完成，耗时: {elapsed_time:.2f}秒")
    print(f"数据库现有 {updated_count} 条记录，本次新增 {new_records} 条记录")

    # 询问是否处理失败批次
    if os.path.exists(FAILED_BATCH_LOG) and os.path.getsize(FAILED_BATCH_LOG) > 0:
        print(f"\n检测到失败批次日志文件 {FAILED_BATCH_LOG}")
        print("是否处理失败批次？(y/n)")
        choice = input().strip().lower()
        if choice == 'y':
            process_failed_batches()

    print("\n所有指定ST_CODE数据处理完毕！")
    conn.close()
    print("数据库连接已关闭。")

if __name__ == "__main__":
    main()