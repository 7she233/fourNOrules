"""
1. 将tu_code更新为ball_code
2. 生成一个Excel文件
3. 将数据保存到数据库SQLite

"""

import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox
import csv
import sqlite3
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 获取数据库配置
DB_PATH = os.getenv('DB_PATH', 'stock_data.db')
BALL_CODE_TABLE = os.getenv('BALL_CODE_TABLE', 'ball_code_data')

def create_table_if_not_exists(conn):
    """创建数据表（如果不存在）"""
    cursor = conn.cursor()
    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {BALL_CODE_TABLE} (
        ts_code TEXT PRIMARY KEY,
        symbol TEXT,
        name TEXT,
        area TEXT,
        industry TEXT,
        cnspell TEXT,
        market TEXT,
        list_date TEXT,
        act_name TEXT,
        act_ent_type TEXT,
        fullname TEXT,
        enname TEXT,
        exchange TEXT,
        curr_type TEXT,
        list_status TEXT,
        is_hs TEXT,
        delist_date TEXT,
        ball_code TEXT
    )
    ''')
    conn.commit()

def modify_csv(input_csv_file, output_csv_file, transformation_rules):
    """
    修改 CSV 文件，新增 "ball_code" 列并根据提供的转换规则转换 "ts_code" 的值。
    同时将数据保存到 SQLite 数据库。

    Args:
        input_csv_file (str): 输入 CSV 文件的路径。
        output_csv_file (str): 输出 CSV 文件的路径。
        transformation_rules (dict): 一个字典，键为需要匹配的后缀，值为转换后的前缀。
                                     例如: {'.SZ': 'SZ', '.SH': 'SH', ".BJ": "BJ"}
    """
    try:
        # 读取 CSV 文件，将所有列都作为字符串读取
        df = pd.read_csv(input_csv_file, dtype=str)

        def transform_ts_code(ts_code, rules):
            if isinstance(ts_code, str):
                for suffix, prefix in rules.items():
                    if ts_code.endswith(suffix):
                        return prefix + ts_code[:-len(suffix)]
            return ts_code  # 如果没有匹配的规则，则返回原始值

        df['ball_code'] = df['ts_code'].apply(lambda x: transform_ts_code(x, transformation_rules))

        # 将修改后的 DataFrame 写入新的 CSV 文件，确保所有字段都用引号包围
        df.to_csv(output_csv_file, index=False, quoting=csv.QUOTE_ALL)

        # 保存到 SQLite 数据库
        conn = sqlite3.connect(DB_PATH)
        create_table_if_not_exists(conn)
        df.to_sql(BALL_CODE_TABLE, conn, if_exists='replace', index=False)
        conn.close()

        print(f"成功将 '{input_csv_file}' 文件处理完毕，结果已保存到 '{output_csv_file}' 和数据库 '{DB_PATH}'")

    except FileNotFoundError:
        print(f"错误：找不到文件 '{input_csv_file}'")
    except Exception as e:
        print(f"发生错误：{e}")


def create_gui():
    def select_input_file():
        filename = filedialog.askopenfilename(
            title="选择输入文件",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        input_entry.delete(0, tk.END)
        input_entry.insert(0, filename)

    def process_file():
        input_file = input_entry.get()
        if not input_file:
            messagebox.showerror("错误", "请选择输入文件")
            return
        
        output_file = input_file.rsplit('.', 1)[0] + '_modified.csv'
        rules = {
            '.SZ': 'SZ',
            '.SH': 'SH',
            '.BJ': 'BJ'
        }
        modify_csv(input_file, output_file, rules)
        messagebox.showinfo("成功", f"文件已处理完成，保存为：{output_file}")

    root = tk.Tk()
    root.title("turnGode小工具")
    root.geometry("500x150")

    # 创建输入文件选择框
    frame = tk.Frame(root)
    frame.pack(padx=20, pady=20)

    tk.Label(frame, text="输入文件：").grid(row=0, column=0, sticky='w')
    input_entry = tk.Entry(frame, width=50)
    input_entry.grid(row=0, column=1, padx=5)
    tk.Button(frame, text="浏览", command=select_input_file).grid(row=0, column=2, padx=5)

    # 创建处理按钮
    tk.Button(root, text="开始处理", command=process_file).pack(pady=10)

    root.mainloop()

if __name__ == "__main__":
    create_gui()