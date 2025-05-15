"""
模块功能：管理自选股列表，包括添加、删除、查看，并支持从文件批量导入。
"""
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import sqlite3
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime
# from get_stock_basic_info import fetch_stock_basic_info # 不再需要，信息将从 ball_code_data 获取

# 加载 .env 文件中的环境变量
load_dotenv()

# 数据库配置
INDEX_DB_PATH = os.getenv('INDEX_DB_PATH', 'stock_index_data.db')
WATCHLIST_TABLE_NAME = os.getenv('WATCHLIST_TABLE_NAME', 'my_watchlist')
STOCK_DATA_DB_PATH = os.getenv('STOCK_DATA_DB_PATH', 'stock_data.db') # 包含 ball_code_data 表的数据库路径

def connect_db(db_path):
    """连接到 SQLite 数据库"""
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error as e:
        messagebox.showerror("数据库错误", f"连接数据库 {db_path} 失败: {e}")
        return None

def create_watchlist_table(conn, table_name):
    """如果自选股表不存在，则创建该表"""
    if conn is None:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ts_code TEXT NOT NULL,
            group_name TEXT NOT NULL,
            symbol TEXT,
            name TEXT,
            area TEXT,
            industry TEXT,
            market TEXT,
            list_date TEXT,
            add_date TEXT,
            notes TEXT,
            act_ent_type TEXT, -- 新增字段：企业实体类型
            PRIMARY KEY (ts_code, group_name) -- 复合主键
        )
        """)
        conn.commit()
        # 检查是否已存在 'group_name' 列，如果不存在则添加 (兼容旧表结构)
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in cursor.fetchall()]
        if 'group_name' not in columns:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN group_name TEXT")
            conn.commit()
            print(f"已向表 {table_name} 添加 group_name 列。")
        if 'notes' not in columns:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN notes TEXT")
            conn.commit()
            print(f"已向表 {table_name} 添加 notes 列。")
        if 'act_ent_type' not in columns:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN act_ent_type TEXT")
            conn.commit()
            print(f"已向表 {table_name} 添加 act_ent_type 列。")
        print(f"自选股数据表 '{table_name}' 检查/创建完毕。")
        return True
    except sqlite3.Error as e:
        messagebox.showerror("数据库错误", f"创建或检查表 '{table_name}' 时出错: {e}")
        return False

def add_stock_to_watchlist(conn, ts_code, group_name="默认分组", notes="", silent_mode=False):
    """
    从 ball_code_data 获取股票基本信息并添加到自选股列表 (my_watchlist)。

    Args:
        conn: 连接到 stock_index_data.db (包含 my_watchlist) 的数据库连接对象。
        ts_code (str): 股票的TS代码 (例如 '000001.SZ')。
        group_name (str): 股票所属的分组名称。
        notes (str): 备注信息。
        silent_mode (bool): 是否为静默模式。True时，不显示messagebox。

    Returns:
        bool: True 如果成功添加或已存在，False 如果失败。
    """
    if not conn: # conn is for INDEX_DB_PATH (my_watchlist)
        if not silent_mode:
            messagebox.showerror("错误", "目标数据库 (my_watchlist) 未连接。")
        return False
    if not ts_code:
        if not silent_mode:
            messagebox.showerror("输入错误", "股票代码不能为空。")
        return False

    # 检查股票是否已在观察列表 (my_watchlist) 中指定的分组
    cursor = conn.cursor()
    cursor.execute(f"SELECT 1 FROM {WATCHLIST_TABLE_NAME} WHERE ts_code = ? AND group_name = ?", (ts_code, group_name))
    if cursor.fetchone():
        if not silent_mode:
            messagebox.showinfo("提示", f"股票 {ts_code} 已在自选列表的 '{group_name}' 分组中。")
        return True

    # 从 stock_data.db 的 ball_code_data 表获取股票基本信息
    stock_info_from_ball_code = None
    conn_stock_data = None
    try:
        conn_stock_data = connect_db(STOCK_DATA_DB_PATH) # connect_db is already defined
        if not conn_stock_data:
            if not silent_mode:
                messagebox.showerror("数据库错误", f"无法连接到源数据库 (stock_data.db): {STOCK_DATA_DB_PATH}")
            return False

        cursor_stock_data = conn_stock_data.cursor()
        # PRD 确认 ball_code_data 包含: ts_code, symbol, name, area, industry, market, list_date, act_ent_type
        query_columns = "ts_code, symbol, name, area, industry, market, list_date, act_ent_type"
        cursor_stock_data.execute(f"SELECT {query_columns} FROM ball_code_data WHERE ts_code = ?", (ts_code,))
        row = cursor_stock_data.fetchone()

        if row:
            stock_info_from_ball_code = {
                'ts_code': row[0],
                'symbol': row[1],
                'name': row[2],
                'area': row[3],
                'industry': row[4],
                'market': row[5],
                'list_date': row[6],
                'act_ent_type': row[7] if len(row) > 7 else None # 兼容可能不存在该列的情况
            }
            if not stock_info_from_ball_code.get('ts_code'):
                if not silent_mode:
                    messagebox.showerror("数据错误", f"从 ball_code_data 查询到股票 {ts_code}，但ts_code字段为空。")
                return False
        else:
            if not silent_mode:
                messagebox.showerror("查询失败", f"未能从源数据库 ball_code_data 中找到股票 {ts_code} 的基本信息。")
            return False
            
    except sqlite3.Error as e:
        if not silent_mode:
            messagebox.showerror("数据库查询错误", f"从源数据库 ball_code_data 查询股票 {ts_code} 时出错: {e}")
        return False
    finally:
        if conn_stock_data:
            conn_stock_data.close()

    if stock_info_from_ball_code is None:
        if not silent_mode:
             messagebox.showerror("未知错误", f"获取股票 {ts_code} 信息时发生未知内部错误。")
        return False

    add_date = datetime.now().strftime('%Y%m%d')

    try:
        insert_query = f"""
        INSERT INTO {WATCHLIST_TABLE_NAME} 
        (ts_code, group_name, symbol, name, area, industry, market, list_date, add_date, notes, act_ent_type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, (
            stock_info_from_ball_code.get('ts_code'),
            group_name, # group_name 是主键的一部分
            stock_info_from_ball_code.get('symbol'),
            stock_info_from_ball_code.get('name'),
            stock_info_from_ball_code.get('area'),
            stock_info_from_ball_code.get('industry'),
            stock_info_from_ball_code.get('market'),
            stock_info_from_ball_code.get('list_date'),
            add_date,
            notes,
            stock_info_from_ball_code.get('act_ent_type')
        ))
        conn.commit()
        if not silent_mode:
            stock_name = stock_info_from_ball_code.get('name', ts_code)
            messagebox.showinfo("成功", f"股票 {stock_name} ({ts_code}) 已成功添加到自选列表 '{group_name}' 分组。")
        return True
    except sqlite3.IntegrityError: # 主要捕获主键冲突 (ts_code, group_name)
        if not silent_mode:
            messagebox.showinfo("提示", f"股票 {ts_code} 已在自选列表的 '{group_name}' 分组中 (主键冲突)。")
        return True 
    except sqlite3.Error as e:
        if not silent_mode:
            messagebox.showerror("数据库错误", f"添加股票 {ts_code} 到自选列表 (my_watchlist) 时发生错误: {e}")
        conn.rollback()
        return False

class WatchlistApp:
    def __init__(self, root):
        self.root = root
        self.root.title("自选管理")
        self.root.geometry("800x600")

        self.conn = connect_db(INDEX_DB_PATH)
        if self.conn:
            create_watchlist_table(self.conn, WATCHLIST_TABLE_NAME)
        else:
            # 如果数据库连接失败，禁用部分UI或给出提示
            messagebox.showerror("严重错误", "无法连接到数据库，应用功能将受限。")
            # return # 可以考虑直接退出或禁用所有数据库相关操作

        # --- 样式 --- #
        style = ttk.Style()
        style.theme_use('clam') # 'clam', 'alt', 'default', 'classic'
        style.configure("TLabel", padding=5, font=('Helvetica', 10))
        style.configure("TButton", padding=5, font=('Helvetica', 10))
        style.configure("TEntry", padding=5, font=('Helvetica', 10))
        style.configure("TCombobox", padding=5, font=('Helvetica', 10))
        style.configure("Treeview.Heading", font=('Helvetica', 10, 'bold'))

        # --- 主框架 --- #
        main_frame = ttk.Frame(root, padding="10 10 10 10")
        main_frame.pack(expand=True, fill=tk.BOTH)

        # --- 输入区域 --- #
        input_frame = ttk.LabelFrame(main_frame, text="添加自选股", padding="10 10 10 10")
        input_frame.pack(fill=tk.X, pady=10)

        ttk.Label(input_frame, text="股票代码 (如 000001.SZ):", width=20).grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.ts_code_entry = ttk.Entry(input_frame, width=30)
        self.ts_code_entry.grid(row=0, column=1, padx=5, pady=5, sticky=tk.EW)

        ttk.Label(input_frame, text="分组名称:", width=20).grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        self.group_name_entry = ttk.Entry(input_frame, width=30)
        self.group_name_entry.insert(0, "默认分组")
        self.group_name_entry.grid(row=1, column=1, padx=5, pady=5, sticky=tk.EW)
        
        ttk.Label(input_frame, text="备注:", width=20).grid(row=2, column=0, padx=5, pady=5, sticky=tk.W)
        self.notes_entry = ttk.Entry(input_frame, width=30)
        self.notes_entry.grid(row=2, column=1, padx=5, pady=5, sticky=tk.EW)

        self.add_button = ttk.Button(input_frame, text="添加股票", command=self.add_stock_gui)
        self.add_button.grid(row=0, column=2, rowspan=3, padx=10, pady=5, sticky=tk.NS)
        
        # --- 批量导入按钮 --- #
        self.upload_button = ttk.Button(input_frame, text="从文件批量导入", command=self.upload_watchlist_file)
        self.upload_button.grid(row=0, column=3, rowspan=3, padx=10, pady=5, sticky=tk.NS)

        input_frame.columnconfigure(1, weight=1) # 让输入框可伸缩

        # --- 状态栏 --- #
        self.status_label = ttk.Label(main_frame, text="状态: 就绪", relief=tk.SUNKEN, anchor=tk.W)
        self.status_label.pack(side=tk.BOTTOM, fill=tk.X, pady=(5,0))

        # --- 自选股列表显示 --- #
        list_frame = ttk.LabelFrame(main_frame, text="我的自选股", padding="10 10 10 10")
        list_frame.pack(expand=True, fill=tk.BOTH, pady=10)

        self.tree = ttk.Treeview(list_frame, columns=("ts_code", "name", "group_name", "industry", "list_date", "add_date", "notes", "act_ent_type"), show="headings")
        self.tree.heading("ts_code", text="TS代码")
        self.tree.heading("name", text="名称")
        self.tree.heading("group_name", text="分组")
        self.tree.heading("industry", text="行业")
        self.tree.heading("list_date", text="上市日期")
        self.tree.heading("add_date", text="添加日期")
        self.tree.heading("notes", text="备注")
        self.tree.heading("act_ent_type", text="企业类型")

        self.tree.column("ts_code", width=100, anchor=tk.W)
        self.tree.column("name", width=100, anchor=tk.W)
        self.tree.column("group_name", width=80, anchor=tk.W)
        self.tree.column("industry", width=120, anchor=tk.W)
        self.tree.column("list_date", width=80, anchor=tk.CENTER)
        self.tree.column("add_date", width=80, anchor=tk.CENTER)
        self.tree.column("notes", width=150, anchor=tk.W)
        self.tree.column("act_ent_type", width=100, anchor=tk.W)

        # 滚动条
        vsb = ttk.Scrollbar(list_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(list_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self.tree.pack(expand=True, fill=tk.BOTH)

        # 右键菜单
        self.popup_menu = tk.Menu(self.root, tearoff=0)
        self.popup_menu.add_command(label="删除选中股票", command=self.delete_selected_stock)
        self.tree.bind("<Button-3>", self.show_popup_menu) # 绑定右键点击事件

        self.load_watchlist()

    def update_status(self, message):
        self.status_label.config(text=f"状态: {message}")
        print(message) # 同时打印到控制台

    def add_stock_gui(self):
        ts_code = self.ts_code_entry.get().strip()
        group_name = self.group_name_entry.get().strip()
        notes = self.notes_entry.get().strip()

        if not ts_code:
            messagebox.showerror("错误", "股票代码不能为空！")
            return
        if not group_name:
            group_name = "默认分组"
        
        self.update_status(f"正在添加 {ts_code}...")
        if self.conn:
            # 手动添加时，不使用静默模式
            if add_stock_to_watchlist(self.conn, ts_code, group_name, notes, silent_mode=False):
                self.update_status(f"股票 {ts_code} 处理完成。")
                self.load_watchlist() # 重新加载列表
                self.ts_code_entry.delete(0, tk.END)
                # self.notes_entry.delete(0, tk.END) # 保留分组，清空备注和代码
            else:
                self.update_status(f"添加股票 {ts_code} 失败。")
        else:
            messagebox.showerror("错误", "数据库未连接，无法添加股票。")
            self.update_status("数据库连接失败。")

    def load_watchlist(self):
        if not self.conn:
            self.update_status("数据库未连接，无法加载自选股。")
            return
        
        # 清空现有列表
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        try:
            cursor = self.conn.cursor()
            # 更新查询以包含 act_ent_type
            cursor.execute(f"SELECT ts_code, name, group_name, industry, list_date, add_date, notes, act_ent_type FROM {WATCHLIST_TABLE_NAME} ORDER BY group_name, ts_code")
            rows = cursor.fetchall()
            for row in rows:
                self.tree.insert("", tk.END, values=row)
            self.update_status(f"成功加载 {len(rows)} 条自选股记录。")
        except sqlite3.Error as e:
            messagebox.showerror("数据库错误", f"加载自选股列表失败: {e}")
            self.update_status(f"加载自选股列表失败: {e}")

    def delete_selected_stock(self):
        selected_items = self.tree.selection()
        if not selected_items:
            messagebox.showinfo("提示", "请先选择要删除的股票。")
            return

        if not self.conn:
            messagebox.showerror("错误", "数据库未连接，无法删除。")
            return

        confirm = messagebox.askyesno("确认删除", f"确定要删除选中的 {len(selected_items)} 条记录吗？")
        if confirm:
            deleted_count = 0
            try:
                cursor = self.conn.cursor()
                for item_id in selected_items:
                    values = self.tree.item(item_id, 'values')
                    ts_code_to_delete = values[0]
                    group_name_to_delete = values[2] # group_name 在第三列 (索引2)
                    cursor.execute(f"DELETE FROM {WATCHLIST_TABLE_NAME} WHERE ts_code = ? AND group_name = ?", (ts_code_to_delete, group_name_to_delete))
                    deleted_count += 1
                self.conn.commit()
                messagebox.showinfo("成功", f"成功删除了 {deleted_count} 条记录。")
                self.load_watchlist() # 重新加载
            except sqlite3.Error as e:
                messagebox.showerror("数据库错误", f"删除股票时出错: {e}")
                self.conn.rollback()
            finally:
                self.update_status(f"删除操作完成，{deleted_count} 条记录被删除。")
    
    def show_popup_menu(self, event):
        """在指定位置显示右键菜单"""
        # 确保有项目被选中时才显示菜单
        if self.tree.selection():
            self.popup_menu.post(event.x_root, event.y_root)

    def upload_watchlist_file(self):
        """处理文件上传并批量导入自选股"""
        if not self.conn:
            messagebox.showerror("错误", "数据库未连接，无法导入。")
            self.update_status("数据库连接失败，无法导入文件。")
            return

        file_path = filedialog.askopenfilename(
            title="选择自选股文件",
            filetypes=(("CSV 文件", "*.csv"), ("文本文件", "*.txt"), ("所有文件", "*.*"))
        )

        if not file_path:
            self.update_status("未选择文件，导入取消。")
            return

        self.update_status(f"开始处理文件: {file_path}")
        
        # 尝试确定文件类型并解析
        added_count = 0
        failed_count = 0
        skipped_count = 0
        
        try:
            if file_path.lower().endswith('.csv'):
                df = pd.read_csv(file_path, dtype=str) # 读取所有列为字符串以避免类型问题
            elif file_path.lower().endswith('.txt'):
                # 假设TXT文件每行一个ts_code，或者ts_code,group_name (逗号分隔)
                # 更复杂的TXT解析可能需要用户指定格式
                data = []
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'): # 跳过空行和注释行
                            continue
                        parts = line.split(',')
                        if len(parts) == 1:
                            data.append({'ts_code': parts[0].strip(), 'group_name': '默认分组(导入)'})
                        elif len(parts) >= 2:
                            data.append({'ts_code': parts[0].strip(), 'group_name': parts[1].strip()})
                        # 可以根据需要添加更多列的解析
                df = pd.DataFrame(data)
            else:
                messagebox.showerror("文件错误", "不支持的文件类型。请选择CSV或TXT文件。")
                self.update_status("不支持的文件类型。")
                return

            if 'ts_code' not in df.columns:
                messagebox.showerror("文件内容错误", "文件中必须包含 'ts_code' 列。")
                self.update_status("文件缺少 'ts_code' 列。")
                return

            total_rows = len(df)
            self.update_status(f"文件解析成功，共 {total_rows} 条记录待处理。")

            for index, row in df.iterrows():
                ts_code = row['ts_code'].strip()
                group_name = row.get('group_name', '默认分组(导入)').strip()
                notes = row.get('notes', '').strip()
                
                if not ts_code:
                    failed_count +=1
                    print(f"跳过空代码，行号: {index + 1}")
                    continue
                
                # 检查是否已存在 (ts_code, group_name 联合检查)
                cursor = self.conn.cursor()
                cursor.execute(f"SELECT 1 FROM {WATCHLIST_TABLE_NAME} WHERE ts_code = ? AND group_name = ?", (ts_code, group_name))
                if cursor.fetchone():
                    skipped_count += 1
                    print(f"股票 {ts_code} 在分组 '{group_name}' 中已存在，跳过。")
                    continue
                
                # 批量导入时，使用静默模式
                if add_stock_to_watchlist(self.conn, ts_code, group_name, notes, silent_mode=True):
                    added_count += 1
                    print(f"成功添加 {ts_code} 到分组 '{group_name}'")
                else:
                    failed_count += 1
                    print(f"添加 {ts_code} 失败。")
                
                # 可以在这里添加一个小的延时，如果API调用频繁的话
                # import time
                # time.sleep(0.1) # 例如，每条记录处理后暂停0.1秒

            self.load_watchlist() # 完成后刷新列表
            summary_message = f"批量导入完成。成功添加: {added_count}, 已存在跳过: {skipped_count}, 失败: {failed_count}。"
            messagebox.showinfo("导入结果", summary_message)
            self.update_status(summary_message)

        except pd.errors.EmptyDataError:
            messagebox.showerror("文件错误", "选择的文件为空或格式不正确。")
            self.update_status("文件为空或格式不正确。")
        except FileNotFoundError:
            messagebox.showerror("文件错误", f"文件 {file_path} 未找到。")
            self.update_status(f"文件 {file_path} 未找到。")
        except Exception as e:
            messagebox.showerror("导入错误", f"处理文件时发生未知错误: {e}")
            self.update_status(f"处理文件时发生错误: {e}")

    def on_closing(self):
        if self.conn:
            self.conn.close()
            print("数据库连接已关闭。")
        self.root.destroy()

if __name__ == '__main__':
    root = tk.Tk()
    app = WatchlistApp(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing) # 处理窗口关闭事件
    root.mainloop()