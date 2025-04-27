import sqlite3
import pandas as pd

# 连接到数据库
conn = sqlite3.connect('f:/8hProject/fourNOrules/stock_data.db')

# 读取数据到DataFrame
df = pd.read_sql_query("SELECT * FROM daily_quotes", conn)

# 查看数据
print(df.head())

# 关闭连接
conn.close()