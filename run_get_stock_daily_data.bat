@echo off
REM 设置当前目录为批处理文件所在的目录
REM 这有助于脚本找到 .env 文件和数据库文件
cd /d "%~dp0"

REM 定义 Python 脚本的名称
set PYTHON_SCRIPT="get_stock_daily_data_by_code.py"

REM 定义 Python 解释器的命令
REM 假设 'python' 命令已在系统环境变量中
set PYTHON_EXE="C:\ProgramData\anaconda3\envs\fourNOrules\python.exe"

REM 检查 Python 解释器是否存在
where %PYTHON_EXE% >nul 2>nul
if %errorlevel% neq 0 (
    echo 错误：找不到 Python 解释器。请确保 Python 已安装并添加到系统环境变量 PATH 中。
    goto end
)

REM 运行 Python 脚本
echo 正在运行 %PYTHON_SCRIPT%...
%PYTHON_EXE% %PYTHON_SCRIPT%

REM 检查脚本执行是否成功
if %errorlevel% neq 0 (
    echo 脚本 %PYTHON_SCRIPT% 执行失败，错误码：%errorlevel%
) else (
    echo 脚本 %PYTHON_SCRIPT% 执行成功。
)

:end
REM 暂停以便查看输出（调试时有用，定时任务中通常不需要）
REM pause