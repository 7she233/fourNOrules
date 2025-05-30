
**1. 基本信息**

* **项目/功能名称**: 股票股息自动计算工具
* **您的角色/部门**: 产品负责人/个人开发者
* **主要联系人**: 8h
* **期望完成时间**: 2025年7月31日 (建议日期，可修改)


**2. 背景与目标**

* **当前面临的问题/挑战**: 手工计算股息率工作量大、效率低，**容易出错**，且**难以同时处理和比较**不复权与前复权两种价格下的股息表现。
* **期望解决的核心问题**: 通过Python脚本，**自动化、准确地**计算股票基于**不复权价**和**前复权价**的两套多年股息率及相关指标，**以支持对比分析和投资决策**。
* **业务目标**:
    * 作为选股指标，需要计算**8年、5年、3年、1年**的平均股息及相关指标。
    * 这些不同时间跨度分别用于评估股票的**长期派息稳定性与历史表现（如8年）、近期表现（如1年）以及整体变化趋势（通过8/5/3/1年序列对比）**。
    * N年范围定义为 `[当前年份-N, 当前年份-1]`，其中“当前年份”为脚本**运行时的日历年份**。
    * 分红归属年份严格按照**除权除息日 (`ex_dividend_date`)** 所在的年份确定，这是明确且符合需求的规则。
* **项目价值**:
    * **提升效率**: 显著减少手动计算的时间和精力。
    * **提高准确性**: 确保计算逻辑统一且准确，避免手动错误。
    * **增强决策支持**: 提供**可靠、自动化**的基础数据，并通过提供**不复权与前复权**两种口径的指标，增加数据的**完整性**和未来分析的**灵活性**，为个人投资决策提供更全面的历史数据支持。
* **明确的“不做”范围**:
    * 不做实时行情计算。
    * 不进行手动的复权计算（**完全依赖并信任**数据源提供的前复权价）。


**3. 用户与使用场景**

* **目标用户群体**: 个人，我自己
* **用户数量**: 1人
* **关键使用场景 (User Stories/Use Cases)**:
    * 场景一：用户手工执行Python脚本，脚本自动处理约5500只股票，计算指标数据，指标计算结果保存到数据库表中，并查看日志文件确认执行情况。
* **用户当前如何完成这些任务**: Excel处理（不准确且无法处理复权）。


**4. 核心功能需求**

* **必须具备的功能 (Must-have)**:
    * 功能点1：执行脚本后，通过基础数据计算基于**不复权价**和**前复权价**的两套指标数据，保证计算正确性，并将结果存入数据库。
    * 功能点2：处理上市时间不足、股价数据缺失等异常情况，并记录状态。
    * 功能点3：记录关键执行信息和错误的日志。

* **期望拥有的功能 (Should-have)**: 无
* **可以有的功能 (Could-have)**: 无

* **功能细节描述**:

    ## 数据获取

    从 `stock_data.db` 数据库获取基础数据:

    1.  **表 `ball_code_data`**: 获取股票列表及基本信息。
        * `ts_code`: 股票代码 (主键，如 600519.SH)。
        * `symbol`: 股票代码 (如 600519)。
        * `list_date`: 上市日期。
    2.  **表 `akshare_bonus_details`**: 获取分红数据。
        * `symbol`: 股票代码 (用于与 `ball_code_data` 关联)。
        * `ex_dividend_date`: **除权除息日** (关键字段，用于确定分红年份)。
        * `cash_dividend_ratio`: 每10股分红金额 (元)，可能为 NULL。
    3.  **表 `daily_quotes`**: 获取**不复权**日线股价数据。
        * `ts_code`: 股票代码。
        * `trade_date`: 交易日期。
        * `close`: **不复权**收盘价。
    4.  **表 `daily_quotes_qfq` [新增]**: 获取**前复权**日线股价数据。
        * `ts_code`: 股票代码。
        * `trade_date`: 交易日期。
        * `close`: **前复权**收盘价。
        * **[依赖说明]**: 此表需要由另一个独立的脚本预先生成并维护，且其数据准确性被**完全信任**。

    ## 数据处理

    5.  **分红年份确定**: 使用 `akshare_bonus_details.ex_dividend_date` 的年份来确定每一笔分红所属的年份。**如果 `ex_dividend_date` 为 NULL，则忽略该条分红记录并记录一条 WARNING 日志。**
    6.  **年度分红计算**:
        * 对每个股票，按确定的分红年份进行分组。
        * 将同一年份内的所有 `cash_dividend_ratio` **加总**。
        * 将年度总额**除以 10**，得到该股票该年度的**每股总分红**。
        * 如果某年在 `akshare_bonus_details` 中无记录 (或因 `ex_dividend_date` 为 NULL 被忽略)，或 `cash_dividend_ratio` 为 NULL/0，则该年度每股分红计为 `0`。

    ## 指标计算

    对每个股票，分别计算 N = 8, 5, 3, 1 年的以下指标。N年范围定义为 `[当前年份-N, 当前年份-1]`，其中“当前年份”为**脚本运行时的日历年份**。

    **1. 状态码 (`status_nX`) 计算**:
        * 检查股票 `list_date`。N年周期的起始年份设为 `StartYear = 当前年份 - N`。若 `list_date` > `StartYear-12-31` (即未在起始年的第一年结束前上市)，则 `status_nX = 'ERR_LISTING_TIME'`，该 N 年周期的所有指标记 `NULL`，**停止该N年周期计算，但继续尝试下一个N值**。
        * 统计 N 年周期内（`StartYear-01-01` 到 `EndYear-12-31`） `daily_quotes` 的实际记录数 (`ActualDays`)。理论交易日数估算为 `TheoreticalDays = N * 252`。若 `(TheoreticalDays - ActualDays) / TheoreticalDays >= 0.1`，则 `status_nX = 'ERR_PRICE_DATA'`，该 N 年周期的所有指标记 `NULL`，**停止该N年周期计算，但继续尝试下一个N值**。
        * 若以上都通过，`status_nX = 'OK'`。

    **2. {N}年平均股价 (`avg_price_nX_unadj`, `avg_price_nX_fadj`)**:
        * 若 `status_nX = 'OK'`：
            * `avg_price_nX_unadj`: 取 N 年周期内 `daily_quotes.close` > 0 的值，计算**算术平均值**。
            * `avg_price_nX_fadj`: 取 N 年周期内 `daily_quotes_qfq.close` > 0 的值，计算**算术平均值**。
            * **如果某个周期内没有 > 0 的价格数据，则对应的平均股价记为 `NULL`**。

    **3. {N}年平均分红 (`avg_dividend_nX`)**:
        * 若 `status_nX = 'OK'`：
            * 构建一个包含 N 个年份的序列。
            * 用计算出的“年度每股总分红”填充，**无分红记录的年份计为 `0`**。
            * 计算这 N 个数字的**算术平均值**。

    **4. {N}年平均股息率 (`dividend_yield_nX_unadj`, `dividend_yield_nX_fadj`)**:
        * 若 `status_nX = 'OK'`：
            * `dividend_yield_nX_unadj` = `avg_dividend_nX` / `avg_price_nX_unadj`。
            * `dividend_yield_nX_fadj` = `avg_dividend_nX` / `avg_price_nX_fadj`。
            * **若分母为 `NULL` 或 `<= 0`（安全检查），则该股息率结果也为 `NULL`**。
            * 若分子为 `0` 且分母 `> 0`，则结果为 `0`。
            * **直接存储计算出的浮点数结果**。

    **5. {N}年分红分布**:
        * 若 `status_nX = 'OK'`：
            * `dividend_pattern_nX`: 生成一个N位字符串（左老右新）。若某年“年度每股总分红” `> 0` 则为 'O'，否则为 'X'。
            * `dividend_years_count_nX`: 计算 `dividend_pattern_nX` 中 'O' 的总数。
            * `consecutive_dividend_years_nX`: 从 `dividend_pattern_nX` **最右边**向左计算连续 'O' 的数量，遇 'X' 停止。

    **6. 现价股息率 (`current_yield_unadj`, `current_yield_fadj`)**:
        * 为每只股票，分别从 `daily_quotes` 和 `daily_quotes_qfq` 中查找**最大的 `trade_date`** (`latest_close_date`) 及其对应的 `close` 值，得到 `latest_close_unadj` 和 `latest_close_fadj`。
        * 使用 `avg_dividend_n1` 分别除以 `latest_close_unadj` 和 `latest_close_fadj`。
        * **若分母为 `NULL` 或 `<= 0`，则该股息率结果也为 `NULL`**。
        * 将 `latest_close_date` 存入数据库。

    **7. 上市年限 (`listing_years`)**:
        * 使用**脚本运行当天的日期** (`CurrentDate`)。
        * 计算 `(CurrentDate - list_date).days / 365.25`。

    ## 数据存储

    将计算结果存储到 `stock_index_data.db` 数据库的 `stock_dividend_ratio` 表中。如果记录已存在，则更新；如果不存在，则插入。表结构如下（与原文档一致，并确认 `latest_close_date` 和 `update_time` 含义）：

    ```sql
    CREATE TABLE stock_dividend_ratio (
        -- 核心标识符
        ts_code TEXT PRIMARY KEY NOT NULL,
        symbol TEXT,
        list_date TEXT,
        listing_years REAL, -- (脚本运行日期 - list_date).days / 365.25

        -- 8年期指标 (N=8)
        status_n8 TEXT,
        avg_price_n8_unadj REAL,
        avg_dividend_n8 REAL,
        dividend_yield_n8_unadj REAL,
        avg_price_n8_fadj REAL,
        dividend_yield_n8_fadj REAL,
        dividend_pattern_n8 TEXT,
        dividend_years_count_n8 INTEGER,
        consecutive_dividend_years_n8 INTEGER,

        -- 5年期指标 (N=5)
        status_n5 TEXT,
        avg_price_n5_unadj REAL,
        avg_dividend_n5 REAL,
        dividend_yield_n5_unadj REAL,
        avg_price_n5_fadj REAL,
        dividend_yield_n5_fadj REAL,
        dividend_pattern_n5 TEXT,
        dividend_years_count_n5 INTEGER,
        consecutive_dividend_years_n5 INTEGER,

        -- 3年期指标 (N=3)
        status_n3 TEXT,
        avg_price_n3_unadj REAL,
        avg_dividend_n3 REAL,
        dividend_yield_n3_unadj REAL,
        avg_price_n3_fadj REAL,
        dividend_yield_n3_fadj REAL,
        dividend_pattern_n3 TEXT,
        dividend_years_count_n3 INTEGER,
        consecutive_dividend_years_n3 INTEGER,

        -- 1年期指标 (N=1)
        status_n1 TEXT,
        avg_price_n1_unadj REAL,
        avg_dividend_n1 REAL,
        dividend_yield_n1_unadj REAL,
        avg_price_n1_fadj REAL,
        dividend_yield_n1_fadj REAL,
        dividend_pattern_n1 TEXT,
        dividend_years_count_n1 INTEGER,
        consecutive_dividend_years_n1 INTEGER,

        -- 现价股息率相关
        latest_close_unadj REAL,
        latest_close_fadj REAL,
        latest_close_date TEXT, -- 每只股票自己的最新交易日期
        current_yield_unadj REAL,
        current_yield_fadj REAL,

        -- 更新信息
        update_time TEXT -- 脚本更新该记录的时间戳
    );
    ```


**5. 数据需求**

* **需要处理/展示的关键数据**:
    * 股票基本信息：股票代码、名称、上市日期。
    * 核心指标：各 N 年周期的状态码、平均股价（不复权与前复权）、平均分红、平均股息率（不复权与前复权）、分红模式字符串、分红总年数、最近连续分红年数。
    * 现价相关：最新收盘价（不复权与前复权）、最新收盘日期、现价股息率（不复权与前复权）。
    * 其他：上市年限。

* **数据来源**: `stock_data.db` 数据库中的以下表：
    * **`ball_code_data`**: 提供股票列表及上市日期 (`ts_code`, `symbol`, `list_date`)。
    * **`akshare_bonus_details`**: 提供分红历史数据 (`symbol`, `ex_dividend_date`, `cash_dividend_ratio`)。
    * **`daily_quotes`**: 提供**不复权**日线价格数据 (`ts_code`, `trade_date`, `close`)。
    * **`daily_quotes_qfq` [新增]**: 提供**前复权**日线价格数据 (`ts_code`, `trade_date`, `close`)。
        * **[重要依赖]**: 此表被假定已由其他流程生成并存在于 `stock_data.db` 中，且数据是准确、可信的。

* **数据流转**:
    * **读取**: Python 脚本从 `stock_data.db` 中读取上述四张表的数据。
    * **计算**: 脚本在内存中或通过临时处理，根据“4. 核心功能需求”中定义的逻辑进行数据清洗、关联、聚合和指标计算。
    * **写入**: 最终计算出的所有指标数据，**插入或更新**到 `stock_index_data.db` 数据库的 `stock_dividend_ratio` 表中。

* **数据可视化/报表需求**: 无，本项目仅负责计算和存储结构化数据到数据库。

**6. 非功能性需求**

* **性能要求**: 期望在 30 分钟内完成约 5500 只股票的计算。
* **安全性要求**: 计算数据能正常保存到数据库表。
* **易用性要求**: 无（脚本执行）。
* **兼容性要求**: 支持 Windows 和 Linux 平台运行 Python 脚本。
* **日志要求**:
    * 同时输出到控制台和日志文件 (`dividend_calc.log`)。
    * 记录 INFO, WARNING, ERROR 三级日志。
    * 包含启动/结束、进度、数据警告（跳过原因）、错误信息和最终总结。
* **可维护性/扩展性**: 无。
* **数据归档/清除策略**: 无。
* **国际化与本地化**: 无。

**7. 现有系统/流程**: 无。
**8. 约束与限制**: Python, 1人（Python初学者）。
**9. 假设与依赖**: 基础数据存在且准确，特别是 `pre_close` 和 `ex_dividend_date` 的准确性。
**10. 风险与挑战**: 无开发经验依赖AI；数据量较大，双重计算对性能要求较高。
**11. 成功衡量标准**: 30 分钟内完成指标数据的计算，且结果符合预期逻辑。
**12. 其他信息**: 无。