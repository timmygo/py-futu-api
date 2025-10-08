# TSLA Options Collector

## 概览
该工具封装了从富途 OpenD 获取 TSLA 当前周期权链 → Delta 预筛 → 实时 QUOTE/TICKER 回调入库 DuckDB → CSV/Parquet 导出的最小流程。所有 Delta 筛选均固定在 Δ ∈ [+0.1,+0.9] ∪ [-0.9,-0.1] 区间，不提供其他条件。

## 前置条件
- 已启动并登录富途 OpenD，且具备美股期权行情权限。
- Python 环境已安装 `duckdb`、`pandas` 等依赖（安装 `py-futu-api` 时会自动拉取）。
- 机器可访问 OpenD 所在的 `host:port`。

## CLI 用法
```bash
python -m futu.tools.tsla_options_collector collect \
  --host 127.0.0.1 \
  --port 11111 \
  --db-path ./tsla_options.duckdb \
  --duration 900 \
  --log-level INFO
```
- `--duration` 省略时将持续运行，按 `Ctrl+C` 退出。
- 默认日志输出到终端，可通过 `--log-path /path/to/run.log` 重定向到文件。

示例日志片段：
```
2024-03-22T09:30:01 [INFO] tsla_options_collector: Using expiration 2024-03-22
2024-03-22T09:30:02 [INFO] tsla_options_collector: Subscribed realtime streams for 42 codes
2024-03-22T09:30:04 [DEBUG] tsla_options_collector: Stored 4 quote rows
2024-03-22T09:45:10 [WARNING] tsla_options_collector: Quote connection lost
2024-03-22T09:45:12 [INFO] tsla_options_collector: 断开→重连→已恢复 84 条订阅
```

## 数据落库
DuckDB 中创建两张表：
- `option_quotes`：逐条写入 QUOTE 回调的所有字段，附带 `raw_json` 与 `ingested_at`。
- `option_tickers`：逐条写入 TICKER 回调字段，同样保留 `raw_json` 与 `ingested_at`。

可使用 DuckDB 交互式查询验证：
```sql
SELECT code, data_time, last_price, delta
FROM option_quotes
ORDER BY ingested_at DESC
LIMIT 10;
```

## 数据导出
```bash
python -m futu.tools.tsla_options_collector export \
  --db-path ./tsla_options.duckdb \
  --table quotes \
  --format parquet \
  --start 2024-03-22T09:30:00 \
  --end 2024-03-22T16:00:00 \
  --output ./exports/quotes_20240322.parquet
```
- `--table tickers` 可导出逐笔数据。
- 省略 `--start/--end` 将导出全部数据。

## 工作原理
1. 通过 [Get Option Expiration Date](https://openapi.futunn.com/futu-api-doc/en/quote/get-option-expiration-date.html?utm_source=chatgpt.com) 获取 TSLA 全部到期日，选取本周到期；再调用 [Get Option Chain](https://openapi.futunn.com/futu-api-doc/en/quote/get-option-chain.html?utm_source=chatgpt.com) 拉取当日全部合约。
2. 先 `subscribe` QUOTE（`subscribe_push=False`），再调用 [Get Real-time Quote](https://openapi.futunn.com/futu-api-doc/en/quote/get-stock-quote.html?utm_source=chatgpt.com) 批量读取 Delta，筛除不在固定区间内的合约并释放订阅额度。
3. 对入选合约订阅 QUOTE+TICKER，并基于 [Subscribe and Unsubscribe](https://openapi.futunn.com/futu-api-doc/en/quote/sub.html?utm_source=chatgpt.com) 的机制监听回调、处理断线重连。
4. QUOTE/TICKER 回调写入 DuckDB，依赖 `COPY` 语句导出到 CSV/Parquet 以便后续分析。

## 常见问题
- **提示权限不足**：请确认账号具备美股期权行情权限且已登录 OpenD。
- **无任何合约入选**：可能当前周到期合约 Delta 均超出设定区间，可适当延长运行时长等待行情变化。
- **导出文件为空**：确认导出时指定的 `--start/--end` 覆盖了采集时间窗口，且数据库路径无误。

