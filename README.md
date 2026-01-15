# trans_data

`trans_data` 是一个 Swing 桌面数据传输工具：定时从 HTTP POST 接口拉取 JSON 数据，按 `(date_no, datetime)` 分组后通过 Leshan 异步 SQL 服务写入 PostgreSQL（逻辑库 `leshan`）表 `leshan.dm_prod_offer_ind_list_leshan`。

## 功能概览

- Swing 可视化控制台（非 JavaFX），包含配置区、进度区、实时日志区。
- 默认启动即开始调度（00:00:00-23:59:59、180 秒间隔），可“立即执行一次 / 开始 / 暂停”。
- 数据处理全程后台线程执行，EDT 仅做 UI 渲染，确保界面不卡死。
- HTTPS 支持“信任所有证书 + 关闭 Hostname 校验”，并提供开关（默认开启）。

## 端到端流程

1. **拉取数据**：HTTP POST 请求数据源接口，`code==1` 且 `data` 为数组才进入写入流程。
2. **解析与分组**：把 `data` 解析为记录并按 `(date_no, datetime)` 分组。
3. **存在性检查**：仅对本次运行的首个 `(date_no, datetime)` 执行 `SELECT 1 ... LIMIT 1`；若已存在则跳过本次运行。
4. **分段插入**：存在性检查通过后，将所有记录按固定 100 条一段构建 `INSERT` 语句（不按分组边界切分）。
5. **加密提交**：使用 AES/CBC/PKCS5Padding 对 SQL 加密（Base64 输出），逐段提交至异步 SQL 服务 `/jobs/submit`。
6. **轮询状态**：指数退避轮询 `/jobs/status`，终止状态：`SUCCEEDED / FAILED / CANCELLED`。
7. **获取结果**：`SUCCEEDED` 后在需要时请求 `/jobs/result` 输出 `rowsAffected / actualRows` 等指标。

## 构建与运行

```powershell
mvn -q -DskipTests package
java -jar target/trans_data-1.0.0.jar
```

## 配置说明

默认配置文件：`src/main/resources/config.properties`（作为模板）。
运行时配置会自动持久化到：`~/.trans_data/config.properties`。

```properties
# source
source.url=http://sctelyidalowcode.paas.sc.ctc.com/app-api/YD2502252B0X/mysql
source.header.x-app-id=940
source.header.easy-app-key=
source.body={}

# async sql
asyncsql.baseUrl=https://leshan.paas.sc.ctc.com/waf/api
asyncsql.token=
asyncsql.dbUser=leshan

# crypto
crypto.aesKey=
crypto.aesIv=
crypto.keyFormat=base64

# schedule defaults
schedule.enabled=true
schedule.intervalSeconds=180
schedule.windowStart=00:00:00
schedule.windowEnd=23:59:59

# logging
logging.sql.maxChars=20000
logging.sql.dumpDir=logs/sql

# https
https.insecure=true

# async sql
asyncsql.maxWaitSeconds=900
```

### 关键配置

- **crypto.aesKey / crypto.aesIv**：必填。为空会阻止提交并在 UI 日志中报错。
- **crypto.keyFormat**：`base64` 或 `hex`，用于解析 Key/IV。
- **https.insecure**：`true` 时信任所有证书并关闭 Hostname 校验（默认开启）。
- **schedule.windowStart / schedule.windowEnd**：支持跨午夜，如 `23:00:00` → `02:00:00`。
- **schedule.intervalSeconds**：调度周期；UI 修改后立即生效。
- **分段大小**：固定 100 行/段（不再使用可配置 batchSize）。
- **logging.sql.maxChars**：SQL/JSON 日志最大展示字符数，超过阈值会截断输出。
- **logging.sql.dumpDir**：SQL 超长时的落盘目录（按日期分目录）。
- **asyncsql.maxWaitSeconds**：轮询最大等待时长（秒），超时会结束轮询并记录错误。

> 注意：EASY-APP-KEY / X-Request-Token / AES Key/IV 属于敏感信息，**不得**写入日志或文档示例中。请在本地用户配置文件中填写。

## 异步 SQL 服务协议说明

所有请求使用 `Content-Type: application/json;charset=UTF-8`，并在配置提供时设置 `X-Request-Token`。加密后的字段为 `encryptedSql`：

- **submit**：`POST /jobs/submit`
  - 请求字段：`dbUser`、`encryptedSql`、`keyFormat`
  - 响应字段（示例）：`jobId`、`status`、`errorMessage`、`errorPosition`

- **status**：`POST /jobs/status`
  - 请求字段：`jobId`
  - 响应字段（示例）：`status`、`errorMessage`、`errorPosition`、`rowsAffected`、`actualRows`

- **result**：`POST /jobs/result`
  - 请求字段：`jobId`
  - 响应字段（示例）：`rowsAffected`、`actualRows`、`errorMessage`、`errorPosition`

轮询策略：起始 500ms，乘 1.5，最大 2000ms；支持取消并显示 `FAILED / CANCELLED` 的错误信息。

## 表结构与幂等策略

目标表：`leshan.dm_prod_offer_ind_list_leshan`（由外部系统管理，本工具不会执行任何 DDL）。

幂等策略：每次拉取按 `(date_no, datetime)` 分组，仅检查首个 groupKey 是否已存在，若存在则跳过本次运行。
存在性检查 SQL：

```sql
SELECT 1
FROM leshan.dm_prod_offer_ind_list_leshan
WHERE date_no = <date_no> AND datetime = <datetime>
LIMIT 1;
```

当首个 groupKey 不存在时，按照固定 100 行/段执行 `INSERT`，不执行任何 `DELETE`，不添加索引，也不包裹事务。

数值字段解析失败将写入 `NULL` 并记录 WARN，不会导致整批失败。

## UI 与日志

- **进度**：展示当前阶段（Fetching/Grouping/Checking/Encrypting/Submitting/Polling/Writing/Done/Skipped/Failed）。
- **统计**：拉取条数、分组数、分段进度、当前 jobId。
- **轮询状态**：轮询阶段会显示当前状态、耗时与进度百分比（若后端返回），并仅在状态变化时记录日志。
- **SQL 记录**：提交前会输出实际 SQL。若 SQL 超过 `logging.sql.maxChars`，日志显示前后片段并提示落盘路径，完整 SQL 写入 `logging.sql.dumpDir/{yyyyMMdd}/sql_{requestId}_{context}_{groupKey}.sql`。
- **日志**：每行包含时间戳与级别，且显示 requestId、jobId、耗时等关键信息；取消/中断会明确标记为“Cancelled by user”或“Polling interrupted by stop()”。

## 排障建议

- **code!=1 或 data 非数组**：源接口返回异常，请检查源接口响应。
- **FAILED/CANCELLED**：查看日志中的 `errorMessage`、`errorPosition` 定位 SQL 失败位置。
- **AES 配置缺失**：UI 日志会提示缺失字段，补全后再执行。
- **HTTPS 证书异常**：可保持 `https.insecure=true`，生产环境请谨慎开启。
