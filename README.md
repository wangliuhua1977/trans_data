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
3. **构建 SQL**：每组创建事务脚本：`BEGIN` → `CREATE TABLE IF NOT EXISTS` → `DELETE` → 批量 `INSERT` → `COMMIT`。
4. **加密提交**：使用 AES/CBC/PKCS5Padding 对 SQL 加密（Base64 输出），提交至异步 SQL 服务 `/jobs/submit`。
5. **轮询状态**：指数退避轮询 `/jobs/status`，终止状态：`SUCCEEDED / FAILED / CANCELLED`。
6. **获取结果**：`SUCCEEDED` 后请求 `/jobs/result` 输出 `rowsAffected / actualRows` 等指标。

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
schedule.batchSize=500

# https
https.insecure=true
```

### 关键配置

- **crypto.aesKey / crypto.aesIv**：必填。为空会阻止提交并在 UI 日志中报错。
- **crypto.keyFormat**：`base64` 或 `hex`，用于解析 Key/IV。
- **https.insecure**：`true` 时信任所有证书并关闭 Hostname 校验（默认开启）。
- **schedule.windowStart / schedule.windowEnd**：支持跨午夜，如 `23:00:00` → `02:00:00`。
- **schedule.intervalSeconds**：调度周期；UI 修改后立即生效。
- **schedule.batchSize**：批量插入大小，默认 500。

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

目标表：`leshan.dm_prod_offer_ind_list_leshan`

```sql
CREATE TABLE IF NOT EXISTS leshan.dm_prod_offer_ind_list_leshan (
  order_item_id varchar,
  date_no numeric,
  obj_id varchar,
  ind_type numeric,
  accept_staff_id varchar,
  accept_channel_id varchar,
  first_staff_id varchar,
  second_staff_id varchar,
  dev_staff_id varchar,
  level5_id varchar,
  datetime numeric,
  accept_date varchar
);
CREATE INDEX IF NOT EXISTS idx_dm_prod_offer_ind_list_leshan_date_no_datetime
  ON leshan.dm_prod_offer_ind_list_leshan (date_no, datetime);
```

幂等策略：每次拉取按 `(date_no, datetime)` 分组，每组执行：

1. `BEGIN`
2. `CREATE TABLE IF NOT EXISTS ...`
3. `DELETE FROM ... WHERE date_no=? AND datetime=?`
4. 批量 `INSERT`（按 batchSize 分批）
5. `COMMIT`

数值字段解析失败将写入 `NULL` 并记录 WARN，不会导致整批失败。

## UI 与日志

- **进度**：展示当前阶段（Fetching/Grouping/Encrypting/Submitting/Polling/Writing/Done/Failed）。
- **统计**：拉取条数、分组数、当前组/总组、批次进度、当前 jobId。
- **日志**：每行包含时间戳与级别，且显示 requestId、jobId、耗时等关键信息。

## 排障建议

- **code!=1 或 data 非数组**：源接口返回异常，请检查源接口响应。
- **FAILED/CANCELLED**：查看日志中的 `errorMessage`、`errorPosition` 定位 SQL 失败位置。
- **AES 配置缺失**：UI 日志会提示缺失字段，补全后再执行。
- **HTTPS 证书异常**：可保持 `https.insecure=true`，生产环境请谨慎开启。
