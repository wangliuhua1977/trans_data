# trans_data

`trans_data` 是一个 Swing 桌面数据传输工具：定时从 HTTP POST 接口拉取 JSON 数据，按 `(date_no, datetime)` 分组后通过 Leshan 异步 SQL 服务写入 PostgreSQL（逻辑库 `leshan`）表 `leshan.dm_prod_offer_ind_list_leshan`。

## 功能概览

- Swing 可视化控制台（非 JavaFX），包含配置区、进度区、实时日志区。
- 默认启动即开始调度（00:00:00-23:59:59、180 秒间隔），可“立即执行一次 / 开始 / 暂停”。
- 数据处理全程后台线程执行，EDT 仅做 UI 渲染，确保界面不卡死。
- HTTPS 支持“信任所有证书 + 关闭 Hostname 校验”，并提供开关（默认开启）。

## 端到端流程

1. **分布式锁**：每次任务启动先抢占 Postgres 锁（`leshan.trans_data_job_lock`），未获取锁则记录 “another instance is running” 并跳过本次运行。
2. **拉取数据**：HTTP POST 请求数据源接口，`code==1` 且 `data` 为数组才进入写入流程。
3. **解析与分组**：把 `data` 解析为记录并按 `(date_no, datetime)` 分组，生成 `runId`。
4. **分段入库（staging）**：按可配置 batchSize 将数据插入临时表 `leshan.stg_dm_prod_offer_ind_list_leshan`（包含 `run_id`）。
5. **应用到目标表**：在事务中删除目标表当前 `runId` 的 `(date_no, datetime)` 范围，再从 staging 按 `run_id` 插入。
6. **稽核校验**：针对当前 `(date_no, datetime)` scope 校验分组 count / 总数 / distinct order_item_id。
7. **失败自动修复**：稽核失败则清理目标 scope 与 staging 并重试；超过最大重试次数则失败并确保无脏数据。
8. **释放锁**：任务成功/失败都会释放锁。

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

# logging
logging.sql.maxChars=20000
logging.sql.dumpDir=logs/sql

# https
https.insecure=true

# async sql
asyncsql.maxWaitSeconds=900

# job lock & retries
lock.name=trans_data_job
lock.leaseSeconds=300
job.maxRetries=3
```

### 关键配置

- **crypto.aesKey / crypto.aesIv**：必填。为空会阻止提交并在 UI 日志中报错。
- **crypto.keyFormat**：`base64` 或 `hex`，用于解析 Key/IV。
- **https.insecure**：`true` 时信任所有证书并关闭 Hostname 校验（默认开启）。
- **schedule.windowStart / schedule.windowEnd**：支持跨午夜，如 `23:00:00` → `02:00:00`。
- **schedule.intervalSeconds**：调度周期；UI 修改后立即生效。
- **schedule.batchSize**：分段大小（默认 500），用于 staging/插入分段。
- **logging.sql.maxChars**：SQL/JSON 日志最大展示字符数，超过阈值会截断输出。
- **logging.sql.dumpDir**：SQL 超长时的落盘目录（按日期分目录）。
- **asyncsql.maxWaitSeconds**：轮询最大等待时长（秒），超时会结束轮询并记录错误。
- **lock.name / lock.leaseSeconds**：分布式锁名称与租约时长（秒）。
- **job.maxRetries**：稽核失败后最多重试次数。

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

目标表：`leshan.dm_prod_offer_ind_list_leshan`（由外部系统管理）。本工具会自动创建并维护以下辅助表：

```sql
CREATE TABLE IF NOT EXISTS leshan.trans_data_job_lock(
  lock_name text PRIMARY KEY,
  owner_id text,
  lease_until timestamptz,
  updated_at timestamptz
);

CREATE TABLE IF NOT EXISTS leshan.stg_dm_prod_offer_ind_list_leshan(
  run_id text,
  order_item_id text,
  date_no numeric,
  obj_id text,
  ind_type numeric,
  accept_staff_id text,
  accept_channel_id text,
  first_staff_id text,
  second_staff_id text,
  dev_staff_id text,
  level5_id text,
  datetime numeric,
  accept_date text
);
CREATE INDEX IF NOT EXISTS idx_stg_dm_prod_offer_run_id
  ON leshan.stg_dm_prod_offer_ind_list_leshan(run_id);
```

幂等策略：每次运行以本次拉取的 `(date_no, datetime)` 组合作为 scope，先删除目标表 scope 内数据，再从 staging 重新插入，并在稽核失败时自动清理重试。不会删除整表。

数值字段解析失败将写入 `NULL` 并记录 WARN，不会导致整批失败。

## 前端开发技术文档（Swing）

- **UI 结构**：配置区（源/异步 SQL/加密/调度）、进度区（阶段+统计+jobId）、日志区（实时日志）。
- **线程模型**：调度/任务均在后台线程执行；UI 更新通过 `ProgressListener` 与日志回调进入 EDT。
- **操作入口**：开始/暂停/立即执行一次均走 `SchedulerService`，并带有取消标志位。
- **日志语义**：每条日志包含阶段、runId/requestId、jobId、耗时与 SQL 片段（必要时落盘）。

## 本轮技术设计要点

- **调度语义**：下一次间隔从“任务成功完成 + 稽核通过”开始计时，避免并发重叠。
- **分布式锁**：基于 `leshan.trans_data_job_lock` 实现跨进程互斥；支持租约续期，确保长任务不被抢占。
- **staging + apply + audit**：先入 staging，再事务性删除 scope 并写入目标表，随后稽核；失败自动清理重试。
- **稽核机制**：按 `(date_no, datetime)` 分组 count 校验 + scope 总数 + distinct `order_item_id` 校验。
- **配置默认值**：`schedule.batchSize=500`、`lock.leaseSeconds=300`、`job.maxRetries=3`。
- **故障自愈**：稽核失败时清理目标 scope 与 staging，确保无脏数据残留。

## UI 与日志

- **进度**：展示当前阶段（Locking/Fetching/Grouping/Preparing/Staging/Applying/Auditing/Cleaning/Encrypting/Submitting/Polling/Done/Skipped/Failed）。
- **统计**：拉取条数、分组数、分段进度、当前 jobId。
- **轮询状态**：轮询阶段会显示当前状态、耗时与进度百分比（若后端返回），并仅在状态变化时记录日志。
- **SQL 记录**：提交前会输出实际 SQL。若 SQL 超过 `logging.sql.maxChars`，日志显示前后片段并提示落盘路径，完整 SQL 写入 `logging.sql.dumpDir/{yyyyMMdd}/sql_{requestId}_{context}_{groupKey}.sql`。
- **日志**：每行包含时间戳与级别，且显示 requestId、jobId、耗时等关键信息；取消/中断会明确标记为“Cancelled by user”或“Polling interrupted by stop()”。

## 排障建议

- **code!=1 或 data 非数组**：源接口返回异常，请检查源接口响应。
- **FAILED/CANCELLED**：查看日志中的 `errorMessage`、`errorPosition` 定位 SQL 失败位置。
- **AES 配置缺失**：UI 日志会提示缺失字段，补全后再执行。
- **HTTPS 证书异常**：可保持 `https.insecure=true`，生产环境请谨慎开启。
- **重复/缺失数据**：检查 audit 日志与清理记录，确认是否出现锁竞争或稽核失败重试。
