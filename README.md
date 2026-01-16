# trans_data

`trans_data` 是一个 Swing 桌面数据传输工具：可配置多个任务，定时从 HTTP POST 接口拉取 JSON 数据，写入 PostgreSQL（逻辑库默认 `leshan`），并以 staging + 稽核 + 修复保证数据一致性。

## 功能概览

- 多任务管理：新增/编辑/删除/启用/停用，每个任务独立日志与进度。
- 任务调度：时间窗口 + 间隔 + 重试 + 租约续期，间隔从“成功完成且稽核通过”后开始计时。
- staging + 稽核 + 修复：全量写入 staging，再按策略写目标表，稽核失败自动清理与重试。
- 插入模板：用占位符把 JSON 字段映射到 SQL 列，支持数值/文本处理。
- 源接口测试：直接展示 HTTP 状态、耗时、响应大小与格式化 JSON。
- 测试样本自动生成：测试源接口后，可自动生成建表 SQL 与插入模板（可编辑后保存）。
- UI 日志最多保留 1000 行，自动丢弃最早记录。

> 注意：界面中不再暴露 “HTTPS 不安全模式” 选项，但配置项仍可保留以兼容旧环境。

## 任务配置持久化（tasks.json）

任务配置以 UTF-8 JSON 存储，路径：

- 默认：`~/.trans_data/tasks.json`（与 `config.properties` 同目录）

### tasks.json 字段结构（示例）

```json
[
  {
    "taskId": "f2f7a5e9-7cdd-4e9f-9fb2-7e0a9c3b5b1a",
    "taskName": "默认任务",
    "enabled": true,
    "sourcePostUrl": "https://example/api",
    "sourcePostBody": "{}",
    "batchSize": 100,
    "schedulePolicy": {
      "intervalSeconds": 180,
      "windowStart": "00:00:00",
      "windowEnd": "23:59:59",
      "maxRetries": 3,
      "leaseSeconds": 300
    },
    "targetConfig": {
      "targetSchema": "leshan",
      "targetTable": "dm_prod_offer_ind_list_leshan",
      "createTableSql": "",
      "insertMode": "TRUNCATE_THEN_APPEND",
      "insertTemplate": "col1,col2 => ${field1?text},${field2?num}",
      "conflictTarget": "",
      "auditSettings": {
        "scopeKeyFields": ["date_no", "datetime"],
        "naturalKeyFields": ["order_item_id"],
        "distinctKeyJsonField": ""
      }
    }
  }
]
```

> 修改任务配置后会立即持久化到 `tasks.json`。

## 插入模板语法（insertTemplate）

格式：

```
列列表 => 值表达式列表
```

- **列列表**：目标表列名，逗号分隔。
- **值表达式列表**：支持占位符 `${字段}` / `${字段?text}` / `${字段?num}`。

占位符说明：

- `${field}` 或 `${field?text}`：等价于 `NULLIF(payload->>'field','')`（空字符串视为 NULL）。
- `${field?num}`：等价于 `NULLIF(payload->>'field','')::numeric`。
- `${field?bool}`：等价于 `NULLIF(payload->>'field','')::boolean`，仅支持 true/false/NULL。
- `${field?jsonb}`：等价于 `payload->'field'`（JSONB 字段，保留原始结构）。

示例：

```
order_item_id,date_no,amount => ${order_item_id?text},${date_no?num},${amount?num}
```

> 任务执行时会从 staging 表 `payload` 中取值并生成 INSERT SELECT。

## 测试样本自动生成 DDL 与插入模板

在“测试”页完成**测试源接口**后，点击“自动生成DDL与模板”：

1. 解析测试返回的真实 JSON，自动定位记录数组（`data`/`rows`/`resultRows`/`data.rows`/`data.data` 等）。
2. 生成 `createTableSql`（PostgreSQL DDL）与 `insertTemplate`，并自动填充到表单。
3. 可在保存前手工修改 DDL 与模板，保存后持久化到 `tasks.json`。

### 类型推断规则（安全优先）

- 数字：全为整数且在 bigint 范围内 → `bigint`；包含小数或超范围 → `numeric`
- 布尔：`boolean`
- 字符串：默认 `text`
- 嵌套对象/数组：不自动展开，统一落在 `payload jsonb`

默认强制添加公共列：

- `task_id text`
- `task_run_id text`
- `created_at timestamptz default now()`

### 空样本与嵌套 JSON 处理

- 若记录数组为空：仍会生成最小安全结构（含 `payload` + 审计列），并在日志提示。
- 若字段为嵌套对象或数组：默认不展开，保留在 `payload jsonb` 中，避免插入失败。

### 预览与排错

- 生成后会使用前 1～3 条记录渲染插入预览 SQL（不落库）。
- 若字段缺失或类型不匹配，会在日志提示；你仍可以手动调整模板。
- `SKIP_DUPLICATES` 模式建议配置唯一约束或 `conflictTarget`，否则可能无法有效去重。

## 插入模式与稽核/修复

支持三种插入模式：

1. **APPEND**：直接追加写入。
2. **TRUNCATE_THEN_APPEND**：按任务范围删除（若未配置范围键则整表 TRUNCATE），再插入。
3. **SKIP_DUPLICATES**：使用 `ON CONFLICT DO NOTHING` 跳过重复，`conflictTarget` 可指定冲突列。

### 稽核逻辑

- 记录数稽核：目标表范围内记录数必须等于本次拉取数量。
- 去重稽核：
  - 若配置 `naturalKeyFields`：按组合去重；
  - 否则若配置 `distinctKeyJsonField`：按单字段去重；
  - 未配置则默认与总数一致。

### 修复策略（强制）

- 稽核失败：清理本次 run scope 与 staging 后重试，最多 `maxRetries` 次。
- 达到上限仍失败：确保该 run scope 无脏数据残留并标记失败。

## 分布式锁（多实例互斥）

- 锁表：`leshan.trans_data_job_lock`。
- 锁名：`trans_data_task:<taskId>`。
- 无法获取锁：本次运行标记为 `SKIPPED_LOCK`，不会写库。
- 任务执行中会定期续期租约，防止被其他实例抢占。

## staging 表结构

```sql
CREATE TABLE IF NOT EXISTS leshan.stg_custom_task_rows(
  task_id text,
  run_id text,
  row_no int,
  payload jsonb,
  created_at timestamptz
);
CREATE INDEX IF NOT EXISTS idx_stg_custom_task_run_id
  ON leshan.stg_custom_task_rows(task_id, run_id);
```

## 调度语义（关键）

- **间隔计时从“成功完成 + 稽核通过”后开始**。
- 失败或锁跳过不会触发下一次“成功间隔”计时。
- 时间窗口支持跨午夜，如 `23:00:00` → `02:00:00`。

## 源接口测试

每个任务提供“测试源接口”面板：

- 使用全局公共请求头（`x-app-id`、`EASY-APP-KEY`、`Content-Type`）。
- 请求体默认与全局一致，允许任务级自定义。
- 显示 HTTP 状态、耗时、响应大小与完整 JSON（可复制）。
- **仅测试，不写库**。

## 构建与运行

```powershell
mvn -q -DskipTests package
java -jar target/trans_data-1.0.0.jar
```

## 配置说明（config.properties）

默认配置文件：`src/main/resources/config.properties`（模板）。
用户配置持久化：`~/.trans_data/config.properties`。

```properties
# 源接口
source.url=https://example/api
source.header.x-app-id=940
source.header.easy-app-key=
source.body={}

# 异步 SQL
asyncsql.baseUrl=https://example/asyncsql
asyncsql.token=
asyncsql.dbUser=leshan

# 加密
crypto.aesKey=
crypto.aesIv=
crypto.keyFormat=base64

# 日志
logging.sql.maxChars=20000
logging.sql.dumpDir=logs/sql

# 异步 SQL
asyncsql.maxWaitSeconds=900

# HTTPS（保留兼容项，UI 不展示）
https.insecure=true
```

> 注意：`EASY-APP-KEY` / `X-Request-Token` / AES Key/IV 属于敏感信息，**不得**写入日志或文档示例中，请在本地配置文件中填写。

## 排障建议

- **源接口返回异常**：检查 `code/message` 是否异常，保持服务返回原样展示。
- **稽核失败**：查看日志中的“稽核差异”与“清理/重试”记录。
- **重复或缺失**：检查 `insertMode` 与 `conflictTarget` 配置，确认范围键是否正确。
- **锁竞争频繁**：检查是否多实例同时运行，确认租约秒数是否足够。
- **AES 配置缺失**：UI 日志会提示缺失字段，补全后再执行。
