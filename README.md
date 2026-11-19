# 多源数据采集编排平台

面向数据源接入、采集编排、内容解析、反爬对抗与数据校验的全链路数据采集平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 采集源管理 | `crawl_source` | 采集源 | 源标识、源名称、协议类型 |
| 种子管理 | `seed_config` | 采集种子 | 种子编号、目标URL模式、列表页XPath |
| 解析规则 | `parse_rule` | 解析规则 | 规则编号、目标字段名、选择器类型 |
| 频次控制 | `rate_limit` | 限流策略 | 策略编号、目标源、请求间隔 |
| 代理池调度 | `proxy_pool` | 代理节点 | 节点编号、代理地址、端口号 |
| 会话管理 | `session_mgr` | 采集会话 | 会话编号、关联源、登录状态 |
| 重试策略 | `retry_policy` | 重试策略 | 策略编号、目标源、最大重试次数 |
| 字段抽取 | `field_extract` | 抽取模板 | 模板编号、关联种子、字段名 |
| 反爬对抗 | `anti_spider` | 对抗策略 | 策略编号、目标源、反爬类型 |
| 渲染编排 | `page_render` | 渲染配置 | 配置编号、目标源、渲染引擎 |
| 数据校验 | `data_validate` | 校验规则 | 规则编号、目标字段、校验函数 |
| 增量游标 | `incre_cursor` | 增量标记 | 标记编号、关联种子、增量字段 |
| 去重过滤 | `dedup_filter` | 去重规则 | 规则编号、目标源、去重键列 |
| 格式转换 | `format_convert` | 转换配置 | 配置编号、源格式、目标格式 |
| 采集编排 | `crawl_schedule` | 采集计划 | 计划编号、关联种子、Cron表达式 |
| 结果落地 | `result_store` | 落地配置 | 配置编号、输出格式、存储路径 |
| 采集监控 | `crawl_monitor` | 监控指标 | 指标编号、关联源、成功率 |
| 内容抽取 | `content_extract` | 提取任务 | 任务编号、来源页面、内容类型 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 配额审计批次（频次控制 → 会话管理）

批量会话请求统一走配额审计批次，避免绕过当日配额、重复扣减与列表/详情错位：

- `POST /api/rate_limit/{id}/quota-batches`：支持批量、多选、一次提交整组，逐条返回
  允许请求、拒绝请求、稳定配额序号（`quota_seq`）与剩余配额。
- 每条请求只消耗一个稳定序号，拒绝项同样登记序号；漏记/乱序序号在批次里标记
  `order_violation`，审计行始终与真实请求逐条对齐。
- 同一 `idempotency_key` 只产生一份有效扣减：默认 `reuse_first` 沿用首次序号，
  重放返回幂等重放行且不二次扣减；`reassign` + `version_preference=latest` 时
  旧序号保留为失败审计项、新序号重新判定，同一幂等键仍只有一份有效扣减。
- 并发提交同一 `batch_id` 只保留一份审计结果（`concurrent_writes` 记录命中次数）。
- `GET /api/rate_limit/{id}/quota-batches` 读取历史批次，存量历史结构自动补齐字段。
- `POST /api/session_mgr/{id}/consume-quota-batch`：会话管理消费审计批次，只把
  已获得序号且结果为允许、且非重放的审计项写入会话请求日志，同一审计项只消费一次。

领域：数据工程
子方向：数据采集、爬虫与RPA
