# 流变工坊 · 实时CDC与流式数据同步平台

面向数据库CDC事件捕获、变更日志解析、流式数据转换、维表关联补全、水位线管理、乱序容忍、结果落湖归档与端到端延监控的实时CDC与流式同步平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/routers/backfill_coverage.py  回填区间数据视图接口
│   ├── app/services/backfill_coverage.py 覆盖视图与写入资格仲裁
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   ├── app/store.py          内存数据仓库与示例数据
│   └── tests/test_backfill_coverage.py  断点恢复缺口演练断言
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
| CDC数据源 | `cdc_source` | 链路源端 | 源端编号、数据库类型、连接地址 |
| 日志解析 | `binlog_parse` | 解析规则 | 规则编号、源端、日志格式 |
| 事件转换 | `event_trans` | 转换管道 | 管道编号、源端、事件类型 |
| 维表关联 | `dim_join` | 关联配置 | 配置编号、事实表、维度表 |
| 水位管理 | `watermark_mgr` | 水位线 | 水位编号、数据流、当前位点 |
| 乱序处理 | `out_of_order` | 乱序策略 | 策略编号、数据流、时间字段 |
| 结果落盘 | `sink_writer` | 落盘管道 | 管道编号、数据流、目标载体 |
| 延迟监控 | `lag_monitor` | 延迟指标 | 指标编号、数据流、源端位点 |
| DDL处理 | `ddl_handle` | DDL适配 | 适配编号、源端、DDL类型 |
| 断点恢复 | `checkpoint` | 检查点 | 检查点编号、数据流、保存位点 |
| Schema演进 | `schema_evo` | 演进记录 | 记录编号、数据流、变更列 |
| 流去重 | `dedup_stream` | 去重窗口 | 窗口编号、数据流、去重键 |
| 湖仓提交 | `iceberg_commit` | 提交快照 | 快照编号、目标表、数据文件 |
| 小文件合并 | `compaction` | 合并任务 | 任务编号、目标表、文件数量 |
| 快照过期 | `expire_snap` | 过期策略 | 策略编号、目标表、保留数量 |
| 流聚合 | `metric_aggr` | 聚合窗口 | 窗口编号、数据流、聚合键 |
| 增量回填 | `backfill_run` | 回填任务 | 任务编号、目标表、起始位点 |
| 告警规则 | `alert_rule` | 告警策略 | 策略编号、监控指标、告警条件 |
| 数据比对 | `data_diff` | 比对任务 | 任务编号、源端、目标端 |
| 编排调度 | `task_schedule` | 调度计划 | 计划编号、关联链路、Cron表达式 |
| 流审计 | `stream_audit` | 审计记录 | 记录编号、数据流、操作类型 |
| 健康报告 | `stream_health` | 健康评估 | 评估编号、数据流、健康分数 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 回填区间数据视图（断点恢复按流核对覆盖）

`backfill_coverage` 把乱序超窗丢弃区间与在途回填批次按数据流归并，扣除已收到落库
回执的范围，输出按区间排列的视图，值班员从待派发缺口发起增量回填：

- **已覆盖**：只统计实际收到落库回执的位点；支付流同号位点不计入订单流（按流隔离）。
- **占用**：在途回填批次仍持有写入租约的未提交位点，含失联执行端；视图逐位点给出
  有效写入方。同一未提交位点同一时刻只能由一笔有效回填占有，重复注册只返回冲突区间。
- **待派发**：缺口内既未落库也无租约的位点，是新回填实际可领取的范围。
- 已落库位点拒绝重写；合法回执使目标表每个位点只保留一行，重复回执按幂等处理。
- 执行端中断只释放未确认占用，已确认回执保留为已覆盖，剩余缺口由下次派发计算。

入口沿用既有链路：乱序超窗（`out_of_order` 动作/编排流）注入丢弃区间，断点恢复
（`checkpoint` 编排流，支持扩展字段 `在途区间/执行端/连接状态/在途批次`）注入在途批次。

### 断点恢复缺口 101-130 演练

启动后端会自动注入题设场景（订单流缺口 101-130、101-110 已落库、失联旧端持有
111-120、支付流同号回执）。后端断言：

```bash
cd backend && .venv/bin/python tests/test_backfill_coverage.py
```

接口演练（也可在前端「回填区间数据视图」页操作，路由 `/backfill-coverage`）：

```bash
# 初始视图：已覆盖 101-110 / 占用 111-120 / 待派发 121-130
curl http://127.0.0.1:8000/api/backfill-coverage/view/order_events_cdc
# 发起新回填：实际受理 121-130，101-110（已落库）与 111-120（占用）被阻挡
curl -X POST .../backfill-coverage/dispatch -d '{"stream":"order_events_cdc","lo":101,"hi":130,...}'
# 旧执行端重连补送 115；新执行端确认 121-124 后中断（释放 125-130）
# 再次派发：111-114、116-120 仍被旧端占用，125-130 可领取；115 目标表仅一行
```

领域：数据工程
子方向：数仓与数据湖
