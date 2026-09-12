# 核心配置与迁移说明

## 模块职责

- `apixis.core.config.base` 统一负责 YAML 读取、远程配置读取、本地优先的递归合并、节点本地配置过滤和点分路径取值。`_config` 在此模块初始化，配置只加载一次。
- `apixis.core.config.core_config` 从共享加载器读取核心配置常量，保留节点 ID 的原有生成方式。
- 配置文件仍按当前工作目录下的 `./config.yaml` 读取；不存在或为空时使用默认值。YAML 和远程 JSON 的类型验证、本地覆盖远程、`EVENT_CHANNEL` 不从远程继承的规则保持不变。

## 保留的配置

| 配置节 | 保留字段 | 用途 |
| --- | --- | --- |
| `REMOTE_GATEWAY` | `enable`、`base_url`、`config_endpoint`、`pipe_endpoint`、`max_retry`、`retry_initial_delay`、`timeout` | 远程配置和事件投递 |
| `SERVER` | `base_dir`、`node_name` | 日志目录、节点名称 |
| `LOG` | `debug_level`、`trace`、`show_event_dispatch`、`max_log_file_size` | 日志输出 |
| `PIPELINE` | `event_pipe_max_len` | 处理队列和外部 mailbox 的缓冲容量，默认 65536；本地 ready 队列始终无限制 |
| `PIPELINE` | `event_loop_backpressure` | 正在执行的事件分发任务数上限，与处理队列容量独立，默认 1024 |
| `PIPELINE` | `background_handler_backpressure` | 独立的后台 handler 并发额度，默认 4096 |
| `EVENT_CHANNEL` | `type`、`kafka`、`rabbitmq` | 外部事件邮箱 |
| `RUNTIME` | `cache_clean_interval` | 核心资源清理间隔 |

资源清理间隔仍导出为 `CACHE_CLEAN_INTERVAL`，默认值仍为 300 秒。新配置使用 `RUNTIME.cache_clean_interval`，缺省时仍读取旧的 `AGENT_RUNTIME.cache_clean_interval`，使已有配置继续生效。

移除了未被核心使用的 `PROXY`、`CACHE`、`DATA_STORE`、`LLM` 配置，以及 Agent 的工具输出限制、重试次数和各项 TTL。`SERVER.base_url`、`SERVER.worker_count`、旧消息队列容量、已废弃的 handler 默认超时和未接入实现的 `BACKPRESSURE` 示例项也已移除。事件循环自身的背压实现及其参数保持不变。

移除了原属于宿主应用的数据库/缓存后端兼容性校验；独立核心启用远程事件模式时，无需再配置 MySQL 或 Redis。

为避免迁移时改变现有运行结果，日志目录、节点名、节点 ID 和邮箱资源命名没有统一改名。随附 `config.yaml` 中已有的 `apix.*` 是 broker 资源名称，不是 Python 导入路径；代码中的 `apixis.*` 默认资源名称同样保留原值。

## 导入与测试迁移

核心导入使用 `apixis.core.*`。上传源码中的核心导入已完成包名迁移，本次核查未发现仍可执行的 `apix.*` 导入。`ApixEvent`、`APIX_EVENT_LOOP` 等已有公共符号继续保留。

旧 `test_tool_graph_integration.py` 仍依赖仓库中不存在的 `apixis.agent.sdk`。现替换为 `test_custom_node_graph_integration.py`，通过 `BaseNode`、`ParallelNode` 和 `Command` 覆盖并发结果顺序、批量命令路由、空命令批次、异常传播和超时取消。Agent 消息模型与 ToolNode 本身的测试应由原 Agent 仓库维护。

配置测试改为针对 `base.py` 的加载器，并补充 `_get_config`、空值/缺失值、真实包导入、只加载一次、远程模式和新旧清理间隔配置的回归验证。

## 依赖与运行

现有运行依赖均被核心功能使用，因此保留：

| 依赖 | 使用位置 |
| --- | --- |
| `pyyaml` | YAML 配置加载 |
| `httpx[socks]` | 远程配置和网关 HTTP 请求；保留已有 SOCKS 支持 |
| `aiokafka` | Kafka mailbox |
| `aio-pika` | RabbitMQ mailbox |

没有引入 Agent SDK、模型客户端、数据库驱动或 Redis。`pytest`、`pytest-asyncio`、`pytest-cov` 仍放在开发依赖组。新增 `uv.lock` 固定本次验证使用的依赖版本，并补齐项目元数据引用的 `source/README.md`。测试工作流的触发分支已从旧仓库的 `NEXT_*` 对齐到当前 `main`。

```bash
cd source
uv sync --locked
uv run pytest -q
```

事件、图、工具辅助模块的 Python 源码保持与上传版本逐字节一致；本次生产代码变更仅发生在配置模块。

## 本次验证结果

- Python 3.12.14：`uv run --locked pytest -q` → **567 passed**。
- `compileall` 和 `git diff --check` 通过。
- 已逐字节核对配置模块之外的 25 个核心 Python 文件，确认与上传版本一致。
- 远程网关和 broker 通道通过模拟传输测试验证；未连接真实外部服务。
