# 核心配置

配置由 `apixis.core.config.base` 在首次导入时从当前工作目录的 `./config.yaml` 加载一次，`core_config.py` 再生成运行时常量。修改文件不会自动热更新；应在导入前准备配置。

当前项目包不附带 `config.yaml`。文件缺失或内容为空时使用默认值；从 `source/` 运行时，可自行创建 `source/config.yaml`。

## 加载与取值

1. 本地 YAML 必须是 mapping；其他非空顶层类型抛出 `ValueError`。
2. 仅当本地 `REMOTE_GATEWAY.enable` 为布尔值 `True` 时读取远程配置。该节必须是 mapping，`enable` 必须是 bool；启用时 `base_url` 和 `config_endpoint` 必须是非空字符串。
3. 远程请求使用同步 `httpx.get(..., timeout=10)`，要求成功状态和 JSON object。请求失败、无效 JSON 或错误类型会向导入方传播，不静默退回本地配置。
4. 远程 `EVENT_CHANNEL` 整节被排除；其余配置与本地递归合并，本地值优先。
5. `_get_config("A.b", default)` 按点分路径读取。键缺失、途中遇到非 mapping，或最终值为 `None` 时返回默认值；`False`、`0` 和空字符串保留原值。

远程配置 HTTP 请求固定使用 10 秒超时。`REMOTE_GATEWAY.timeout` 与重试参数配置的是事件网关请求，不改变配置加载请求。

## 网关与节点

| YAML 键 | 默认值 | 导出常量或作用 |
| --- | --- | --- |
| `REMOTE_GATEWAY.enable` | `false` | `REMOTE_GATEWAY_ENABLE`；同时控制导入时拉取远程配置与默认远程通道模式 |
| `REMOTE_GATEWAY.base_url` | `http://localhost:8080` | `REMOTE_GATEWAY_BASE_URL` |
| `REMOTE_GATEWAY.config_endpoint` | `/api/config` | `REMOTE_GATEWAY_CONFIG_ENDPOINT`；启用远程加载时仍须在本地显式配置 |
| `REMOTE_GATEWAY.pipe_endpoint` | `/api/pipe` | `REMOTE_GATEWAY_PIPE_ENDPOINT` |
| `REMOTE_GATEWAY.max_retry` | `5` | `GATEWAY_MAX_RETRY`，最多额外重试 5 次 |
| `REMOTE_GATEWAY.retry_initial_delay` | `1.0` | `GATEWAY_RETRY_INITIAL_DELAY`，指数退避基数，单位秒 |
| `REMOTE_GATEWAY.timeout` | `10.0` | `GATEWAY_TIMEOUT`，事件网关 HTTP 客户端超时 |
| `SERVER.base_dir` | `./.apix_data/` | `BASE_DIR`，日志根目录 |
| `SERVER.node_name` | `apix_service` | `NODE_NAME`，节点展示名称 |

`NODE_ID` 在导入时生成：远程模式使用 `uuid4().hex`，本地模式为 `apix_service`。它与可配置的 `NODE_NAME` 不同。

## 日志

| YAML 键 | 默认值 | 导出常量 |
| --- | --- | --- |
| `LOG.debug_level` | `DEBUG` | `DEBUG_LEVEL`，读取后转为大写；内置级别为 `DEBUG`、`INFO`、`WARN`、`ERROR` |
| `LOG.trace` | `true` | `TRACE` |
| `LOG.show_event_dispatch` | `true` | `SHOW_EVENT_DISPATCH` |
| `LOG.max_log_file_size` | `10485760` | `MAX_LOG_FILE_SIZE`，10 MiB 的日志切换阈值 |

普通日志先输出到终端并加入内存缓存。`Logger.start()` 启动刷盘任务，`Logger.stop()` 停止并刷盘；`Logger.flush()` 可主动刷盘。文件大小在一批内容写入前检查，因此并非严格的单文件字节上限。

## 队列与背压

| YAML 键 | 默认值 | 实际控制范围 |
| --- | --- | --- |
| `PIPELINE.event_pipe_max_len` | `65536` | `EVENT_PIPE_MAX_LEN`，默认外部 mailbox 本地缓冲容量 |
| `PIPELINE.event_loop_backpressure` | `1024` | `EVENT_LOOP_BACKPRESSURE`，处理队列容量和前台分发并发额度；有效值至少 128 |
| `PIPELINE.background_handler_backpressure` | `4096` | `BACKGROUND_HANDLER_BACKPRESSURE`，后台 handler 任务额度 |

本地 builtin ready 队列始终无限制。处理队列与分发信号量分别限制排队和执行，但当前实现使用同一个 `max(128, EVENT_LOOP_BACKPRESSURE)` 值；`EVENT_PIPE_MAX_LEN` 不控制处理队列。

## 外部邮箱

| YAML 键 | 默认值 |
| --- | --- |
| `EVENT_CHANNEL.type` | `kafka`；远程模式支持 `kafka` 或 `rabbitmq` |
| `EVENT_CHANNEL.kafka.bootstrap_servers` | `["localhost:9092"]` |
| `EVENT_CHANNEL.kafka.topic_prefix` | `apixis.mailbox` |
| `EVENT_CHANNEL.kafka.group_id_prefix` | `apixis.node` |
| `EVENT_CHANNEL.rabbitmq.url` | `amqp://guest:guest@localhost/` |
| `EVENT_CHANNEL.rabbitmq.exchange` | `apixis.events` |
| `EVENT_CHANNEL.rabbitmq.queue_prefix` | `apixis.mailbox` |
| `EVENT_CHANNEL.rabbitmq.prefetch_count` | `100` |

这些值只从本地配置读取，缺省时使用代码默认值。Kafka topic/group 和 RabbitMQ queue 均会拼接当前节点 ID，具体协议见[事件通道](../event/channels.md)。

## 生命周期清理

```yaml
LIFESPAN:
  resource_clean_interval: 300
```

`ResourceCleaner.start()` 使用 `RESOURCE_CLEAN_INTERVAL or 30`，所以配置 `0` 时实际使用 30 秒，不表示禁用。启动方法不接收 interval 参数；导入只注册清理服务，周期运行需显式启动 `resource_cleaner` 或 `auto_init`。

## 最小本地配置示例

```yaml
REMOTE_GATEWAY:
  enable: false
LOG:
  debug_level: INFO
  show_event_dispatch: false
PIPELINE:
  event_loop_backpressure: 1024
  background_handler_backpressure: 4096
LIFESPAN:
  resource_clean_interval: 300
```

核心不加载 Agent SDK、数据库驱动或 Redis，也不要求 `CACHE`、`DATA_STORE`、`LLM` 等宿主应用配置。加载器允许其他键存在，但只有代码实际读取的键才影响核心行为。