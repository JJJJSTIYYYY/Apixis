# Configuration

APIXIS 在导入 core 配置时从当前工作目录的 `./config.yaml` 读取一次配置。没有文件时使用默认值。

配置并不是顶层公共 API；这里记录的是 PyPI 使用方可通过 `config.yaml` 调整的运行参数。

## 示例

```yaml
LOG:
  debug_level: INFO
  show_event_dispatch: false

PIPELINE:
  event_pipe_max_len: 65536
  event_loop_backpressure: 1024
  background_handler_backpressure: 4096

LIFESPAN:
  resource_clean_interval: 300
```

## 主要配置

| 路径 | 默认值 | 说明 |
| --- | ---: | --- |
| `LOG.debug_level` | `DEBUG` | 日志级别 |
| `LOG.trace` | `true` | trace 开关 |
| `LOG.show_event_dispatch` | `true` | event dispatch 日志 |
| `PIPELINE.event_pipe_max_len` | `65536` | 外部 mailbox 默认缓冲限制 |
| `PIPELINE.event_loop_backpressure` | `1024` | event dispatch 并发背压；最小值 128 |
| `PIPELINE.background_handler_backpressure` | `4096` | 后台 handler 并发限制 |
| `LIFESPAN.resource_clean_interval` | `300` | 资源清理周期 |

远程 gateway、Kafka 和 RabbitMQ 还可通过 `REMOTE_GATEWAY` / `EVENT_CHANNEL` 配置。仅使用本地图和 builtin event channel 时无需配置这些项。

本地配置优先于远端配置；`EVENT_CHANNEL` 属于节点本地配置，不从 gateway 继承。
