# 文档逐项核对清单

本轮以此前交付的 `Apixis-aligned.zip` 中的源码为准，逐份核对原有 14 份 Markdown 文档。修正 13 份文档并新增本清单；所有 Python 源码、测试和配置/依赖文件保持原字节内容。

## 逐份结果

下表路径相对于项目根目录。结果描述的是本轮发现并修正的差异；原本对齐的章节予以保留。

| 文档 | 核对依据 | 结果 |
| --- | --- | --- |
| [README.md](../README.md) | 项目结构与文档路径 | 导航有效，无需修改；原许可证声明保留 |
| [source/README.md](../source/README.md) | `pyproject.toml`、`config/base.py` | 明确配置只加载一次、项目包未附 config.yaml，并修复配置文档链接 |
| [docs/README.md](README.md) | 当前文档目录 | 补充配置、辅助工具与本清单的导航 |
| [docs/core/README.md](core/README.md) | Core 各模块、事件循环、图生命周期 | 修正 Block 文件路径、模块结构、缓存旧描述、并发配置和活跃图替换行为 |
| [docs/core/config/README.md](core/config/README.md) | 配置加载器、常量、ResourceCleaner、项目依赖和 CI | 按当前配置键与默认值重写；删除过期迁移断言及 567 项测试的历史结论 |
| [docs/core/event/README.md](core/event/README.md) | event/base、event_loop、event_registry、event_pipe | 补齐 INTERNAL；修正处理队列容量来源；明确事件观察与底层通道的边界 |
| [docs/core/event/handlers.md](core/event/handlers.md) | handler_registry、ApixEventHandler、event_loop | 修正缓存删除、回调/名称规则、后台额度和重复取消边界；补齐元数据引用与回调替换接口 |
| [docs/core/event/channels.md](core/event/channels.md) | 通道接口、网关请求、序列化及管道生命周期 | 修正容量、HTTP 编码与 encode_event 的差异；明确显式发送不受 remote_enabled 拦截、目录浅拷贝和配置文件状态 |
| [docs/core/graph/README.md](core/graph/README.md) | graph_manager、node、node_graph、graph/utils | 补齐导出/校验、no_snapshot、节点对象复用、router 更新丢弃、同步执行超时边界、stream 显式关闭和分支中断 |
| [docs/core/graph/state.md](core/graph/state.md) | graph/base、utils/state、NodeGraph.apply_command | 修正 Command 结构示意、schema 解析时机；明确多命令空路由和自定义 BaseNode 返回值边界 |
| [docs/core/graph/context/README.md](core/graph/context/README.md) | GraphContext、NodeGraph、context/manager、StreamChannel | 修正 is_active、两种 abort 的幂等性、live state 不原地回滚；补齐快照开关和 writer 边界 |
| [docs/core/graph/interrupter/README.md](core/graph/interrupter/README.md) | Block、interrupt、BlockEventHandler、默认 hook | 修正接口示意和超时计时范围；明确 Block.cancel 的间接中止机制、图身份、hook 注册与 stream 关闭边界 |
| [docs/core/utils/README.md](core/utils/README.md) | 异常定义/导出、logger、lifespan | 补齐 InvalidContextError 和 BlockHookNotRegisteredError；修正 GraphNodeError 来源，补充日志与清理服务的真实行为 |
| [source/apixis/core/utils/snow/README.md](../source/apixis/core/utils/snow/README.md) | 本地 Snowflake 模块与 id_generator.py | 删除无效引用标记和未附带 LICENSE 文件的错误说明，补充实际接入方式；原第三方署名保留 |

## 关键行为差异

| 原说明或容易产生的理解 | 当前代码行为 |
| --- | --- |
| 处理队列容量由 EVENT_PIPE_MAX_LEN 控制 | 处理队列容量和分发信号量都使用 `max(128, EVENT_LOOP_BACKPRESSURE)`；EVENT_PIPE_MAX_LEN 用于默认 mailbox 缓冲 |
| handler 链缓存失效时写入 None | 删除受影响缓存键；查询仍兼容 None，空列表是有效缓存 |
| 使用 RUNTIME / AGENT_RUNTIME 的清理配置 | 仅读取 `LIFESPAN.resource_clean_interval`；导出名为 RESOURCE_CLEAN_INTERVAL，配置 0 时清理器使用 30 秒 |
| is_active 同时验证运行资源和 completion | 仅判断 status 是否为 running |
| abort 接口均幂等 | `context.abort()` 对 aborted 状态幂等；`graph.abort(context)` 移除管理记录后，再调用会报 ValueError |
| abort 把 live state 恢复为快照 | 只选择快照作为调用结果，不原地覆盖 context.state |
| break 会立即清理 stream | 需要关闭异步生成器；文档给出 aclosing 示例。旧节点可能继续执行，但结果不再提交 |
| 每次节点执行都有自动快照 | 默认如此；直接构造 NodeGraph 时可用 no_snapshot=True 禁用 |
| ParallelNode 分支不能各自中断 | 分支可以各自创建 Block，共享一次图调用的身份和快照 |
| router 的 Command.update 会提交 | 只读取 goto，业务更新被忽略 |
| asyncio timeout 能中断同步阻塞函数 | 同步函数直接在事件循环线程内调用，不能在其阻塞期间强行打断 |
| 网关发送与 encode_event 共用自定义编码器 | HTTP 网关直接发送字典，未调用 encode_event 的 Enum/dataclass 编码逻辑 |
| 关闭 remote_enabled 禁止所有发送 | 自动远程生命周期与 broadcast 受开关控制，显式 send/put(mailtruck) 仍可发出 HTTP 请求 |
| 自动生成 namespace 足以保证跨进程唯一 | 当前 worker_id 固定为 23，不能据此承诺独立进程之间无冲突 |

## 验证范围

- 对原有 14 份 Markdown 逐份与源码核对；相对链接与带锚点的链接另做静态检查。
- 108 个 Python 代码块通过语法解析；103 处 apixis 导入名称均可在对应模块解析。接口示意及依赖上下文的片段不等同于独立可运行脚本。
- 实际运行 7 个关键示例：Core 快速开始、事件发布订阅、线性图、延迟中断回复、核心函数替换、StreamChannel、aclosing 关闭流，均通过。
- 另运行 7 项行为检查：处理队列配置、缓存键删除、INTERNAL 排除、当前清理配置路径、abort 边界、关闭自动快照、并行分支各自中断，均通过。
- 与上一轮交付压缩包逐字节比较，生产代码、测试及其他非 Markdown 文件均未改变。

本轮没有连接真实网关、Kafka 或 RabbitMQ；相关说明依据当前源码核对，未宣称外部服务联调通过。根目录和第三方文档的原许可证声明保留，本清单不以程序代码推断授权条款。
