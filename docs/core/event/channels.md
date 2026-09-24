# Event channels

`ApixEventPipe` 提供统一 channel 访问。公共 channel 类型包括：

- `BuiltinChannel`
- `GatewayChannel`
- `KafkaChannel`
- `RabbitMQChannel`

基础接口按能力拆分为：

- `BaseEventChannel`
- `ReadableEventChannel`
- `WritableEventChannel`
- `ReadWriteEventChannel`

## 默认 builtin channel

本地事件发布默认进入 builtin channel：

```python
await pipe.put(event)
await pipe.post_event(
    event_type=EventType.INFO,
    event_name="demo.event",
)
```

队列兼容常见 `asyncio.Queue` 风格方法：

```python
await pipe.get()
pipe.get_nowait()
pipe.qsize()
pipe.empty()
pipe.full()
pipe.task_done()
await pipe.join()
await pipe.clear()
```

## 获取 channel

```python
channel = pipe.get_channel("builtin")
```

具体可用 channel 取决于 `ApixEventPipe` 初始化时是否配置对应远程组件。

## 点对点发送

```python
await pipe.send(event, recipient="node-id")
```

用于显式发送到指定 recipient。

## 广播

```python
await pipe.broadcast(event)
```

用于远程广播。

## 生命周期

```python
await pipe.start()
await pipe.stop()
```

应用通常不需要直接管理共享 pipe 生命周期，优先使用 `await start_core()`。直接构造独立 `ApixEventPipe` 时才需要自行启动/停止。

`stop()` 关闭远程连接与转发任务，但保留本地队列中的事件。已经从 mailbox 取出、尚未成功转入 builtin 的事件也保留在当前 pipe 对象中，下次 `start()` 时优先继续转发。因此，即使 builtin 队列已满，停止也不需要等待队列腾出空间。

mailbox 的 `join()` 要等到这些事件成功转入 builtin 并完成 mailbox 确认后才返回；它不代表事件的 handler 已执行完成。上述保留仅限当前进程中的同一个 pipe 对象。

停止期间取消调用任务，仍会先完成转发任务和各 channel 的清理，再抛出 `asyncio.CancelledError`；重复取消也不会打断清理，因此取消的返回时间取决于 channel 的关闭耗时。并发调用 `start()` 或 `stop()` 会等待当前生命周期操作结束，避免旧连接尚未关闭就重新启动。

## 注意

远程 channel 涉及网络连接、序列化和服务端配置；它们属于可选运行路径。仅使用本地图执行和事件发布时，不需要显式操作这些 channel 类。
