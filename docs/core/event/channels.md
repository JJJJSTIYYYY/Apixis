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

## 注意

远程 channel 涉及网络连接、序列化和服务端配置；它们属于可选运行路径。仅使用本地图执行和事件发布时，不需要显式操作这些 channel 类。
