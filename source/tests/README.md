# 测试

所有命令均在仓库根目录执行。先安装开发依赖：

```bash
uv sync --extra dev --locked
```

## 组织

| 目录 | 范围 |
| --- | --- |
| `core/event` | 事件、handler、订阅、通道、背压与生命周期的组件契约 |
| `core/graph` | 节点、Command、context、快照、流与图执行的组件契约 |
| `core/config`、`core/utils` | 配置与日志 |
| `integration/event` | 大规模通配符匹配与完整事件分发的正确性 |
| `integration/graph` | 插件、中断、取消、并发、运行期所有权与图分解 |
| `performance` | 大量多节点图同时调度，含建图耗时的性能测试 |

`source/tests/conftest.py` 集中管理共享 asyncio runtime 的隔离与清理，以及等待前台分发完成的 helper；根目录 `conftest.py` 统一提供测试筛选与性能参数。仅在这些测试基础设施和已有白盒组件测试中检查内部状态；新增性能 workload 通过 `apixis.core` 的公开接口构建、执行和检查图。

## 正确性测试

```bash
uv run --no-sync pytest
uv run --no-sync pytest -m unit
uv run --no-sync pytest -m integration
uv run --no-sync pytest source/tests/integration/graph
```

默认不收集性能测试，原有规模正确性测试仍参与默认测试。CI 在默认测试后额外运行四种性能 workload 的小规模版本，验证测试本身能执行和清理，不设与硬件绑定的速度阈值。

## Graph 性能测试

```bash
uv run --no-sync pytest source/tests/performance --run-performance
```

运行结束后自动生成仓库根目录下的 `test-results/README.md`，文件仅包含本次运行场景的性能结果表格，并覆盖上次生成的表格。无需指定 `--graph-report`；该参数用于额外保存 JSON 原始指标。

默认每组同时提交 1,000 次调用，每次执行 16 个节点。四组场景为：

| 场景 | 建图数量 | 调度方式 |
| --- | ---: | --- |
| `independent/chain` | 1,000 | 每张独立图执行 16 个串行节点 |
| `independent/parallel` | 1,000 | 每张独立图执行 4 层，每层 4 个并发节点 |
| `shared/chain` | 1 | 同一张 16 节点图并发调用 1,000 次 |
| `shared/parallel` | 1 | 同一张 4 层并发图并发调用 1,000 次 |

每组预热一轮，随后测量三轮；**每轮都重新注册节点并编译图**。同组所有调用通过共同的启动门同时释放；节点使用同步函数立即返回最小 `Command`，携带下一跳和一个用于事后校验的访问标记。保留默认状态复制、AutoMerge 与快照行为。普通日志及事件分发日志关闭，错误日志保留；默认配置下使用本地运行时，无需 Kafka、RabbitMQ 或外部服务。

例如，增加到 3,000 张图、每张 32 个节点，并保存 JSON 指标：

```bash
uv run --no-sync pytest source/tests/performance --run-performance \
  --graph-count 3000 --graph-nodes 32 --graph-width 4 \
  --graph-repeats 3 --graph-timeout 180 \
  --graph-report=graph-performance.json
```

只跑独立图或串行场景：

```bash
uv run --no-sync pytest source/tests/performance --run-performance -k independent
uv run --no-sync pytest source/tests/performance --run-performance -k chain
```

| 参数 | 默认值 | 含义 |
| --- | ---: | --- |
| `--graph-count` | 1,000 | 每轮并发调用数；独立图模式的建图数也等于此值 |
| `--graph-nodes` | 16 | 每次调用执行的节点总数 |
| `--graph-width` | 4 | 并发图每层最大节点数；不整除时最后一层使用剩余节点 |
| `--graph-repeats` | 3 | 测量轮数，另有一轮预热 |
| `--graph-timeout` | 120 | 单轮建图至执行结束的超时秒数 |
| `--graph-report` | 无 | JSON 输出路径；保留每轮原始指标及运行环境 |

主指标 `total_seconds` 从首次建图开始，到全部调用完成为止，**包含建图、context 创建、任务创建、排队及执行**。吞吐量使用这个总耗时计算；p50/p95/p99 也从同一轮首次建图开始计时，因此包含建图等待。`build_seconds`、`prepare_seconds`、`execute_seconds` 分列对应开销，三项之和等于总耗时。终端显示各轮指标的中位数，JSON 保留完整原始值。

结果校验、等待剩余前台分发回调、分解图和测试清理在计时之外。每轮校验所有 context 的状态、执行步骤、快照数量、run ID 唯一性、命名空间、访问顺序和节点执行总数，避免漏执行产生虚假的高吞吐量。

节点不包含 sleep、等待、I/O、业务计算或运行期探针；执行次数在计时结束后根据访问标记核对。基准覆盖框架的建图、调度、Command 合并、状态复制及快照开销。比较结果时使用相同 Python 版本、机器、参数及运行时配置；并发调用数可以超过 `event_loop_backpressure`，但不代表所有节点都能同时持有事件许可。

如果使用 pip 安装开发依赖，将以上 `uv run --no-sync pytest` 替换为 `python -m pytest` 即可。
