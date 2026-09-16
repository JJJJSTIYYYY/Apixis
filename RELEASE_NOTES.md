# 本次修复与发布验证

## 修改内容

- 修复背压测试的运行时注入：同步与异步 getter 使用同一个独立 event core，并完整清理任务与通道。
- 按现有中断协议修复延迟响应测试：hook 返回前调用 `block.accept()`；增加 7 个未接管 Block 的报错用例，覆盖不同注册方式、通配符及自行注销。
- 对齐生命周期测试：停止时离线广播失败仍保留停止状态；`start_core()` 按启动信号语义验证，不要求等待所有启动任务结束。
- 清理测试中的旧 Python 3.10 兼容代码；项目最低版本仍为 Python 3.12。
- 将 `pyproject.toml`、`uv.lock`、`.python-version` 统一到仓库根目录，保留 `source/apixis` 源码布局。
- 配置 setuptools 构建后端、运行依赖、可选开发依赖、包发现范围、类型标记及许可证文件。
- 版本统一为原打包配置中的 `0.1.0`，由 `source/apixis/_version.py` 维护；保留 `VERSION` 并增加 `__version__`。
- wheel 仅分发运行包及元数据；源码包额外包含文档、测试、锁文件和安装验证脚本。
- 保留内置 IdGenerator 的第三方声明，并随发行包附带 MIT 许可证。
- 更新 CI，在 Python 3.12 / 3.13 / 3.14 上运行测试、构建、元数据检查及发行包安装验证。
- 补充 `docs/releasing.md`，修正 `Block.accept()` 的说明：声明接管不会恢复图执行。

本次没有更改事件调度、图执行或中断处理的业务逻辑。

## 本地验证结果

环境：Python 3.12.14、pytest 9.1.1、pytest-asyncio 1.4.0。

| 验证 | 结果 |
| --- | --- |
| 修改前完整测试 | 807 通过、16 失败、16 个初始化错误 |
| 修改后开发安装完整测试 | 846 通过 |
| wheel 通过 pip 安装后的完整测试 | 846 通过 |
| 源码包通过 pip 安装后，运行源码包自带测试 | 846 通过 |
| 隔离构建源码包，再从源码包构建 wheel | 通过 |
| `twine check --strict`，wheel 与源码包 | 均通过 |
| `pip check` | 无依赖冲突 |
| 安装位置、版本、类型标记、许可证检查 | 通过 |
| 安装后事件发布与图执行验证 | wheel 与源码包均通过 |
| `uv lock --check` | 通过 |

发行包测试使用 `python -I` 和独立虚拟环境，未通过 `PYTHONPATH` 或 pytest `pythonpath` 导入工作区源码。
本地实际验证了 Python 3.12；3.13、3.14 已配置到 CI，尚未在本环境运行。远程通道测试使用现有模拟实现，没有连接真实 Kafka / RabbitMQ 服务。

## 使用方式

解压后进入项目根目录：

```bash
python -m pip install .
```

预构建发行文件位于 `dist/`。需要重新运行测试或构建：

```bash
python -m pip install -e ".[dev]"
python -m pytest
python -m build
python -m twine check --strict dist/*
```

尚未上传 PyPI。确认账号拥有 `apixis` 名称的发布权限后，按 `docs/releasing.md` 完成上传。
