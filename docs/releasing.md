# 打包与发布

所有命令均在仓库根目录执行，要求 Python 3.12+。包名为 `apixis`，当前发行版本为 `0.1.0`。

## 安装与测试

建议先创建并激活虚拟环境，再安装开发依赖：

```bash
python -m venv .venv
# macOS / Linux
source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
python -m pytest
```

使用 uv 时：

```bash
uv sync --extra dev --locked
uv run --no-sync pytest
```

仅安装运行时依赖使用 `python -m pip install .`。Kafka、RabbitMQ、HTTP 和 YAML 依赖会一并安装，无需手动安装；本地事件与图执行不要求运行消息代理。

测试通过安装后的 `apixis` 导入包，不再通过 pytest 的 `pythonpath` 注入源码目录。

## 构建发行文件

版本号只维护在 `source/apixis/_version.py` 中。`apixis.VERSION`、`apixis.__version__` 与发行包元数据使用同一个版本。

修改版本或依赖后，运行 `uv lock` 更新锁文件。发布新的版本前清空旧的 `dist/`、`build/` 和 `source/apixis.egg-info/`，避免上传旧文件。

```bash
python -m build
python -m twine check --strict dist/*
```

默认构建会先生成源码包，再从源码包构建 wheel：

- `dist/apixis-0.1.0-py3-none-any.whl`
- `dist/apixis-0.1.0.tar.gz`

wheel 包含 `apixis`、类型标记和 Apache-2.0 许可证；源码包还包含文档、测试和安装验证脚本。开发依赖只在指定 `[dev]` 时安装。

在另一个干净虚拟环境中验证 wheel，以下为 macOS / Linux 示例（Windows 使用 `Scripts/python.exe`）：

```bash
python -m venv .package-venv
.package-venv/bin/python -m pip install "dist/apixis-0.1.0-py3-none-any.whl[dev]"
.package-venv/bin/python -I scripts/smoke_test.py
.package-venv/bin/python -I -m pytest
.package-venv/bin/python -m pip install --force-reinstall --no-deps dist/apixis-0.1.0.tar.gz
.package-venv/bin/python -I scripts/smoke_test.py
```

`-I` 排除工作目录和 `PYTHONPATH` 的影响。验证脚本检查实际安装位置、版本、发行文件、事件发布和图执行。CI 对 Python 3.12、3.13、3.14 执行测试与发行包验证。

## 上传到 PyPI

本项目配置好后可以上传，但构建和测试不会自动发布。

1. 确认你拥有 PyPI 上 `apixis` 项目的发布权限；若该名称已被其他人占用，需要先修改发行包名称。
2. 确认许可证与版本号；同一发行文件不能重复上传。作者、项目主页和仓库地址可按你的实际信息补入 `pyproject.toml`。
3. 可以先上传到 TestPyPI 验证，再上传到正式 PyPI。两者使用各自的账号和 API token。

```bash
python -m twine upload --repository testpypi dist/*
# After checking the TestPyPI release, publish to PyPI.
python -m twine upload dist/*
```

按提示输入用户名 `__token__` 和对应站点的 API token；不要把 token 写入仓库。正式发布成功后，用户即可安装：

```bash
python -m pip install apixis
```

构建与发布流程参考 [Python Packaging User Guide](https://packaging.python.org/en/latest/tutorials/packaging-projects/)；元数据配置参考 [Writing your pyproject.toml](https://packaging.python.org/en/latest/guides/writing-pyproject-toml/)。
