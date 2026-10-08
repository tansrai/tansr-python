# 隔离环境依赖锁

本目录提供可复现的运行依赖与开发工具锁。锁文件固定版本及官方 PyPI wheel 的 SHA256，不改变 SDK 在 `pyproject.toml` 声明的 Python 下限。SDK 自身为 Python 源码；cryptography/cffi 和部分开发工具包含原生二进制。SDK 与 Demo 采用 [MIT 许可证](https://github.com/tansrai/tansr-python/blob/main/LICENSE)，依赖保留各自许可证。

| 锁文件 | 适用范围 | 本批实际环境检查 |
| --- | --- | --- |
| `runtime-py37.lock` + `tools-py37.lock` | CPython 3.7 | Windows x64 3.7.9、Linux x64 3.7.17、Mac x86_64/Rosetta本地源码构建3.7.17 |
| `runtime-modern.lock` | CPython 3.10—3.14运行依赖 | Windows 3.12.14/3.14.8；Linux 3.10.22/3.11.17/3.12.15/3.13.16/3.14.8；Mac arm64 3.14.8 |
| `tools-modern.lock` | 现代构建及测试工具，与运行锁分开 | 集中工具环境为Windows 3.12.14/3.14.8、Linux 3.14.8、Mac arm64 3.14.8；不冒称每个minor都用相同工具池 |
| `quality-modern.lock` | 本批现代解释器的 ruff/mypy 门禁工具 | Windows x64 3.12.14/3.14.8；仅工具启动与依赖检查 |
| `runtime-py38.lock` | CPython 3.8 | Linux非root 3.8.20实际安装与代表行为通过 |
| `runtime-py39.lock` | CPython 3.9.2+ 的 3.9 系列 | Linux非root 3.9.25实际安装与代表行为通过；pip按元数据拒绝3.9.0/3.9.1 |

这里的“环境检查”包括实际安装、`pip check`、密码算法已知向量及认证失败检查；SDK 行为的已验证范围见[发行说明](https://github.com/tansrai/tansr-python/blob/main/doc/%E5%8F%91%E8%A1%8C%E8%AF%B4%E6%98%8E.md)。锁含同版官方多平台 wheel 哈希，不代表每个平台均已验收。现代运行锁不能套用于 3.7/3.8/3.9；3.10 所需 `typing_extensions` 以 marker 显式锁入，不能依赖测试工具顺带安装。3.8—3.13 的代表行为矩阵仅覆盖所列 Linux 版本，不扩大成全平台矩阵。

在已经建立的空 venv 中执行，例如：

```text
python -m pip --isolated --disable-pip-version-check install --require-hashes --only-binary=:all: --index-url https://pypi.org/simple -r requirements/runtime-py37.lock -r requirements/tools-py37.lock
python -m pip --isolated check
python tools/environment_info.py
```

现代验证线将两个 `py37` 文件换成 `modern`；格式和类型门禁另装 `quality-modern.lock`。所有操作使用 venv 的完整解释器路径，不依赖系统 `python` 默认指向。`--only-binary=:all:` 会在无适配 wheel 时明确失败，不临时引入源码构建链。

解释器版本号不能替代运行时能力：Windows旧版需要已验SSL运行库和子进程DLL路径，Mac旧版必须具有实际可用的目录描述符及相关系统调用。精确兼容环境与启动方法见[发行说明](https://github.com/tansrai/tansr-python/blob/main/doc/%E5%8F%91%E8%A1%8C%E8%AF%B4%E6%98%8E.md)；安装依赖成功不自动满足这些条件。

PEP 517 的隔离构建环境独立于调用 venv，构建后端版本还必须遵守根 `pyproject.toml` 的分段声明；安装这些工具锁不会自动约束另一个隔离构建环境。Python 3.7 的 pip 24.0 不支持后来新增的 `--build-constraint`。

Python 3.7 已结束上游支持，45.0.7 为兼容锁，保留 cryptography 的弃用警告。`pip check` 只检查包依赖一致性，不是漏洞或许可证审计，也不能证明旧解释器的 TLS 安全维护。实际探针分别记录 stdlib `ssl` 与 cryptography 使用的 OpenSSL，两者不可混同。已知上游公告 `GHSA-p423-j2cm-9vmq`、`GHSA-537c-gmf6-5ccf`、`GHSA-g6cj-pr64-35w5` 仍须纳入依赖安全评估和发布判断，不通过降级或屏蔽告警冒充已修复。

复现时应在自选证据目录保留官方元数据、下载哈希校验、安装日志和环境回执。官方来源为 [PyPI](https://pypi.org/)、[Python Windows NuGet 说明](https://docs.python.org/3/using/windows.html#the-nuget-org-packages) 和 [Docker Official Python Image](https://hub.docker.com/_/python)。

## 冻结候选后的包消费门禁

`tools/package_check.py` 先将 SDK 根工程与 `demo/` 的打包输入复制到归档，记录源码哈希，再分别构建 wheel/sdist。两个工程必须各产生一个 `py3-none-any` wheel 和一个 sdist；SDK/Demo 不互相复制源码，Demo 必须精确依赖同版 SDK。

```text
python tools/package_check.py --source <冻结源码目录> --evidence <不存在的新归档目录> --consumer-python <已准备的3.7解释器> --consumer-python <已准备的现代解释器>
```

门禁工具本身由现代环境调用。每条消费者解释器分别建立 wheel 与 sdist 两个全新 venv；清除 PYTHONPATH/PYTHONHOME、用户 site 和本 SDK 凭据继承，在空工作目录安装同轮本地产物。运行依赖与后端工具先按精确哈希下载到该证据目录，再离线消费。sdist 保留默认 PEP517 隔离构建，构建依赖只从同轮已核哈希 wheelhouse 获取；不使用 `--no-build-isolation`。验收包括 `pip check`、公开 API 导入路径归属 venv 和三个已安装 console script 的 `--help`。本门禁不联系 Serve，也不替代真实三 Demo 会话验收。

仅检查锁结构时可加 `--check-locks-only`；这不会构建、安装或运行产品。失败原日志、命令和回执保留，不自动清除旧证据，也不发布包。

重复候选可追加一个或多个 `--wheelhouse-source <前轮wheelhouse目录>`。工具先把其中每个 wheel 的版本/实际 SHA256 对照精确锁，再用 `--no-index --find-links` 离线复制适配 wheel；新一轮仍检查哈希、创建全新消费者并保留 PEP517 隔离，不重新联网下载相同闭包。未提供此参数时才使用官方 PyPI；3.7 的下载由现代解释器 TLS 按实际目标平台/ABI 标签完成，旧解释器只作离线安装消费。
