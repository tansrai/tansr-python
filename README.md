# Tansr Python SDK

原生 Python 3.7+ SDK，通过统一 `/api` 连接 Serve。Serve 负责运行、会话、上下文、记忆、权限和工具调度；Python 提供同步与 asyncio 接口、显式业务工具、分块输出和私有本地档案。不捆绑 Node、C++ 或 Rust 运行时。

`0.1.0` 的 SDK 与 Demo 均采用 [MIT 许可证](https://github.com/tansrai/tansr-python/blob/main/LICENSE)。源码渠道为 [tansrai/tansr-python](https://github.com/tansrai/tansr-python)，包渠道为 PyPI 的 [tansr-sdk](https://pypi.org/project/tansr-sdk/) 与 [tansr-sdk-demo](https://pypi.org/project/tansr-sdk-demo/)。已验证环境与适用限制见发行说明。

使用文档：[中文指南](https://github.com/tansrai/tansr-python/blob/main/doc/%E4%BD%BF%E7%94%A8%E6%8C%87%E5%8D%97.md) · [English guide](https://github.com/tansrai/tansr-python/blob/main/doc/guide.md) · [发行说明](https://github.com/tansrai/tansr-python/blob/main/doc/%E5%8F%91%E8%A1%8C%E8%AF%B4%E6%98%8E.md) · [Demo 入口](https://github.com/tansrai/tansr-python/blob/main/demo/README.md)。English readers: version 0.1.0 includes the SDK and separate Demo package, both under the [MIT license](https://github.com/tansrai/tansr-python/blob/main/LICENSE).

## 从这里开始

| 你要做什么 | 阅读入口 |
| --- | --- |
| 将 SDK 嵌入自己的 Python 应用 | 下方安装步骤，然后阅读[中文指南](https://github.com/tansrai/tansr-python/blob/main/doc/使用指南.md)或[English guide](https://github.com/tansrai/tansr-python/blob/main/doc/guide.md)的连接检查与宿主接线 |
| 先运行聊天、工具输出和本地档案示例 | [Demo 安装与运行说明](https://github.com/tansrai/tansr-python/blob/main/demo/README.md) |
| 使用 Python 3.7、离线安装或核对发行包 | [发行说明与已验证环境](https://github.com/tansrai/tansr-python/blob/main/doc/发行说明.md) |
| 参与源码开发、准备对应依赖 | [依赖与构建说明](https://github.com/tansrai/tansr-python/blob/main/requirements/README.md) |

正式安装版本为 `0.1.0`，可从 [PyPI](https://pypi.org/project/tansr-sdk/0.1.0/) 或 [GitHub Release](https://github.com/tansrai/tansr-python/releases/tag/v0.1.0) 获取。运行应用需要一个已配置的 Serve 服务及其签发的当前用户凭据；安装 SDK 不会创建服务或签发凭据。

## 公开能力

| 能力 | 公开入口 |
| --- | --- |
| 81 项统一 API、严格 JSON 与错误分类 | `Client`、`AsyncClient` |
| 会话、事件、权限、提问、输入与检查点 | `SessionClient`、`AsyncSessionClient` |
| 显式工具、续租、耐久业务回执与独立输出确认 | `ExecutorClient`、`Runner`、`AsyncRunner` |
| 创建意图、加密档案、耐久 ACK 与材料供给 | `ArchiveClient`、`AsyncArchiveClient`、`FileStore` |
| 独立 Demo 包 | `tansr-py-chat`、`tansr-py-tools`、`tansr-py-archive` |

嵌入自己的应用只安装 `tansr-sdk`；需要试用三个命令行示例时再安装独立的 `tansr-sdk-demo`。SDK 不依赖 Demo，宿主通过 `AuthToken` provider 接入自己的登录和短期凭据，不必沿用 Demo 的文件布局。同步和异步高层共享同一协议实现；已有 asyncio 应用直接 `await`，不接管宿主事件循环。

## 安装与导入

先选择已安装、满足发行说明要求的 Python。以下命令不需要激活脚本，也不修改系统 Python。Windows PowerShell：

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install tansr-sdk==0.1.0
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe -c "import sys, tansr_sdk; print(sys.executable); print(tansr_sdk.__file__)"
```

Linux / macOS：

```bash
python3 -m venv .venv
./.venv/bin/python -m pip install tansr-sdk==0.1.0
./.venv/bin/python -m pip check
./.venv/bin/python -c "import sys, tansr_sdk; print(sys.executable); print(tansr_sdk.__file__)"
```

之后用同一 venv 的完整解释器路径运行自己的脚本。导入位置应在该 venv 的 `site-packages`；下面用 `python` 简写该解释器。只需要 SDK 时，上述安装已足够；想运行示例再安装 Demo：

```text
python -m pip install tansr-sdk-demo==0.1.0
python -m tansr_demo.chat --help
```

使用已下载的 wheel 时，普通 `pip install` 仍可能联网解析依赖。完整离线安装须准备目标系统/解释器的依赖 wheelhouse；以下路径需替换为实际位置：

```text
python -m pip install --no-index --find-links /absolute/wheelhouse /absolute/artifacts/tansr_sdk-0.1.0-py3-none-any.whl /absolute/artifacts/tansr_sdk_demo-0.1.0-py3-none-any.whl
python -m pip check
tansr-py-chat --help
tansr-py-tools --help
tansr-py-archive --help
```

Demo 精确依赖 `tansr-sdk==0.1.0`，不复制 SDK。模块入口分别为 `python -m tansr_demo.chat`、`python -m tansr_demo.tools`、`python -m tansr_demo.archive`；另有可执行的 `python -m tansr_demo.async_chat --message ...` asyncio 示例。

## 连接 Serve

接线顺序：先由开发者部署和配置 Serve，再取得本应用/用户的身份与能力，选定会话族并创建或接入会话，最后按需装配终端工具及档案。HTTP 请求提交控制操作，SSE 传输事件；本实现不以 WebSocket 为必需。已有完整 Node/Electron SDK 的运行方式不受此包影响。

`Client` 接收服务根地址，例如 `https://serve.example.com`，不追加 `/api`。SDK 的 token provider 接入应用自己的认证系统；`TANSR_TOKEN_FILE` / `TANSR_SCOPE_FILE` 是 Demo 和文档示例的约定，不是 SDK 对所有应用强制的认证方式。可先按指南运行仅依赖 SDK 的只读连接检查，再接入会话发送与事件处理。

`sdk1` 保留原完整会话存储路径；`sdk2-offload-v1` 需要 Serve 的授权 Source 装配，并使用稳定创建意图。两族不得对同一会话随意切换。应用能力、权限、审批和核心工具路由仍由 Serve 决定；Python `Runner` 只执行宿主显式注册的业务工具，不把设备操作回落到 Serve 机器，也不附送通用 Shell。

`ArchiveClient` 提供的是原档案与材料供给高层，不是另一套上下文/记忆管理器。记忆配置、记忆命令与缓存等已有冻结操作可经 `Client.call` 消费；首版未提供独立 `MemoryClient` / `CacheClient`，不会把通用 API 数量宣传为这些高层已经实现。能力范围和恢复处理见双语指南。

使用 Demo 连接已有 Serve 前，按指南准备 `TANSR_TOKEN_FILE`、`TANSR_SCOPE_FILE` 与服务地址；档案另需宿主管理的 `TANSR_ARCHIVE_KEY_FILE`。token/scope 共用私有凭据目录，创建意图、journal 和档案文件使用另外的私有状态目录；Demo 拒绝两种目录重合，避免授权回调重入事务锁。凭据不由 Demo 签发，也不打印到日志。恢复必须保留原会话、请求、截止、档案和密钥；本地取消不证明远端撤销，`received` 不代表 `core-consumed`。

运行源码最低支持 CPython 3.7，排除 3.9.0/3.9.1。旧解释器兼容不恢复其上游安全维护。实际依赖锁、平台环境和独立 wheel/sdist 消费方法见 [依赖说明](https://github.com/tansrai/tansr-python/blob/main/requirements/README.md)；SDK 自身的纯 Python wheel 不代表加密依赖没有原生二进制。

默认 HTTP 传输在 HTTPS 的 DNS/连接之前检查 stdlib SSL 运行时；已知受影响 OpenSSL 或未验证的其他 provider 被拒绝为 `unsupported_tls_runtime`，没有绕过开关。Windows 官方 Python 3.7.9 所带 OpenSSL 1.1.1g 因此不能使用默认 HTTPS；回环 HTTP 成功不能替代 TLS 正向证据。

最低 Python 版本与 SSL 安全基线是两个条件。本轮已验证独立 prefix 中的 Anaconda CPython 3.7.9 `h60c2a47_0` + OpenSSL 1.1.1w `h2bbff1b_0`，安装原 SDK wheel 后默认信任 HTTPS 与原传输验收通过；精确包/SHA、安装和子进程运行方式见 [发行说明](https://github.com/tansrai/tansr-python/blob/main/doc/%E5%8F%91%E8%A1%8C%E8%AF%B4%E6%98%8E.md)。这是 legacy 兼容路线，Python 3.7 与 OpenSSL 1.1.1 均已结束维护，不称当前受维护的 TLS 栈。新项目优先选择受维护的解释器；原 1.1.1g 拒绝保持，不关闭验证、不覆盖系统 DLL。

SDK 面向 UAPI r7；操作与 schema 已嵌入运行包，正常运行不读取 `contract/`，也不需要生成器。公开源码检出中的开发检查示例：

```text
python tools/contract_check.py --mode public
python tools/generate_api.py --mode public --check
python -m pytest
```

公开源码检查使用显式 `public` 模式，并核验仓库中的公开合同清单；缺少或不匹配时检查失败。真实 Serve 验证使用合成身份、平台及模型，不代表已调用付费模型。源码测试、各系统环境、安装消费和正式发布分别记录，适用范围见发行说明。
