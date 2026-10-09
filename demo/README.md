# Tansr Python Demo

版本 `0.1.0`；SDK 与 Demo 均采用 [MIT 许可证](https://github.com/tansrai/tansr-python/blob/main/LICENSE)。Demo 的 PyPI 项目为 [tansr-sdk-demo](https://pypi.org/project/tansr-sdk-demo/)。

本包精确依赖 `tansr-sdk==0.1.0`，不包含 SDK 源码。三个入口只调用公开 Python SDK：

- `tansr-py-chat` / `python -m tansr_demo.chat`
- `tansr-py-tools` / `python -m tansr_demo.tools`
- `tansr-py-archive` / `python -m tansr_demo.archive`

只嵌入 SDK 时不需要安装本包。在自己的 venv 执行 `python -m pip install tansr-sdk-demo==0.1.0`，再执行 `python -m pip check`；离线环境可安装已核验的同版 SDK 和 Demo wheel。源码与发行状态以 [tansrai/tansr-python](https://github.com/tansrai/tansr-python) 为准。

| 示例 | 用途 | 需要准备 |
| --- | --- | --- |
| chat | 多轮/事件、权限和问答、同轮插入与显式中断 | 已配置的 Serve、当前身份；恢复时保留原会话 ID |
| tools | `DemoOrderStatus` 合成订单业务、实时 stdout/stderr、耐久业务回执 | 独立私有 journal、工具授权与执行器绑定 |
| archive | 原创建意图、加密档案、ACK 恢复、点名材料供给 | Serve 授权 Source、独立私有状态目录、外置档案密钥 |

## 1. 安装后先检查入口

使用 [SDK README](https://github.com/tansrai/tansr-python/blob/main/README.md) 中对应系统的 venv 创建方法。下文的 `python` 都表示该 venv 的完整解释器路径，Windows 为 `.\.venv\Scripts\python.exe`，Linux/macOS 为 `./.venv/bin/python`。

先运行 `--help`。连接已有 Serve，使用受保护目录中的 `TANSR_TOKEN_FILE` 和 `TANSR_SCOPE_FILE`；不自动启动或下载 Serve。两文件必须共用凭据目录，意图/档案/journal 必须使用另外的目录。档案密钥由 `TANSR_ARCHIVE_KEY_FILE` 或 `--key-file` 指向的私有文件提供；密钥文件是 64 个十六进制字符，不能用 key-id 代替。

```text
python -m tansr_demo.chat --help
python -m tansr_demo.tools --help
python -m tansr_demo.archive --help
python -m tansr_demo.async_chat --help
```

这些 help 命令无需凭据或 Serve。实际运行前还需完成下面的配置；`pip install` 和 help 成功只证明包及入口可用。

## 2. 配置服务与当前身份

由开发者提供已配置的 Serve 根地址，以及对应本应用/用户的短期 token 和可信 scope。服务地址不带 `/api` 后缀；不要填 Tansr 官网地址或上游模型地址。token/scope 的私有文件创建方法见[中文指南](https://github.com/tansrai/tansr-python/blob/main/doc/使用指南.md)或[English guide](https://github.com/tansrai/tansr-python/blob/main/doc/guide.md)。先按指南写入已经签发的真实身份，不要将示例 scope 当作授权。

Windows PowerShell，路径替换为自己实际的私有凭据目录：

```powershell
$env:TANSR_BASE_URL = 'https://serve.example.com'
$env:TANSR_TOKEN_FILE = 'C:\Users\YOUR_USER\tansr-demo-credentials\token'
$env:TANSR_SCOPE_FILE = 'C:\Users\YOUR_USER\tansr-demo-credentials\scope.json'
```

Linux / macOS：

```bash
export TANSR_BASE_URL='https://serve.example.com'
export TANSR_TOKEN_FILE="$HOME/tansr-demo-credentials/token"
export TANSR_SCOPE_FILE="$HOME/tansr-demo-credentials/scope.json"
```

普通参数默认 `--family sdk1`、整进程 `--timeout 600`、单个普通请求 `--request-timeout 30`；offload 使用 `--family sdk2-offload-v1` 且新建时提供稳定 `--request-id`。两族不能随意互换恢复。以下首次运行命令先使用 `sdk1`；offload 的创建意图与档案 Source 装配按指南单独接线。

## 3. 聊天、接入与恢复

```text
python -m tansr_demo.chat --message "你好，请简单介绍你的能力"
python -m tansr_demo.chat --attach SESSION_ID
python -m tansr_demo.chat --resume SESSION_ID
```

第一条新建会话，输出 `session: ...`，然后发送一轮并等待其终态；请保存返回的 ID。后两条是替代操作，`SESSION_ID` 替换为这个可信原 ID：attach 观察/交互接入，resume 显式恢复。不要把三条当作必须连续执行的初始化步骤。遇到审批或提问时，以交互方式接入同一个会话；非交互命令不会代替用户批准。

需要 asyncio 示例时，对空闲会话执行 `python -m tansr_demo.async_chat --attach SESSION_ID --message "继续讨论"`。没有 `--attach` 时会创建新会话。自己的应用可只依赖 `tansr-sdk` 并按双语指南接入，无需复制 Demo 凭据文件方案。

## 4. 两个终端演示工具分块输出

两个窗口使用相同的 venv、服务地址、身份和会话族。终端 A 先启动工具执行器，journal 使用与凭据分离的绝对私有目录：

```text
python -m tansr_demo.tools --journal ABSOLUTE_PRIVATE_JOURNAL_DIRECTORY --require-output --run-once
```

等待输出 `session: ...` 和 `ready: ...`。终端 B 使用打印的会话 ID：

```text
python -m tansr_demo.chat --attach SESSION_ID --message "请调用 DemoOrderStatus 查询订单 DEMO-001"
```

示例订单不是实际业务接口。`--require-output` 同时要求输出确认，输出 seal 与业务回执分别判断。

A 在实际业务回执提交后结束；stdout/stderr 由 SDK 的输出通道传送，业务回执与输出确认分别显示。模型是否发起工具调用取决于当前 Serve/模型/应用配置；等待中的执行器不表示已经执行成功。重开 A 继续使用原 journal，不能通过删除 journal 重做已有业务。

## 5. 档案同步与恢复

先由 Serve 宿主配置授权 Source，并按指南创建或查得当前会话的绑定。档案密钥生成、`prepare-create/create`、材料供给完整步骤在双语指南中，下面三个命令分别用于查询、同步和已有 pending ACK 的恢复：

```text
python -m tansr_demo.archive --mode target --session SESSION_ID
python -m tansr_demo.archive --mode sync --binding BINDING_ID --file ABSOLUTE_ARCHIVE_FILE --key-id local-key-1 --key-file ABSOLUTE_KEY_FILE
python -m tansr_demo.archive --mode recover --binding BINDING_ID --file ABSOLUTE_ARCHIVE_FILE --key-id local-key-1 --key-file ABSOLUTE_KEY_FILE --request-id ORIGINAL_RECOVERY_ID
```

所有大写占位符必须替换为真实绑定/原请求身份或绝对路径。`target` 不自动创建绑定；`recover` 需要原档案和原密钥，只恢复 pending ACK，不替代后续 `sync`，也不重新上传全历史。档案 `received` 只代表材料受理，只有 `material-status` 返回 `core-consumed` 才是核心已消费。

## 故障与退出

失败时保留原 session、journal、意图、key-id、密钥与 deadline。`/quit` / Ctrl+C 停止本地观察；`/interrupt` 才请求远端中断。网络断开或超时不证明远端没有受理，不要删除目录或更换请求 ID 来“重试”。退出码 0 只对应命令声明的成功阶段，运行失败/未知为 1，参数错误为 2。

仓库文档：[中文指南](https://github.com/tansrai/tansr-python/blob/main/doc/%E4%BD%BF%E7%94%A8%E6%8C%87%E5%8D%97.md)、[English guide](https://github.com/tansrai/tansr-python/blob/main/doc/guide.md)、[发行说明](https://github.com/tansrai/tansr-python/blob/main/doc/%E5%8F%91%E8%A1%8C%E8%AF%B4%E6%98%8E.md)。文档链接指向公开源码仓库；SDK/Demo 安装不会额外安装整个文档树。

These commands connect to an existing Serve and use the public SDK only. The SDK package can be installed without this Demo package. Keep credentials separate from transaction state, retain original identities after unknown outcomes, and distinguish business completion from output confirmation and material consumption. See the bilingual guides for exact commands, resource ownership and troubleshooting. Version 0.1.0 of the SDK and Demo uses the [MIT license](https://github.com/tansrai/tansr-python/blob/main/LICENSE). Tested platform combinations and their limits are listed in the release notes.

For first use, create a venv, install `tansr-sdk-demo==0.1.0`, and run the four module `--help` commands above. Configure your own Serve origin and issued credentials using the [English guide](https://github.com/tansrai/tansr-python/blob/main/doc/guide.md). Start with the chat example; for tools, keep executor terminal A running and attach terminal B to the exact printed session. Archive operations additionally require an authorized Source, binding and retained key. Uppercase values in the commands are placeholders, not supplied service credentials or generated IDs.
