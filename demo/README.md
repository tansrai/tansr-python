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

### 专用 publication host（PST 候选）

新增模块 `python -m tansr_demo.publication --help`，消费 SDK 的 FileStore/Host/EncryptedJournal。未新增模型工具、云存储服务或 Source 注册入口；此候选尚未发布。

受信 Serve 宿主先按原协议安装 provider、登记 `Host.registration()` 所示保留工具、建立connection/workspace/session/binding，并提供私有JSON配置：`identity`（scope的app/user及sourceId/sourceGeneration/domainKey）、`sessionId`、完整 `binding`、有效 `connection`、`workspace` 五字段。`connection`须包含原expiresAt/heartbeatAfterMs，workspace含workspaceId/revision；不要凭文档示例捏造授权。配置只是恢复锚，Runner继续核当前scope、代际、远端operation状态和许可。

```powershell
python -m tansr_demo.publication --base http://127.0.0.1:8787 --family sdk1 `
  --token-file C:/private/auth/token.txt --scope-file C:/private/auth/scope.json `
  --config C:/private/config/publication-host.json `
  --file C:/private/publication/body.enc --journal-file C:/private/publication-journal/receipts.enc `
  --key-file C:/private/keys/body.key --key-id body-v1 `
  --journal-key-file C:/private/keys/journal.key --journal-key-id journal-v1 `
  --mode create
```

两密钥文件均为64位hex、来自宿主既有私有密钥设施，必须不同，不在输出中打印。父目录由宿主建立，末级存储目录可由SDK创建；凭据、配置、密钥与存储使用分别的私有目录。已存在的原文件用 `--mode reopen`；只读容量用 `--mode capacity`。新建中途失败保留已创建的原件，经SDK以分别的create/reopen模式核对缺失介质后再启动，不能清目录重试。容量参数重开必须不变。

每条操作打印安全receipt状态及原operationId/digest/action/transferId，不打印请求、正文或完整回执JSON。unknown后停止接新操作并以outcome_unknown退出，保留原双卷和原键；查询原operation与transfer，不能改写unknown或换键重做。Ctrl+C合作停止、等Runner实际静止再关闭介质；取消不等于已提交CAS回滚。重连配置由可信宿主更新，旧transfer只读恢复须在SDK明确提供authorize_recovery；Demo不自动批准跨owner恢复、不生成新binding或清锚。通用terminal.memory成功与本机正文耐久是不同事实。控制端需先按原协议协商terminal.binding的memory-lifecycle-v1；真实Serve验证与多OS、安装消费分别记证据。


第四批新增显式保源操作：在上述完整凭据/配置/原路径/原密钥参数上，将 `--mode` 改为 `copy-publication` 或 `copy-journal`，并追加 `--target-file C:/private/next/body.enc --target-key-file C:/private/config/next-key.hex --target-key-id next-v2`。目标位于源目录之外，文件必须不存在。旧明文日志使用 `migrate-journal`，另传 `--legacy-journal C:/private/old-journal --operations-file C:/private/config/all-original-operations.json`；必须完整列出原 operation，不能遗漏 pending 或永久回执。先停止全部写者；两个介质分别复制、分别以原键对账，再由宿主切换配置。失败保留原件及可能已提交的目标。

Preserved-source modes are `copy-publication`, `copy-journal`, and `migrate-journal`. Supply an explicit new target path/key/key ID; legacy migration also needs the existing source directory and full original operation inventory. Quiesce writers, copy each store, reopen and reconcile original keys, then explicitly change host configuration. Existing `rotate_key` remains in-place. These Demo modes perform local storage operations and do not start the executor loop.

The host must negotiate the original terminal memory-lifecycle-v1 binding before driving terminal.memory.read. Receipt output includes original operation/digest/action/transfer metadata without sensitive bodies. An unknown receipt stops polling and exits with outcome_unknown; reopen both original stores and reconcile the original operation and transfer. A committed transfer does not authorize rewriting a permanent unknown receipt. Wait for actual Runner quiescence before releasing storage; local cancellation is not a remote session close or CAS rollback.

本地 completed 不代表 Serve 已受理；`Runner.run` 沿原 poll 操作提交及失回对账，启动不扫全部历史 journal。`reopen` 只重开介质，不自动恢复已关闭会话。可信控制端明确恢复原会话时，先调用现有 SessionClient.resume 原 ID 再按原 operationId/digest 查询；缺 runtime 的 source_unavailable 不证明未执行，resume 不恢复旧 binding 授权。保留永久 unknown 与原文件，不以历史审计阻塞无关新会话。

Local completed receipts do not prove Serve acceptance. Runner follows original polled operations and reconciles lost submissions without a startup history sweep. Demo reopen reopens storage only. An authorized controller explicitly resumes the original session before reconciling original operation IDs/digests; source_unavailable is not proof of no execution, and resume does not restore binding authority. Keep unknown receipts and original media; unrelated new sessions do not wait for a full history audit.

### 显式 terminal-persistence-v1

`from tansr_sdk.terminal_persistence import FileStore, Host` 提供独立的 v1 原子存储。可信宿主以 `FileStore(path, key, key_id, identity, read_context, mode="create"|"reopen")` 创建或重开，`identity` 是精确五字段 `{applicationScopeId,endUserId,sourceId,sourceGeneration,domainKey}`；`read_context` 读取当下已认证 scope。用 `Host(store, require_encryption=True)` 与 `Runner(..., terminal_persistence=host)` 显式装配，加密 journal 用另一把钥。不能同时装配旧 `memory_publication` Host，也不能把保留工具注册成普通业务工具。

Demo 加 `--profile terminal-persistence-v1`，原可信 identity 配置经明确五字段投影，旧 profile 默认不变；新文件路径必须独立。协议正文支持任意 bytes，终端不解析记忆业务 JSON。Store 执行 `head/read/lookup/begin/put/commit/query`，begin 和 commit 的 intent/完整 root CAS、双键索引与原 transfer 结果在同一密文快照提交。query-only 恢复回调不授新 owner 写资格；unknown 需原路径/钥重开并按原键查询。

head 实际配额最高 active=8、staging=16MiB、receiptEntries=8192、transferFacts=4096、objects=16384、retainedBytes=32MiB，用户只可降低；单密文快照另有128MiB帽。每次写整份重写、冷开整份审计，不宣称 O(1)。已无共同引用的正文/页在同一提交中回收；永久索引 value 和 transfer 不删除。新格式尚无 copy/轮钥/旧库转换入口，不能使用旧 Demo 迁移模式。逻辑预留不保证实际磁盘/掉电成功；没有备份或跨机接管承诺。新profile Python实际Serve HTTP与非Windows原生运行尚未验证。


新 `terminal-persistence-v1` 同格式维护使用原 `--mode copy-publication`，显式传入新目标和不同的 fresh key/key ID。目标经完整冷重开验证但持久只读，输出 `readOnly=true/cutover=pending`；不能用普通 Demo 启动写入，源不被自动封存。旧库导入、writer cutover 和两库原子切换未提供。故障保留源和已发布目标，按原路径/钥查验，重复目标拒绝。完整参数沿上方旧维护示例，再显式加 `--profile terminal-persistence-v1`。
