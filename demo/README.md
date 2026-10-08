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

先运行 `--help`。连接已有 Serve，使用受保护目录中的 `TANSR_TOKEN_FILE` 和 `TANSR_SCOPE_FILE`；不自动启动或下载 Serve。两文件必须共用凭据目录，意图/档案/journal 必须使用另外的目录。档案密钥由 `TANSR_ARCHIVE_KEY_FILE` 或 `--key-file` 指向的私有文件提供；密钥文件是 64 个十六进制字符，不能用 key-id 代替。

```text
python -m tansr_demo.chat --help
python -m tansr_demo.tools --help
python -m tansr_demo.archive --help
python -m tansr_demo.chat --message "你好"
python -m tansr_demo.async_chat --message "你好"
```

最后两条需要先配置 `TANSR_BASE_URL`、token 和 scope。普通参数默认 `--family sdk1`、整进程 `--timeout 600`、单个普通请求 `--request-timeout 30`；offload 使用 `--family sdk2-offload-v1` 且新建时提供稳定 `--request-id`。两族不能随意互换恢复。

工具示例打印 `ready` 后，再从另一终端接入打印出的同一会话；示例订单不是实际业务接口。`--require-output` 同时要求输出确认，输出 seal 与业务回执分别判断。档案 `received` 只代表材料受理，只有 `material-status` 返回 `core-consumed` 才是核心已消费。

失败时保留原 session、journal、意图、key-id、密钥与 deadline。`/quit` / Ctrl+C 停止本地观察；`/interrupt` 才请求远端中断。网络断开或超时不证明远端没有受理，不要删除目录或更换请求 ID 来“重试”。退出码 0 只对应命令声明的成功阶段，运行失败/未知为 1，参数错误为 2。

仓库文档：[中文指南](https://github.com/tansrai/tansr-python/blob/main/doc/%E4%BD%BF%E7%94%A8%E6%8C%87%E5%8D%97.md)、[English guide](https://github.com/tansrai/tansr-python/blob/main/doc/guide.md)、[发行说明](https://github.com/tansrai/tansr-python/blob/main/doc/%E5%8F%91%E8%A1%8C%E8%AF%B4%E6%98%8E.md)。文档链接指向公开源码仓库；SDK/Demo 安装不会额外安装整个文档树。

These commands connect to an existing Serve and use the public SDK only. The SDK package can be installed without this Demo package. Keep credentials separate from transaction state, retain original identities after unknown outcomes, and distinguish business completion from output confirmation and material consumption. See the bilingual guides for exact commands, resource ownership and troubleshooting. Version 0.1.0 of the SDK and Demo uses the [MIT license](https://github.com/tansrai/tansr-python/blob/main/LICENSE). Tested platform combinations and their limits are listed in the release notes.
