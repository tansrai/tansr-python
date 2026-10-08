# Python 合同消费锁

来源固定为 CLI `83c64b2c519623a79994c942a5132300eb8174d4`。内部维护树保存39份原始资产，公开发行树只保存EXPORT-POLICY明确批准的20份资产；各含5份Python消费元数据，以DISTRIBUTION声明为准。原资产保持Git blob字节，来源与摘要登记在LOCK和PROVENANCE；不从当前CLI HEAD自动更新。LOCK保留39份来源摘要用于溯源，不代表公开树携带这些文件的正文。

`python tools/contract_check.py --mode internal` 校验完整内部清单；加 `--source <本地CLI检出>` 逐项比较冻结Git对象。`python tools/generate_api.py --mode internal --check` 检查81操作及10个schema的内嵌生成物。两个工具均要求显式模式，缺件不会降级为公开子集。

公开树使用 `python tools/contract_check.py --mode public` 与 `python tools/generate_api.py --mode public --check`。只有EXPORT-POLICY为approved且含明确授权记录才允许公开消费；pending必须拒绝。`tools/export_source.py`依据逐文件清单导出公开源码，不继承其他语言的许可或公开授权。内部reference、私有源正文、Git历史和Serve打包体不得外发。来源路径仅是溯源元数据，不能被解释为对应正文已经获准公开。

运行包只消费源码内生成的操作/错误目录与schema，不读取此目录、不需要生成器或其他语言工具链。后续公开需Python范围的明确授权和独立白名单审查。
