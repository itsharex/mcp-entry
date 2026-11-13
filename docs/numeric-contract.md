# JSON 数字合同与调用预算

本轮修复的是校验与转发使用不同数值的问题，不是数据库或部署架构变更。
仍为单进程 Go 网关、SQLite 持久状态、固定下游与每用户授权。

## 保真的范围

- 参数按十进制 JSON 数字解析和验证，不先转为 `float64`。例如 integer
  拒绝 `1.00000000000000001`、`9007199254740992.5`，但接受
  `9007199254740993`；`multipleOf: 0.1` 接受 `0.3`。
- 下游目录的 inputSchema/outputSchema 数字必须在 SDK 舍入前保留，
  包括 minimum/maximum、const/enum、multipleOf 及嵌套/引用约束。
  对外公布的 schema 不被转换为近似数值，也不改写约束。
- 合格参数仍转发验证过的原始数值表示，不舍入后放行原值。
  既有 JSON 编码可能规范化空白与转义，不保证整个请求逐字节不变。
- structuredContent、结果/工具/content 元数据和嵌入资源自身元数据中的
  不透明数字保留 JSON 表示；不增加输出 schema 校验。
  协议本身的类型化字段仍遵循 SDK 类型，不承诺对未知协议扩展完全透传。
- 调用方若已经把数字舍入为 Go float64，网关不能恢复丢失的信息；应使用
  原始 JSON 或 json.Number。第三方客户端也可能在收到响应后自行舍入。

## 实现与兼容边界

输入校验使用固定版本 `github.com/santhosh-tekuri/jsonschema/v6 v6.0.3`，
以精确有理数处理数字。不是简单地对原验证器开启 UseNumber；原验证器会把
该值当字符串。原 google/jsonschema-go 依赖仍供 SDK/现有关键字准入使用。

继续限定 draft-07、2020-12，默认 2020-12；保留同文档无环引用、Go 正则、
禁止外部加载和 header 映射，以及既有节点/深度/工作量准入。
所有真实 schema 位置都检查方言，拒绝 $recursiveRef/$recursiveAnchor，避免
嵌套资源开启未纳入图预算的递归规则；普通 const/examples 数据不受此限制。
新引擎更严格拒绝非法 schema（例如 multipleOf=0、负长度、错误关键字类型），
不保证兼容此前意外接受的非法 schema。format 继续只是注解，私有编译副本
去掉 format 和空 dialect 标记，不更改对外 schema 或 const/example 数据。

每个数字 token 最多 1024 字节，显式十进制指数绝对值最多 4096。
超过 float64 有限范围的非零大数仍拒绝，以兼容现有 SDK 接收边界；极小非零值
可保留精度，不变成零。长度/元素数等 cardinality 必须是本机 int 可表示的
非负整数，避免验证器计数转换溢出。这些是明确的兼容/资源限制，不是数学上
任意精度、任意大小输入的承诺。

SDK 仍处理 HTTP/SSE、JSON-RPC、会话与错误。网关仅按本次方法和请求 ID
捕获有界响应字节，在同一次 SDK 调用成功后还原上述字段；不把 SDK 错误变成
成功，不重试业务调用。捕获上限与既有响应上限同为 1 MiB；请求捕获预留
JSON-RPC 封包空间，不改变直接 API 的 64 KiB 参数限额或 HTTP 的整请求限额。
SDK 升级时须重跑 framing/关联和双跳回归，不能默认为兼容。

## 预算及未完成项

Call 在最初授权通过后、参数准备前启动服务调用预算，准备/校验消耗该预算。
到期后不再获得新的下游时间；最终授权/代次/目录 revision 检查仍保留。
同步 JSON 编码、schema 编译/验证仍不能被 context 强制中止。
P0 本身不包含 CPU 隔离。P1 的一次性 helper、启用方式和未隔离库兼容模式见
[schema-execution.md](schema-execution.md)，不采用遗留计算的后台 goroutine，未做生产部署。

## 验证

`TestExact*` 覆盖精确数字、组合/引用、方言、注解不变性及数字/计数资源边界。
`TestNumericContractAcrossGateway` 使用真实 MCP 客户端 → 网关 → 合成下游，
覆盖 JSON/SSE、直接 API 和 HTTP、非法参数零下游请求、合法值及 schema/
structuredContent/元数据保真，以及原有大请求边界。
`TestWireCapture*` 覆盖请求 ID、错误、截断、SSE 多行/CRLF/注释/EOF、一次消费
及有界缓存。保留全量普通/race、vet、build、模块及前端回归作为合并门槛。
这些是合成测试，不代表真实业务联调或生产认证。
