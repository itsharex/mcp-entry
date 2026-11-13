# HTTP 写期限与业务预算合同

## 修复范围

此前 CLI 的 HTTP WriteTimeout 固定为 60 秒，但服务 profile 合法允许最长
120 秒。真实合成实验中，70 秒 profile 的下游在 61 秒返回，HTTP/1.1 客户端
得到 EOF，HTTP/2 客户端约 60 秒得到 stream reset；下游已经执行，不能自动重试。

当前传输期限由共享上界推导，不关闭计时器、不根据每次写入无限续期：

- TLS/头部处理余量：5 秒；
- 入口读取余量：15 秒；
- 合法单次服务操作上限 `gateway.MaxOperationTimeout`：120 秒；
- 开始审计使用独立 context，跨过操作 deadline 时的额外余量：1 秒；
- 正常终态审计事务上限 `gateway.AuditCommitTimeout`：1 秒；
- 响应交付余量：10 秒。

当前合计 **152 秒**。头部与读取时间在部分协议路径中会重叠，这里保守计入；
构造传输服务器时不依赖某份启动 profile，避免合法运行时 profile 更新后再次
出现传输上限较短的问题。实际操作仍使用自己的更短 profile/父 context，不会
因为传输余量增加就获得额外业务执行时间。

## 不能推断的保证

这是单次服务操作的有限传输总上限，不是整个请求绝不超时的保证。锁等待、OS
调度、同步不可中止的宿主代码以及多服务批处理管理操作都不能据此获得无限时间。
客户端、代理或网关前置设施仍可能有更短期限，需独立配置和验证。

10 秒交付余量是总预算的组成部分，不是 HTTP/1.1 对每个响应单独强制的“最后
10 秒”计时器。HTTP/2 原有 `WriteByteTimeout=10s` 约束阻塞写入的进度，不是
无输出业务计算只能运行 10 秒；保持其原值。连接总量、H2 并发流、头部、读取和
空闲预算均未放宽。

下游完成、网关记录终态审计、客户端收到完整响应是三种不同证据。只有成功收到
完整结果的客户端才有接收证据；连接失败不能因为内部审计 success 就改称客户端
成功，也不能据此自动重放业务调用。关闭/取消仍不等于远端回滚。

## 回归证据

`TestHTTPWriteBudgetCoversLargestAcceptedOperation` 先验证最大 profile 可被实际
配置准入接受，再检查传输总预算公式。旧代码红测为 60 秒不足新合同所需预算。

`TestHTTPWriteBudgetDeliversConfirmedSlowOperation` 经过真实 MCP 客户端 → 网关 →
合成下游，分别使用真实 HTTP/1.1 和 TLS HTTP/2，检查完整结果、只派发一次、
单条 success 终态审计。常规 CI 只压缩传输写计时器及模拟等待，不代替生产时间
尺度验收；可运行：

```sh
MCP_ENTRY_LONG_HTTP_TEST=1 go test -count=1 -v ./cmd/gateway \
  -run '^TestHTTPWriteBudgetDeliversConfirmedSlowOperation$'
```

该模式使用未修改的生产传输计时器、合法 70 秒 profile 和真实 61 秒下游等待。
两个协议并行，总耗时约一分钟。它验证本次时间合同，不构成生产负载基准，也
没有触碰真实业务系统或凭据。

本轮 Linux 合成实跑中，H1 与 H2 均约 61.06 秒收到完整结果，各只派发一次，
且分别保留一条 success 终态审计。常规缩放回归另已连续通过 3 轮 race 检查。
