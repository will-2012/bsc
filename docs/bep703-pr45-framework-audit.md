# BEP-703 Payment Lane · PR #45 逻辑框架审计

审计对象：`will-2012/bsc` PR #45（分支 `payment-lane`，上游基线 `ece8248c4`）。
方法：四个独立角度并行审计（闭环完整性 / BEP 一致性 / MEV 路径 / 矿工-验证方分歧面），结论由我逐条复核后取舍——**下文标注了哪些 agent 结论被我否掉或降级**。

---

## 结论先行

按「builder 负责算对、validator/sentry 负责校验」这个责任划分，**闭环现已成立**：

| 轴 | 状态 |
|---|---|
| **1. 出块符合规则** | ✅ **构造性成立**。三条本地出块路径（Parlia `commit`、dev `generateWork`、MEV simBid）全部经 `makeEnv` 拿到配额、经准入谓词打包、经 `sealLane` 自检。准入谓词被穷举证明是紧的。 |
| **2. 验证块符合规则** | ✅ **本轮补齐**。`laneSize` 的递推真实性移入 `parlia.verifyCascadingFields`（纯 header 函数）；两个桶的真实性由 `core/state_processor.go` 重放执行后逐字比对。**这是全套规则唯一的权威强制点。** |
| **3. bid/bidblock 符合规则** | ✅ **正确性闭合**，⚠️ 活性有既存缺口。SendBid 由 validator 自建承诺 ⇒ 构造性强制；BidBlock 的承诺由 builder 提供、签名前只能静态预筛（说谎拦不住），但说谎的块进不了 canonical 链、拿不到奖励、还会被 revoke。 |
| **与 BEP 一致性** | 4 条真实偏离，其中 3 条必须改 BEP 文本；另有 2 条 BEP 声明被实现证伪。 |

**上一版报告有两条判断被这个责任划分推翻，我原来错了**：

1. 我把 BidBlock 判为「实质无强制」。错在**把签名前预筛当成了强制点**。强制点在导入侧，而导入侧的检查本轮已落地——说谎的块被拒，责任划分完整闭合。
2. 我进一步说车道「第一次让丢槽攻击面变得有利可图」。**也错了**：说谎者的块拿不到 canonical 地位，就拿不到出块奖励，纯自损。剩下的只是让该 validator 丢一个槽位的捣乱，而这与 `Root`/`ReceiptHash`/`Bloom`/`GasUsed` 完全同类——它们同样只靠事后 `InsertChain` 兜底，是 BEP-675 拓扑固有的既存面。**车道只是往这个既存面上多加一个字段，没有新增攻击类别。**

---

## 一、闭环的权威强制点：导入侧校验（本轮落地）

### 1.1 为什么签名前拦不住，以及为什么这不要紧

原注释断言这道门关闭了「零成本让指定 validator 丢槽」的攻击面。**不成立。** builder 只要声称 `PaymentGasUsed = laneSize`：

```
max(payment, laneSize) = laneSize
system + general + laneSize
  = system + (GasUsed − system − laneSize) + laneSize
  = GasUsed
```

而 `GasUsed <= GasLimit` 已由 `VerifyUnsealedHeader`（`parlia.go:759`）保证 ⇒ **不等式恒真**。数值验证：

```
GasLimit=140M GasUsed=139M system=1M laneSize=5.6M
承诺 general=132.4M payment=5.6M
  DeriveSystemGas = 1M ✓ 与真实 system 相等（自洽）
  CheckInequality  = 139M <= 140M ✓ 通过
真实 general = 138M —— 车道被完整绕开
```

所以签名前的门只挡得住「builder 算错了」（承诺畸形、桶溢出、不等式自身不成立），挡不住「builder 说谎」。**真实性需要执行，签名前在原理上无法判定**——这与 `Root`/`ReceiptHash` 的处境完全一样。

### 1.2 强制点：`core/state_processor.go` 重放后比对

规则是执行后规则，唯一能拿到真实两个桶的地方就是导入侧重放。本轮落地：

```go
laneOn := paymentlane.Enabled(config, blockNumber, block.Time())
if laneOn { laneCommit, err = paymentlane.Decode(header.UncleHash); laneBudget.LaneSize = laneCommit.LaneSize }
// 用户交易循环内：
class := paymentlane.ClassGeneral
if laneOn { class = paymentlane.Classify(tx) }
usedBefore := gp.Used()
receipt, err := ApplyTransactionWithEVM(...)
if laneOn { laneBudget.Account(class, gp.Used()-usedBefore) }
// Finalize 之后：
if laneOn { laneBudget.VerifyCommitment(header.GasLimit, gasUsed-gp.Used(), gp.Used(), laneCommit) }
```

**导入侧的结构比出块侧更有利，这一点是决定性的**：

- `Process` 的 `gp = NewGasPool(block.GasLimit())` 与矿工是**同一个类型、同一个 `Used()` 语义、同一批交易**（系统交易在循环里被 `IsSystemTransaction` 剥离）。池的初值不同（矿工扣了 `gasReserved`），但**差分相消** ⇒ 两侧的桶构造性相等，诚实块不可能被误拒。
- `state_processor.go:206` 的 `gasUsed := gp.Used()` 取在 `Finalize` **之前**，而 Parlia 系统交易的 gas 由 `Finalize` 直接累加到那个**局部变量**上、不碰 `gp`。所以 `Finalize` 之后 `gp.Used()` 仍是用户交易口径，`gasUsed - gp.Used()` 就是**真实的** `systemGasUsed`——导入侧完全不需要 `DeriveSystemGas` 那个从 builder 可控字节反推的减法。
- 出块侧 `Verify` 传的是 `gasReserved`（上界估计），导入侧传真实值，而真实值 ≤ `gasReserved` ⇒ **不等式在导入侧更松，出块侧过了导入侧必过**。方向安全。

已核实全仓只有**一个** `Process` 实现（无并行 EVM 分支），所以没有第二条执行路径需要同样的检查。

### 1.3 测试注释与代码矛盾（上一轮遗留）

`paymentlane_test.go` 仍写着「payBidTx 被强制归 general」，而代码已改成交给分类器。该测试断言的是更强的性质所以不会失败，但**它看起来提供的保护是虚的**。已改成「若将来 BEP 加排除条款改判 general 时，`Quota` 必须满足的前提」。

---

## 二、本轮补上的关键缺口：`laneSize` 校验前移

**问题**：`laneSize` 是**深度 1 的递推态**——下一块的配额由父 header 的承诺推出。而它此前只在 bid block 准入里被校验，**普通导入路径完全不校验**。

后果不是「单块无效」而是**递推污染**：任何一个未被拒的错误 `laneSize` 会成为它**全部后代**的递推起点。所有节点读同一个父 header，所以**不分叉**，而是静默地把车道摧毁（写 0）或永久饿死 general（写大值）。

**修法**：`laneSize == Quota(parent, header)` 是**纯 header 函数**（只依赖 parent 与本 header 的共识可见字段），所以放进 `consensus/parlia/parlia.go` 的 `verifyCascadingFields`。三个白拿的好处：

- `verifyCascadingFields` 在 `VerifyUnsealedHeader` 内部（`parlia.go:676`），而后者**同时被区块导入（`verifyHeader`）与 bid block 准入（`preSealVerifyBidBlock`）调用** ⇒ 两条路径一次覆盖；
- 它已经有 `parent`（`p.getParent`），无需新增参数；
- **header-only 同步阶段就能拦住**，不会让一条配额已跑偏的 header 链先被 `HeaderChain` 接受。

bid_simulator 里的重复校验已删除。

---

## 三、闭环完整性

### 3.1 出块路径全枚举

| 路径 | 强制点 | 承诺 |
|---|---|---|
| Parlia 本地出块 | 准入谓词 + `sealLane` | ✅ |
| dev / Engine API (`generateWork`) | 同上（共用 `fillTransactions`） | ✅ |
| MEV simBid | 逐笔准入 + 记账；胜出后经 `commit` → `sealLane` | ✅ |
| **BEP-675 BidBlock** | 签名前只有算术门；**权威强制在导入侧** | 沿用 builder 提供的值，重放比对 |
| `eth_simulateV1` | ❌ 无 | ❌ 硬写 `EmptyUncleHash` |
| `core/chain_makers.go`（测试） | ❌ 无 | ❌ 归一化为 `EmptyUncleHash` |

导入侧（`core/state_processor.go`）是唯一的权威强制点，覆盖上表**全部**进入 canonical 链的路径。两条 `❌` 只影响本地模拟与测试构造，不影响共识。

**确认干净的部分**：miner 里 `commitTransaction` 只有三个调用点，全部被差分记账包住；`applyTransaction` 失败时 `gasPool.Set(gp)` 回滚使 delta 为 0。**没有第四条推进 `gasPool` 而不记账的路径。**

`chain_makers.go` 没有 `sealLane` 的直接后果：**激活态在全仓零测试覆盖，而且构造不出来**——`GenerateChain` 产出的块 `UncleHash` 恒为 `EmptyUncleHash`，激活态下必被 `Decode` 拒。这是把「配额误配导致停链」这类 bug 在 CI 里抓住的前提。

### 3.2 `UncleHash` 复用的传播/同步链路：比原先记录的多 6 组

设计文档原先记了 3 处，实际 grep 出 9 处。新发现的 6 组里有两条性质不同：

| 位置 | 激活后 | 严重度 |
|---|---|---|
| `ethclient/ethclient.go:203-205` | `head.UncleHash != EmptyUncleHash && len(body.UncleHashes) == 0` ⇒ **`BlockByNumber`/`BlockByHash` 对每个块报错**。这是交易所、索引器、监控、本仓自己的工具与 e2e 脚本大量使用的 SDK ⇒ **整个下游生态断裂** | **最高** |
| `eth/protocols/eth/handlers.go:489`（`hashBodyParts`） | 是 `queue.go:803` 比较值的**生产者**，恒填 `EmptyUncleHash`。修法不是一行：`queue` 与 `eth/protocols/eth` 包**都拿不到 `ChainConfig`** | 高 |
| `core/types/block.go:213` `EmptyBody()` + `queue.go:83` | 激活后**所有块（含空块）都被判「有 body」**，下载器为每个空块多调度一次 body 请求 | 中 |
| `eth/fetcher/block_fetcher.go:674` | 空块短路永不命中，空块也走 body 检索 | 中 |
| `internal/ethapi/api.go:1553` vs `:1612` + `graphql/graphql.go` | `sha3Uncles` 返回承诺、`uncles` 返回 `[]` ⇒ **自相矛盾的响应**，正是 `ethclient` 所拒绝的东西 ⇒ 服务端与客户端两边都要改 | 中 |
| `beacon/engine/types.go:308` + `beacon/types/exec_payload.go:79,98` | 硬写 `EmptyUncleHash`，而 `ExecutableData` **没有字段可以承载任意 32 字节** ⇒ 走 `engine_newPayload` 的部署激活后 100% 失败。**这是结构性阻塞，加 fork gate 解决不了** | 中（取决于部署形态） |

**确认为安全、不必改的**（避免误扩范围）：`WithBody` / `WithSeal` / `WithSidecars` / `NewBlockWithHeader` / `CopyHeader` 全部对承诺透明；RLP 往返安全（`Body.Uncles` 空列表编码为 `0xc0`，解码不回推 header）；`core/rawdb` 按整块 RLP 存取；`Header.SanityCheck()` 不涉及该字段；承诺在 `encodeSigHeaderWithoutVoteAttestation` 内 ⇒ **免费获得 validator 签名认证**。

### 3.3 `sealLane` 自检的完备性：一条被误判为「丢一个槽位」的停链风险

原注释把 `laneSize > GasLimit − gasReserved` 的情形定性为「放弃出块是唯一正确的响应」。展开代数：

- `laneSize > capacity` ⇒ general headroom 恒为 0，`paymentUsed <= capacity < laneSize` ⇒ `max(paymentUsed, laneSize) = laneSize`
- `Verify` 的不等式变成 `gasReserved + 0 + laneSize > gasReserved + capacity = GasLimit` ⇒ **恒真，必然失败**

而 `laneSize = Quota(parent, header)` 只依赖 `(parent, header)`。**没有块产出 ⇒ parent 不前进 ⇒ 下一个候选块的 laneSize 不变 ⇒ 全网每个槽位都失败。协议内没有恢复路径。**

`breathe` 块把门槛降了 20 倍（`gasReserved` 从 1M 变 20M），而 `isBreatheBlock(lastBlockTime, blockTime) = lastBlockTime != 0 && !sameDayInUTC(...)` 是**粘性**的——已核实：父块一旦落在上一个 UTC 日，之后每个候选块都是 breathe 块，直到有一个被产出。所以：

> **`laneSize ∈ (GasLimit − 20M, GasLimit − 1M]` 时链正常运行，直到下一个 UTC 日边界，然后永久停止。**

主网 GasLimit ≈ 137.8M ⇒ 需要 85.5% 的配额比例，正常治理参数摸不到。**但小 GasLimit 的 devnet / testnet（≤ 30M）上，breathe 块的可用容量骤降到 ≤ 10M，长跑必撞。**

**这与报告 §1 的 M6（不变量缺 `MAX <= GasLimit`）是同一件事的客户端表现**，但机制比我原先写的严重一档。正确修法是把边界收进 `Quota`，且**必须用协议常量而非 `gasReserved`**（后者验证方看不到，钳了就是共识分歧）：

```go
// MaxLaneSize 保证「general 与 payment 都为空」的块一定满足 §3.2，因此任何
// (parent, header) 下都存在合法块。必须用协议常量 —— gasReserved 是矿工本地的
// 启发式，验证方看不到，钳了就是分歧。
func MaxLaneSize(gasLimit uint64) uint64 {
	return satSub(gasLimit, params.SystemTxsGasHardLimit)
}
```

`Quota` 的真实实现末尾无条件过一遍它；导入侧因为也调 `Quota`，自动一致。**这一条应与 `Quota` 的实现一起落地，并写进 BEP 的第 5 条不变量。**

### 3.4 `systemGasUsed` 派生的逐形态核对：本地三条路径成立

派生成立的充要条件是 `header.GasUsed(最终) = gasPool.Used() + 真实系统 gas`，且 `桶和 == gasPool.Used()`。

| 块形态 | 结论 |
|---|---|
| 空块 / 只有系统交易 | ✅ 承诺 `{L,0,0}` 因版本位非全零，不会被 `NewBlock` 覆写 |
| out-of-turn（多一笔 slash） | ✅ 余量充足（550k vs 1M） |
| breathe 块（`updateValidatorSetV2` ~12.16M） | ✅ 12.71M < 20M，且全部落在派生出的 `systemGasUsed`、不污染 general 桶 |
| bid 块 | ✅ `SubGas(25000)` 的虚高窗口在 `SetBestBid` **之前**关闭，payBidTx 提交后 `header.GasUsed` 再写一次，最终值正确 |
| BidBlock | ⚠️ 算术自洽 ✓，真实性 ✗ |

**`SubGas` 窗口的一条隐含陷阱**（值得记下）：窗口内 `桶和 == gasPool.Used()` 的偏差恒为 25000。今天靠时序不误报（`sealLane` 只在 `AddGas` 之后跑），但**任何加在 simBid 内部的自检都必须知道这件事**，否则必然误报 `ErrBucketMismatch`。差分记账让两个桶本身始终是真实值——这是差分法一个未被记录的额外收益。

---

## 四、MEV 路径

### 4.1 两条拓扑的强制程度差一个量级

| 强制点 | SendBid（sentry 拓扑） | SendBidBlock（直连拓扑） |
|---|---|---|
| 谁构造承诺 | **validator** | **builder** |
| 配额来源 | `Quota` 本地算 | builder 自算，validator 用 `Quota` 复核（本轮已前移到 header 校验） |
| 逐笔分类 + 准入 | ✅ | ✗ 不执行任何交易 |
| 承诺真实性 | **构造性成立** | **完全未校验** |
| 签名前拦截 | 逐笔拦，bid 作废回退本地 | 只拦畸形/溢出/`laneSize` 不符 |
| **导入侧重放校验** | ✅（同一套检查） | ✅ **权威强制点**：`VerifyCommitment` 逐字比对两个桶 |
| 事后兜底 | 不需要 | `InsertChain` + revoke，**在广播之后** |
| 激活块保护 | 不需要（自建） | **缺失** |

**差的那一格是「什么时候拦」，不是「拦不拦」**：SendBid 在签名前逐笔拦，BidBlock 只能在签名后由导入侧拦。两条路径下不合规的块都进不了链，所以**正确性等价**；差别是 BidBlock 下 validator 会白丢一个槽位。这是 BEP-675「先广播后验证」的固有代价，车道沿用它、没有放大它。

### 4.2 bid 路径的准入判据用 gas *limit*，会造成大量假拒收

这是 agent 的发现里我认为最值得跟进的一条（但**不是本轮修**，需要实测支撑）。

规则约束的是**实耗**，而 `bid_simulator.go` 的逐笔准入用 `tx.Gas()`（**limit**）。实测：

```
C=10M  L=3M
一笔 payment：limit=实耗=1M    → pu=1M, shared=9M, IdleLane=2M
下一笔 general：limit=7.1M, 实耗=100k
  Headroom(general) = 9M − 2M = 7M  → 拒
  若真的执行：gu=100k, max(pu,L)=3M → 3.1M <= 10M  ← 完全合法
```

BSC 上合约调用的 gas limit 普遍是实耗的数倍，而 bid 路径**一笔被拒 = 整个 bid 作废**（不是像本地循环那样只 `Pop` 一个账户）。

`TestAdmissionIsExactlyTight` 证明的「无假拒收」是**在「这一笔烧满 limit」的假设下**成立的；bid 路径恰好是这个假设最不成立、代价最高的地方。

**修法方向**：把逐笔的 limit 判据换成 bid 循环结束后（以及 payBidTx 之后）用**实耗**做一次 `CheckInequality`。这同时修掉另一个结构缺陷——车道自检目前发生在 `w.commit`，那时 `bestWork` 已经是 bid 的 env 且 `commit` 的返回值被丢弃（`worker.go:1671`），**没有回退本地块的路径**。前移之后「这个 env 一定能通过 `sealLane`」变成结构性事实。

**为什么不本轮改**：它改变检查的位置与语义，需要先在 devnet 上量一下真实拒收率，否则是又一次没有数据支撑的改动。

### 4.3 其它 MEV 发现（已核实，未修）

- **BidBlock 缺激活块自建保护**。Pasteur 有明确的门（「validators must self-produce hard-fork activation blocks」），车道没有。而激活块恰恰是唯一「父 header 无承诺」的块——把一个规格未定义的边界交给不可信方。
- **`MevParams` 无任何车道字段** ⇒ builder 无法在 BidBlock 路径构造合法承诺、也无法在 SendBid 路径按配额调整打包量。分叉后未升级的 builder：BidBlock 路径得到 `commitment is malformed: version 240`（fail-closed，正确但不可读）；SendBid 路径整体拒收。**上线前必须做 `MevParams` 扩字段 + `Version` 协商 + typed error 明示回退。**
- **分类器的 state 绑定在 bid 路径上是 builder 可利用的**，不只是出块者可利用。builder 完全控制 bid 交易的顺序与内容，可以用一笔部署交易（或 EIP-7702 `SetCodeTx`）翻转后续 mempool 交易在 greedy merge 阶段的分类，从而扩大自己 bid 的 general headroom。⇒ 绑定 `parent.Root` 的只读 reader 不是洁癖，是 MEV 路径上的**安全要求**。
- **诚实但分类器有细微差异的 builder 会被封 24h 并让 validator 丢槽**——`InsertChain` 眼里「版本偏斜」与「故意说谎」完全一样。建议车道类失败先按较短的 revoke 处理并单独打点。
- **车道拒收原因对 builder 不可区分**：返回裸 `ErrViolated`，不含 `laneSize`/`IdleLane`/`shared`/类别。`CheckInequality` 的错误串已经带全五个量，走 4.2 的修法可以白拿。

**确认正确的**：承诺穿过 `CopyHeader`/`NewBlockWithHeader`/`WithBody`/`WithSidecars`/`WithSeal` 全链路存活，且在 builder 的 `BidBlock.Hash()` 签名覆盖内；payBidTx 不特判、交给分类器（论证扎实）；greedy merge 与 bid 两侧记账不重不漏；无任何失败路径会让不一致的 env 被继续使用；`packReward`/`bestBid` 比较不被车道扭曲。

---

## 五、与 BEP-703 的一致性

### 5.1 一致的部分（规则本体）

`core/paymentlane` 的**规则本体与准入代数**与 BEP 逐条一致，且被穷举证明是紧的：

- §3.2 不等式的两个 regime、「配额是地板不是天花板」、「未用配额空转不回收」、「只约束总量不约束排序」——全部一致，`miner/ordering.go` 与 `core/txpool` 零改动。
- §3.6「list change never affects the block that contains it」的语义方向一致（读父块末态）。
- §6 的「lane 内 spam 要付费」「waste 有界」「list governance 的爆炸半径限于车道资格」——实现层面支持。

### 5.2 四条真实偏离

| # | 偏离 | 为什么 | 安全性 | BEP 该怎么改 |
|---|---|---|---|---|
| 1 | **三项不等式**（BEP 两项，实现加了 `systemGasUsed`） | Parlia 系统交易由 `Finalize` 在用户 tx 循环**之外**追加，既非 payment 也非 general。两项版本只能把它们并进 general，会污染 §3.3 的 signal 分子——`updateValidatorSetV2` 在 70M 块上是 17.4pp，**每天误触发一次配额扩张** | 安全（方向保守） | §3.2 加第三项与 `systemGasUsed` 定义；§3.3 明写 signal 分子排除它 |
| 2 | **递推跑在 gas 空间**（BEP 说对 ratio ±一步） | 承诺的值恰好就是矿工要用的打包预算，不需要额外字段，也没有「配额与 ratio 两个派生值必须一致」这条无人强制的关系 | 安全（两侧同求值一个纯函数，不分歧）；GasLimit 恒定时完全等价 | §3.3 改为在 gas 空间描述，给出规范的 `stepGas` 公式，**并固定单位与舍入方向**（现在写作「+2pp/block」，既无单位也无舍入规则——任何实现自定的舍入都是共识分裂） |
| 3 | **复用 `header.UncleHash`**（BEP 说「no new header field is required」） | 配额是从激活块起所有 ±step 的累积和，**不是 signal 的函数**，必须显式携带。承诺进 header 把递推深度压到 1 | 机制安全（seal 签名覆盖、版本位、溢出防护）。**严格说不算「new header field」**——Header 结构、RLP、字段数、block hash 全部未变；BEP-696 已授权复用。但它推翻了同一句话更强的那半：配额从「computed」变成「committed」 | 删去那句话；新增**规范的字节 layout**（BEP 现在完全没有编码规格）；明确与 BEP-696 的字段共存规则 |
| 4 | **tx type 白名单 + accessList 必须为空**（收紧） | 照 BEP 字面：一笔 `0x02 + 空 data + 非空 accessList` 满足类别① 三条判据，而它的 gas 是 `21000 + 2400/地址 + 1900/键`，**可被任意抬高**却什么都不做——纯稀释车道的向量。`SetCodeTx(0x04)` 与 `BlobTx(0x03)` 同理 | 方向是收紧，安全 | §3.1 类别① 补两条：tx type ∈ {0x00,0x01,0x02}；accessList 为空。并把「no execution-bearing transaction may enter the lane」从叙述提升为规范约束 |

### 5.3 两条 BEP 声明被实现证伪

**§5「changes no RPC interface」** —— `internal/ethapi/api.go:1553` 原样返回 `head.UncleHash`，激活后 `sha3Uncles` 是车道承诺，而 `eth_getUncleCountByBlockNumber` 仍返回 0，**两个回答互相矛盾**。

**§5「Wallets, exchanges, and applications require no modification」** —— `ethclient/ethclient.go:203-205` 会对每个激活后的块报 `"server returned empty uncle list but block header indicates uncles"`。这是 go-ethereum 自带的 SDK，被交易所与索引器基础设施大量使用。**必须收窄为「不需要改交易构造；断言 `sha3Uncles == EmptyUncleHash` 的 header 消费方必须更新（见 BEP-696）」。**

### 5.4 BEP 文本自身的问题（实现过程中暴露）

1. **§3.2 与 §3.3 对 `generalGasUsed` 给了两个互斥定义**。§3.2 说「general 交易消耗的总 gas」，§3.3 说「includes payment gas consumed beyond the quota」。采纳后者会让 `max(paymentGasUsed, laneSize)` **重复计一次**超额 payment gas。实现选了 §3.2 的读法，代价是 signal 比 §3.3 的散文本意**更不敏感**。修法：引入独立符号 `signalGasUsed = general + max(payment − laneSize, 0)`，或删掉那句话。三个量都在承诺里，实现零成本可算。
2. **§3.1「classification adds no consensus overhead」不成立**。「不需执行」成立；「无共识开销」不成立——导入侧必须为每笔交易多做一次绑定 `parent.Root` 的 state 读，而那个 root **不是执行正在推进的 root**，复用不了预热的 state。它给区块验证引入了一条新的、共识关键的**父块 state 依赖**——BEP 完全没提，而这正是「K 窗口折叠 K=1 就断」的根因。
3. **§3.4「behavior is defined in every configuration」掩盖了「无合法块」这种定义**。见 §3.3 的停链分析。§3.5 的四条不变量**不排除**它，需要加第五条。
4. **§3.5 完全没规定参数的读取时点与存放位置**。§3.6 为白名单规定了「as of the parent block's final state」，§3.5 对 8 个可治理参数没有对应句子。两侧不同时点读参数 = 共识分歧。这一条直接卡住实现签名：`Quota(parent, header)` 是纯函数、拿不到 state reader。
5. **§3.3「At the activation block, initializes to the effective minimum」缺一句「父块无承诺」的处理**。激活块的父 header 携带 `EmptyUncleHash`，`Decode` 对它**必然失败**（版本位 `0xf0 ≠ 1`，这是刻意设计）。真实 `Quota` 必须显式识别「parent 未激活」这一分支，BEP 应写成规范语句而不是留给实现推断。
6. **payBidTx 会被判成 payment，客户端无法单方面修**。它是投标方提供的普通外部签名交易（sentry → builder 的纯转账、`data` 为空），不满足 `IsSystemTransaction`，在验证方眼里**没有任何结构标记可识别**。量级小（实耗 21000，配额的 0.375%~1.05%），方向错。**排除它只能改 §3.1，且判据必须两侧都能求值。** 对照：Parlia 系统交易的排除是结构性的、两侧共用 `IsSystemTransaction`。
7. **§5「no ordering rule」字面成立但误导**。排序确实没变，但**包含性**变了：一笔 general 交易可能因未填满的配额而落选，且打包循环在装不下时 `txs.Pop()` 丢弃整个账户。措辞应改为「no ordering or priority rule; inclusion is affected only through the gas budget」。
8. **§6「signal manipulation 持续付费」的论证前提变了**。承诺化之后，出块者可以打满区块却承诺一个低 `general` / 高 `payment`，零成本把配额钉在最小值。安全论证必须补上「依赖导入侧重新推导并校验承诺」，并把「committed laneSize 必须等于从父块推出的配额」写成显式的区块有效性规则——这与原来的纯手续费经济学**性质不同**。

---

## 六、缺口清单（按严重度，去重后）

### 阻塞上线

| # | 缺口 | 归属 |
|---|---|---|
| **A1** | **`Quota` 缺协议常量层面的上界** ⇒ `laneSize > GasLimit − gasReserved` 时 `sealLane` 恒失败 ⇒ **永久停链**（breathe 块粘性，跨过 UTC 日边界不可恢复）。修法见 §3.3。**这是目前唯一的真正阻塞项** | 本仓 + BEP 第 5 条不变量 |
| **A2** | **`UncleHash` 复用还有 6 组未处理**，其中 `ethclient`（整个下游生态断裂）与 `beacon/engine`（`ExecutableData` 无字段可承载，**结构性阻塞**）**不是加 fork gate 能解决的**，需要上游改动，应尽早进入 BEP-696 的执行清单 | 上游 + BEP-696 |
| **A3** | **BidBlock 缺激活块自建保护**（Pasteur 有明确的门，车道没有）。激活块是唯一「父 header 无承诺」的块，规格边界未定义，不该交给不可信方——现在最坏结果是丢一个槽位（不再是分歧），但仍应加 | 本仓 |

**已关闭**：上一版的「导入侧承诺真实性未校验」（本轮落地，见 §1.2）。

**已撤回**：上一版的「`MevParams` 无车道字段 ⇒ builder 无法构造合法承诺」。**这条我判错了。** 车道是 `(parent, header)` 的纯函数——`Enabled` 是链配置、`Quota` 是纯函数、`Classify` 读父块末态，而 BidBlock 的 builder 自己就包含系统交易、连 `gasReserved` 都不需要。`GasCeil`/`GasPrice` 出现在 `MevParams` 里是因为它们是**validator 本地配置、链上无来源**，车道**没有**任何这类量。所以升级过的 builder 全部本地可算，**不需要新增 RPC 字段**；真正需要的是 builder 客户端升级（仓外）与可读的拒收原因（B 级）。

**已降级**：上一版的「BidBlock 实质无强制 / 车道让丢槽变得有利可图」。见结论先行。

### 应修但不阻塞

| # | 缺口 |
|---|---|
| B1 | bid 路径用 gas **limit** 做逐笔准入 ⇒ 假拒收（需 devnet 实测支撑后再改，方向见 §4.2） |
| B0 | 车道类导入失败与「故意说谎」在 `InsertChain` 眼里一样 ⇒ 应给 `ErrCommitmentUntruthy` 单独打点并考虑更短的 revoke（版本偏斜 ≠ 恶意） |
| B2 | `w.commit()` 返回值被丢弃 ⇒ 车道自检失败静默丢槽，只有一行 `log.Error`，无 counter |
| B3 | 激活态零测试覆盖，且 `chain_makers.go` 无 `sealLane` ⇒ 连单元级闭环测试都构造不出（是抓住 A2 类 bug 的前提） |
| B4 | `Classify` mock 签名 ≠ 真实签名；`environment` 无 `parent.Root` reader，落地时两个调用点必须改，而最省事的改法恰好是被警告的操纵面 |
| B5 | `Quota` 无 `config`、无 error ⇒ 无法区分「父块分叉前（合法 bootstrap）」与「父块承诺畸形」 |
| B6 | `gasReserved` 被从打包启发式提升为承载正确性的上界，而它的两个常量按源码注释是**经验观测值**，全仓无「实际 <= 预留」断言。已核实普通块 550k vs 1M、breathe 12.71M vs 20M 都有余量；唯一缺口是块 #1 的 `initContract`（只影响从块 1 激活的 devnet） |
| B7 | 车道空转与 MEV 侧全无指标 ⇒ A1 的「说谎者系统性胜出」在数据上不可见。必须是 Gauge/Counter |
| B8 | 车道静态门消耗 `maxBidsPerBuilder` 配额（`ReservePending` 在前），与报告 §3 的 F1 期望不符 |
| B9 | general headroom 归零而 `shared` 仍大时（**车道的常态**），打包循环会逐账户 `Resolve()` + `Pop()` 走空整个堆，终止判据不触发。原先的开销估算（「每块个位数到几十次」）在这个前提下不成立，需 devnet 实测 |
| B10 | 三个 mock 是可变的包级 `var`，任何 init/测试/插件都能改共识判据；落地时应改成 `ChainConfig` 方法 + 纯函数 |

### 我否掉或降级的 agent 结论

- 「`SubGas(25000)` 会让 `sealLane` 误报」——**否**。已核实时序：`sealLane` 只从 `commit`/`generateWork` 调用，而 bid env 成为 `bestWork` 必在 `SetBestBid` 之后，那一行在 `AddGas` 之后。窗口内偏差存在但不可观察。降级为「陷阱记录」。
- 「`Quota` 块内是常量、轮间可比较」——注释**是错的**（`blockTimeForRamanujanFork` 有 `time.Now()` 钳制，节点落后时不同轮的 `header.Time` 可以不同），但**不构成分歧**（承诺与 header 一起走，自洽）。降级为「注释待改」。
- 「`Enabled` 五个调用点可能不一致」——**否**。已核实五处入参全部取自同一个 header 的同名字段，而 `header.Time` 从 `Prepare` 之后不再变动。**一致**。
- 「`Budget` 零值退化可能与上游不等价」——**否**。已穷举证明 `satSub(0,0)=0` 使两个 `Headroom` 分支同值，且 `ltx.Gas == tx.Gas()`。**逐字等价，无输入例外。**
- 「`gasPool.Used()` vs `receipt.GasUsed` 会分歧」——**当前一致**（Amsterdam 为 nil）。它是**对导入侧实现的契约要求**，不是本 PR 的缺陷。

---

## 七、本轮已改动

| 改动 | 理由 |
|---|---|
| `consensus/parlia/parlia.go` `verifyCascadingFields` 新增 `laneSize == Quota(parent, header)` 校验 | 关闭递推污染；纯 header 函数，导入与 bidblock 两条路径白拿，header-only 同步阶段即生效 |
| `miner/bid_simulator.go` 删除重复的 `laneSize` 校验 | 已由上面那条覆盖 |
| `miner/bid_simulator.go` 更正静态门的安全断言 | 原注释声称关闭了丢槽攻击面，**不成立**（见 §1.1） |
| `core/paymentlane/paymentlane_test.go` 更正过时注释 | 仍写着 payBidTx 强制归 general，与代码矛盾 |
| **`core/paymentlane` 新增 `VerifyCommitment` + `ErrCommitmentUntruthy`** | 导入侧唯一的权威强制点：重放算出的两个桶与 header 承诺逐字比对，再复核不等式 |
| **`core/state_processor.go` 接入车道校验**（解承诺 + 逐笔分类记账 + `Finalize` 后 `VerifyCommitment`） | 「验证块符合规则」原本完全不成立；接上之后说谎的块拿不到 canonical 地位，责任划分闭合 |
| `miner/bid_simulator.go` 重写静态门注释 | 上一版把签名前预筛当成强制点，把残余风险描述得过重（说谎者其实纯自损） |
| `core/paymentlane/paymentlane_test.go` 新增 `TestVerifyCommitmentCatchesTheLie` | 钉住唯一权威检查；两个变异（只比桶总和 / 检查失效）均被捕获 |

`go build ./...` 通过；`consensus/parlia`、`miner`、`core/paymentlane`、`core/types` 测试全绿。改动都在工作树，未 commit。
