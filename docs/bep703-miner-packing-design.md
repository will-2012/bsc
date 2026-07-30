# BEP-703 Payment Lane · 出块打包侧实现说明

对应代码：工作树 `payment-lane` 分支，上游基线 `ece8248c4`。
本文说明**代码为什么长成现在这样**，以及被否决的方案各自错在哪 —— 后者是主要价值，它防止有人把已经证伪的写法加回来。

假设两个接缝已备（`core/paymentlane` 里的包级 `var`，默认关闭）：

```go
Enabled(config, number, time) bool                  // 硬分叉门，真实实现是 IsPaymentLane
Classify(tx) Class                                  // §3.1 分类，真实签名要多收 parent.Root 只读 reader
Quota(parent, header) uint64                        // §3.3/§3.4 配额推进，自带全部钳制
```

---

## 1. 机制的全部复杂度就是一次减法

有效性规则（§3.2）：

```
systemGasUsed + generalGasUsed + max(paymentGasUsed, laneSize) <= GasLimit
```

`max(p, L) ≡ p + max(L−p, 0)` 对任意无符号数成立，所以规则等价于

```
system + general + payment + IdleLane <= GasLimit        IdleLane := max(L − payment, 0)
```

**`IdleLane` 因此是一笔虚拟交易的 gas 消耗**：它像真交易一样占区块空间、参与容量竞争，却不产生手续费，存在的唯一目的是替还没到来的支付流量顶住门。BEP 里的 `max()` 只是这件事的另一种写法。

于是准入谓词只有两行（`gasPool` 一个字节不动，容量仍是 `GasLimit − gasReserved`）：

```
payment 可准入 ⟺ p <= shared
general 可准入 ⟺ g <= shared − IdleLane          shared := gasPool.Gas()
```

general 交易必须和那笔虚拟交易挤同一个块，payment 不必——这就是全部。

### 正确性（`paymentlane.Budget.Headroom` 的注释里有同一份证明）

记 `C` 为池容量、`gu/pu` 为两桶实耗。归纳不变式 `I: gu + max(pu, L) <= C`：

- **general 准入**（实耗 `a <= g`）：`pu >= L` 时 `IdleLane=0` ⇒ `gu+a+pu <= C` ✓；`pu < L` 时 `g <= C−gu−pu−(L−pu) = C−gu−L` ⇒ `gu+a+L <= C` ✓
- **payment 准入**（实耗 `a <= p`）：`pu+a >= L` 时 `I' = gu+pu+a <= C` ✓；否则 `I' = gu+L`，与变更前相同 ✓

三条推论，都是代码里在用的：

| 推论 | 用在哪 |
|---|---|
| 两个 headroom 单调不增 ⇒「现在装不下」＝「永远装不下」 | `txs.Pop()` 永久丢弃账户才是对的 |
| 每个前缀都是合法区块 | `commitTransactions` 可被 `interruptCh` 随时打断、半成品交给共识引擎 |
| general 谓词严格更严（只多减一个非负量） | 循环终止判据 `gasPool.Gas() < params.TxGas` **一个字都不用改** |
| `L <= C` 时 `shared >= IdleLane` 全程成立 | general 偷不走车道空间，配额到打包结束时一定还在 |

准入谓词是**紧的**：`TestAdmissionIsExactlyTight` 穷举证明 `Admits(class, g)` 与「这一笔烧满 `g` 后区块仍合法」逐位等价——既无假接受（出无效块），也无假拒收（白丢收入，且不报错）。

---

## 2. 承诺：`header.UncleHash`

Parlia 不允许 uncle，该字段恒为 `EmptyUncleHash`、从不被读取。BEP-696（已 MERGED）正是「复用 `UncleHash` 作通用元数据」，并已批准配套声明：客户端必须接受任意 32 字节值，uncle 为空改由 body 校验、**不得从这个 hash 反推**。

```
[0:8]   laneSize        uint64 big-endian
[8:16]  generalGasUsed  uint64 big-endian
[16:24] paymentGasUsed  uint64 big-endian
[24]    版本，恒为 1
[25:32] 保留，必须为零
```

`systemGasUsed` 由 `header.GasUsed − general − payment` 派生（走 `DeriveSystemGas`）。存两个显式桶而不是「一个桶 + 派生另一个」，是为了让 §3.3 的 signal 分子干净：Parlia 的 `updateValidatorSetV2` 在每个 breathe 块烧约 12.16M gas（70M 块的 17.4pp），并入 general 会每天误触发一次配额扩张。

**承诺进 header 把递推深度压到 1** —— 节点只需要父 header，于是 snap sync、reorg、重启、历史裁剪四个问题一次性消失；而且 `UncleHash` 本来就在 `encodeSigHeaderWithoutVoteAttestation`（`consensus/parlia/parlia.go:1872` 附近）内，被 validator 的 seal 签名覆盖，认证是白送的。

**版本位不是为将来扩展，是一条正确性要求。** `core/types/block.go:328` 用「`UncleHash` 是零值」判断调用方有没有写过该字段，而 `Commitment{0,0,0}` 恰好编码成全零（配额为 0 的空块，分叉激活初期最常见的形状）。没有版本位时它会被 `NewBlock` 覆写成 `EmptyUncleHash`、再被导入侧 `Decode` 拒掉，表现为间歇性 BAD_BLOCK。

副产品：`EmptyUncleHash` 的 `[24]` 字节非 1，所以 `Decode` 必然拒它——「parlia 的 `EmptyUncleHash` 覆写回归」因此是**确定性可检测**的故障，而不是诡异的记账偏差。

---

## 3. 代码触点

`gasPool`、`miner/ordering.go`、`core/txpool` **零改动**。

| 位置 | 改动 |
|---|---|
| `core/paymentlane/`（新包，115 行代码） | 规则本体（`CheckInequality`/`DeriveSystemGas`）、承诺编解码、准入代数（`Budget`）、三个 mock 接缝 |
| `miner/worker.go:147-150` | `environment` 加 `laneOn` / `laneBudget` / `gasReserved` 三个字段 |
| `miner/worker.go:161` | `laneAdmits` —— 一行，转发给 `Budget.Admits` |
| `miner/worker.go:167` | `laneClassOf` —— 车道关闭时恒 general，「未分类」因此不是一种状态 |
| `miner/worker.go:179` | `sealLane` —— 写承诺 + 自检 |
| `miner/worker.go:799-801` | `makeEnv` 初始化，**不做任何钳制**（见 §4） |
| `miner/worker.go:991-996` | 单点分类 + 单点准入，替换上游那句 `gasPool.Gas() < ltx.Gas` |
| `miner/worker.go:1030-1034` | 记账：循环里取快照、`commitTransaction` 返回后差分 |
| `miner/worker.go:1345`、`:1711` | 两条 assemble 路径在 `AssembleBlock` 之前调 `sealLane` |
| `miner/bid_simulator.go:755-775` | bid block 签名前的静态门 |
| `miner/bid_simulator.go:1429-1444` | bid 逐笔准入 + 记账 |
| `miner/bid_simulator.go:1242` | payBidTx 强制归 general |
| `consensus/parlia/parlia.go:645` | `VerifyUnsealedHeader` 的 uncle gate |
| `consensus/parlia/parlia.go:1613` | `finalizeAndAssemble` 不再覆写 `UncleHash` |
| `core/block_validator.go:72` | `ValidateBody` 的 `CalcUncleHash` 相等检查加门 |
| `core/types/block.go:328` | `NewBlock` 保留调用方写入的 `UncleHash` |

**`w.commitTransaction` / `w.commitBlobTransaction` / `w.applyTransaction` 相对上游逐字未改。** 记账放在循环里而不是穿过这三个签名，前提是两条已核实的事实：`commitTransaction` 只有一个调用者；从循环取快照到 `core.ApplyTransaction` 之间没有任何代码改动 `gasPool`（只有类型分发、blob sidecar 构造与数量检查、两次只读 `Snapshot()`）。

分叉前的零回归靠**结构**而不是靠 `if !laneOn` 特判：`laneBudget` 是零值 ⇒ `IdleLane` 为 0 ⇒ 两类 headroom 都等于 `shared` ⇒ 谓词退化成上游的 `gasPool.Gas() < tx.Gas()`。

---

## 4. 被否决的方案

### 4.1 双 gas pool —— 会拒绝规则本来允许的区块

把预算静态切成 `general = C−L` 与 `payment = L`、payment 溢出转 general。等价于装箱问题，首次适应不是最优。

反例（`C=200, L=100`，总和恰好 200，`TestInvariantAdmissionBeatsStaticPools` 钉死）：

| 序 | 类别 | gas |
|---|---|---|
| A | payment | 60 |
| B | payment | 50 |
| C | payment | 50 |
| G | general | 40 |

双池贪心：`A→payment`(剩 40) → `B` 装不下 payment → `general`(剩 50) → `C→general`(剩 0) → `G=40` **失败**。
而 `B,C→payment`(占满 100)、`A→general`(剩 40)、`G→general` 全部装得下。

bid 场景下交易集顺序由 builder 给定、不可重排，这个差别直接表现为**整个 bid 被拒**。

### 4.2 K 窗口折叠取代 header 承诺 —— 数学上不成立

设更新算子 `f_σ(x) = clamp(x+σ, min, max)`，`σ ∈ {+E, −S, 0}`。每个 `f_σ` 单调不减且 1-Lipschitz，故 K 步复合 `F_K` 亦然，于是

> `F_K` 能唯一确定配额 ⟺ `F_K(min) = F_K(max)`

两条轨迹的间距只在发生 clamp 时收缩。**若窗口内 signal 全部落在滞后带（σ=0），间距永久保留** —— 两条链可以有逐字节相同的最近 K 个块却得到相差满量程的配额。而这个反例正是 BEP 自己的不变量 (1)（滞后带不能收窄）与 (3)（比率区间不能收窄）保证其存在的，治理调参消不掉。

叠加：`generalGasUsed(n−1)` 本身也不在 header 里，重算它需要**块 n−2 的 state**（分类要读 `to` 的 codeHash 与白名单），而 snap sync 节点在 pivot 落地时只有 pivot 一个 state root —— **K=1 就断**。

### 4.3 递推态存 ratio 而不是 laneSize —— 多一个字段和一条要人守的关系

曾经在 `environment` 上存 `laneRatioBps`，`Quota` 返回 `(laneSize, ratioBps)` 两个值。问题：`laneSize` 就是 ratio 换算成 gas 再施加绝对边界的结果，拆成两个值就多出一条**没人强制**的相等关系；一旦漂移，矿工按一个配额打包却承诺另一个 ratio，下一块的递推基于一个从未被执行过的 ratio。

在 gas 空间上递推是自洽的：状态就是钳制后的绝对值，没有隐藏状态可丢。数值验证（"拥堵顶满 → GasLimit 腰斩 → 恢复"）显示两种递推**最终收敛到同一状态**，gas 空间只是在 GasLimit 恢复后多花约一个块爬回去。

代价：BEP §3.3 的字面表述是对 **ratio** ±一步，所以这是一处对文本的偏离，需与作者确认。两者在 GasLimit 恒定时完全等价，**前提是步长必须按当前 GasLimit 折算**（`stepGas = EXPAND_STEP × GasLimit / 10000`）——写成固定 gas 步长会让扩张的相对速度随链容量增长而悄悄变慢，那正是 §4 说 ratio 设计要避免的。

### 4.4 出块侧钳制 `laneSize` —— 是共识分歧，不是保护

曾经在 `makeEnv` 里写 `if laneSize > capacity { laneSize = capacity }`。出块侧唯一能用来钳的量是 `gasReserved`，而它是矿工本地的启发式上界（普通块 `SystemTxsGasSoftLimit=1M`、breathe 块 `SystemTxsGasHardLimit=20M`），**验证方看不到**。矿工钳了验证方没钳 ⇒ `IdleLane` 偏小 ⇒ general 超打包 ⇒ 无效块，而自检用的是同一个被钳的 `L`，抓不到。触发面还被 breathe 块放大（约束从 `GasLimit−1M` 骤降到 `GasLimit−20M`），一天一次，devnet 短跑撞不上。

现在不钳：配额真的大于可用预算时，`Headroom` 把 general 压到 0（只能打包 payment），`sealLane` 的 `Verify` 再裁决这个块能不能出。全程无分歧，且 `Verify` 从此是一条**真能失败**的检查。边界应当由 `Quota` 自己完成，那是一个 `(parent, header)` 的纯函数，两侧求值相同。

### 4.5 车道补填轮（第三轮 `MinTip=nil` 重查池）—— 在 BSC 上是死代码，且有副作用

前提是「payment 交易因 tip 低而进不了候选集」。三条互相独立的事实推翻它：

1. `eth/backend.go` 的 `StartMining` 把 `miner.gasprice` 推进 txpool（`txPool.SetGasTip`），而 `legacypool.SetGasTip` 用 `TxsBelowTip` 逐出低于新门限的交易 —— 池的准入门限与 `w.tip` 同源；
2. `core/txpool/validation.go` 的 `ErrTipAboveFeeCap` 强制 `GasFeeCap >= GasTipCap`；
3. `consensus/misc/eip1559.CalcBaseFee` 对 `IsInBSC()` 恒返回 `InitialBaseFeeForBSC = 0`。

合起来，`legacypool.Pending` 的截断判据 `effectiveTip = min(GasTipCap, GasFeeCap−BaseFee)` 退化成 `GasTipCap >= pool.gasTip = MinTip`，**永不触发**——重查一次拿回来的候选集与前两轮逐字相同。

而副作用是真的：它的进入条件「配额还有空间」在不拥堵时恒真，于是会把 `MinTip` 以下的 **general** 交易也打包进来，等于悄悄把矿工对普通流量的 tip 底线降成池门限。代价那侧也不小：`legacypool.Pending` 在 `pool.mu.RLock` 下为每笔交易分配一个 `LazyTransaction`，`MinTip=nil` 时是整个池，还要乘上 `commitWork` 的重试次数和每个 bid 的 greedy merge。

**配额由前两轮直接填满**：general headroom 耗尽后 payment 交易仍按 `shared` 准入，所以循环会一路 Pop 掉装不下的 general 账户、继续把堆走到空，而不是在 general 满时收工。唯一的例外是区块先撞到体积上限（`txFitsSize` 会 `break`），而那在 `GasLimit <= 73.9M` 时不可达 —— 见 §7。

### 4.6 三个桶做进 `core/gaspool.go` —— 不必要

`receipt.GasUsed`（扣退款）、`result.MaxUsedGas`（不扣）、`gasPool.Used()` 三个口径今天恰好相等，但 `core/state_transition.go` 的 `IsAmsterdam` 分支会让它们漂移。曾经想把桶做进 `ReturnGas` 以保证「和恒等」。

不需要：把每笔的类别增量**从 `gasPool.Used()` 自身差分**出来，`PaymentUsed + GeneralUsed ≡ Used()` 就成了构造性事实。附带两个好处——回滚免费（`applyTransaction` 出错时 `gasPool.Set(gp)` 已恢复池，差分自然为 0，两个桶不需要单独回滚路径）；`GasPool.Snapshot()`/`Set()` 不需要记得复制新字段（漏了不报错）。

### 4.7 `class` 穿过三个函数签名 —— 不必要

曾经给 `commitTransaction`/`commitBlobTransaction`/`applyTransaction` 各加一个 `class` 参数。改成在循环里取快照、`commitTransaction` 返回后差分记账之后，三个函数**相对上游逐字未改**，`class` 只活在循环内部（从算出它到用它记账跨 4 行）。

### 4.8 短路：先用 general 谓词试、只在边界分类 —— 投机优化，不值

`general` 谓词严格更严，所以凡它准入的交易无论真实类别都可准入。据此可以让常态路径完全不分类、`Resolve()` 也留在 gas 判据之后。测算过收益：

- 省下的 `Resolve()`：短路自己在尾部也在解析（条件 `laneAdmits(payment, ltx.Gas)` 就是「抛开车道还装得下」），真正省掉的只有 `ltx.Gas > shared` 那批，而紧接着 `shared < TxGas` 就终止循环。**每块个位数到几十次 `RLock` + map 查询**（`legacypool.Get → lookup.Get`，无 I/O；blob 交易在短路里已被判死，blobpool 的磁盘读走不到）。
- 省下的 `Classify()`：它的廉价判据在前（tx type 白名单 → accessList 为空 → `To != nil` → `len(Data)==0`），拥堵块里占多数的带 data 交易在第二三步就返回，**走不到 state 读**。真读 codeHash 的只有「长得像原生转账」的那些，而它们本来就要进块——那次读几微秒后 `evm.Call` 也要做，等于预热。

代价是 20 行代码 + 17 行注释 + 一个跨 75 行的标记 + 两个特例 + 三处 `Pop; continue`。**而决定性的不是行数：短路让同一笔交易上出现两个 `Classify` 调用点，它们必须一致。** 按 §3.1 真实的 `Classify` 要多收一个绑定 `parent.Root` 的 reader，那时「两处必须拿到同一个 reader」就成了一条有人能破坏的不变式，破坏后果是分类漂移 → 承诺与执行不符 → BAD_BLOCK 且日志无指向。

单点分类结构性消掉它。若将来尾部开销真在 `pendingPlainTxsTimer` 旁边测出来，再作为**已测量**的优化加回。

---

## 5. 会静默出错的地方

| 陷阱 | 症状 | 防御 |
|---|---|---|
| `types.NewBlock` 无条件覆写 `UncleHash` | 本地出块成功、全网 BAD_BLOCK，`ValidateBody`/`VerifyHeader` 都指不出原因 | `core/types/block.go:328` 的零值哨兵 + 版本位 + `TestNewBlockPreservesCallerUncleHash`（做过变异验证） |
| `parlia.finalizeAndAssemble` 在 `sealLane` **之后**覆写 `UncleHash` | 同上 | `consensus/parlia/parlia.go:1613` 的分叉门 |
| `ValidateBody` 从 body 反推 `UncleHash` | 每个带承诺的块导入即 BAD_BLOCK | `core/block_validator.go:72` 的分叉门；uncle 为空仍由 `VerifyUncles` 强制 |
| 用 `receipt.GasUsed` 累计桶 | 今天全绿，Amsterdam 上线后出块方与验证方对同一个块给出不同判定 | 从 `gasPool.Used()` 差分 |
| 快照取早了 | bid 路径 `SubGas(PayBidTxGasLimit)` 的 25000 被算成某一笔的消耗，承诺虚高 | 快照紧贴 `commitTransaction`；中间不得插入动 `gasPool` 的代码 |
| bid 侧漏掉逐笔准入 | 一笔 gas 落在 `(shared−IdleLane, shared]` 的 general 交易会被 `ApplyTransaction` 正常执行并计入 general 桶，直到 `sealLane` 才发现越界 —— 那时整个块（不只这个 bid）都要放弃 | `bid_simulator.go:1429` 的检查 |
| bid block 只靠事后 `InsertChain` 拦 | `mux.Post` 在 `InsertChain` 之前，validator 已签名广播 → 零成本让指定 validator 丢槽 | `bid_simulator.go:755` 的签名前静态门 |
| payBidTx 被判成 payment | MEV 回扣搭上「保障普通转账」的车道 | `bid_simulator.go:1242` 强制 general |
| 承诺桶用朴素加减法 | `general=2^64-1 / payment=1` 绕回小数值 → 通过检查 → bid block 静态门整个废掉 | `CheckInequality` 与 `DeriveSystemGas` 用 `bits.Add64` |

---

## 6. 测试与验证现状

`core/paymentlane` 14 个测试，做过变异验证（把 `CheckInequality` 还原成朴素加减法、把 `SatSub` 退化成裸减法、让 `Headroom` 对 payment 也减 `IdleLane`，各能触发多条失败）。

| 测试 | 守住什么 |
|---|---|
| `TestAdmissionInvariants` | 随机序列逐步断言四条不变式（桶和 == 池、前缀合法、headroom 单调、`shared >= IdleLane`） |
| `TestAdmissionIsExactlyTight` | 穷举证明谓词紧 —— 无假接受**且无假拒收** |
| `TestGeneralHeadroomFlatBelowLane` | 「配额是地板」：payment 在配额内增长完全不挤压 general |
| `TestIdleLaneBoundaries` | `IdleLane > shared` 时饱和减法必须兜到 0 |
| `TestPaymentPredicateIsTheLooserOne` | 循环终止判据可以不改 |
| `TestLaneSizeExceedsCapacity` | 不钳制时 general 被完全挤出，且自检能裁决 |
| `TestPayBidTxAlwaysFitsAfterLaneAdmission` | 25000 预留的代数闭合，阈值精确落在 `capacity − PayBidTxGasLimit` |
| `TestPackingIsOrderSensitive` | 记录事实：不同到达顺序能打包的总量不同（60 vs 50） |
| `TestInvariantAdmissionBeatsStaticPools` | §4.1 的反例，含「双池确实会拒」的正向断言 |
| `TestLaneIsFloorNotCeiling` | §3.2 两个 regime 的 ±1 gas 边界 |
| `TestOverflowIsNotAWayIn` | bid 准入门的溢出攻击面 |
| `TestVerifyFailureTriggers` | 哪种编码错误 → 哪个 error |
| `TestCommitmentEncoding` | 字节布局 + `Encode` 永不返回全零 |
| `TestDecodeRejectsMalformed` | 版本位与保留位逐字节 + `EmptyUncleHash` 必被拒 |

`core/types` 新增 `TestNewBlockPreservesCallerUncleHash`。

**只能靠 devnet 验的**：分类器用 `parent.Root` 只读 reader（同块内先部署 code 再转账过去的漂移，也是出块者操纵车道归属的攻击面）；不存在账户的 codeHash 是零 hash 而非 `EmptyCodeHash`；承诺穿过 `finalizeAndAssemble` 与 `AssembleBlock` 存活；breathe 块的 12.16M 系统 gas 落在派生的 `systemGasUsed` 里而非 general 桶；bid / bidblock 两条 MEV 拓扑下的配额填充与 payBidTx。

---

## 7. 未完成

- **导入侧**：`core/state_processor.go` 还没有分类、三桶累计与 `CheckInequality`。当前承诺只被校验编码合法性与自洽性，**真实性完全未校验**，任何值都能上链。
- **`UncleHash` 复用还需放开的地方**（都在打包流程之外，不改则块无法传播/同步）：`eth/protocols/eth/handlers.go` 的 `handleNewBlock` 会静默丢弃广播来的块；`eth/fetcher/block_fetcher.go` 的 body 永远配不上 header；`eth/downloader/queue.go` 每块 `errInvalidBody` 并掉 peer。
- **三个 mock 接缝的真实实现**：`Enabled`（`params.ChainConfig` 的新分叉门，约 14 处挂载）、`Classify`（§3.1，含 tx type 白名单与预编译排除）、`Quota`（§3.3/§3.4，含四条不变量校验与治理参数读取）。
- **可观测性**：空转配额是 §3.2 明定不回收的真实吞吐损失，代码里没有任何指标暴露它。至少需要 `laneSize` / `paymentUsed` / `IdleLane` 三个 Gauge（**必须是 Gauge 而非 ResettingTimer**——后者在开了 `--metrics` 而无 scraper 时会无界增长）。
- **BEP 文本待确认**：递推跑在 gas 空间而非 ratio 空间（§4.3）。
- **`txFitsSize` 的 `break` 会成为配额空转来源 —— 但只在 `GasLimit > 73.9M` 之后。**
  上游在区块撞到体积上限时 `break` 整个循环，于是本来装得下的小体积 payment 交易
  也进不来。曾经加过一个「配额还有空间时改成 `Pop` + `continue`」的分支，后来删掉，
  因为当前不可达：体积上限是 `MaxBlockSize(8MB) − maxBlockSizeBufferZone(1MB) = 7.39MB`，
  而填满它最便宜的方式是全零 calldata（EIP-7623 floor 10 gas/字节），需要
  **73.9M gas**；全非零 calldata（40 gas/字节）要 296M。生产配置的 `GasCeil` 是 55M，
  撞不到。
  **重新引入的判据是机械的：`GasLimit × 10 > 7_388_608`，即 `GasLimit > 73.9M`。**
  注意即使可达，损失也有限 —— 那种块是体积受限而非 gas 受限，general 交易本来也加
  不进来，空转配额不构成额外损失，真实损失只是那些本可装下的小体积 payment 交易的
  手续费。

---

## 8. 与可行性报告的关系

`docs/bep703-payment-lane-report.md` 的 §1（十项机制级问题与规格修订清单）与 §3（测试矩阵）仍然有效。它的 §2 是实现计划，已由本文与代码取代——报告开头的修订说明列了被推翻的条目。
