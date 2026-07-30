# BEP-703 Payment Lane 可行性、落地与测试评估报告

| 项 | 值 |
|---|---|
| 提案 | [BEP-703: Payment Lane on BNB Smart Chain](https://github.com/bnb-chain/BEPs/pull/703)（Draft，2026-07-17，PR OPEN） |
| 提案版本 | commit `7d22923207ceb48c16f9213d56505976c2ba6dc2`（自提交以来未修改；唯一评论为 hashdit bot 例行扫描） |
| 代码基线 | `bnb-chain/bsc` 分支 `payment-lane` @ `ece8248c4`（已并入 upstream go-ethereum v1.17.3） |
| 首发范围 | 类别① 原生 BNB 转账 + 类别② 白名单稳定币；类别③（BEP-702 native token）只留接口不实现 |
| 报告立场 | 以工程落地为主，同时对 BEP 文本提出修订建议 |
| 报告日期 | 2026-07-29 |

---

## 结论先行

**机制可以实现，但 BEP 现文本不能直接实现。** 十项机制级问题中，一项（M1）在规格层面自相矛盾且已有明确修法，四项（M2/M4/M5/M6）是安全或经济层面的实质缺陷，其余为定义精度与缺失章节。

三句话总结：

1. **§3.3「no new header field is required」不成立。** `paymentLaneSize` 是逐块递推量，而 signal 的分子 `generalGasUsed(n−1)` 不在 header 里，重算它需要**块 n−2 的 state**。snap sync 节点在 pivot 落地时只有 pivot 一个 state root，`laneSize(pivot+1)` 就已经推不出来。**修法已定：复用 BSC 恒为 `EmptyUncleHash`、当前无任何语义的 `header.UncleHash`（32 字节）承载递推态**——BEP-696（已 MERGED）已批准复用该字段并铺好生态兼容性声明。这一步把递推深度降到 1，snap sync / reorg / 重启 / 历史裁剪四个问题一次性消失，并白拿 validator 签名认证。

2. **§3.1 的核心安全断言「category ① admits no code execution at all」被三处证伪**，且 §3.6 的「只看 `to` 不看 selector」使**滥用上限就是整条 lane 的 100%**：2 笔发给 USDT 的 128KB 垃圾 calldata（EIP-7623 floor 各 5,263,880 gas，revert 也照收）即占满 8M 配额，无需任何合约配合。

3. **最实质的问题不在实现层，在经济层**：payment 只与 payment 竞争，所以拥堵期 lane 内清算价必然远低于大盘价——**lane 是一条打折 gas 通道**。攻击者能以数十倍折扣把 calldata DA、7702 批量授权、precompile 燃 gas、blob 空间塞进链。§6「flooding the lane costs fees like any other spam」漏掉了这个套利面；叠加 BEP-226 下 priority fee 全额归 coinbase，**出块者向自己 lane 灌水的净成本为零**。

工程量（geth 侧 + 合约侧 + 测试，不含第三方审计与 Chapel 灰度的日历时间）：**约 172–207 人日**，其中测试与测试基建占 55%，且 BSC 现有测试基建在本机制最关键的三条路径（parlia 单测驱动、miner fill→seal、MEV bid）上**基本为零**。

> **修订说明 —— 出块侧已实现，本报告 §2 已被取代**
>
> 出块打包侧的最终设计与代码见 **[`bep703-miner-packing-design.md`](./bep703-miner-packing-design.md)**。
> 本报告的 **§1（十项机制级问题与规格修订清单）与 §3（测试矩阵）仍然有效**；
> **§2 是早期实现计划，其中八处推荐在落地过程中被推翻**，仅作为权衡记录保留：
>
> | # | §2 的原推荐 | 实际结论 |
> |---|---|---|
> | 1 | 双 gas pool（§2.5.1） | **推翻**。静态分池等价装箱问题，首次适应会拒绝规则本来允许的区块（反例：`C=200, L=100`，payment 60/50/50 + general 40）。正解是单池 + 直接把不等式当准入谓词 |
> | 2 | 三个桶做进 `core/gaspool.go`（§2.4.1） | **不必要**。从 `gasPool.Used()` 差分即可让「桶和 ≡ Used()」成为构造性事实，同样对 Amsterdam 口径切换免疫，且回滚免费、`GasPool` 零改动 |
> | 3 | `PendingFilter` 必须加 payment 维度（§2.5.2） | **判断有误**。`filter` 本不按类别过滤；而「MinTip 挡住低 tip 支付交易」这个前提在 BSC 上不成立（`StartMining` 把 `miner.gasprice` 推进 txpool + `ErrTipAboveFeeCap` + `baseFee ≡ 0` ⇒ `Pending` 的 MinTip 截断永不触发）。补填轮是死代码且会取消矿工的 tip 底线，已删除 |
> | 4 | 承诺 `laneRatioBps`（4 字节 ratio） | **改为承诺 `laneSize`（8 字节 gas）**。递推直接跑在 gas 空间，承诺的值就是矿工本来要用的打包预算，于是不需要额外字段、也没有「配额与 ratio 两个派生值必须一致」这条要人守的关系 |
> | 5 | 出块侧钳制 `laneSize` | **删除**。唯一可钳的量 `gasReserved` 是矿工本地启发式（普通块 1M / breathe 块 20M），验证方看不到，钳了就是共识分歧，且自检抓不到、只在 breathe 块触发。改为不钳 + 由 `sealLane` 的自检裁决 |
> | 6 | `class` 穿过三个函数签名 | **不必要**。记账放在打包循环里，`commitTransaction` / `commitBlobTransaction` / `applyTransaction` 相对上游**逐字未改** |
> | 7 | 循环终止判据改成双预算 | **不必要**。payment 谓词严格更松，「两类都装不下 `TxGas`」⟺「共享余量 < `TxGas`」，与上游原判据等价，那一行一个字都不用改 |
> | 8 | 先用 general 谓词短路、只在边界分类 | **删除**。投机优化：省下的只是每块几十次带锁 map 查询（`Classify` 的廉价判据在前，拥堵块里多数交易走不到 state 读）；而它让同一笔交易上出现两个 `Classify` 调用点，真实实现要带 `parent.Root` reader 时那会成为一条有人能破坏的不变式 |
>
> §2 的工期估算仍可参考，但出块侧因上述简化已从 6.5 人日降到约 2 人日。

---

## 目录

- [第 1 节 · 背景、动机及可行性](#第-1-节--背景动机及可行性)
- [第 2 节 · 落地难度、步骤与工期拆分](#第-2-节--落地难度步骤与工期拆分)
- [第 3 节 · 测试场景矩阵](#第-3-节--测试场景矩阵)
- [附录 A · 待决策开放项](#附录-a--待决策开放项)
- [附录 B · 代码锚点索引](#附录-b--代码锚点索引)

---

# 第 1 节 · 背景、动机及可行性

## 1.1 机制解读（工程师视角，以 BEP 文本为准）

### 1.1.1 三类分类（§3.1）

| 类别 | 判据 | 是否需要 state |
|---|---|---|
| ① 原生 BNB 转账 | `to != nil` 且 `data` 为空 且 `to` 账户**在父块末状态下无 code**（EIP-7702 delegation designator 算有 code，被排除） | **需要**（读 `to` 的 codeHash） |
| ② 白名单稳定币 | `to` 在系统合约维护的名单里（初始仅 USDT/USDC） | **需要**（读名单） |
| ③ BEP-702 native token | 协议原生有效性检查 + 治理提名（初始名单为空） | 需要 |

其余一切交易为 general。BEP 声称「任何节点无需执行即可分类，分类不增加共识开销」。

**这句话一半对一半错**：分类确实不需要执行 EVM，代价也确实接近零（读的是 account leaf，且紧接着执行时本来就要读同一个账户，第二次是缓存命中）。但它**隐含地假设不需要新增状态视图**——而这是错的，见 M3：分类必须绑定 `parent.Root` 的独立只读视图，而出块/MEV 路径上今天不存在这样的视图。

### 1.1.2 记账不等式（§3.2）

```
generalGasUsed + max(paymentGasUsed, paymentLaneSize) <= GasLimit
```

两个 regime：
- `paymentGasUsed >= paymentLaneSize` → 退化为普通的 `payment + general <= GasLimit`，**配额是地板不是天花板**；
- `paymentGasUsed < paymentLaneSize` → 缺额一对一挤压 general，**未用配额空转不回收**（§4 论证：若缺额回流给 general，排除 payment 交易对出块者就零成本，地板恰在最需要它的拥堵时刻变成虚设）。

规则只约束总量，不约束排序——这是本提案最好的一个设计决定，意味着 `transactionsByPriceAndNonce`（`miner/ordering.go:89`）一行都不用改。

**但等式本身不闭合**：BSC 每个块必有 Parlia 系统交易，其 gas 既不是 payment 也不是 general，却直接加进 `header.GasUsed`（`consensus/parlia/parlia.go:2198` `*usedGas += gasUsed`）。必须补第三桶，见 M9-b。

### 1.1.3 动态配额（§3.3）与双重边界（§3.4）

```
signal(n−1) = generalGasUsed(n−1) / GasLimit(n−1)

signal >= EXPAND_TRIGGER_RATIO  → ratio += EXPAND_STEP （快涨）
signal <  SHRINK_TRIGGER_RATIO  → ratio -= SHRINK_STEP （慢跌）
否则                            → 滞后带，不变
```

边界是「比例边界 ∩ 绝对边界」，交集为空时**绝对边界优先**。建议初值：ratio `[2%, 8%]`、绝对 `[2M, 8M]`、触发 `[70%, 80%]`、步长 `+2pp / −0.5pp`。

**这是整个提案在工程上唯一真正困难的地方**，见 M1。

### 1.1.4 参数与治理（§3.5 / §3.6）

8 个可治理参数 + 2 个协议常量（`TRIGGER_GAP_MIN`、`RATIO_GAP_MIN`），4 条不变量声称「由协议在参数变更生效前校验，违反者提案无效且永不激活」。白名单由系统合约维护，只能通过 validator voting 更新，分类读父块末状态的名单，所以「名单变更不影响包含它的那个块」。

## 1.2 动机审视

### 1.2.1 问题的结构性通道是真实的

BSC 的 base fee 恒为 0（[BEP-226](https://github.com/bnb-chain/BEPs/blob/master/BEPs/BEP226.md)，代码上 `consensus/misc/eip1559.CalcBaseFee` 在 BSC 直接短路返回 `params.InitialBaseFeeForBSC`），所以**打包与否 100% 由 priority fee 竞价决定，且 priority fee 全额归 coinbase**。这意味着异质流量共享唯一一个竞价维度，一类流量的需求脉冲会把所有其他类的准入价格顶起来——这个传导通道在代码层面确实存在，不是臆想。

支付类流量在这个通道里处境最差也说得通：单笔 gas 小（21000 或约 5 万）、时间敏感、发起方（钱包、交易所提币系统）最不具备逐块重新定价的能力。

### 1.2.2 但「损害已发生」在 BEP 里零证据 —— 这是动机层最大的软点

§2 通篇是定性论述，**没有任何一个链上数据点**。具体缺失的：

- 历史拥堵窗口里，支付类交易的**实际延迟分布**（p50/p95/p99 从进池到上链的块数）；
- 同一窗口内支付类交易的**淘汰率**（进池后超过 N 块未上链的比例）；
- 支付类交易为了保持上链所付出的**溢价倍数**（对比非拥堵基线）；
- 反事实：若 lane 已存在，上述指标能改善多少。

这不是学术洁癖。它直接决定三件事：(a) 参数初值该定多少（2% 还是 8% 完全没有依据）；(b) §5「Outside congestion… observable behavior is essentially identical」这句话的适用比例；(c) 提案值不值得付出后文的 175–210 人日 + 一次硬分叉 + 一个新的共识关键系统合约。

**建议**：在 BEP 定稿前补一份实证附录，用主网历史数据回答上述四问。口径建议为：选取若干个 `gasUsed/gasLimit` 持续高位的窗口，按本 BEP §3.1 的判据回溯分类，测量支付类与 general 类的延迟/淘汰/溢价三组指标的差值。**如果差值不显著，这个提案的动机就不成立**——报告不替 BEP 编造数据，但必须指出这个门槛存在。

### 1.2.3 业界定位

同类机制的先例都不是「为某类交易预留 gas」，而是「为某类资源开独立维度」：EIP-4844 的 blob gas 是完全独立的第二维度、独立定价、独立目标；Solana 的 write-lock 是并发控制而非配额；Celestia 的 namespace 是数据分区而非执行配额。

BEP-703 的形态更接近**在同一维度内做优先级预留**，这在公链上罕见，原因正是 M5：只要预留区间的定价与主区间解耦，就必然产生跨区间套利面。这不代表方案不可做，但代表它需要比先例更强的滥用防护，而 §6 目前提供的防护不足。

## 1.3 可行性总判

| 维度 | 判定 | 依据 |
|---|---|---|
| 共识规则可实现性 | ✅ 可实现 | 不等式落在 `core/block_validator.go:152` 旁；分类可在 tx 循环前一次性完成 |
| 递推态可引导性 | ⚠️ **BEP 现文本不可实现**，改用 `UncleHash` 承诺后 ✅ | 见 M1 |
| 分类安全性 | ⚠️ 现文本有 5 个可利用缺口 | 见 M2、M3 |
| 滥用防护 | ❌ **现文本滥用上限 = 100% lane** | 见 M4 |
| 经济自洽性 | ❌ **存在跨区间套利面，§6 未覆盖** | 见 M5 |
| 参数安全性 | ⚠️ 不变量缺一条，可致链停 | 见 M6 |
| 抗操纵性 | ⚠️ §6 的成本论证在离散边界失效 | 见 M7 |
| 治理可落地性 | ⚠️ 「提案无效」的实现语义与文本不符 | 见 M8 |
| MEV 兼容性 | ❌ **章节完全缺失，且存在零成本丢槽攻击面** | 见 M9 |
| 覆盖率可持续性 | ⚠️ 随钱包升级单调下降 | 见 M10 |

**结论：有条件可行。** 前置条件是 M1–M10 全部在 BEP 层面裁决完毕；其中 M4、M5 需要产品层面拍板（是否接受、还是要求机制层面缓解），M1、M6、M8、M9 有明确修法。

## 1.4 十项机制级问题

### M1 · §3.3「不需要新 header 字段」不成立 —— 已裁决为 `UncleHash` 承诺

**问题拆解为三层，每层都独立致命：**

**(a) 配额是递归量，不是函数。** §3.3 说它是「a deterministic function of chain history」。实际形式是 `q(n) = clamp(q(n−1) ± step)`——要算 `q(n)` 必须先有 `q(n−1)`，一路回溯到激活块。Fermi 后块间隔 450ms（`consensus/parlia/parlia.go:64`），约 19.2 万块/天，激活一年后递推深度约 7000 万块。全量重推不是引导流程，是重新同步。

**(b) signal 的分子不可从存储恢复。** `generalGasUsed(n−1)` 不在 header 里。要重算它需要：块 n−1 的 receipts（有，逐笔 `GasUsed` 可由 `CumulativeGasUsed` 差分重建，`core/types/receipt.go:388-415`）**加上块 n−2 的末状态**（分类类别① 要读 `to` 的 codeHash、类别②③ 要读白名单）。

于是：**snap sync 节点在 pivot 提交那一刻，有从创世到 pivot 的全部 receipts，但只有 pivot 这一个 state root**（`eth/downloader/downloader.go:1694-1709` 的 `commitPivotBlock` → `core/blockchain.go:1370-1405` 的 `SnapSyncComplete`）。它执行的第一个块是 `pivot+1`，需要 `laneSize(pivot+1)` ← `signal(pivot)` ← `generalGasUsed(pivot)` ← `state(pivot−1)`，**没有**。K=1 就断。

雪上加霜：BSC 有生产在用的历史裁剪 `--history.blocks`（`cmd/utils/flags.go:431-436` → `core/rawdb/chain_freezer.go:519-542`），receipts 表 `prunable: true`（`core/rawdb/ancient_scheme.go:52`），最小保留 60 万块 ≈ 3 天。而 `HistoryPruningCutoff()` 在 BSC 上永远返回 `(0, genesisHash)`（`core/blockchain.go:885-890`），所以这类节点**对外宣称自己有完整历史**——任何「回溯到激活块」的逻辑会相信这个声明，然后在任意深度撞上 nil body。

**(c)「只回溯最近 K 块」在数学上不 sound。** 这是最诱人的省事解法，必须明确否掉。

设递推态为 ratio `r ∈ R = [R_min, R_max]`，步进映射 `f_σ(x) = clamp(x + σ, R_min, R_max)`，`σ ∈ {+E, −S, 0}` 由 `signal` 三分支决定。令 `F_K = f_{σ_{n−1}} ∘ … ∘ f_{σ_{n−K}}`。

每个 `f_σ` 单调不减且 1-Lipschitz，故 `F_K` 亦然。因此

> `F_K` 在 `R` 上为常数（即 K 块窗口足以唯一确定 `r_n`）⟺ `F_K(R_min) = F_K(R_max)`。

**引理（间距只减不增）**：令 `d_j` 为从 `R_max` 与从 `R_min` 出发两条轨迹在第 j 步后的差，`d_0 = W := R_max − R_min`。两条轨迹接受同一个 `σ_j` 并 clamp 到同一区间，故 `d_j <= d_{j−1}`，且 `d_j = d_{j−1}` ⟺ 该步 `σ_j = 0` 或该步无 clamp 生效。

**反例（不存在任何有限 K）**：若窗口内**全部** `σ_j = 0`，即 `signal(m) ∈ [SHRINK_TRIGGER, EXPAND_TRIGGER) = [70%, 80%)` 对所有 `m ∈ [n−K, n−1]` 成立，则 `d_K = W > 0`，于是 `F_K(R_min) = R_min ≠ R_max = F_K(R_max)`。

**两条链可以有逐字节相同的最近 K 个块（全部 75% general 利用率），却得到相差 `W` 的 `laneSize`。** `W = 6pp`、`GasLimit = 70M` 时相差 4.2M gas——一条链接受块 n，另一条拒绝，共识分裂。

**关键讽刺：这个反例是 BEP 自己的不变量保证其存在的。** 不变量 (1) `EXPAND_TRIGGER − SHRINK_TRIGGER >= TRIGGER_GAP_MIN = 10%` 保证 `σ=0` 的滞后带永远存在且宽达 10pp；不变量 (3) `MAX_RATIO − MIN_RATIO >= RATIO_GAP_MIN = 5%` 保证 `W >= 5pp > 0`。**治理无法通过调参消除它。§3.3 与 §3.5 在数学上互相排斥。**

（附带否掉「带默认种子的 K 窗口」——定义 `laneSize(n) := F_K(R_min)`：确定性靠定义找回，但 (i) 连续 >K 个滞后带块后配额从任意高度瞬间跌回 `R_min`，直接违背不变量 (2) 的立意；(ii) 它给出了一个比 §6 所排除的更便宜的操纵——只需把 general 利用率保持在滞后带内 K 个块，仍收约 75% 的费用收入，而 Maxwell 后单个 validator 的 turnLength 已达 16，一个 turn 就接近 K；(iii) **它根本没解决引导问题**，见 (b)。）

---

**裁决：复用 `header.UncleHash` 承载递推态。**

BSC 的 Parlia 不允许 uncle，`UncleHash` 恒为 `types.EmptyUncleHash`，是一个**完全没有信息量的 32 字节字段**。[BEP-696](https://github.com/bnb-chain/BEPs/blob/master/BEPs/BEP-696.md)（**已 MERGED**）正是「复用 `UncleHash` 与 `ParentBeaconRoot` 作通用元数据」，并已批准配套的生态兼容性声明：

> Clients MUST accept any 32-byte value in `UncleHash` and `ParentBeaconRoot` when validating Parlia blocks.
> The uncle list in the block body MUST remain empty. Clients MUST verify this directly on the body, and MUST NOT derive the expected uncle list from `UncleHash`.

BEP-696 在本仓尚未实现（`grep BEP-696 --include=*.go` 零命中），**且没有部署任何具体的元数据编码**，因此不存在字段占用冲突——BEP-703 可以先定义布局。

**收益（这是本方案压倒性优于其他选项的原因）：**

| 问题 | `UncleHash` 承诺后 |
|---|---|
| 递推深度 | 7000 万块 → **1**（只读父 header） |
| snap sync 引导 | header 永远随链下载，**零特例** |
| reorg | 值随 header 走，`core/blockchain.go` 的 reorg / setHead / rewind / insertSideChain / recoverAncestors **全部零改动** |
| 重启持久化 | header 本来就持久化，**零新格式** |
| 历史裁剪 | 只需父 header，**无历史依赖** |
| header-only 校验 | ✅ 可（`generalGasUsed` 与 `paymentGasUsed` 都在 header 里，不等式变成纯算术） |
| 认证 | **白送**：`UncleHash` 已在 `encodeSigHeaderWithoutVoteAttestation`（`consensus/parlia/parlia.go:1872`）内 → 被 validator 的 seal 签名覆盖，也进 block hash |
| RLP 布局 | **零改动**，不触碰 upstream 的 optional 尾部字段序列（对比：新增 header 字段会撞上 `BlockAccessListHash`(EIP-7928) / `SlotNumber`(EIP-7843)，因 `*common.Hash` 为 nil 时 optional 编码不可 round-trip，只能插在它们之前 → 与上游 header 布局永久分叉） |
| MEV BidBlock | **顺带关闭丢槽攻击面**，见 M9 |

**建议的 32 字节布局：**

| 偏移 | 宽度 | 字段 | 说明 |
|---|---|---|---|
| `[0:8]` | 8B | `laneSize` | 递推态本体，big-endian uint64，单位 gas |
| `[8:16]` | 8B | `generalGasUsed` | big-endian uint64，signal 的分子 |
| `[16:24]` | 8B | `paymentGasUsed` | big-endian uint64 |
| `[24]` | 1B | 版本，恒为 1 | **不是为扩展**：`NewBlock` 用「`UncleHash` 是零值」判断调用方写过没写过，而 `Commitment{0,0,0}` 恰好编码成全零（配额为 0 的空块，激活初期最常见的形状），会被覆写成 `EmptyUncleHash` 再被导入侧拒掉 |
| `[25:32]` | 7B | 保留，必须为零 | 留给 BEP-696 后续元数据 |

`systemGasUsed = header.GasUsed − generalGasUsed − paymentGasUsed`，派生即可。

**递推态用 gas 而不是 ratio（对 BEP 文本的偏离，需与作者确认）**：§3.3 的字面表述是对 **ratio** ±一步。两者在 `GasLimit` 恒定时完全等价，**前提是步长必须按当前 `GasLimit` 折算**（`stepGas = EXPAND_STEP × GasLimit / 10000`）——写成固定 gas 步长会让扩张的相对速度随链容量增长而悄悄变慢，那正是 §4 说 ratio 设计要避免的。`GasLimit` 变动时两者会短暂分歧（gas 空间要多花约一个块爬回去），但边界钳制会把它拉回同一稳态（已数值验证）。选 gas 空间的理由：承诺的值恰好就是矿工要用来打包的那个量，于是不需要额外字段，也不存在「配额与 ratio 两个派生值必须一致」这条无人强制的关系。

**为什么必须存三桶而不是两桶**：Parlia 系统交易的 gas 直接加进 `header.GasUsed`（`consensus/parlia/parlia.go:2198`），breathe 块的 `updateValidatorSetV2` 单笔实测约 12,160,000 gas（`consensus/parlia/parlia.go:1365-1383` 注释给出该值，也正是 `SystemTxsGasHardLimit = 20M` 的由来）。若把系统交易并入 general，则 70M 块上 `12.16M / 70M = 17.4pp` 的假拥堵抬升会**每天一次**把 signal 顶向 `EXPAND_TRIGGER = 80%`，触发一次与拥堵无关的配额扩张。存 `general` 与 `payment` 两个显式值、system 派生，signal 就天然干净。

**代价（必须诚实列出）：**

1. 三处兼容性放松必须 fork-gate（这相当于顺带实现 BEP-696 的客户端部分）：
   - `consensus/parlia/parlia.go:638` `if header.UncleHash != types.EmptyUncleHash { return errInvalidUncleHash }` → 激活后改为只在 body 上验 uncle 为空；
   - `core/block_validator.go:67` `if hash := types.CalcUncleHash(block.Uncles()); hash != header.UncleHash` → **这是从 body 反推 header 的检查，正是 BEP-696 明文禁止的形态**，激活后改为 `len(block.Uncles()) != 0` → 报错；
   - `core/types/block.go:320-321` `if len(uncles) == 0 { b.header.UncleHash = EmptyUncleHash }` → **最大陷阱：`types.NewBlock` 无条件覆写，所有 assemble 路径都过它**，激活后必须保留调用方写入的值。
2. `core/types/block.go:217` `EmptyBody()` 用 `UncleHash == EmptyUncleHash` 判空 → 写入 lane 数据后恒返回 false。唯一消费者是 `eth/downloader/queue.go:83`，后果是**空块也会去拉 body**（多余但安全，非正确性问题）。建议一并 fork-gate。
3. RPC 的 `sha3Uncles` 会暴露原始值 → 假设它恒为空 uncle hash 的浏览器、indexer、跨链 light client、reth-bsc 都需要更新。BEP-696 已把这个兼容性代价声明并批准，但**BEP-703 是第一个真正付这个代价的提案**。

### M2 · §3.1「类别① 不承载任何代码执行」被三处证伪

§4 的原话是「category ① admits no code execution at all」。这是整个机制的安全边界。实际上：

**(a) 预编译地址在 state 里没有 code，但会执行。** `core/vm/evm.go:313` 的 `isPrecompile` 分支在 code 解析**之前**判断，`RunPrecompiledContract` 会真的跑。而 `GetCodeHash(0x02)` 对不存在的账户返回零 hash——按 §3.1 的机械判据（`to` 无 code + data 空）**被判为 payment**。

受影响地址：以太坊 `0x01`–`0x11`、`0x0100`（P256）、以及 BSC 自有的 `0x64`–`0x69`（`core/vm/contracts.go:405-410`，含 `iavlMerkleProofValidate` `0x65`、`blsSignatureVerify` `0x66`、`cometBFTLightBlockValidatePasteur` `0x67`、`verifyDoubleSignEvidence` `0x68`）。

**且后果不是「无害地快速返回」**：预编译 `Run` 返回**非 revert** error 时，`core/vm/evm.go:337-341` 会 `gas.Exhaust()` 把剩余 gas 一次烧光。所以一笔 `to=0x65, data=∅, gas=16,777,216` 是 payment 类，且**单笔烧尽整条 lane**（`MaxTxGas = 1<<24`，`params/protocol_params.go:31`；主网 Osaka 已于 2026-04-28 激活、Amsterdam 为 nil，故该上限当前在生效，见 `core/state_transition.go:457`）。

**(b) `SetCodeTx`（type `0x04`）的 authorization 在顶层调用之前安装。** `core/state_transition.go:636-641` 的 `applyAuthorization` 先于顶层 call 执行，`:787` 做 `SetCode(authority, AddressToDelegation(...))`，**没有 per-tx 撤销**。而 `SetCodeTx.To` 恒非 nil（`core/types/tx_setcode.go:199`），`data` 可空。于是：一笔 `to=A`、`data=∅`、authorization 授权 A 的 SetCodeTx，在父状态下 A 无 code → 判为 payment ①，执行时 A 已被装上 delegation → `core/vm/evm.go:317` 的 `resolveCode` 解析出 delegate 代码并**真的执行**。

**「父块末状态无 code」这个检查救不了 SetCodeTx 自己。** 而每条 authorization 25000 gas（`params.CallNewAccountGas`），320 条 = `21000 + 320×25000 = 8.02M` —— 单笔占满 8M lane 且全部是状态写入。

**(c) `BlobTx`（type `0x03`）的 `to` 恒非 nil（`core/types/tx_blob.go:296`），`data` 可空** → 判为 payment ①。执行 gas 只有 21000，却带走独立的 blob 维度预算，等于用 lane 的折扣价买 DA 空间。另注意 BSC 只在 `header.Number % params.BlobEligibleBlockInterval == 0` 的块收 blob 交易（`miner/worker.go:1120`），所以 lane 会周期性被 DA 流量侵占。

**(d) 附带：纯 access-list 交易不执行任何代码但能吃满 lane。** `accessList` 计入 intrinsic gas（2400/地址，`params.TxAccessListAddressGas`），一笔 `to=EOA, data=∅, accessList=3300 个地址` ≈ 8M gas，判为 payment。

**修法**：类别① 改为**白名单式**——只允许 tx type `0x00`/`0x01`/`0x02`，显式排除 `BlobTxType`/`SetCodeTxType` 及一切未来新增类型；要求 `len(accessList) == 0`；排除当前分叉激活的所有预编译地址；并把 §4 的措辞从「no code execution at all」改为「不执行 state 中的 bytecode」。

### M3 · 分类必须用独立的 parent-root 只读视图，否则出块者可武器化

§3.1 要求对**父块末状态**判定。但两条路径上手边的 statedb 都已被污染：

- 导入侧 `core/state_processor.go` 的 `statedb` 在进入 tx 循环（`:127`）前已被写过三次：`:96` `TryUpdateBuildInSystemContract`（`SetCode`）、`:111` `ProcessBeaconBlockRoot`、`:114` `ProcessParentBlockHash`；随后每笔交易继续推进。
- 出块侧 `miner/worker.go` 的 `env.state` 同理（`:1070`/`:1073`/`:1077`）。

于是**同一区块内分类会漂移，两个方向都有**：无码→有码（普通 CREATE/CREATE2 部署到确定地址；EIP-7702 `SetCode`）、有码→无码（delegation 置零，`core/state_transition.go:780-784`；Cancun 前的 SELFDESTRUCT）。

**为什么这不只是「不精确」而是「可武器化」**：用推进中的 statedb 本身是确定性的（所有节点按同序重放结果一致），**不会直接裂链，测试也会全绿**。真正的问题是：**出块者只要在一批转账前插一笔便宜的 CREATE2 或 SetCodeTx（授权目标 = 那批转账的收款地址），就能决定「哪些用户的转账进 lane」**。对某个热门收款地址（交易所充值地址）装/卸 delegation，就是一个成本一笔交易的准入开关。这恰是 §4 声称机制免疫的那类操纵——「信号由选交易的人书写」。

这类 bug 的症状在测试里完全不可见，只在主网上被人利用时才显形。

**修法**：从第一天就走绑定 `parent.Root` 的独立只读 `state.StateReader`。另有两个具体实现陷阱（见 2.3）：**不存在的账户 `GetCodeHash` 返回零 hash 而不是 `EmptyCodeHash`**（`core/state/statedb.go:415-421`），写成 `GetCodeHash(to) == types.EmptyCodeHash` 会把每一笔转向全新地址的转账判成 general——恰好是 lane 最核心的用例（首次充值、新钱包收款）；以及应当用 `codeHash` 而非 `GetCodeSize`（后者在 witness 模式下无条件加载整个 code blob）。

### M4 · ②/③ 只看 `to` 不看 selector → 滥用上限就是整条 lane 的 100%

§4 声称「categories ②/③ admit only contracts whose entire function surface is value transfer」。但 USDT/USDC 的函数面远不止转账（`approve`/`permit`/`transferFrom`/`blacklist`…），而且 §3.1 的判据是**纯 `to` 查表，任何 calldata 都算 payment**，包括无效 selector。

**量化滥用上限：**

| 手法 | 单笔 gas | 占满 8M lane 所需 | 前置条件 |
|---|---|---|---|
| 发给 USDT 的 128KB 非零 calldata | **5,263,880**（EIP-7623 floor = `21000 + 10 × 4 × 131072`，`params/protocol_params.go:100-101`；**revert 也照收**，`core/state_transition.go:663-674`） | **2 笔** | 无（不需要有效 selector、不需要合约配合） |
| `approve` 洪水 | ≈46,000 | 174 笔 | 无 |
| 预编译燃 gas（M2-a） | 最多 16,777,216 | **1 笔** | 无 |
| 7702 批量授权（M2-b） | `21000 + 25000×n` | 1 笔（n=320） | Prague 已激活（主网 2025-03-20） |
| 最小额自转账 | 21,000 | 381 笔 | 无 |

**⇒ 滥用上限 = lane 的 100%，成本 = `laneSize × 车道内清算价`，无任何技术门槛。**

按 `p_lane = 0.05 gwei`、BNB $1000 估算：封锁全网支付车道一个块约 $0.4，450ms 出块 → **约 $77k/日**即可持续 100% 占用「为保障基础转账而设」的车道。

§6 的「flooding the lane costs fees like any other spam and displaces only other payment traffic」前半句对，后半句在这个量级下失去意义——被 displace 的正是这条车道存在的全部理由。

**修法选项**（需产品裁决，见附录 A）：(i) ②/③ 增加 selector 白名单（只允许 `transfer`/`transferFrom`）；(ii) 给 lane 内交易设单笔 gas 上限（例如 100,000）；(iii) 接受并在 §6 明确披露。**(ii) 的实现代价最低且顺带解决 M4 与下面 M9-a 的「保底笔数」问题。**

### M5 · lane 在拥堵期是一条打折 gas 通道 —— 最实质的经济漏洞

这一条不是实现缺陷，是机制设计的结构性后果，且 BEP 完全没有讨论。

payment 交易只与 payment 交易竞争 lane 内的配额（§6 明确这一点，并把它当作优点）。于是**拥堵期 lane 内的清算价 `p_lane` 必然远低于大盘价 `p_gen`**——大盘在抢 `GasLimit − laneSize` 的空间，lane 内只有支付流量在抢 `laneSize`。

**攻击者据此以数十倍折扣把非支付负载塞进链**：
- calldata DA（M4 的 128KB 手法，本质是用 lane 价买数据可用性）；
- 7702 批量授权（M2-b，一笔完成 320 个账户的 delegation 安装）；
- 预编译燃 gas（M2-a）；
- blob 空间（M2-c）。

**叠加 BEP-226 的效应更糟**：base fee 恒 0，priority fee **全额归 coinbase**。所以**出块者自己**往自己的 lane 里塞自付转账时，钱在自己的两个地址间打转、手续费付给自己，**净成本为零**。§6 的「flooding the lane costs fees」对出块者本人不成立，要求 `value > 0` 也堵不住（给自己转 1 wei）。

**这条无法靠 §3.1 的判据修补**，它是「预留区间与主区间定价解耦」的必然产物。§6 必须承认并重新分析；若要缓解，需要机制层面的手段（例如 lane 内设最低 priority fee 地板、或把 lane 定价与大盘挂钩），而那会实质改变提案形态。

### M6 · 不变量缺 `PAYMENT_LANE_MAX <= GasLimit` → 治理误设可致链停

四条不变量只要求 `PAYMENT_LANE_MAX > PAYMENT_LANE_MIN > 0`，**不禁止它们超过 `GasLimit`**。

若治理把 `PAYMENT_LANE_MIN` 设为 `GasLimit + 1`（合法通过全部四条不变量），则 §3.2 的不等式 `generalGasUsed + max(paymentGasUsed, laneSize) <= GasLimit` 对**任何块，包括空块**（`0 + L > GL`）恒不成立 → **全网无法产出任何有效块 → 链停**。

更温和但同样糟的情形**不需要误设参数，只需要 GasLimit 下行**：`GasLimit = 10M` + 建议初值 → ratio 区间 `[200k, 800k]` 与绝对区间 `[2M, 8M]` 交集为空 → 按 §3.4「absolute bounds prevail」有效区间变成 `[2M, 8M]` = **块的 20%–80%**，`MAX_RATIO = 8%` 的保护完全失效；`GasLimit = 2M` 时 `laneSize = 2M = 100%`，general 被彻底锁死。而 `CalcGasLimit` 每块可动 `±GasLimit/1024`（`core/block_validator.go:210-223`），从 70M 走到 2M 约需 3600 块 ≈ 27 分钟。

**修法**：补第五条不变量把绝对边界与 `GasLimit` 绑定（例如 `PAYMENT_LANE_MAX <= GasLimit / 4`），并在客户端侧对 `laneSize` 做最终钳制（fail-safe，见 2.6）。

### M7 · §6 的 signal 压制成本论证在离散边界上不成立

§4/§6 论证：「Suppressing the expansion signal requires producing under-filled blocks during peak fee demand — the manipulation is directly and continuously costly」「Denying congestion is possible only at the cost of forgoing income」。

但触发阈值是**硬边界**。validator 只需把 `generalGasUsed` 停在 `ceil(0.80 × GasLimit) − 1`，**放弃的收益是 1 gas 的费用，趋近于零**。真实成本只是「把边际交易挪到下一块」的延迟成本，而非「放弃整个 pulse 的收益」。

**修法**：把 signal 改为多块滑动窗口平均（例如最近 8 块的 `generalGasUsed/GasLimit` 均值），使压制必须持续多块才有效，并在 §6 给出压制成本的实际下界。注意这与 M1 的 `UncleHash` 方案兼容——滑动窗口只需读最近 K 个 header，深度仍然有界且 header-only 可算。

### M8 · §3.5「提案无效且永不激活」的实现语义是假的

BSC 的治理链路是 `BSCGovernor.propose → 7天投票 → BSCTimelock 24小时 → GovHub.updateParam(key, value, target) → target.updateParam(key, value)`。

**`GovHub` 用 try/catch 吞掉 target 的 revert**，且 `GovHub.updateParam` 忽略返回码。于是违反不变量的提案会：Timelock 执行成功、Governor 把提案标记为 `Executed`、**参数完全没改**、唯一线索是一条 `failReasonWithStr` 事件。

这在字面上满足「无效且永不激活」，在安全上甚至是好事（不停链、不改半个 state），但**在运维上是陷阱**：治理会以为提案生效了。

**更严重的是 `IParamSubscriber.updateParam(string key, bytes value)` 是单键写**，多参数变更靠打包多次调用，**每次独立校验不变量**。看具体后果：把滞后带从 `[70, 80]` 移到 `[85, 95]`（`TRIGGER_GAP_MIN = 10`）——
- 先写 `SHRINK=85` → 检查 `80 − 85 = −5 < 10` → revert，被吞掉；
- 再写 `EXPAND=95` → 成功；
- **结果 `[70, 95]`**，每一步都「合法」，配置是半应用的，而治理看到的是 `Executed`。

**修法**：(a) §3.5 措辞改为准确描述（参数写入回滚并被丢弃，提案在链上仍记为已执行，唯一信号是事件）；(b) **强制单一原子 setter**：`updateParam("paymentLaneParams", abi.encode(8 个参数))`，在整个元组上一次性校验四条不变量；(c) 要求提供只读接口让治理在执行后验证实际生效值。

**顺带的运维事实**：端到端治理延迟 ≥ 8 天（7 天投票 + 24 小时 timelock，quorum 晚到再 +1 天），现实是 10–14 天。**这意味着 8 个参数在拥堵事件的时间尺度（分钟到小时）上完全不可调**，§3.5「Suggested initial values, for discussion rather than locked by this BEP」低估了初值的分量——一旦主网上线，改一次要两周。规格应显式承认：严重误配的唯一快速补救是紧急硬分叉（有 `FeynmanFix`/`HaberFix` 先例）。

### M9 · MEV / builder 一节完全缺失

BEP 一个字没提 builder。三个具体缺口：

**(a) BidBlock（BEP-675）路径存在零成本丢槽攻击面。** `miner/bid_block.go:265` 的 `mux.Post(NewSealedBlockEvent)` 发生在 `:278` 的 `InsertChain` **之前**——validator **不执行交易就签名并广播**。而 `preSealVerifyBidBlock`（`miner/bid_simulator.go:714-768`）检查了 coinbase、parent、`GasLimit == CalcGasLimit(...)`、`VerifyUnsealedHeader`、`TxHash`、per-tx gas cap、系统交易，**从来没有检查 `header.GasUsed`**（`miner/bid_block.go:273` 的注释明确把它归为「事后 InsertChain 才验」）。

lane 不等式是执行后规则，不执行拿不到 `paymentGasUsed`。⇒ **一个 lane 超限但其余全部合法的 BidBlock 会被 validator 签名、广播，然后全网拒收：validator 丢一个槽位，builder 只被 revoke 一段时间。攻击者以一笔 bid 的成本让指定 validator 丢槽。**

隐蔽点在于 `preSealVerifyBidBlock` 里已经有 `header.GasLimit` 的检查，review 时很容易误以为「gas 相关的已经查了」。

**M1 的 `UncleHash` 方案顺带关闭这个攻击面**：`BidBlock` 携带完整 `*types.Header`，`paymentGasUsed`/`generalGasUsed` 天然带过来，admission 阶段一行纯算术即可在签名前拒掉。**这是选择 `UncleHash` 而非 state slot 的决定性理由之一。**

**(b) 系统交易与 payBidTx 的归类未定义。** `params.PayBidTxGasLimit = 25000` 的 payBidTx 是 builder 向 validator 地址的**纯转账、data 为空、收款方是 EOA**——按 §3.1 类别① 的机械判据**它就是一笔 payment 交易**，MEV 回扣搭上了「保障普通转账」的便车。Parlia 系统交易目前恰好都发给有 code 的系统合约，机械上不会误判，但**必须显式规定归 general/system 类**，否则未来某个系统交易改成发给 EOA 就会静默漏出。

**(c) 协议面变更未定义。** `RawBid.GasUsed`（`core/types/builder/bid.go:89`）是 builder 自报的**标量，没有类别拆分**；`miner/bid_simulator.go:1068` 的 `bid.GasUsed > env.gasPool.Gas()` 是唯一的块级 gas 门，类别不感知 → 一个 `GasUsed` 合法但 general 部分超限的 bid 会浪费整个模拟时间预算才失败，且报错是笼统的 `invalid tx in bid`，builder 完全不知道原因。`MevParams`（`core/types/builder/bid.go:308-318`，经 `mev_params` 暴露）必须新增 `PaymentLaneSize`，否则 builder 只能靠猜。分叉后 builder 的既有算法（「填到 GasLimit 为止」）会**系统性地**产出被拒的 bid，验证者 MEV 收入断崖。

### M10 · 排除 EIP-7702 智能账户 → 覆盖率随钱包升级单调下降

§3.1 明文把带 delegation designator 的账户排除。而 EIP-7702 已在 BSC 主网激活（`params/config.go:248-249`，PascalTime = PragueTime = 1742436600，2025-03-20）。

**覆盖率盘点**（以「用户主观认为自己在做支付」为分母）：

| 场景 | ①/② 覆盖 | 原因 |
|---|---|---|
| 用户→用户 BNB 转账 | ✅ ① | EOA→EOA |
| CEX **单笔** BNB 提币 | ✅ ① | 热钱包直接 EOA→EOA |
| CEX BNB 充值 | ✅ ① | 用户 EOA → 充值地址 |
| 直接 `USDT.transfer` | ✅ ② | `to` = USDT |
| CEX **批量**提币 | ❌ | disperse/multisend 合约，一笔内 N 个内部 CALL |
| 多签（Safe）转账 | ❌ | `to` = 代理合约 |
| router / 聚合器转稳定币 | ❌ | `to` = router |
| ERC-4337 智能账户 | ❌ | `to` = EntryPoint |
| **EIP-7702 智能账户** | ❌ **且被显式排除** | §3.1 明文排除 delegation |
| 商户批量代付 / 桥 | ❌ | 合约批处理 |

覆盖率大致在一半上下——EOA→EOA BNB 转账与直接 `USDT.transfer` 确实是量最大的两块，但批量提币、多签、路由、智能账户这四块整体不小，**且趋势是上升的**。

**最需要指出的结构性矛盾**：BEP 的分类规则与支付 UX 的演进方向相反。7702 智能账户（gas 代付、批量、社交恢复）正是「更好的支付体验」的主流路线，而 §3.1 把它逐字排除。随着钱包升级到 7702，lane 的覆盖率会**单调下降**——一个用户在钱包升级后，他的转账会自动被踢出这条为他设的车道。§4「Classification by destination, not declaration」那段的自洽性建立在「支付永远由裸 EOA 发起」这个假设上。

技术上纳入并非不可能（要求 `to` 的 delegate code hash 在一个白名单里），但那等于把 ② 的治理负担扩展到所有钱包实现商。**BEP 至少应写明这条路被考虑过并给出放弃理由。**

## 1.5 规格修订清单

按 BEP 章节归类，共 27 条。**M** = 必须（不改则无法实现或有安全问题），**S** = 应当（定义精度，不改则实现分歧）。

### §3.1 交易分类（12 条）

| # | 级 | 条款 |
|---|---|---|
| 1.1 | M | 明确「父块末状态」= `parent.stateRoot` 对应的状态，**不含**本块的 pre-execution system call（`TryUpdateBuildInSystemContract` / EIP-4788 / EIP-2935） |
| 1.2 | M | 明文禁止用块内推进状态分类，并说明理由（出块者可操纵，M3） |
| 1.3 | M | 「无 code」判据精确化为 `codeHash == keccak256("")`，且**不存在的账户视为无 code**；禁止用 `codeSize`（实现上会加载 code blob） |
| 1.4 | M | 类别① 限定 tx type 白名单 `{0x00, 0x01, 0x02}`；显式排除 `BlobTxType`、`SetCodeTxType`；未来新增类型默认不进 lane（M2-b/c） |
| 1.5 | M | 类别① 要求 `len(accessList) == 0`（M2-d） |
| 1.6 | M | 类别① 排除当前分叉激活的所有预编译地址（含 BSC 自有 `0x64`–`0x69`）（M2-a） |
| 1.7 | M | 显式规定 Parlia 系统交易与 MEV payBidTx **不属于** payment 类（M9-b） |
| 1.8 | S | 类别① 要求 `value > 0` |
| 1.9 | S | 禁止把系统合约地址与预编译地址列入 ②/③ |
| 1.10 | S | 修正 §4「category ① admits no code execution at all」→「不执行 state 中的 bytecode」 |
| 1.11 | S | 明确 txpool 与 block builder 必须使用同一状态视图；说明池内交易跨块时分类会重新求值 |
| 1.12 | S | 回应 7702 智能账户被排除后覆盖率单调下降的问题（M10） |

### §3.2 记账不等式（4 条）

| # | 级 | 条款 |
|---|---|---|
| 2.1 | M | 引入第三桶 `systemGasUsed`，不等式改为 `systemGasUsed + generalGasUsed + max(paymentGasUsed, paymentLaneSize) <= GasLimit`；说明它与既有 `EstimateGasReservedForSystemTxs`（普通块 1M / breathe 块 20M）如何叠加 |
| 2.2 | M | 明确 `paymentGasUsed` 的口径是**块级 refund-exclusive**（同 `header.GasUsed`），**不是** receipt 的 `CumulativeGasUsed`（refund-inclusive，见 `consensus/parlia/parlia.go:2200-2207`）。两条链取值不同，必须挑一条 |
| 2.3 | M | 解决 §3.2 与 §3.3 note 的自相矛盾：§3.2 定义 `generalGasUsed` 只含 general 交易，§3.3 note 说它「includes payment gas consumed beyond the quota」。必须二选一，并写出对应公式 |
| 2.4 | M | 若采用 §3.3 note 的口径，明确溢出的 payment gas 是**按 gas 数量切**还是**按 tx 边界切**（`paymentGasUsed = 9M, L = 8M`，某笔用 3M 跨过边界时，这 3M 全算 general / 全算 payment / 拆成 2M+1M——三种口径给出三个不同的下一块配额） |

### §3.3 / §3.4 配额推进与边界（6 条）

| # | 级 | 条款 |
|---|---|---|
| 3.1 | M | **删除「no new header field is required」**，改为规定递推态承诺进 `header.UncleHash`，并给出 32 字节布局（M1） |
| 3.2 | M | 定死递推态是 **bps 整数 ratio**，只钳制在 `[MIN_RATIO, MAX_RATIO]`；绝对边界只在换算成 gas 时事后施加（消除 §3.3「ratio」与 §3.4「gas 钳位」的语义冲突） |
| 3.3 | M | 定死 signal 比较的整数形式（交叉相乘 `generalGasUsed × 10000 >= trigger × GasLimit`，不做除法、禁止浮点），并给出 `GasLimit` 硬上界以证明无溢出 |
| 3.4 | M | 定死 ratio→gas 的换算顺序与**舍入方向**（先除后乘 / 向下取整）。这是逐 bit 一致性要求，BSC 是多实现生态 |
| 3.5 | M | 明确「参数变更后递推态**每块**重新钳制」，而非只在发生 step 时钳制（否则参数收窄后旧递推态会停在区间外） |
| 3.6 | S | 明确激活块语义：Feynman 后系统合约升级发生在**区块末**（`core/systemcontracts/upgrade.go:1112-1115`），故 lane 规则最早在「激活块 + 1」生效，激活块本身豁免；并明确用于计算 `laneSize(激活块+1)` 的 signal 如何从「无类别拆分的历史块」取得 |

### §3.5 参数与治理（4 条）

| # | 级 | 条款 |
|---|---|---|
| 5.1 | M | 补第五条不变量把绝对边界与 `GasLimit` 绑定（M6） |
| 5.2 | M | 强制单一原子 setter，禁止逐参数键（M8） |
| 5.3 | M | 修正「a governance proposal violating any of them is invalid and never activates」的措辞为准确描述（M8） |
| 5.4 | M | 规定客户端侧的独立钳制义务，并给出失败处理二分法：「读成功但值非法」→ 确定性，必须钳制；「读取本身失败」→ 非确定性，必须上抛并重试，**禁止回落默认值**（回落 = 真分叉）。为此要求系统合约 getter 契约上永不 revert |

### §3.6 白名单（3 条）

| # | 级 | 条款 |
|---|---|---|
| 6.1 | M | 规定读取接口形态（固定 storage 布局直读 vs view 函数）与容量硬上限；若选后者必须规定 gas cap（不能沿用节点本地配置进共识） |
| 6.2 | S | 规定规范序（地址升序）与摘要算法，用于跨节点一致性交叉校验 |
| 6.3 | S | 明确读取节奏与失效边界；若采用 epoch 缓存，实际语义是「下一个 epoch 边界生效」而非「下一个区块」，与现文本有实质差异 |

### §6 安全考量（3 条）

| # | 级 | 条款 |
|---|---|---|
| 7.1 | M | 重写「Spam within the lane」：给出滥用上限的量化（现状 = 100% lane），并承认 BEP-226 下出块者自灌水净成本为零（M4） |
| 7.2 | M | 新增「lane 打折通道」小节，分析跨区间套利面（M5） |
| 7.3 | M | 修正「Signal manipulation」的成本论证（离散边界上放弃收益 ≈ 1 gas），并给出改进方案（M7） |

### 新增章节（2 条）

| # | 级 | 条款 |
|---|---|---|
| 8.1 | M | 新增「MEV / Builder 集成」一节：BidBlock admission 的可校验形式、`RawBid` 的类别拆分、`mev_params` 暴露 `laneSize`、payBidTx 归类、失败反馈错误码（M9） |
| 8.2 | S | 修正 §5「observable behavior essentially identical」：拥堵期 `gasUsed/gasLimit` 会系统性偏低（差额最多 `laneSize`），影响所有以此为拥堵指标的下游工具（浏览器、gas price oracle、告警） |

---

# 第 2 节 · 落地难度、步骤与工期拆分

## 2.0 总体架构与数据流

```
                      ┌─────────────────────────────────────────────┐
                      │  header.UncleHash (32B)  ← 递推态的唯一载体   │
                      │  [0:8] laneSize                             │
                      │  [8:16] generalGasUsed                      │
                      │  [16:24] paymentGasUsed                     │
                      │  [24] 版本 | [25:32] 保留(零)                 │
                      └─────────────────────────────────────────────┘
                                  ▲                    │
             写(出块/Finalize)      │                    │  读(父 header)
                                  │                    ▼
  ┌───────────────────────────────────────────────────────────────────────────┐
  │ 块 n 的处理                                                                │
  │                                                                           │
  │  ① laneSize(n) = Size(params, Step(params, ratio(n-1),                    │
  │                                   generalGasUsed(n-1), gasLimit(n-1)),    │
  │                        gasLimit(n))            ← 纯父 header 算术，深度 1   │
  │                                                                           │
  │  ② classes[] = ClassifyBlock(parentReader, txs)  ← 绑定 parent.Root       │
  │     （必须在任何交易执行之前一次性完成）                                      │
  │                                                                           │
  │  ③ 执行；GasPool 内部按 class 累计三桶                                      │
  │     不变式：paymentUsed + generalUsed + systemUsed ≡ gp.Used()             │
  │                                                                           │
  │  ④ 校验 §3.2 不等式；写回 UncleHash                                        │
  └───────────────────────────────────────────────────────────────────────────┘
```

**关键约束：三条造块路径 + 一条系统交易旁路，漏改任何一条都是「本地自认有效、对端拒收」。**

| # | 路径 | gas 预算入口 | `header.GasUsed` 写入点 |
|---|---|---|---|
| 1 | 导入（校验） | `core/state_processor.go:79` `NewGasPool(block.GasLimit())`，**无任何预留** | 不写，在 `core/block_validator.go:152` 对账 |
| 2 | Parlia 本地出块 | `miner/worker.go:733` `NewGasPool(header.GasLimit - gasReserved)` | `miner/worker.go:801` |
| 3 | MEV simBid | 复用 `env.gasPool`，另 `miner/bid_simulator.go:1053` `SubGas(PayBidTxGasLimit)` | `miner/bid_simulator.go:1383` |
| 4 | MEV BidBlock | **不执行、无 gasPool** | admission 阶段静态校验（新增） |
| 旁路 | Parlia 系统交易 | `getSystemMessage` 给 `GasLimit = MaxUint64/2`（`consensus/parlia/parlia.go:2099`），完全绕开 GasPool | `consensus/parlia/parlia.go:2198` `*usedGas += gasUsed` |

这类 bug 的表现极其阴险：验证者自认成功出块并广播，全网 `ValidateState` 拒收，验证者持续出坏块直至被 jail，而 `ValidateBody`/`VerifyHeader` 都查不出来，日志里只有 `BAD_BLOCK`。

## 2.1 P0 · 规格返工（前置，无代码）

第 1.5 节的 27 条，其中 20 条标 M。**这一阶段不产出代码，但它是关键路径的起点**——M1（递推态载体）、M2/M4（分类白名单与滥用上限）、M6（不变量）不裁决，后面的分类器与不等式实现就没有确定的目标。

额外产出两份工程资产：

1. **定点算术规格**：所有 ratio/trigger/step 用 bps 整数，signal 用交叉相乘，ratio→gas 先除后乘向下取整。
2. **跨客户端一致性向量集**：JSON 形式 `(params, [gasLimit_i], [general_i], [payment_i]) → [laneSize_i]`，覆盖第 3 节 C 组的全部边界。**必须与 BEP 定稿同步产出**，否则事实标准会变成「某个 Go 实现恰好的舍入行为」——BSC 有 reth-bsc 等多实现。

工期：**5 人日**（含与 BEP 作者/社区的往返，日历时间另计）。

## 2.2 P1 · fork 挂载 + `UncleHash` 承诺

### 2.2.1 fork 管道（照 Pasteur 模板，14 处）

```go
// ── params/forks/forks.go ──────────────────────────────────────────────────
// 序数枚举有语义（params/config.go 里有 `fork >= forks.Osaka` 这类比较），
// 插在中间会整体重编号。BSC 跳过 BPO1-5，所以接在 Pasteur 之后最安全。
const ( ... Pasteur = 48; /* NEW */ PaymentLane = 49; BPO1 = 50; ... )
var forkToString = map[Fork]string{ ..., PaymentLane: "paymentLane", ... }

// ── params/config.go（共 11 处）────────────────────────────────────────────
type ChainConfig struct {
    // ...
    PasteurTime     *uint64 `json:"pasteurTime,omitempty"`      // :725
+   PaymentLaneTime *uint64 `json:"paymentLaneTime,omitempty"`  // 插在 Pasteur 之后
    // ...
}
// 三张网络配置各加一行：BSCChainConfig(:255)、ChapelChainConfig(:309)、RialtoChainConfig(:365)
// String() 三处：*big.Int 转换块(:918-921)、格式串(:933-936)、vararg(:984)
+func (c *ChainConfig) IsPaymentLane(num *big.Int, time uint64) bool {          // 仿 :1448 IsPasteur
+    return c.IsLondon(num) && isTimestampForked(c.PaymentLaneTime, time)
+}
+func (c *ChainConfig) IsOnPaymentLane(cur *big.Int, lastT, curT uint64) bool { /* 仿 :1453 */ }
// CheckConfigForkOrder 顺序表(:1554-1586)、checkCompatible(:1807)、
// LatestFork(:1863)、Timestamp(:1952)、BlobConfig() 的 case(:1901)、
// Rules 字段(:2125) 与 Rules() 赋值(:2170)
//   注意：纯 BSC fork 用裸 `isTimestampForked` 形式，不要用上游的 `(isMerge || c.IsInBSC())` 包裹

// ── override 五件套 + 生成物 ────────────────────────────────────────────────
// cmd/utils/flags.go:317-321   OverridePaymentLane cli.Uint64Flag
// cmd/geth/main.go:75          全局旗标注册
// cmd/geth/chaincmd.go:78/:372 initGenesis 应用
// cmd/geth/config.go:290-292   makeFullNode 应用
// eth/ethconfig/config.go:256  OverridePaymentLane *uint64 `toml:",omitempty"`
// eth/ethconfig/gen_config.go  已提交的 gencodec 生成物，需重新生成（4 处）
// core/genesis.go:295/:333     ChainOverrides 字段 + apply()
// eth/backend.go:265-268       chainConfig.PaymentLaneTime = config.OverridePaymentLane
```

**不需要改的**：`core/forkid`（`forkid.go:281-282` 用反射按字段名后缀 `Time` 自动收集，命名成 `PaymentLaneTime` 即自动进 eth/68 握手）；`core/genesis_alloc.go`（BSC 的分叉期合约字节码走 `IsOn<Fork>` 直写 statedb 路径，不碰 allocs）。

**CI 不会提醒你的缺口**：`params/config_test.go` 只有 3 个测试，**没有 `TestCheckConfigForkOrder`**，顺序表与 `LatestFork()`/`Timestamp()` 两个 switch 零覆盖。加 fork 时必须手动补。

### 2.2.2 `UncleHash` 编解码

```go
// ── core/paymentlane/header.go（新包）──────────────────────────────────────
// 布局：[0:4] ratioBps | [4:12] generalGasUsed | [12:20] paymentGasUsed | [20:32] 零
type Commitment struct {
    RatioBps       uint32
    GeneralGasUsed uint64
    PaymentGasUsed uint64
}

func Encode(c Commitment) common.Hash {
    var h common.Hash
    binary.BigEndian.PutUint32(h[0:4], c.RatioBps)
    binary.BigEndian.PutUint64(h[4:12], c.GeneralGasUsed)
    binary.BigEndian.PutUint64(h[12:20], c.PaymentGasUsed)
    return h // [20:32] 保持零
}

// Decode 必须校验保留位为零 —— 否则未来 BEP-696 的元数据写进来会被静默当成 lane 数据。
func Decode(h common.Hash) (Commitment, error) {
    for _, b := range h[20:32] {
        if b != 0 { return Commitment{}, ErrReservedBitsSet }
    }
    return Commitment{
        RatioBps:       binary.BigEndian.Uint32(h[0:4]),
        GeneralGasUsed: binary.BigEndian.Uint64(h[4:12]),
        PaymentGasUsed: binary.BigEndian.Uint64(h[12:20]),
    }, nil
}
```

### 2.2.3 三处兼容性放松（fork-gated）

```go
// ── consensus/parlia/parlia.go:637-640 ─────────────────────────────────────
-    // Ensure that the block doesn't contain any uncles which are meaningless in PoA
-    if header.UncleHash != types.EmptyUncleHash {
-        return errInvalidUncleHash
-    }
+    // BEP-703: UncleHash 承载 payment lane 记账承诺。uncle 为空改为在 body 上验证
+    // （VerifyUncles / ValidateBody），符合 BEP-696 的强制要求。
+    if !chain.Config().IsPaymentLane(header.Number, header.Time) {
+        if header.UncleHash != types.EmptyUncleHash {
+            return errInvalidUncleHash
+        }
+    } else if _, err := paymentlane.Decode(header.UncleHash); err != nil {
+        return err
+    }

// ── core/block_validator.go:63-69 ──────────────────────────────────────────
     header := block.Header()
     if err := v.bc.engine.VerifyUncles(v.bc, block); err != nil { return err }
-    if hash := types.CalcUncleHash(block.Uncles()); hash != header.UncleHash {
-        return fmt.Errorf("uncle root hash mismatch ...")
-    }
+    // BEP-696 明文禁止从 UncleHash 反推 body 的 uncle 列表。
+    // 安全依据：uncle 为空仍由上一行的 engine.VerifyUncles 强制
+    //（consensus/parlia/parlia.go:927 `len(block.Uncles()) > 0` → error），
+    // 所以放松这里不会放开 uncle。
+    if v.config.IsPaymentLane(block.Number(), block.Time()) {
+        if len(block.Uncles()) != 0 {
+            return fmt.Errorf("non-empty uncle list under payment lane")
+        }
+    } else if hash := types.CalcUncleHash(block.Uncles()); hash != header.UncleHash {
+        return fmt.Errorf("uncle root hash mismatch ...")
+    }

// ── core/types/block.go:320-321 ★★★ 最大陷阱 ★★★ ─────────────────────────
// NewBlock 无条件覆写 UncleHash，而所有 assemble 路径都过它
// （core/state_processor.go:468 AssembleBlock、consensus/parlia/parlia.go:1604）。
// 不改这里，出块侧写进 header 的 lane 数据会被静默擦掉，
// 表现为「本地出块成功、全网 BAD_BLOCK」。
-    if len(uncles) == 0 {
-        b.header.UncleHash = EmptyUncleHash
+    if len(uncles) == 0 {
+        // 保留调用方写入的值：BEP-703 用它承载 lane 承诺。
+        // 无 uncle 时不再覆写；uncle 为空由 ValidateBody / VerifyUncles 保证。
+        if b.header.UncleHash == (common.Hash{}) {
+            b.header.UncleHash = EmptyUncleHash   // 未设置过 → 沿用旧默认
+        }
     } else {
         b.header.UncleHash = CalcUncleHash(uncles)
         // ...
     }
```

> `NewBlock` 拿不到 `ChainConfig`，所以这里只能用「零值 = 未设置」的判据而不是 fork gate。这个写法必须配一条单测断言：fork 前 `UncleHash` 恒为 `EmptyUncleHash`，fork 后等于调用方写入的值。

```go
// ── core/types/block.go:213-218（附带，非正确性）───────────────────────────
// EmptyBody() 用 UncleHash == EmptyUncleHash 判空，写入 lane 数据后恒 false。
// 唯一消费者 eth/downloader/queue.go:83 → 空块也会去拉 body（多余但安全）。
 func (h *Header) EmptyBody() bool {
-    return h.TxHash == EmptyTxsHash && h.UncleHash == EmptyUncleHash && emptyWithdrawals
+    // BEP-703 后 UncleHash 不再是 body 的摘要，不参与判空。
+    return h.TxHash == EmptyTxsHash && emptyWithdrawals
 }
```

**签名与 hash：零额外工作。** `UncleHash` 已在 `encodeSigHeaderWithoutVoteAttestation`（`consensus/parlia/parlia.go:1872`）的编码列表内，且是 `types.Header` 的必需字段（RLP 位置 2），所以 lane 承诺**自动被 validator 的 seal 签名覆盖，也自动进 block hash**，无需任何新增认证逻辑。

工期：**4 人日**（fork 管道 2 + 编解码与三处 gate 1.5 + 覆盖既有测试 fixture 0.5）。

## 2.3 P2 · 分类器

```go
// ── core/paymentlane/classify.go ───────────────────────────────────────────
type Class uint8
const (ClassGeneral Class = iota; ClassPayment; ClassSystem)

// Classifier 每块构造一次，绑定 parent.Root，块内不可变。
// ★ 持有的是 state.StateReader（父块末状态的只读视图），
//   绝不是执行中的 *state.StateDB —— 见 M3（防分类漂移与出块者操纵），
//   同时避免 core/state/statedb.go:628 的 stateReadList（EIP-7928 BAL）污染。
type Classifier struct {
    rules    params.Rules                    // 预编译地址集合与 fork gate 需要
    parent   state.StateReader               // 只用 Account()，不用 Storage()/Code()
    list     map[common.Address]uint8        // §3.6 白名单快照，按 parentHash 从 LRU 取
    codeMemo map[common.Address]bool         // 块内 memo，每个 to 只读一次
    err      error                           // ★ 必须冒泡，不能沿用 statedb 的延迟语义
}

func (c *Classifier) Classify(tx *types.Transaction, isSystemTx bool) Class {
    if isSystemTx { return ClassSystem }        // 条款 1.7
    to := tx.To()
    if to == nil { return ClassGeneral }        // CREATE

    // ── 纯静态门（不读 state）：白名单式，向前兼容（条款 1.4）──────────────
    switch tx.Type() {
    case types.LegacyTxType, types.AccessListTxType, types.DynamicFeeTxType:
    default:
        return ClassGeneral   // BlobTx(0x03) / SetCodeTx(0x04) / 一切未来类型
    }
    if len(tx.AccessList()) != 0 { return ClassGeneral }   // 条款 1.5

    // ── ② / ③：对 to 的静态查表（data 非空是正常的：transfer calldata）────
    if cls, ok := c.list[*to]; ok {
        if isPaymentTxToListed(tx) {                        // 条款 4（若采纳 selector 白名单）
            return ClassPayment
        }
        return ClassGeneral
    }

    // ── ①：原生 BNB 转账 ────────────────────────────────────────────────
    if len(tx.Data()) != 0 { return ClassGeneral }
    if tx.Value().Sign() == 0 { return ClassGeneral }       // 条款 1.8
    if _, isPre := vm.ActivePrecompiledContracts(c.rules)[*to]; isPre {
        return ClassGeneral                                 // 条款 1.6（M2-a）
    }
    if c.hasCode(*to) { return ClassGeneral }               // 覆盖 7702 designator
    return ClassPayment
}

// hasCode 只读 account leaf，永不加载 code blob。
func (c *Classifier) hasCode(addr common.Address) bool {
    if v, ok := c.codeMemo[addr]; ok { return v }
    acct, err := c.parent.Account(addr)
    if err != nil {
        // ★ fastnode（--tries-verify-mode none）下可达：account 只能来自单一
        //   snapshot reader，snapshot 未覆盖该 root 时返回 ErrNotCoveredYet。
        //   绝不能像 core/state/statedb.go:641-645 那样吞掉 —— 否则会被当成
        //   「无 code」→ 误判 payment → 三桶不匹配 → 拒绝合法块，
        //   而日志里只有一条 invalid gas used，没有任何指向。
        c.err = err
        return true   // 保守；本块最终会因 c.err != nil 整体失败
    }
    // ⚠️ 陷阱：不存在的账户 acct == nil。
    //    不能写 GetCodeHash(to) == types.EmptyCodeHash —— core/state/statedb.go:415-421
    //    对不存在账户返回 common.Hash{}，那样会把每一笔转向全新地址的转账判成
    //    general，恰好是 lane 最核心的用例（首次充值、新钱包收款）。
    has := acct != nil && !bytes.Equal(acct.CodeHash, types.EmptyCodeHash.Bytes())
    c.codeMemo[addr] = has
    return has
}
```

### 四个调用点如何拿到 `parent state.StateReader`

| 调用点 | 做法 |
|---|---|
| 导入 `core/state_processor.go:71` | `core/blockchain.go:2641` 的 `ReadersWithCacheStats(parentRoot)`（`core/state/database_mpt.go:108-117`）当前返回 2 个共享 `stateReaderWithCache` 的 reader，扩成返回第 3 个（共用同一 account cache，边际成本≈0）；非 prefetch 分支（`:2626`）用 `sdb.Reader(parentRoot)`。经 `Process` 签名传入 |
| 本地出块 `miner/worker.go:makeEnv` | `:900` 已在调 `w.chain.StateWithCacheAt(parent.Root)`（内部就是 `ReadersWithCacheStats` + `NewWithReader`），新增一个返回 `(*state.StateDB, state.Reader)` 的变体，只读 reader 挂到 `environment` |
| MEV bid `miner/bid_simulator.go:1026` | 共用 `prepareWork`/`makeEnv`，挂到 `bidRuntime.env`，改动自动继承 |
| txpool `legacypool.go:1409` | **不需要新视图**：`pool.currentState`（`:242`）本身就是 chain head 的 post-state = 下一块的父块末状态。但为避免 BAL 污染与 code blob 加载，建议同样取裸 reader |

**分类时机**：导入路径**可以**在 tx 循环前批量分类（`core/state_processor.go:127` 之前 statedb 还没被污染），但用了独立 reader 后随时分类都一样。miner/bid 路径**不能**批量（交易在 `fillTransactions` 过程中才到达），所以按笔调用 + memo 是唯一可行形态——这也正是必须用独立 reader 的原因。

**白名单快照**：按 `parentHash` 做 LRU（1280 项，抄 `consensus/parlia/parlia.go:247` 的 `recentSnaps`），挂在 `*core.BlockChain` 上（四个调用点都能拿到）。**不要挂 parlia**——`prepareWork`/`Process` 拿 engine 要类型断言，且 `consensus/parlia` 已经有 `p.ethAPI` 这个方向错误的依赖。

**真实边际成本**：每个**不同**的 `to` 一次 `stateReaderWithCache.account()`，而这个账户在紧接着的 `ApplyTransaction → evm.Call → resolveCode` 里**必然会被再读一次**，所以第二次是缓存命中，等价于把一次读提前了。70M gas 块上量级是几十 µs。**BEP「分类不增加共识开销」在成本意义上基本成立；不成立的是它隐含的「不需要新增状态视图」。**

工期：**4 人日**。

## 2.4 P3 · gas 三桶记账与不等式校验

### 2.4.1 为什么桶必须做进 `GasPool`

> ⚠️ **已推翻**（修订说明第 2 项）：口径漂移的分析成立，但结论过重。见 [`bep703-miner-packing-design.md`](./bep703-miner-packing-design.md) §4.6。

三个口径不同的 gas 量同时存在，且**今天恰好对齐、测试全绿**：

| 来源 | 语义 | 与 `header.GasUsed` 的关系 |
|---|---|---|
| `receipt.GasUsed`（`core/state_processor.go:285`） | 扣退款、含 EIP-7623 floor | 仅 **pre-Amsterdam** 时 `Σ = gp.Used()` |
| `result.MaxUsedGas`（`core/state_transition.go:658,725`） | 不扣退款 | 仅 **post-Amsterdam** 时 `Σ = gp.Used()` |
| `gp.Used()`（`core/gaspool.go:90`） | `initial - remaining` | **恒等**于 `header.GasUsed` 的用户交易部分 |

分水岭是 `core/state_transition.go:679-685` 的 `IsAmsterdam` 分支。BSC 当前 `AmsterdamTime = nil`（`params/config.go:258`），所以三者刚好相等。**一旦 Amsterdam 上线，用 `receipt.GasUsed` 累计的桶就会与 `header.GasUsed` 漂移，不等式左侧凭空少算退款部分 → 全网出块方与验证方对同一个块给出不同判定。**

本仓在这个精确位置已经出过两次共识级事故（`header.GasUsed` 与 Parlia SubGas 预留双重计数；bid 路径漏写 `header.GasUsed`）。**唯一稳的做法是把桶做进 `GasPool`，让「三桶之和 ≡ `Used()`」成为定义而非约定。**

```go
// ── core/gaspool.go ───────────────────────────────────────────────────────
 type GasPool struct {
     remaining, initial, cumulativeUsed uint64
+    // BEP-703. laneOn == false 时以下字段不参与任何逻辑，
+    // fork 前的行为与今天逐字节一致。
+    laneOn                              bool
+    class                               paymentlane.Class
+    paymentUsed, generalUsed, systemUsed uint64
 }
+func (gp *GasPool) EnablePaymentLane()                   { gp.laneOn = true }
+func (gp *GasPool) SetClass(c paymentlane.Class)         { gp.class = c }
+func (gp *GasPool) AddSystemUsed(g uint64)               { gp.systemUsed += g }
+func (gp *GasPool) PaymentUsed() uint64                  { return gp.paymentUsed }
+func (gp *GasPool) GeneralUsed() uint64                  { return gp.generalUsed }
+func (gp *GasPool) SystemUsed() uint64                   { return gp.systemUsed }

 func (gp *GasPool) ReturnGas(returned, gasUsed uint64) error {   // :60
     // ... 原逻辑不动 ...
     gp.cumulativeUsed += gasUsed
+    if gp.laneOn {
+        // ★ 加的必须是与 Used() 同口径的增量，而不是 gasUsed 本身。
+        //   用 initial-remaining 的差分保证「和恒等」，从而对 Amsterdam 免疫。
+        delta := (gp.initial - gp.remaining) - (gp.paymentUsed + gp.generalUsed)
+        switch gp.class {
+        case paymentlane.ClassPayment: gp.paymentUsed += delta
+        default:                       gp.generalUsed += delta
+        }
+    }
     return nil
 }
// ★ Snapshot()(:98) / Set()(:107) 必须一并复制全部新字段 —— 矿工回滚依赖它
//   （miner/worker.go:793,799）。漏掉的话回滚后桶值偏高，且不会报错。
```

### 2.4.2 配额推进（纯函数，全整数）

```go
// ── core/paymentlane/quota.go ─────────────────────────────────────────────
const (
    BpsDenom         = 10_000
    TriggerGapMinBps = 1_000   // TRIGGER_GAP_MIN = 10%，协议常量
    RatioGapMinBps   =   500   // RATIO_GAP_MIN   =  5%，协议常量
)

type Params struct {
    MinRatioBps, MaxRatioBps           uint32
    MinAbsGas, MaxAbsGas               uint64
    ExpandTriggerBps, ShrinkTriggerBps uint32
    ExpandStepBps, ShrinkStepBps       uint32
}

// Valid 实现 §3.5 四条不变量 + 条款 5.1 新增的第五条。
func (p Params) Valid(gasLimit uint64) error {
    switch {
    case p.ExpandTriggerBps < p.ShrinkTriggerBps+TriggerGapMinBps: return errTriggerGap  // (1)
    case !(p.ExpandStepBps > p.ShrinkStepBps && p.ShrinkStepBps > 0): return errStepOrder // (2)
    case p.MaxRatioBps < p.MinRatioBps+RatioGapMinBps:             return errRatioGap    // (3)
    case !(p.MaxAbsGas > p.MinAbsGas && p.MinAbsGas > 0):          return errAbsRange    // (4)
    case p.MaxAbsGas > gasLimit/4:                                 return errAbsVsGasLimit // (5) 新增，M6
    }
    return nil
}

// Step 是 §3.3 的递推。纯函数、全定义、无 I/O、无浮点、无除法。
// ★ 只钳制到 ratio 界；绝对界留给 Size（这样 §3.4 的「absolute bounds prevail」才成立）。
func Step(p Params, ratioBps uint32, generalGasUsed, gasLimit uint64) uint32 {
    if gasLimit == 0 { return ratioBps }
    // 交叉相乘避免除法与浮点：general/GL >= trig/10000 ⟺ general*10000 >= trig*GL
    // 溢出：GL ≤ ~1e8，×10000 = 1e12，uint64 安全（上限 1.8e19）。
    lhs := generalGasUsed * BpsDenom
    switch {
    case lhs >= uint64(p.ExpandTriggerBps)*gasLimit:
        ratioBps = satAdd32(ratioBps, p.ExpandStepBps)
    case lhs < uint64(p.ShrinkTriggerBps)*gasLimit:
        ratioBps = satSub32(ratioBps, p.ShrinkStepBps)
    }   // 否则：滞后带，不变
    // ★ 条款 3.5：每块都重新钳制，不只在发生 step 时 —— 否则参数收窄后
    //   旧递推态会永久停在新区间之外。
    return clamp32(ratioBps, p.MinRatioBps, p.MaxRatioBps)
}

// Size 实现 §3.4。ratio 界已在 Step 施加，这里施加绝对界并让它胜出。
func Size(p Params, ratioBps uint32, gasLimit uint64) uint64 {
    q := gasLimit / BpsDenom * uint64(ratioBps)   // 先除后乘、向下取整（条款 3.4）
    lo, hi := p.MinAbsGas, p.MaxAbsGas
    if rl := gasLimit / BpsDenom * uint64(p.MinRatioBps); rl > lo { lo = rl }
    if rh := gasLimit / BpsDenom * uint64(p.MaxRatioBps); rh < hi { hi = rh }
    if lo > hi { lo, hi = p.MinAbsGas, p.MaxAbsGas }   // 交集为空 → 绝对界胜出
    size := clamp64(q, lo, hi)
    // ★ M6 的客户端侧 fail-safe：配额永不超过块容量，
    //   保证任何参数配置下空块都是有效的。
    if size > gasLimit { size = gasLimit }
    return size
}

// CheckInequality 实现 §3.2（含条款 2.1 的第三桶）。
func CheckInequality(gasLimit, system, general, payment, laneSize uint64) error {
    if system+general+max64(payment, laneSize) > gasLimit {
        return fmt.Errorf("%w: system %d general %d payment %d lane %d limit %d",
            ErrPaymentLaneViolated, system, general, payment, laneSize, gasLimit)
    }
    return nil
}
```

### 2.4.3 导入路径接线

```go
// ── core/state_processor.go Process ───────────────────────────────────────
     gp = NewGasPool(block.GasLimit())                        // :79 不动
     // ...
     systemcontracts.TryUpdateBuildInSystemContract(...)      // :96
+    // ★ 必须在 :127 主循环之前：此刻还能安全地建立与 parent 一致的分类上下文。
+    var lane *paymentlane.Ctx
+    if config.IsPaymentLane(blockNumber, block.Time()) {
+        parentCommit, err := paymentlane.Decode(lastBlock.UncleHash)   // 深度 1！
+        if err != nil { return nil, err }
+        lp := paymentlane.LoadParams(chain, lastBlock)                 // epoch 缓存，见 2.6
+        ratio := parentCommit.RatioBps
+        if config.IsOnPaymentLane(blockNumber, lastBlock.Time, block.Time()) {
+            ratio = lp.MinRatioBps          // 激活块：直接取有效下界（条款 3.6）
+        } else {
+            ratio = paymentlane.Step(lp, ratio, parentCommit.GeneralGasUsed, lastBlock.GasLimit)
+        }
+        lane = &paymentlane.Ctx{
+            Params: lp, RatioBps: ratio,
+            Size:   paymentlane.Size(lp, ratio, block.GasLimit()),
+            Cls:    paymentlane.NewClassifier(config.Rules(...), parentReader, lp.List, len(txs)),
+        }
+        gp.EnablePaymentLane()
+    }
     for i, tx := range block.Transactions() {                // :127
         if isPoSA { /* :128-136 系统交易剥离，不进 gp —— 不动 */ }
+        if lane != nil { gp.SetClass(lane.Cls.Classify(tx, false)) }
         receipt, err := ApplyTransactionWithEVM(msg, gp, ...) // :156
     }
+    if lane != nil && lane.Cls.Err() != nil {
+        return nil, lane.Cls.Err()   // ★ 分类读失败必须使整块失败（fastnode，见 2.3）
+    }
     gasUsed := gp.Used()                                     // :178
+    before := gasUsed
     err = p.chain.Engine().Finalize(..., &gasUsed, cfg.Tracer) // :180
+    if lane != nil {
+        // 系统交易 gas 由 Parlia 通过 *usedGas 体外注入（parlia.go:2198），
+        // 用差值归入 system 桶，避免改动 consensus.Engine 接口
+        //（否则 beacon/clique/ethash 都要跟着改）。
+        gp.AddSystemUsed(gasUsed - before)
+    }
     return &ProcessResult{..., GasUsed: gasUsed,
+        PaymentGasUsed: gp.PaymentUsed(), GeneralGasUsed: gp.GeneralUsed(),
+        SystemGasUsed:  gp.SystemUsed(),  PaymentLaneSize: lane.SizeOrZero(),
+        RatioBps:       lane.RatioOrZero()}, nil

// ── core/block_validator.go ValidateState（唯一的有效性判据落点）───────────
 func (v *BlockValidator) ValidateState(...) error {           // :147
     if block.GasUsed() != res.GasUsed { ... }                 // :152 不动
+    if v.config.IsPaymentLane(block.Number(), block.Time()) {
+        // (a) 三桶之和必须等于 header.GasUsed（防实现漂移）
+        if res.SystemGasUsed+res.GeneralGasUsed+res.PaymentGasUsed != res.GasUsed {
+            return ErrLaneBucketMismatch
+        }
+        // (b) header 承诺必须与本地重算一致 —— 出块方算错立刻 BAD_BLOCK，
+        //     而不是几百块之后表现为神秘的配额失配
+        want := paymentlane.Commitment{RatioBps: res.RatioBps,
+            GeneralGasUsed: res.GeneralGasUsed, PaymentGasUsed: res.PaymentGasUsed}
+        if block.UncleHash() != paymentlane.Encode(want) {
+            return ErrLaneCommitmentMismatch
+        }
+        // (c) §3.2 不等式
+        if err := paymentlane.CheckInequality(block.GasLimit(), res.SystemGasUsed,
+            res.GeneralGasUsed, res.PaymentGasUsed, res.PaymentLaneSize); err != nil {
+            return err
+        }
+    }
     // ... bloom / receiptRoot / stateRoot 并行校验不动 ...
```

**为什么不等式落在 `ValidateState` 而不是 `GasPool` 的增量强制**：后者会让区块有效性依赖交易顺序（同一 multiset 换序可通可不通），违反 §3.2「只约束总量、排序自由」。

**为什么不落在 `consensus/parlia/verifyCascadingFields`**：那条路径被 `VerifyHeaders` 批量并发调用，还被 header-only 的 skeleton sync 走，拿不到 receipts 与 state。（**但 header 承诺的语法校验可以放那里**，见 2.2.3——语法与语义分离。）

工期：**4 人日**。

## 2.5 P4 · 出块器双 gas pool + MEV

### 2.5.1 双池方案与正确性证明

> ⚠️ **已推翻**（修订说明第 1 项）：双池会拒绝本该合法的区块。反例与最终设计见 [`bep703-miner-packing-design.md`](./bep703-miner-packing-design.md) §4.1。下面内容仅作对比保留。

设 `general = GasLimit − gasReserved − laneSize`、`payment = laneSize`。payment 类交易优先走 payment 池，装不下溢出到 general 池；general 类交易只能走 general 池。

**正确性**：
- 若 `paymentGasUsed <= laneSize`：payment 全来自 payment 池，故 `generalGasUsed <= GasLimit − laneSize`，即 `generalGasUsed + laneSize <= GasLimit`，而此时 `max(...) = laneSize` ✓
- 若 `paymentGasUsed > laneSize`：溢出量从 general 池取，故 `generalGasUsed + (paymentGasUsed − laneSize) <= GasLimit − laneSize`，即 `generalGasUsed + paymentGasUsed <= GasLimit`，而此时 `max(...) = paymentGasUsed` ✓

**且此贪心最优**：payment 池只有 payment 能用，所以 payment 优先吃专用池不损失任何容量。

**且不需要改排序器**——`transactionsByPriceAndNonce`（`miner/ordering.go:89`）一行不改，选择顺序完全不变，符合 §3.2「不引入新排序规则」。

（对比「单池 + 尾部回填」方案：也永远合法，但严格更浪费——阶段一已被包含的 payment gas 白占 general 预算，而规则本来允许它算进 lane，直接损失验证者收入；且有三个实现雷：`AddGas` 加池只改 `remaining` 不改 `initial`，`core/gaspool.go:91-93` 会在 `initial < remaining` 时 **panic**；与 `miner/bid_simulator.go:1053/1201` 的 payBidTx `SubGas`/`AddGas` 配平纠缠；需要第二遍 txpool 迭代器。**否决。**）

```go
// ── miner/worker.go ──────────────────────────────────────────────────────
 type environment struct {
     // ...
     gasPool     *core.GasPool   // GENERAL 池。★ 保留原名，让所有既有调用点
                                 //   （prefetcher :826、payBidTx SubGas/AddGas）语义不变
+    paymentPool *core.GasPool   // 只有 payment 类能用
+    laneSize    uint64
+    payCls      *paymentlane.Classifier
 }

+// gasUsed 是 header.GasUsed 的唯一真源。
+// 替换 miner/worker.go:801 与 miner/bid_simulator.go:1383 的 env.gasPool.Used()
+func (env *environment) gasUsed() uint64 {
+    return env.gasPool.Used() + env.paymentPool.Used()
+}

// ── makeEnv: miner/worker.go:733 ────────────────────────────────────────
     avail := header.GasLimit - gasReserved
+    laneSize := uint64(0)
+    if w.chainConfig.IsPaymentLane(header.Number, header.Time) {
+        laneSize = paymentlane.NextSize(w.chain, parent, header.GasLimit)  // 读父 header
+        if laneSize > avail { laneSize = avail }   // 极端 GasLimit 下的 clamp，必须有
+    }
     env := &environment{
-        gasPool: core.NewGasPool(avail),
+        gasPool:     core.NewGasPool(avail - laneSize),
+        paymentPool: core.NewGasPool(laneSize),
+        laneSize:    laneSize,
+        payCls:      paymentlane.NewClassifier(rules, parentReader, list, 0),
     }

+// pickPool 是唯一的分配策略函数。返回 nil 表示两池都装不下。
+// 策略纯本地、无共识含义（规则只约束聚合量），故「整笔 all-or-nothing、
+// 不跨池拆分」是允许且推荐的。
+func (env *environment) pickPool(isPayment bool, need uint64) *core.GasPool {
+    if isPayment {
+        if env.paymentPool.Gas() >= need { return env.paymentPool }  // 优先吃专用池
+        if env.gasPool.Gas() >= need     { return env.gasPool }      // 溢出转 general
+        return nil
+    }
+    if env.gasPool.Gas() >= need { return env.gasPool }              // general 永不碰 payment 池
+    return nil
+}

// ── commitTransactions: miner/worker.go:805-964 ─────────────────────────
 LOOP:
     for {
-        if env.gasPool.Gas() < params.TxGas { signal = commitInterruptOutOfGas; break }  // :850
+        // ★ 坑 1：忘了改这里 → general 池干了立刻 break，lane 全空转，等于白做
+        if env.gasPool.Gas() < params.TxGas && env.paymentPool.Gas() < params.TxGas {
+            signal = commitInterruptOutOfGas; break
+        }
         // ...Peek + plain/blob 择优逻辑完全不变（排序器零改动）...
-        if env.gasPool.Gas() < ltx.Gas { txs.Pop(); continue }                           // :898
+        // ★ 坑 2/3：写成「任一池装不下就 Pop」→ general 池干了以后把堆迅速掏空，
+        //   payment 交易一起被丢掉，lane 永远填不满；
+        //   写成「两池都装不下才 Pop，否则 continue」→ 堆顶不动、条件不变 → 死循环。
+        //   pickPool 已经保证语义正确：走到 nil 说明这笔确实永远装不下。
+        isPayment := ltx.IsPayment                    // ← LazyTransaction 新字段，见下
+        gp := env.pickPool(isPayment, ltx.Gas)
+        if gp == nil { txs.Pop(); continue }
         tx := ltx.Resolve()
+        // Resolve 后用真实交易复核类别（防 LazyTransaction 标记与 txpool reset 竞态）
+        if got := env.payCls.Classify(tx, false) == paymentlane.ClassPayment; got != isPayment {
+            if gp = env.pickPool(got, tx.Gas()); gp == nil { txs.Pop(); continue }
+        }
-        _, err := w.commitTransaction(env, tx, bloomProcessors)
+        _, err := w.commitTransaction(env, tx, gp, bloomProcessors)   // gp 一路传下去
     }

// ── applyTransaction: miner/worker.go:790-803 ───────────────────────────
 func (w *worker) applyTransaction(env *environment, tx *types.Transaction,
+    gp *core.GasPool,
     rp ...core.ReceiptProcessor) (*types.Receipt, error) {
     snap := env.state.Snapshot()
-    gpSnap := env.gasPool.Snapshot()
+    gpSnap := gp.Snapshot()                       // 只快照被选中的那个池
+    gp.SetClass(classOf(isPayment))
-    receipt, err := core.ApplyTransaction(env.evm, env.gasPool, ...)
+    receipt, err := core.ApplyTransaction(env.evm, gp, ...)
     if err != nil {
         env.state.RevertToSnapshot(snap)
-        env.gasPool.Set(gpSnap)
+        gp.Set(gpSnap)
     }
-    env.header.GasUsed = env.gasPool.Used()
+    env.header.GasUsed = env.gasUsed()            // 两池求和
     return receipt, err
 }
```

**必须补一个 metric**：`paymentlane/fill_ratio`（`paymentPool.Used() / laneSize`）。上面三个坑写错任何一个的表现都是「验证者出块 gas 使用率下降 / MEV 收入下降」，**不会报错**。没有这个 metric，bug 会静默上线。

### 2.5.2 txpool：`LazyTransaction` 加分类字段（性能刚需）

> ⚠️ **已推翻**（修订说明第 3、8 项）：`LazyTransaction.IsPayment` 与 `PendingFilter.OnlyPayment` 都不需要；「MinTip 挡住支付交易」这个前提在 BSC 上不成立。见 [`bep703-miner-packing-design.md`](./bep703-miner-packing-design.md) §4.5、§4.8。

`core/txpool/subpool.go:33-44` 的 `LazyTransaction` **只有 `Hash / Gas / BlobGas / GasFeeCap / GasTipCap / Time`，没有 `To`**。想在不 `Resolve()` 的情况下分类是不可能的，而在堆顶扫描阶段对每个候选都 `Resolve()`（可能回 pool 拉全量交易）代价不可控。

最小做法：加 `IsPayment bool`，在 `legacypool.Pending()` 与 blobpool 里填充。legacypool 已持有 `pool.currentState`（`:242`）和 `pool.currentHead`（`:241`），且已经在 `:629` 做过 `GetCodeHash(from) == types.EmptyCodeHash` 这类查询——**分类所需的状态访问在 txpool 里已经是现成能力**，边际成本很低。这也让分类结果在 `reset()`（`:1414-1415`）时随 head 一起刷新，天然对齐「父块末状态」语义。

**不要为 lane 建独立 subpool**：`TxPool.Pending()`（`core/txpool/txpool.go:366-377`）按 subpool 聚合，加一个需要改 nonce 归属、reorg、`Has()`、`Get()` 一整套，diff 巨大；而 lane 只是 gas 会计不是独立费市场（§3.2 明确 payment 之间按现有 priority fee 竞争），分池零语义收益。

**但 `PendingFilter` 必须加维度。** `core/txpool/subpool.go:75-83` 现有 `MinTip / BaseFee / BlobFee / GasLimitCap / BlobTxs / BlobVersion`，**没有「只取 payment」的能力**。BEP 完全没提这一点，但它是把提案从「纸面正确」变成「实际有效」的必要工程补充：

> 拥堵时池里按 tip 排序的前排全是 general，矿工填到 `GasLimit − laneSize` 就撞墙停下，剩下的 `laneSize` 因为堆里找不到 payment 交易而白白空转。§3.2 说的「shortfall reduces general space one-for-one」会在**正常运行中天天发生**，而不是只在真的没有支付需求时发生。

### 2.5.3 MEV 三处改动

```go
// ── (a) BidBlock admission：miner/bid_simulator.go:714 preSealVerifyBidBlock ──
// 紧邻 :731 的 GasLimit 检查。★ 这是关闭 M9-a 丢槽攻击面的关键，
//   必须在 miner/bid_block.go:265 的 mux.Post（签名广播）之前生效。
+if p.chainConfig.IsPaymentLane(h.Number, h.Time) {
+    c, err := paymentlane.Decode(h.UncleHash)
+    if err != nil { return err }
+    if c.GeneralGasUsed+c.PaymentGasUsed > h.GasUsed { return errLaneBucketOverflow }
+    system := h.GasUsed - c.GeneralGasUsed - c.PaymentGasUsed
+    laneSize := paymentlane.NextSize(chain, parent, h.GasLimit)
+    if err := paymentlane.CheckInequality(h.GasLimit, system,
+        c.GeneralGasUsed, c.PaymentGasUsed, laneSize); err != nil {
+        return err   // 签名前拒掉，保住槽位
+    }
+    // 承诺的 ratio 必须等于本地从父 header 推导的值
+    if c.RatioBps != paymentlane.NextRatio(chain, parent) { return errLaneRatioMismatch }
+}
// ⚠️ 注意：这里只能校验「承诺自洽 + 不等式成立」，不能校验「承诺真实」
//    （真实性要执行）。真实性由事后 InsertChain 的 ValidateState (b) 兜底 +
//    builder revoke 惩罚。这个组合已足够：作恶者无法再让 validator 丢槽，
//    最多是自己的 bid 被拒 + 被 revoke。

// ── (b) simBid 早期准入：miner/bid_simulator.go:1068 ────────────────────
-if bidRuntime.bid.GasUsed > bidRuntime.env.gasPool.Gas() { err = ...; return }
+// bid.GasUsed 是不区分类别的标量 → 早期检查会漏判，浪费整个模拟时间预算。
+// 升级为类别感知（需 builder 自报，见 (c)）：
+if bidRuntime.bid.GeneralGasUsed > bidRuntime.env.gasPool.Gas() {
+    err = errLaneGeneralOverflow; return   // ★ 失败原因必须可区分，否则 builder 无法自适应
+}
// :1053/:1201 的 payBidTx SubGas/AddGas → 显式打到 general 池
//   （payBidTx 按条款 1.7 归 general，与 pickPool 结果自洽）
// :1383 header.GasUsed = env.gasUsed()
// :1186 greedy merge 走 fillTransactions → 自动继承 lane 逻辑

// ── (c) 协议面：core/types/builder/bid.go ────────────────────────────────
 type RawBid struct {
     // ... :84-91
+    GeneralGasUsed uint64   // ★ 进 bid hash → 签名格式破坏性变更，
+                            //   需走 MevParams.Version 版本协商
 }
 type MevParams struct {
     // ... :308-318
+    PaymentLaneSize uint64  // 经 mev_params（internal/ethapi/api_mev.go:138）暴露，
+                            //   实现在 miner/miner_mev.go:256-268。不给则 builder 只能猜
 }
```

**排期风险**：(c) 涉及外部 builder 团队改造，是**日历时间**风险而非工程量风险。分叉后 builder 的既有算法会系统性产出被拒的 bid，必须提前数周通知并提供测试网。

工期：**7 人日**（双池 3 + txpool 2 + MEV 2；不含 builder 侧）。

## 2.6 P5 · 系统合约与治理

### 2.6.1 不要开「每块读系统合约」的新范式

BSC 今天**几乎不读系统合约**：验证人集合只在 epoch 边界读（`consensus/parlia/parlia.go:983-992`、`:1208-1217`，epochLength Maxwell 后为 1000），turnLength 同样 epoch 门控（`:1018-1029`、`:1253-1268`），StakeHub 两个读只在 breathe block（每天一次，`consensus/parlia/feynmanfork.go:130`、`:177`）。**非 epoch 块的系统合约读次数 = 0。**

且读法只有一种：`p.ethAPI.Call`（即一次完整 eth_call，`consensus/parlia/parlia.go:1934-1947`），内部**不是 `statedb.Copy()` 而是全新开一个 StateDB**（`internal/ethapi/api.go:1173-1181` → `eth/api_backend.go:257-275` → `bc.StateAt(header)`），冷路径数百 µs–ms。**按笔交易调用完全不可行**（2000 笔交易 = 2000 次 eth_call），按块调用在 450ms 出块间隔下也不划算。

**采用 turnLength 范式**（BSC 唯一干净的先例）：

```
epoch 块（number % epochLength == 0）:
  出块侧 SetExtraData（照 prepareTurnLength，parlia.go:1019-1039）
    → ethAPI.Call 读 (params, list) @ parent hash
    → sanitize
    → append 到 header.Extra：32B params 摘要 || 32B list 摘要
  验证侧 Finalize（照 verifyTurnLength，parlia.go:1253-1279）
    → 同样读 + sanitize + 比对，不一致 → errMismatchingPaymentLaneConfig（BAD_BLOCK）

非 epoch 块:
  只读 parlia Snapshot 里缓存的 params 与 list，零合约访问

缓存失效：epoch 边界即失效点。Snapshot 是 content-addressed（key = block hash，
  parlia.go:247 recentSnaps LRU 1280 + checkpointInterval 1024 落盘），
  reorg 天然正确 —— 不需要「名单变更事件」这种脆弱机制。
```

**代价**：参数/名单变更延迟 ≤ 1 epoch（1000 块 ≈ 7.5 min @450ms）。这与 §3.6「a list change never affects the block that contains it」有实质差异（实际是「下一个 epoch 边界生效」），必须写进规格（条款 6.3）。对这类参数完全可接受，且给了运维一个明确的生效边界。

**风险提示**：`header.Extra` 的偏移全靠手算（`parseValidators`、`parseTurnLength`、`getVoteAttestationFromHeader` 在 `consensus/parlia/parlia.go:846`/`:855`/`:414-431`），新增 64B 会移动 vote attestation 的偏移，波及**链外**的中继器与跨链 light client。这是本阶段最大的风险项。

### 2.6.2 fail-closed：整个提案最容易搞出分叉的地方

**第一原则：分叉风险不来自校验策略，来自读取本身的非确定性。** 必须严格区分两类失败：

| 失败类型 | 确定性 | 允许的处理 |
|---|---|---|
| 合约 revert / ABI decode 失败 / 值违反不变量 | ✅ 是（同 state + 同 calldata + 同硬编码 gas cap ⇒ 同结果） | 可 sanitize |
| **state 不可用 / pruned / context cancel / 磁盘错误** | ❌ **否** | **必须返回 error 并重试，绝不能回落默认值** |

最后一行是唯一的真分叉源：节点 A 读到 5%、节点 B 超时回落到 2% → 算出不同 `laneSize` → 对同一区块给出不同裁决。**而这是极容易犯的错，因为它看起来是防御性编程。**

麻烦在于 `p.ethAPI.Call` 把两类都压成同一个 `error`（`consensus/parlia/bohrFork.go:74-76` 就是这个形状）。**解法：要求系统合约的 getter 契约上永不 revert**（storage 全零时也返回一组合法默认值，照 `BSCValidatorSet.getTurnLength()` 的 `if (turnLength == 0) return 1;` 兜底）。这样「读失败」就只剩基础设施一类，统一上抛。

**第二原则：sanitize 必须是全函数钳制，不是 if/else 分支。**

```go
// ── consensus/parlia/paymentlane.go ─────────────────────────────────────
// 任意 uint 输入都映射到满足全部不变量的配置 → 可 go-fuzz 全覆盖，
// 不存在「共识路径上的未测试 fallback 分支」这种东西。
func sanitize(r Params, gasLimit uint64) Params {
    var o Params
    // (3) 比率区间：先钳 min，再让 max 至少高出 RATIO_GAP_MIN
    o.MinRatioBps = clamp32(r.MinRatioBps, HardMinRatioBps, HardMaxRatioBps-RatioGapMinBps)
    o.MaxRatioBps = clamp32(r.MaxRatioBps, o.MinRatioBps+RatioGapMinBps, HardMaxRatioBps)
    // (4)(5) 绝对区间，且与 GasLimit 绑定
    hardAbsMax := min64(HardMaxAbsGas, gasLimit/4)
    o.MinAbsGas = clamp64(r.MinAbsGas, HardMinAbsGas, hardAbsMax-1)
    o.MaxAbsGas = clamp64(r.MaxAbsGas, o.MinAbsGas+1, hardAbsMax)
    // (1) 滞后带
    o.ExpandTriggerBps = clamp32(r.ExpandTriggerBps, TriggerGapMinBps, BpsDenom)
    o.ShrinkTriggerBps = clamp32(r.ShrinkTriggerBps, 0, o.ExpandTriggerBps-TriggerGapMinBps)
    // (2) 快涨慢跌；ExpandStep >= 2 保证 ShrinkStep 有 [1, expand-1] 的合法空间
    o.ExpandStepBps = clamp32(r.ExpandStepBps, 2, HardMaxStepBps)
    o.ShrinkStepBps = clamp32(r.ShrinkStepBps, 1, o.ExpandStepBps-1)

    if o != r {
        // 唯一的「合约与客户端不一致」可观测信号
        log.Crit("PAYMENT LANE PARAMS SANITIZED", "raw", r, "effective", o)
        paymentLaneSanitizeCounter.Inc(1)
    }
    return o
}
// ★ 客户端的 Hard* 边界应比 Solidity 的 require 范围明显更宽（建议 ≥2× 余量），
//   这样将来放宽治理空间只需升级合约，不需要动客户端。
```

**明确否决**「参数非法即拒块/拒绝出块」——它确定性、不分叉，但会因为一次**治理动作**而停链。

### 2.6.3 合约侧最小接口

```solidity
interface IPaymentLaneConfig {
    /// @notice 一次返回全部 8 个参数。geth 侧只调这一个 getter。
    /// ratio/trigger/step 单位统一为 basis point (uint32)，绝对边界单位 gas (uint64)。
    /// ★ 实现必须保证：storage 全零时也返回一组满足全部不变量的默认值，永不 revert。
    function getPaymentLaneParams() external view returns (
        uint32 minRatioBps, uint32 maxRatioBps, uint64 minAbsGas, uint64 maxAbsGas,
        uint32 expandTriggerBps, uint32 shrinkTriggerBps,
        uint32 expandStepBps, uint32 shrinkStepBps);

    /// @notice 全量白名单，地址按升序（geth 侧摘要计算依赖此顺序）。
    function getPaymentContracts() external view
        returns (address[] memory addrs, uint8[] memory categories);
    function MAX_PAYMENT_CONTRACTS() external view returns (uint256);
    function paymentContractsDigest() external view returns (bytes32);

    /// @notice IParamSubscriber。★ 必须是单一原子 setter（条款 5.2）：
    ///   key 只接受 "paymentLaneParams" / "addPaymentContracts" / "removePaymentContracts"，
    ///   前者 value = abi.encode(8 个参数)，在整个元组上一次性 require 四条不变量。
    ///   禁止逐参数键 —— 否则移动滞后带时顺序错误会留下半应用配置（M8）。
    function updateParam(string calldata key, bytes calldata value) external;
}
```

**geth 侧新合约的落地清单**：地址常量接在 `core/systemcontracts/const.go:22` 之后（`0x…3001`）；加入 `consensus/parlia/parlia.go:104-119` 的 `systemContracts` map（否则它的零 gas price 交易不被识别为 system tx）；ABI 常量 + `New` 里解析（`parlia.go:280-301`）；升级注册四步（`core/systemcontracts/upgrade.go`：import、`make(map)`、`init()` 三网络、`upgradeBuildInSystemContract` 的 `IsOnPaymentLane` 分支 `:1228-1230`）；新合约的 `initialize()` 走 Feynman 的范式（`consensus/parlia/feynmanfork.go:33-63`，分叉块用系统交易调用）。

**⚠️ 时序陷阱**：Feynman 之后所有合约升级发生在**区块末**（`core/systemcontracts/upgrade.go:1112-1115` 的 `atBlockBegin` 开关，`consensus/parlia/parlia.go:1419`/`:1528` 传 `false`）。所以 PaymentLane 合约的代码在分叉块末尾才落地，**lane 规则最早只能在「分叉块 + 1」生效，分叉块本身必须豁免**（条款 3.6）。

工期：geth 侧 **7 人日**（epoch 读取 + header.Extra 承诺 4 + sanitize 与 fuzz 2 + Snapshot 扩展与序列化向后兼容 1）；合约侧 **13 人日**（合约 3 + Foundry 测试 3 + genesis 集成 2 + BEP-702 registry 对接 2 + 审计准备与响应 3）。

## 2.7 工期拆分表

| 阶段 | 工作项 | geth | 合约 | 测试 |
|---|---|---:|---:|---:|
| **P0** | 规格返工（27 条）+ 定点规格 + 跨客户端向量集 | 5 | — | — |
| **P1** | fork 管道（14 处 + override 五件套 + 生成物） | 2 | — | — |
| | `UncleHash` 编解码 + 三处兼容性 gate + 既有 fixture 覆盖 | 2 | — | — |
| **P2** | 分类器 + 四处 reader 打通 + 白名单 LRU | 4 | — | — |
| **P3** | 类别计数（从 `gasPool.Used()` 差分）+ `ProcessResult` 扩展 | 1 | — | — |
| | 配额推进纯函数（`Step`/`Size`/`Valid`/`CheckInequality`） | 1 | — | — |
| | 导入路径接线 + `ValidateState` 三段校验 | 1.5 | — | — |
| **P4** | 不等式准入（`environment`/`makeEnv`/`commitTransaction(s)`/自检） | 2 | — | — |
| | lane 补填轮（复用 `MinTip`）；`IsPayment` 提示位为可选优化 | 0.5 | — | — |
| | MEV（BidBlock admission + simBid 准入 + `RawBid`/`MevParams` + 版本协商） | 2 | — | — |
| **P5** | epoch 读取 + `header.Extra` 承诺 + Snapshot 扩展 | 5 | — | — |
| | sanitize 全函数 + fuzz | 2 | — | — |
| | `PaymentLaneConfig.sol` + Foundry + genesis + registry + 审计响应 | — | 13 | — |
| **RPC** | `eth_getBlock*` 新字段 + `eth_paymentLaneStatus` + `feeHistory` + metric | 2 | — | — |
| **测试** | 用例实现（105 场景 / ~249 用例，见第 3 节） | — | — | 55 |
| | fuzz ×3 | — | — | 3 |
| | 测试基建 ×12 项 | — | — | 34 |
| | 规格裁决后的返工缓冲（15%） | — | — | 14 |
| **合计** | | **30** | **13** | **106** |
| | **总计** | | | **149 人日** |
| | + code review / 联调 / 文档 / buffer（15%） | | | **≈172 人日** |
| | 悲观情形（M4/M5 要求机制层面缓解 → 规格与实现返工） | | | **≈207 人日** |

**折合日历时间**：3 人并行（1 geth 核心 + 1 合约 + 1 测试）约 **3 个月**，其中前 4–5 周测试同学基本全在做基建（见 3.4）。**不含**：第三方审计轮次、Chapel 灰度观察期、builder 生态改造的协调时间。

## 2.8 关键路径

```
P0 规格裁决（5d，日历上可能数周）
  └─→ P1 fork + UncleHash（4d）
        ├─→ P2 分类器（4d）──┐
        └─→ P3 三桶记账（4d）─┴─→ P4 出块器 + MEV（7d）
                                    └─→ devnet e2e
        └─→ P5 系统合约（geth 7d ∥ 合约 13d）─→ devnet e2e

测试基建（34d）与 P1–P3 可并行启动，但其中 I2/I5/I6/I7 四项
是硬前置，完成前约 40% 的用例跑不了 —— 这是排期的真实瓶颈。
```

**最长链是合约侧的 13 人日 + 审计**，不是 geth 侧。若 M4 采纳「lane 内单笔 gas 上限」的修法，geth 侧只多半天，是性价比最高的缓解手段。

## 2.9 三大实现风险

### 风险 1（最高）· 矿工/MEV 侧的 gas 记账

§3.2 意味着 general 交易的有效预算是 `GasLimit − max(laneSize − paymentPacked, 0)`——一个**随打包进度移动**的预算。而今天的单一 `GasPool`（`core/state_processor.go:79`；`miner/worker.go:733` 及其 `:713-718` 那段专门为对齐口径而写的注释）表达不了这个语义。

**本仓在这个精确位置已经出过两次共识级事故**：`header.GasUsed` 与 Parlia `SubGas` 预留双重计数；bid 路径漏写 `header.GasUsed`。**第三次是最可能的结局。**

缓解：桶做进 `GasPool` 让「和恒等」成为定义（2.4.1）；加「验证者侧重算并逐块断言与出块方一致」的 devnet 断言；MEV 的 bid（经 sentry）与 bidblock（直连）两条拓扑都必须跑；强制在 breathe 块（`updateValidatorSetV2` 12.16M 系统 gas）上跑满四种组合。

### 风险 2 · 空块/低需求块的吞吐损失会被 KPI 抓到

§3.2 的「unused quota is idle, not reclaimable」意味着零 payment 需求的块**无条件**损失至多 `laneSize` 容量。`MAX_RATIO = 8%` 于 70M 块 = 5.6M gas。而 §3.3 的信号**不区分拥堵原因**（§4 明文接受这点），所以一次与支付无关的大流量脉冲（NFT mint、空投）会把配额推到上限，随后在约 10 块的回落期里持续空转 5.6M gas。

叠加系统交易预留后更明显：breathe 块上 `70M − 20M(系统预留) − 5.6M(lane) − 25k(payBidTx)` → general 可用空间约 44M，**掉 37%**。§5「general transactions have marginally less space」在 breathe 块上不成立。

缓解：`MAX_RATIO` 取保守值；提前与运维/监控沟通（`gasUsed/gasLimit` 长期低于 100% 会被当成出块 bug 报上来）；`paymentlane/fill_ratio` metric 必须先上。

### 风险 3 · 分类的边界情形 + fastnode 的 state 读

「`to` 无 code」是整个机制的安全边界。三个失效模式：(a) 「父块末状态 vs 交易前状态」的选择会改变「同块内更早被委托的账户」的判定结果（M3）；(b) 不存在账户的 codeHash 是零 hash 不是 `EmptyCodeHash`（2.3 的陷阱）；(c) 在 fastnode（`--tries-verify-mode none`）上，account 只能来自单一 snapshot reader（`core/state/database_mpt.go:82-90` 跳过 trie reader；`eth/backend.go:190-193` 强制 HashScheme），失败模式是**静默返回「账户不存在」**（`core/state/snapshot/disklayer.go:121` `ErrNotCoveredYet` → `core/state/statedb.go:641-645` `setError` 后 return nil）。

而 fastnode **仍然真实执行交易并校验 `header.GasUsed`**（`core/block_validator.go:152-154`，无 NoTries 豁免）。BEP-703 把「有无 code」变成 gas 会计的输入，也就变成共识关键读。**结果：fastnode 把有码地址当无码 → 三桶不匹配 → 拒绝合法块，且日志里只有一条 invalid gas used。** 本仓已出过 NoTries StateReader 回归（`missing trie node`），在一条此前从不触碰这些账户的路径上新增读取，有把它重新激活的风险。

缓解：分类读的错误必须显式冒泡成块处理失败（2.3 的 `c.err`）；补 7702/CREATE2/selfdestruct 显式测试向量；**合并前必须跑 fastnode devnet 拓扑（5 validator + 1 fastnode）**。

---

# 第 3 节 · 测试场景矩阵

**图例**：层级 `U`=unit / `C`=core 集成 / `M`=miner 集成 / `E`=e2e devnet / `F`=fuzz / `S`=Solidity。
优先级 `P0`=上主网前必须绿 / `P1`=上 Chapel 前必须绿 / `P2`=可后置。
标 **⚠规格** 的用例在对应规格条款裁决前**写不了**（不知道断言什么），不是工作量问题。

## 3.1 现有测试基建结论

一句话：**BSC 现有测试基建在本机制最关键的三条路径上基本为零，基建投入会超过用例投入。**

| 场景类别 | 能否承载 |
|---|---|
| 分类纯函数、配额推进纯函数、不等式 | ✅ 新建 unit 即可，最好测的部分 |
| 不等式的 core 层集成（用 `mockParlia`） | 🟡 需要 lane-aware `BlockGen` + 「造非法块」逃生口 |
| 含**真实系统交易**的不等式 / signal | ❌ `mockParlia`（`core/blockchain_test.go:4099`）不实现 `consensus.PoSA` → `core/state_processor.go:129` 的 `isPoSA` 为 false，系统交易路径被整条绕过；真 parlia 又跑不起来 |
| miner fill→seal（含 lane 预留） | ❌ `miner/worker_test.go:126-136` 的引擎 switch **只认 clique/ethash，parlia 落 `t.Fatalf`**；唯一 parlia 测试 `:224` 是 `t.Skip`（TODO 在 `:223-229` 明写缺 parlia 创世）。于是 `worker.go:713-718` 注释里写死的 `header.GasUsed == gasPool.Used()` 不变量**今天零覆盖** |
| MEV / bid | ❌ **全仓从未调用 `newBidSimulator(...)`**；所有 bid 测试用 `&bidSimulator{}` 零值字面量（`miner/bid_block_permission_test.go:295-299`、`miner/bid_quota_race_test.go:39-42`） |
| txpool 按 lane 供给交易 | ❌ `PendingFilter` 无此维度 |
| reorg / 重启 | 🟡 骨架齐（`core/blockchain_test.go:571` `testReorgLong`、`:983` `testChainTxReorgs`、`blockchain_repair_test.go`、`blockchain_sethead_test.go`） |
| snap sync / fastnode | ❌ 只能 e2e；`TriesVerifyMode` **零 Go 测试覆盖**（`grep TriesVerifyMode --include=*_test.go` 零命中） |
| 分叉边界 | 🟡 `core/eip3529tests/` 是最贴合的范式（`eip3529_parlia_test.go:12-34` 的 config 克隆），但 `ParliaTestChainConfig`（`params/config.go:378-414`）**停在 Cancun** |
| 跨客户端一致性 | ❌ 无向量集、无 runner |
| `tests/` 目录 | ❌ **不要往里塞**：`Forks` 映射（`tests/init.go:30-779`）42 条全是上游分叉（`grep Feynman\|Lorentz\|Maxwell\|Parlia` 零命中）；block test 引擎硬编码 `beacon.New(ethash.NewFaker())`（`tests/block_test_util.go:157`）；state test 只执行一条 message（`tests/state_test_util.go:345`），per-block 共识规则根本不可表达；fixture submodule 本地未初始化（0 文件） |
| e2e | 🟡 `make truffle-test`（`Makefile:39-50`）只跑一个 BEP20 代理 smoke（套件从仓外 clone），`bootstrap.sh:66` 的 genesis 生成被注释掉，`Makefile:48` 是盲等 `sleep 60` → **对共识规则覆盖为零**。仓内无 devnet 目录，需用仓外 `node-deploy` |
| fuzz | ✅ 统一原生 Go `testing.F` 形制（`tests/fuzzers/difficulty/difficulty_test.go:21` 是模板）。但 `consensus/parlia`、`core/block_validator.go`、fork 调度**零 fuzzer** |

## 3.2 A 组 · 分类边界（§3.1）· 24 场景

| # | 场景 | 前置 | 构造 | 期望 | 层级 | 优先 |
|---|---|---|---|---|---|---|
| A1 | 基线原生转账 | `B` 无 code | `to=B, data=∅, value=1 wei, gas=21000` | payment，`paymentGasUsed += 21000` | U+C | P0 |
| A2 | data 空但 to 有 code | `C` 是合约（fallback 空实现） | `to=C, data=∅, value=1` | **general**（执行了 fallback） | U+C | P0 |
| A3 | to 带 7702 delegation | 父块里对 `B` 设过 delegation（code = `0xef0100‖impl`，23B） | `to=B, data=∅, value=1` | **general** | U+C | P0 |
| A4 | delegation 已撤销 | 父块里 `auth.Address == 0` → `core/state_transition.go:781-783` `SetCode(B, nil)` | `to=B, data=∅` | **payment**（code 完全清空，不是 23B 零地址 designator） | U+C | P0 |
| A5 | **raw code vs resolved code** | `B` delegation 指向 `impl`，而 `impl` **本身无 code** | `to=B, data=∅` | **general**。实现若先 resolve delegation 再取 code size 会得到 0 → 误判 payment。必须用 raw codeHash | U+C | **P0** |
| A6 | 三种 tx type 同语义 | 同 A1 | 分别用 Legacy / AccessList(空 list) / DynamicFee | 三者分类一致 | U | P0 |
| A7 | **BlobTx** | `B` 无 code | `BlobTx{To=B, Data=∅, BlobHashes=[6]}`（`To` 恒非 nil，`core/types/tx_blob.go:296`） | 按条款 1.4 → **general**。⚠规格：现文本字面 → payment | U+C | **P0** |
| A8 | **SetCodeTx，1 条 auth** | `B` 无 code | `SetCodeTx{To=B, Data=∅, AuthList=[1]}` | 按条款 1.4 → **general**。⚠规格 | U+C | **P0** |
| A9 | SetCodeTx 撑满 lane | `laneSize=8M` | 320 条 auth：`21000 + 320×25000 = 8.02M` | 若未按条款 1.4 排除，单笔吃掉整条 lane 且全是状态写入 | C | P0 |
| A10 | value=0 | `B` 无 code | `to=B, data=∅, value=0, gas=21000` | 按条款 1.8 → **general**。⚠规格：现文本 → payment（最廉价占位单元） | U+C | P0 |
| A11 | to = nil（部署） | — | `to=nil, data=initcode` | general | U | P0 |
| A12 | **to = 以太坊 precompile** | `to=0x01`(ecrecover)，state 无 code | `to=0x01, data=∅, gas=100000` | 按条款 1.6 → **general**。⚠规格：现文本 → payment（且 ecrecover 对空输入不报错，扣 3000 gas） | U+C | **P0** |
| A13 | **to = BSC 跨链 precompile，烧光全部 gas** | `to=0x65` `iavlMerkleProofValidate`（`core/vm/contracts.go:405-410`） | `to=0x65, data=∅, gas=params.MaxTxGas=16777216` | precompile 返回非 revert error → `core/vm/evm.go:337-341` `gas.Exhaust()` → **单笔烧 16.7M**。按条款 1.6 → general；现文本 → payment，**证伪 §4「no code execution at all」** | U+C | **P0** |
| A14 | 白名单 to + 非转账 selector | USDT 在白名单 | `to=USDT, data=approve(spender, max)` ≈46k | 现文本 → **payment**（只看 `to`）。断言实现行为与 §4 措辞的冲突已被规格裁决（条款 4） | U+C | **P0** |
| A15 | **白名单 to + 128KB 垃圾 calldata** | USDT 在白名单，Prague 已激活 | `to=USDT, data=` 128KB 全非零（无效 selector → revert）。floor = `21000 + 10×4×131072 = 5,263,880` | revert **不退** floor gas → `paymentGasUsed += 5.26M`。**2 笔即占满 8M lane** | C | **P0** |
| A16 | 白名单块内被移除 | 块 n 的治理 tx 删掉 USDT | 同块另有 `to=USDT` 转账 | 块 n 内仍 **payment**（读父态）；n+1 起 general。若采 epoch 缓存则为「下个 epoch 边界生效」（条款 6.3） | C | P0 |
| A17 | 白名单块内被加入 | 块 n 加入 token T | 同块 `to=T` 转账 | 块 n 内仍 **general** | C | P0 |
| A18 | **同块内先给 to 部署 code，再转账过去** | `A` 在父态无 code | tx0：CREATE2/工厂部署到确定地址 `A`（或 SetCodeTx 给 `A` 装 delegation）；tx1：`to=A, data=∅, value=1` | tx1 **仍是 payment**（父态无 code）。**若实现顺序地用被 tx0 修改过的 statedb 分类 → 判 general → 与其他节点分歧。这是最容易写错的一条** | C | **P0** |
| A19 | to = 系统合约 | `0x…1000` 有 code | `to=0x…1000, data=∅` | general | U | P1 |
| A20 | 纯 access-list 填充 | `B` 无 code | `to=B, data=∅, accessList=3300 地址`（2400/addr） | 按条款 1.5 → general。⚠规格：现文本 → payment，≈8M gas 且**无任何执行** | C | P1 |
| A21 | to 是全新账户 | `B` 不存在 | `to=B, data=∅, value=1` | **payment**。★ 断言实现没写成 `GetCodeHash(to) == EmptyCodeHash`（会误判 general），这是 lane 最核心用例 | U | **P0** |
| A22 | payment tx 执行失败 | `B` 无 code，sender 余额不足 | `to=B, data=∅, value=巨额` | 入池即拒；若绕过入池则整块无效。断言分类逻辑不因失败 tx 产生记账偏差 | C | P1 |
| A23 | SELFDESTRUCT 过的地址 | 父块里 `C` 被 selfdestruct（EIP-6780 后 code 保留） | `to=C, data=∅` | 按父态 codeHash → general | C | P2 |
| A24 | 类别③（BEP-702） | **仓内零实现**（`grep BEP-702\|NativeToken --include=*.go` 零命中） | — | **不可测**。只做接口占位 + 「地址未通过有效性检查则拒绝上架」的负路径 | U | P2 |

## 3.3 B 组 · 记账不等式（§3.2）· 16 场景

记 `GL = GasLimit`、`L = laneSize`、`S = systemGasUsed`。

| # | 场景 | 构造 | 期望 | 层级 | 优先 |
|---|---|---|---|---|---|
| B1 | 恰好取等 | `payment = L`，`general = GL − L − S` | **合法**（`<=` 含等号） | C | P0 |
| B2 | general 超 1 gas | `payment = L`，`general = GL − L − S + 1` | **非法**，`InsertChain` 报错 | C | P0 |
| B3 | payment 超 1 gas | `payment = L + 1`，`general = GL − L − S − 1` | **合法**（退化为普通 `p+g+s <= GL`） | C | P0 |
| B4 | payment 欠 1 gas | `payment = L − 1`，`general = GL − L − S + 1` | **非法**（缺额一对一挤压 general） | C | P0 |
| B5 | payment 为 0 | `payment = 0`，`general = GL − L − S` | 合法 | C | P0 |
| B6 | payment 为 0 且 general 多 1 | `payment = 0`，`general = GL − L − S + 1` | 非法 | C | P0 |
| B7 | lane 是地板不是天花板 | `payment = GL − S`，`general = 0` | **合法** | C | P0 |
| B8 | 空块 | 0 笔用户 tx | 合法（`S + 0 + max(0,L) <= GL`，前提 `L <= GL − S`，见 B13） | C | P0 |
| B9 | 只有系统交易的块 | 真 parlia，非 breathe，`S ≈ 60k–550k` | 按条款 2.1 三桶 → `S` 单列，不进 payment/general；**⚠规格：现文本未定义** | C | **P0** |
| B10 | general 填满后再来一笔 payment | `general = GL − L − S`，追加 21000 payment | **合法**（payment 吃 lane，`max(21000, L) = L`，总和不变） | C+M | P0 |
| B11 | general 填满后 payment 溢出 | `general = GL − L − S`，追加 payment 共 `L + 21000` | **非法** | C | P0 |
| B12 | 溢出 tx 跨界如何切分 | `L = 8M`，payment 三笔 3M+3M+3M（第三笔跨 8M 边界） | **⚠规格（条款 2.4）**：按 gas 切（2M payment / 1M general）还是按 tx 边界切？两种口径 signal 不同 → 直接决定下一块配额 | C | **P0** |
| B13 | `L > GL − S` | 治理把 `MIN` 设为 `GL+1`（现文本四条不变量全部满足！） | 采纳条款 5.1（第五不变量）+ `Size()` 的客户端钳制后：`L <= GL`，空块仍有效。**⚠不修则链停（M6）** | U+S+C | **P0** |
| B14 | 与系统 gas 预留的口径交叉 | 真 parlia，`gasReserved = 1M`，矿工填到 general 池见底 | 矿工产出的块必然满足共识不等式；断言 `env.gasUsed() == header.GasUsed` 且三桶之和相等 | M | **P0** |
| B15 | refund 口径 | payment tx 触发 SSTORE 清零退款（EIP-3529 上限 gasUsed/5） | 三桶用 `gp.Used()` 同口径累计（2.4.1）；断言 `Σ桶 == header.GasUsed` | C | P0 |
| B16 | breathe 块 | 真 parlia breathe 块，`S ≈ 12.16M` | 用户 tx 填到边界；断言 signal 分子**不含** `S`（否则每天误触发扩张） | C+E | P0 |

## 3.4 C 组 · 配额递推边界（§3.3/§3.4）· 18 场景

默认参数：ratio `[2%,8%]`、绝对 `[2M,8M]`、触发 `[70%,80%]`、步长 `+2pp/−0.5pp`；`GL=70M` → 有效区间 `[2M, 5.6M]`。

| # | 场景 | 构造 | 期望 | 层级 | 优先 |
|---|---|---|---|---|---|
| C1 | 激活块初值 | fork 在块 n 激活 | `L(n) = max(MIN_RATIO×GL, MIN) = 2M`；且激活块本身豁免 lane 校验（条款 3.6） | U+C | P0 |
| C2 | 激活块 +1 的 signal | 块 n 是激活块，n−1 在 fork 前（`UncleHash` 无 lane 承诺） | 按条款 3.6 明确取值（建议：`L(n+1) = L(n)`，即激活后第一次 step 从 n+1→n+2 才生效） | U | P0 |
| C3 | signal 恰等 EXPAND | `general = 0.80 × 70M = 56,000,000` | **扩张**（`>=` 含等号）：`L → 3.4M` | U | P0 |
| C4 | 差 1 gas 到 EXPAND | `general = 55,999,999` | 落滞后带 → **不变** | U | P0 |
| C5 | signal 恰等 SHRINK | `general = 0.70 × 70M = 49,000,000` | **不变**（`<` 不含等号） | U | P0 |
| C6 | 差 1 gas 到 SHRINK 之下 | `general = 48,999,999` | **收缩**：`L −= 0.5pp×70M = 350k` | U | P0 |
| C7 | 连续拉满钳到 MAX | 从 2M 起，15 块连续 `signal >= 80%` | `2M→3.4M→4.8M→5.6M(钳)`，第 4 块起恒 5.6M | U | P0 |
| C8 | 连续缩到 MIN | 从 5.6M 起，30 块连续 `signal < 70%` | 每块 −350k，第 11 块到 2M(钳)，之后恒 2M | U | P0 |
| C9 | **钳位后递推态是否回写** | 从 2M 起 5 块拥堵（累计 +10pp，「影子 ratio」=12%，钳到 8%），第 6 块 `signal < 70%` | 采纳条款 3.2（递推态存 bps 且**每块重新钳制**）→ **saturating**：从 5.6M 降到 5.25M。⚠若实现成 shadow 需先缩 8 块才有可见变化，**两种实现在拥堵结束后差 8 个块** | U | **P0** |
| C10 | 交集为空（上界方向） | `GL = 500M` → `ratio_min = 10M > abs_max = 8M` | 按 §3.4「absolute bounds prevail」→ 区间 `[2M, 8M]` | U | P0 |
| C11 | 交集为空（下界方向） | `GL = 10M` → `lo = max(200k,2M) = 2M`，`hi = min(800k,8M) = 800k`，空 | 现文本 → 区间 `[2M,8M]` = **块的 20%–80%**，`MAX_RATIO` 保护完全失效。采纳条款 5.1 后被第五不变量禁止 | U | **P0** |
| C12 | GL 块间变化时的换算 | `CalcGasLimit` 每块 ±`GL/1024`；20 块内 GL 从 70M 升到 71.4M | 递推态是 bps（条款 3.2）→ ratio 不变、`L` 随 GL 线性变；断言舍入为先除后乘向下取整（条款 3.4） | U | **P0** |
| C13 | GL 骤降到配额之下 | `L = 5.6M`，GL 连降至 6M | `Size()` 的钳制立即生效（不按 SHRINK_STEP 走）；断言空块仍有效 | U | P0 |
| C14 | 锯齿需求 | 20 个「1 块 `signal=85%` + 1 块 `signal=60%`」周期 | 配额单调上行至 MAX 并停留；断言 `L(2k) >= L(2k−2)` 且 K 轮后 `L = 5.6M`（不变量 2 的立意） | U | P0 |
| C15 | breathe 块对 signal 的污染 | 真 parlia breathe 块，`S = 12.16M`，用户 general = 0 | 三桶方案下 `signal = 0/70M = 0%` → 收缩。**若误把 `S` 并入 general → `signal = 17.4%`；GL 较小时可直接顶过 80% → 系统交易自己把配额顶到 MAX** | C+E | **P0** |
| C16 | 决定论 | 同一条链两次独立推导；再并发 100 次 | 结果完全一致；无 map 迭代序、无时间依赖、无浮点 | U+F | P0 |
| C17 | **跨客户端定点向量** | JSON 向量 `(params, [GL_i], [general_i], [payment_i]) → [L_i]`，含 C3–C13 全部边界 | bsc-geth 与其他实现（reth-bsc）**逐 bit 一致** | U | **P0** |
| C18 | 状态机 fuzz | 随机合法参数 + 随机 10k 块 signal 序列 | 不变量恒成立：`L ∈ [lo,hi]`、`L <= GL`；`signal>=EXPAND ⇒ L 不减`；`signal<SHRINK ⇒ L 不增`；无 uint64 上下溢 | F | P0 |

## 3.5 D 组 · reorg / sync · 10 场景

**注**：`UncleHash` 承诺方案（M1）使递推深度为 1，D 组从「规格阻塞」变成「常规回归」——这是该方案最大的工程节省。

| # | 场景 | 构造 | 期望 | 层级 | 优先 |
|---|---|---|---|---|---|
| D1 | reorg 后配额重算 | 两条等长分支（A 全 general 拥堵、B 全空闲），先 insert A 再 insert 更高 TD 的 B | 切到 B 后 `L` 按 B 的父 header 重算（= MIN），不残留 A 的 5.6M | C | P0 |
| D2 | 深 reorg 跨拉满区间 | A 已把配额拉到 MAX 并持续 10 块，reorg 到 12 块深的 B | 沿 B 逐块重校验；断言每块的 `UncleHash` 承诺与本地重算一致 | C | P0 |
| D3 | 缓存键正确性 | reorg 到同高度不同哈希 | 白名单/参数缓存必须按 **blockHash**（parlia Snapshot 已是内容寻址）；断言不复用旧值 | C | **P0** |
| D4 | 重启后恢复 | 节点带 5.6M 配额运行，`kill -9` + 重启 | 从父 header 直接读出，**零回放** | C+E | **P0** |
| D5 | snap sync 从 pivot 接手 | 新节点 snap sync，pivot 在激活块之后 | `laneSize(pivot+1)` 从 pivot 的 header 直接算出，**零特例、零回填**。★ 这条在 BEP 现文本下无解（M1-b），是验证 `UncleHash` 方案的核心用例 | E | **P0** |
| D6 | full sync 从 genesis | 全量同步含激活块的链 | 与出块节点结论一致 | E | P0 |
| D7 | **fastnode（`--tries-verify-mode none`）** | 同步含 lane 规则的链 | 配额来自 header（不依赖 state）；但**分类仍需读 account** → 断言分类读失败会使整块失败而非静默误判（风险 3） | E | **P0** |
| D8 | 三模式结论一致 | full / snap / fastnode 各 1 节点，喂同一条链（含 B2/B4/B6/B11 的非法块） | accept/reject **完全一致** | E | P0 |
| D9 | header-first sync | BSC 先插 header 后插 body | `VerifyHeader` 阶段只做 `UncleHash` 的**语法**校验（保留位为零、桶不溢出），语义校验留到 `ValidateState`；断言不误拒 | C+E | P0 |
| D10 | `--history.blocks` 节点 | 开启历史裁剪（最小保留 60 万块），随后深度回溯 | 配额只依赖父 header → **不受影响**。★ 对照：BEP 现文本下递推链会断（M1-b） | C+E | P1 |

## 3.6 E 组 · 治理参数（§3.5）· 13 场景

| # | 场景 | 构造 | 期望 | 层级 | 优先 |
|---|---|---|---|---|---|
| E1 | 违反不变量 1 | `EXPAND=75%, SHRINK=70%`（gap 5% < 10%） | 合约 revert，参数不生效；**且断言链上提案仍显示 `Executed` + `failReasonWithStr` 事件**（M8 的真实语义） | S+U | P0 |
| E2 | 违反不变量 2（三种） | (a) `EXPAND_STEP = SHRINK_STEP`；(b) `<`；(c) `SHRINK_STEP = 0` | 全部拒绝 | S+U | P0 |
| E3 | 违反不变量 3 | `MAX_RATIO − MIN_RATIO = 4% < 5%` | 拒绝 | S+U | P0 |
| E4 | 违反不变量 4（三种） | (a) `MAX = MIN`；(b) `MAX < MIN`；(c) `MIN = 0` | 全部拒绝 | S+U | P0 |
| E5 | `MIN = MAX` | 显式测 `MIN=MAX=4M` | 被不变量 4 拒绝（`MAX > MIN` 严格） | S+U | P0 |
| E6 | `EXPAND_STEP` 极大 | `EXPAND_STEP = +100pp` | 现文本未禁止 → 合法；一块直达 MAX 后被钳位。断言无溢出、无越界 | U | P0 |
| E7 | 定点下 `SHRINK_STEP` 为 0 | `SHRINK_STEP = 0.0000001pp`（bps 下取整为 0） | 形式满足不变量 2 但语义违反（永不收缩）→ 断言 `sanitize` 钳到 `>= 1` bps（条款 5.4） | U | **P0** |
| E8 | `MAX > GasLimit` | `MAX = 100M`，`GL = 70M`（现文本四条不变量全满足） | 被条款 5.1 的第五不变量拒绝；且客户端 `Size()` 钳制兜底。**⚠不修则链停** | S+U+C | **P0** |
| E9 | 参数变更生效边界 | 块 n 内治理 tx 改参数 | 采 epoch 缓存 → **下一个 epoch 边界生效**（条款 6.3）；断言 n..epoch 边界之间用旧值，且所有节点一致 | C+E | **P0** |
| E10 | Go 侧读到非法参数 | 直写 storage 绕过 require（或模拟合约升级 bug）使 `MAX < MIN` | `sanitize` 全函数钳制到合法配置 + `log.Crit` + metric；**断言不 panic、不停链、所有节点钳制结果相同** | U+C | **P0** |
| E11 | 参数变更使当前配额越界 | `L = 5.6M`，治理把 `MAX` 降到 3M | 条款 3.5：每块重新钳制 → **立即钳到 3M**（不按 SHRINK_STEP 走） | U | P0 |
| E12 | 参数 fuzz | 随机 8 元参数组（含全域非法值）→ `sanitize` → `Step`/`Size` 10k 块 | **通过 sanitize 的任意输入都不导致链停/溢出/配额越界**；用于发现遗漏的不变量 | F | P0 |
| E13 | 半应用配置 | 打包两次 `updateParam` 移动滞后带 `[70,80] → [85,95]`，顺序错误 | 现状会留下 `[70,95]` 且提案显示 `Executed`（M8）→ 断言采纳原子 setter 后整体 revert | S | **P0** |

## 3.7 F 组 · MEV / bid · 8 场景

| # | 场景 | 构造 | 期望 | 层级 | 优先 |
|---|---|---|---|---|---|
| F1 | bid 违反不等式 | builder 已注册，`L=5.6M`，bid 中 `general = GL − L − S + 1` | `simBid` 阶段拒绝；**不消耗 builder 的 `maxBidsPerBuilder` 配额**；不触发 revoke；失败原因可区分（`errLaneGeneralOverflow`） | M | P0 |
| F2 | bid 全是 payment 且溢出 | `payment = 20M, general = 0` | 合法，接受 | M | P0 |
| F3 | bid 全是 payment 但欠额 | `payment = 2M`，`general = GL − 5.6M − S + 1` | 拒绝 | M | P0 |
| F4 | bid + local tx 混合 | bid 覆盖部分块空间，本地 greedy merge 补填 | 两部分累加后满足不等式；`header.GasUsed == env.gasUsed()`；`UncleHash` 承诺与三桶一致 | M | P0 |
| F5 | **PayBidTx 的归类** | 一个 bid + PayBidTx（`params.PayBidTxGasLimit = 25000`，纯转账到 EOA） | 按条款 1.7 → **general**。⚠规格：按现文本机械判据它就是 payment，MEV 回扣搭 lane 便车 | M | **P0** |
| F6 | **BidBlock 违反不等式** | 提交一个 lane 超限但其余全部合法的 BidBlock | **`preSealVerifyBidBlock` 在签名前拒收**（2.5.3-a）；断言 `mux.Post`（`miner/bid_block.go:265`）**从未被调用** → validator 不丢槽 | M | **P0** |
| F7 | BidBlock 承诺不自洽 | `UncleHash` 里 `general + payment > header.GasUsed`，或 `ratioBps` 与父 header 推导值不符 | 签名前拒收（`errLaneBucketOverflow` / `errLaneRatioMismatch`） | M | **P0** |
| F8 | BidBlock 承诺自洽但虚假 | 承诺满足不等式，但实际执行后三桶与承诺不符 | admission 放过（无法零执行检测）→ `InsertChain` 的 `ValidateState` (b) 拒绝 → builder revoke。断言 revoke 被触发且原因可区分 | M+E | P0 |

## 3.8 G 组 · 攻击与滥用（每条给量化上限）· 11 场景

价格假设：lane 内清算价 `p_lane`、大盘 `p_gen`；拥堵时 `p_gen >> p_lane`。BNB 按 $1000、`p_lane = 0.05 gwei` 估算，块间隔 450ms。

| # | 场景 | 构造 | 量化上限与期望 | 层级 | 优先 |
|---|---|---|---|---|---|
| G1 | 最小额自转账封锁 lane | 24 账户 × 16 笔 pending（`to=self, data=∅, value=1 wei`，21000）→ 381 笔 = 8.001M | **lane 100% 占用**。成本 `8M × p_lane` = 0.0004 BNB ≈ **$0.4/块** → **≈$77k/日**持续封锁全网支付车道 | C+E | P0 |
| G2 | **白名单合约 + 垃圾 calldata** | 2 笔 `to=USDT, data=128KB 非零`（各 5,263,880 floor gas） | **2 笔占满 8M lane**，成本同 G1 量级。这是「用白名单稳定币的非转账调用挤进 lane」的**最优形式**：不需合约配合、不需有效 selector、revert 也照收 | C+E | **P0** |
| G3 | approve 洪水 | 174 笔 `approve`(46k) = 8.0M | lane 占满，额外产生真实状态写入 | C | P1 |
| G4 | precompile 烧 gas | 1 笔 `to=0x65, gas=16.7M`（A13） | **单笔烧尽整条 lane 且超出 MAX**；成本 `16.7M × p_lane` | C | **P0** |
| G5 | 7702 批量授权 | 1 笔 320 auth SetCodeTx（A9） | 单笔 8M，全部状态写入，顺带完成 320 个账户的 delegation 安装 | C | P0 |
| G6 | BlobTx 借道 | `BlobTx{To=EOA, Data=∅, 6 blobs}`（A7） | 21000 执行 gas 换 lane 优先权 + blob 预算；拥堵时以 `p_lane` 买到 DA 空间 | C | P0 |
| G7 | **单笔 tx > laneSize** | 1 笔 8M gas 的合法 payment tx（`MaxTxGas = 16.7M > MAX = 8M`） | **§3.5「5.6M ≈ 90+ transfers per block guaranteed」被单笔交易作废**：保底是 gas 额度不是交易笔数。断言是否加 lane 内单笔 gas 上限（条款 4） | U+C | **P0** |
| G8 | **lane 定价套利** | 拥堵（`p_gen = 3 gwei`），在 lane 内投放 G2/G4/G5/G6 型负载 | 测量 `p_lane / p_gen` 折扣倍数。预期数十倍 → 攻击者以折扣把非支付负载塞进链（M5）。**这是机制级结论，测试只负责给出实测倍数** | E | **P0** |
| G9 | signal 压制 | 每块只填到 `general = ceil(0.80×GL) − 1` | 配额永不扩张，放弃收益 = **1 gas 的费用 ≈ 0**（M7）。断言采纳滑动窗口后压制成本的实际下界 | E | **P0** |
| G10 | signal 拉高（反向） | 用最便宜的 general gas（同样是 7623 floor）持续把 signal 顶到 ≥80% | 配额被顶到 MAX 并停留 → **持续冻结 5.6M/70M = 8% 块空间**，成本由 general 用户承担。攻击成本 `56M × p_min` | C+E | P0 |
| G11 | 出块者自灌水 | validator 用自己的地址对自己发 payment tx | **净成本为零**（BEP-226：base fee=0，priority fee 全额归 coinbase）→ §6「costs fees like any other spam」对出块者不成立（M5）。断言这一事实并在 §6 披露 | E | **P0** |

## 3.9 H 组 · 分叉兼容（§5）· 7 场景

| # | 场景 | 构造 | 期望 | 层级 | 优先 |
|---|---|---|---|---|---|
| H1 | fork 前按老规则 | 块时间 `< PaymentLaneTime`，构造 `payment=0, general=GL` 的块 | **必须被接受**；且 `UncleHash` 必须恒为 `EmptyUncleHash`（老 gate 仍生效） | C | P0 |
| H2 | 激活块 | `IsOnPaymentLane` 为 true 的块 | 系统合约在**块末**升级（`core/systemcontracts/upgrade.go:1112-1115`）→ 激活块本身**豁免** lane 校验，`L(激活块+1) = MIN`（条款 3.6） | C | P0 |
| H3 | 激活块 ±1 三连 | 块 n−1（老规则）、n（激活，豁免）、n+1（`L = MIN`，`UncleHash` 首次承载 lane 数据） | 三块逐一断言；n+1 的 `UncleHash` 解码成功且 `ratioBps = MIN_RATIO` | C | P0 |
| H4 | **新旧节点在 fork 高度分歧** | 3 新节点 + 2 旧节点；旧 validator 出一个 `UncleHash != EmptyUncleHash` 的块 | 旧节点**因 `errInvalidUncleHash` 拒绝**（`consensus/parlia/parlia.go:638`），新节点接受 → 明确分叉。断言新节点不会因此被 jail/卡死，finality 表现符合预期。★ 这是 `UncleHash` 方案特有的分歧形态，必须实测 | E | **P0** |
| H5 | fork 未排期回归 | `PaymentLaneTime = nil`，跑现有全套 core/miner/parlia 测试 | 行为与今天**逐 bit 一致**（零回归）；特别断言 `types.NewBlock` 仍把 `UncleHash` 置为 `EmptyUncleHash` | C+M | **P0** |
| H6 | 与其他 fork 同时刻激活 | `PaymentLaneTime == 下一个 BSC fork time`（先例：mainnet `OsakaTime == MendelTime == 1777343400`） | 顺序无关；两 fork 的初始化互不干扰 | C | P1 |
| H7 | 分叉调度校验 | 乱序配置 `PaymentLaneTime < PasteurTime` | `CheckConfigForkOrder` 拒绝（★ 该函数**今天零测试覆盖**，必须一并补 `TestCheckConfigForkOrder`） | U | P0 |

## 3.10 Fuzz · 3 项

| # | 目标 | 断言 | 人日 |
|---|---|---|---|
| F-1 | 配额递推状态机（C18/E12） | 随机合法参数 + 10k 块随机 signal 序列 → 不变量恒成立、决定论、无溢出、`L <= GL` | 1.5 |
| F-2 | 分类函数 | 随机 tx 全字段（含 type 0x00–0x04）+ 随机父 state → 分类是纯函数、不执行代码、不改 state、与 gas 无关；tx type 白名单不可绕过 | 1 |
| F-3 | 不等式 uint64 全域 | 随机 `(system, general, payment, lane, GL)` → 无溢出、与参考实现一致、`CheckInequality` 对空块永不误报 | 0.5 |

形制照 `tests/fuzzers/difficulty/difficulty_test.go:21`（原生 `testing.F`）。

## 3.11 工期汇总

| 组 | 场景数 | 表驱动用例数 | 人日 | 规格阻塞项 |
|---|---:|---:|---:|---|
| A 分类边界 | 24 | ~55 | 7 | A7/A8/A10/A12/A13/A14/A20（条款 1.4/1.5/1.6/1.8/4）、A24（BEP-702 未实现） |
| B 记账不等式 | 16 | ~32 | 6 | B9（条款 2.1）、B12（条款 2.4）、B13（条款 5.1） |
| C 配额递推 | 18 | ~70（含向量表） | 5 | C2（条款 3.6）、C9（条款 3.2）、C11（条款 5.1）、C15（条款 2.1） |
| D reorg/sync | 10 | ~16 | 8 | 无（`UncleHash` 方案已解锁；BEP 现文本下 D5/D7 不可测） |
| E 治理参数 | 13 | ~28 | 5 | E7/E8/E9/E10/E13（条款 5.1–5.4、6.3） |
| F MEV/bid | 8 | ~16 | 7 | F5（条款 1.7） |
| G 攻击/滥用 | 11 | ~22 | 9 | G7/G8/G11 需产品裁决（附录 A） |
| H 分叉兼容 | 7 | ~14 | 5 | H2/H3（条款 3.6） |
| **小计** | **107** | **~253** | **52** | |
| Fuzz | 3 | — | 3 | |
| 基建（3.12） | 12 项 | — | 34 | |
| 返工缓冲（15%） | — | — | 14 | |
| **合计** | | | **103** | |

## 3.12 测试基建缺口（排期的真实瓶颈）

| # | 项 | 人日 | 说明 |
|---|---|---:|---|
| **I1** | lane-aware `BlockGen` + 「造非法块」逃生口 | 3 | `BlockGen` 只有单 `gasPool`（`core/chain_makers.go:48`），`SetCoinbase` 硬编码 `NewGasPool(header.GasLimit)`（`:70`），`addTx` 直接 `header.GasUsed = gasPool.Used()`（`:128`）。需要 `SetPaymentLaneSize()` + 三桶 + **一个能产出「gas 记账违法但 stateRoot/receiptRoot 全部正确」的块**的构造器。`AddUncheckedTx`（`:183`）只跳过执行，产出的块状态根就错了 → B 组用例会**因为错误的原因被拒绝**，这比没测试更危险 |
| **I2** | parlia 系统合约读取从 `ethAPI.Call` 改为 state 直读路径 | 6 | 测试里 `p.ethAPI` 恒为 nil（`consensus/parlia/parlia.go:273` 所有调用点传 nil），`ethapi.NewBlockChainAPI` 需要完整 `ethapi.Backend`，仓内无轻量替身。**这不是纯测试需求**：BEP-703 的白名单与参数读取本来就应该走 state 直读 + epoch 缓存（2.6.1）。做完的副产品是 parlia 第一次可以在单测里跑起来 |
| **I3** | 带系统合约 + 已知私钥的 parlia 测试创世 fixture | 2 | `core.DefaultChapelGenesisBlock()`（`core/genesis.go:719`）已含全套系统合约 code+storage 且 extraData 是合法 21-validator epoch extra，可做基座；但 Chapel validator 私钥没有，无法签块。需产出 devnet alloc 并 checked in 成 **Go fixture**（`tests/truffle/storage/genesis.json` 已存在但零 Go 消费者） |
| **I4** | `ParliaLatestTestChainConfig`（全 fork = 0） | 1 | `ParliaTestChainConfig`（`params/config.go:378-414`）**停在 Cancun**（无 Prague/Bohr/Lorentz/Maxwell/Fermi/Osaka）→ SetCodeTx、EIP-7623 floor、`MaxTxGas` 这些 A/G 组最关键的用例在它下面**根本不可达**。★ **不要直接改它** —— `core/eip3529tests/eip3529_parlia_test.go:12-34`、`core/blockchain_test.go:4157`、`core/data_availability_test.go` 都依赖它当前的分叉集 |
| **I5** | `newTestWorkerBackend` 支持 parlia | 4 | `miner/worker_test.go:126-136` 的引擎 switch 只认 clique/ethash；唯一 parlia 测试 `:224` 是 `t.Skip`。接进 I2+I3 后打通。解锁 B14 与 M 层全部用例 |
| **I6** | `bidSimulator` 端到端 harness | 4 | 全仓从未调用 `newBidSimulator(...)`；无 builder 注册、无签名路径、无 `simBid → commitBidBlock → seal` 流程。F 组 8 条全部要从零建 |
| **I7** | `PendingFilter.OnlyPayment` + legacypool/blobpool 双侧实现 | 3 | 见 2.5.2。**不补则 lane 名义存在、实际天天空转**，而且现在连测量它的手段都没有 |
| **I8** | 新建 `core/paymentlanetests/` 包 | 1 | 照 `core/eip3529tests/` 形制（`eip3529_test_util.go:22 TestGasUsage` + config 克隆）。**不要塞进 `tests/`**，理由见 3.1 |
| **I9** | 跨客户端定点一致性向量集 + runner | 3 | 见 C17。**必须与 BEP 定稿同步产出**，不能等实现完成后倒推 |
| **I10** | node-deploy devnet 场景脚本 | 5 | 拓扑：5 validator + 1 full + 1 fastnode(`--tries-verify-mode none`) + 1 snap-sync 新节点 + 混合版本（新/旧二进制，用于 H4）；外加攻击流量注入器（G1/G2/G4/G5/G6）与 signal 压制脚本（G9）。⚠ devnet 运维陷阱：**不要单独重启孤立 validator（会被 jail）**，配置变更要 bake 进模板 + reset，或全网同步停启 |
| **I11** | 配额可观测性（RPC + metric） | 1 | 无此则 D/G/H 组只能刮日志。至少：`eth_getBlockByNumber` 增加 `paymentLaneSize`/`paymentGasUsed`/`generalGasUsed`（照 `totalDifficulty` 的注入模式，`internal/ethapi/api.go:1628/1638`）；`eth_paymentLaneStatus(blockNrOrHash)` 返回 `nextLaneSize`（builder 与 wallet 真正需要的）；`feeHistory` 加 `paymentLaneRatio`（`eth/gasprice/feehistory.go:108` 已有 `gasUsedRatio`）；metric `paymentlane/{quota,fill_ratio,payment_gas,general_gas,sanitize_count}` |
| **I12** | fork 调度接线测试 | 1 | 补 `TestCheckConfigForkOrder`（**今天不存在**）；`core/systemcontracts/upgrade_test.go:16-29` 的哈希数组停在 `feynmanFixUpgrade`，Bohr 之后的升级从未纳入 |
| | **合计** | **34** | |

**硬前置关系**：**I2 → I3 → I5 → I6**，以及独立的 I7。这四项完成前，B9/B14/B16、C15、D7、F 全组、G8/G9/G11 一个都跑不了 —— **约 40% 的用例被基建卡住**。双人并行的现实墙钟约 2.5–3 个月，**前 4–5 周基本全是基建**。

## 3.13 上线门

| 门 | 条件 |
|---|---|
| **G-1 合并前** | A/B/C/E/H 组全部 P0 绿；F-1/F-2/F-3 fuzz 各跑满 10 分钟无 crash；`PaymentLaneTime = nil` 下现有全套测试逐 bit 零回归（H5） |
| **G-2 devnet 门** | D 组全部 P0 绿；F 组全部 P0 绿（**bid 与 bidblock 两条拓扑都必须跑**）；breathe 块 × {本地打包, simBid, BidBlock} 三组合各连续 200 块无 BAD_BLOCK；跨节点区块哈希逐块一致 |
| **G-3 Chapel 门** | 连续 7 天无 BAD_BLOCK、无 `paymentlane/sanitize_count > 0`、无 validator jail；`paymentlane/fill_ratio` 在拥堵期 > 0.5（否则说明 I7 没做对，lane 在空转）；G8 的 `p_lane/p_gen` 折扣倍数已实测并记录 |
| **G-4 主网门** | Chapel 门满足 + 至少一个第三方审计轮次闭环 + builder 生态已完成 `RawBid.GeneralGasUsed` 改造并在 Chapel 验证 + 跨客户端向量集（I9）在 reth-bsc 上通过 |

---

# 附录 A · 待决策开放项

以下四项需要产品/社区层面拍板，报告不代为决定。前两项**直接决定 A 组与 G 组约 30 个用例的期望值**。

| # | 决策 | 选项 | 影响 |
|---|---|---|---|
| **A-1** | 类别① 是否限定 tx type 白名单并排除预编译地址段？ | (a) 是（条款 1.4/1.6，**推荐**）；(b) 否，接受 M2 的三个证伪 | 决定 A7/A8/A12/A13/A20 的期望值。选 (b) 则 §4「no code execution at all」必须删除，且 G4 的「单笔烧尽 lane」成为已披露行为 |
| **A-2** | ②/③ 是否加 selector 白名单或 lane 内单笔 gas 上限？ | (a) lane 内单笔 gas 上限（例如 100k，**推荐**：geth 侧多半天，同时解决 M4 与 G7）；(b) selector 白名单（治理负担大，且 ②/③ 的函数面会随合约升级漂移）；(c) 否，接受滥用上限 = 100% lane | 决定 A14/A15、G2/G3/G7 的期望值。选 (c) 则 §6 必须重写并量化披露 |
| **A-3** | 是否接受「lane 打折通道」（M5）作为已披露风险？ | (a) 接受并在 §6 量化披露；(b) 要求机制层面缓解（lane 内最低 priority fee 地板，或与大盘价挂钩） | 选 (b) 会**实质改变提案形态**并使工期上浮至约 210 人日 |
| **A-4** | 首发是否同时补 `PendingFilter` 的 payment 维度（I7）？ | (a) 是（**强烈推荐**）；(b) 后置 | 选 (b) 则 lane 名义存在但实际空转——拥堵时堆顶全是 general，矿工撞墙即停，`laneSize` 白白浪费，等于付了全部代价却拿不到任何收益 |

另有两项已在报告中给出推荐、但值得复核：

- **`UncleHash` 32 字节布局**（2.2.2）：是否需要为 BEP-696 的后续元数据预留更多空间？当前布局用了 20B、留 12B。若 BEP-696 将来定义的编码需要超过 12B，需要协调——建议在 BEP-703 里显式声明占用 `[0:20]` 并要求 BEP-696 的编码限定在 `[20:32]`。
- **signal 是否改多块滑动平均**（M7）：与 `UncleHash` 方案兼容（读最近 K 个 header，深度仍有界且 header-only 可算），但会增加 `Step` 的输入并使 C 组用例数上升约 20%。

# 附录 B · 代码锚点索引

基线 `payment-lane` 分支 @ `ece8248c4`。

### 需要修改

| 文件:行 | 内容 | 阶段 |
|---|---|---|
| `params/forks/forks.go:48,91` | fork 枚举与字符串映射 | P1 |
| `params/config.go`（11 处） | ChainConfig 字段 `:725` 附近、三网络配置 `:255/:309/:365`、`String()` `:918-921/:933-936/:984`、`IsPaymentLane` 仿 `:1448`、`IsOnPaymentLane` 仿 `:1453`、forkOrder `:1554-1586`、checkCompatible `:1807`、`LatestFork` `:1863`、`Timestamp` `:1952`、`BlobConfig` case `:1901`、Rules `:2125/:2170` | P1 |
| `core/types/block.go:320-321` | **`NewBlock` 覆写 `UncleHash` —— 最大陷阱** | P1 |
| `core/types/block.go:213-218` | `EmptyBody()` 用 `UncleHash` 判空 | P1 |
| `consensus/parlia/parlia.go:637-640` | `errInvalidUncleHash` gate | P1 |
| `core/block_validator.go:63-69` | `CalcUncleHash` 从 body 反推（BEP-696 禁止的形态） | P1 |
| `core/state/database_mpt.go:108-117` | `ReadersWithCacheStats` 扩返回第 3 个 reader | P2 |
| `core/blockchain_reader.go:495-515` | `StateWithCacheAt` 新增返回 reader 的变体 | P2 |
| `core/gaspool.go:26-30,60,98,107` | 三桶 + `ReturnGas` + `Snapshot`/`Set` | P3 |
| `core/state_processor.go:79,96,127,156,178,180` | 分类上下文 + `SetClass` + system 桶 | P3 |
| `core/block_validator.go:147,152` | `ValidateState` 三段校验 | P3 |
| `core/types.go:64` | `ProcessResult` 新字段 | P3 |
| `miner/worker.go:115-138,733,790-803,805-964,1103` | `environment` + `makeEnv` + 双池 | P4 |
| `core/txpool/subpool.go:33-44,75-83` | `LazyTransaction.IsPayment` + `PendingFilter` | P4 |
| `core/txpool/legacypool/legacypool.go:242,500,1409` | 填充 `IsPayment` + reset 对齐 | P4 |
| `miner/bid_simulator.go:714,1053,1068,1201,1383` | BidBlock admission + simBid 准入 + payBidTx + `header.GasUsed` | P4 |
| `core/types/builder/bid.go:84-91,308-318` | `RawBid.GeneralGasUsed` + `MevParams.PaymentLaneSize` | P4 |
| `consensus/parlia/parlia.go:1186-1207,1253-1279,1397,1506,1591,1594` | `SetExtraData` 承诺 + `Finalize` 校验 + 出块侧对称检查 + `UncleHash` 写入点 | P1/P5 |
| `core/systemcontracts/const.go:22`、`upgrade.go:1228-1230` | 新合约地址 + 升级注册 | P5 |
| `consensus/parlia/parlia.go:104-119,247,280-311` | `systemContracts` map + Snapshot LRU + ABI | P5 |
| `consensus/parlia/snapshot.go:47-49` | Snapshot 扩展（params + list）+ 向后兼容默认值 | P5 |
| `internal/ethapi/api.go:1628,1638`、`eth/gasprice/feehistory.go:108` | RPC 暴露 | RPC |

### 只读（关键事实来源）

| 文件:行 | 事实 |
|---|---|
| `consensus/parlia/parlia.go:1872` | `UncleHash` 在 seal 签名编码列表内 → 承诺自动被认证 |
| `consensus/parlia/parlia.go:927` | `VerifyUncles` 在 body 上强制 uncle 为空 → 放松 `block_validator.go:67` 的安全依据 |
| `consensus/parlia/parlia.go:2198` | 系统交易 gas 通过 `*usedGas` 体外注入 `header.GasUsed` |
| `consensus/parlia/parlia.go:2099` | 系统 message `GasLimit = MaxUint64/2`，完全绕开 GasPool |
| `consensus/parlia/parlia.go:1365-1383` | `updateValidatorTxGas ≈ 12,160,000`（`SystemTxsGasHardLimit = 20M` 的由来） |
| `core/state_transition.go:679-685` | `IsAmsterdam` 分支决定 `Used()` 是否 refund-exclusive |
| `core/state_transition.go:636-641,787` | 7702 authorization 在顶层调用**之前**安装，无 per-tx 撤销 |
| `core/state_transition.go:457` | `MaxTxGas` 门控 = `isOsaka && !isAmsterdam`（主网 Osaka 已激活） |
| `core/state_transition.go:663-674` | EIP-7623 floor（revert 也照收） |
| `core/vm/evm.go:313,337-341` | `isPrecompile` 先于 code 解析；非 revert error → `gas.Exhaust()` |
| `core/state/statedb.go:415-421` | **不存在的账户 `GetCodeHash` 返回零 hash，不是 `EmptyCodeHash`** |
| `core/state/statedb.go:628` | `getStateObject` 无条件污染 EIP-7928 BAL |
| `core/state/statedb.go:641-645` | `setError` 后 return nil → 静默「账户不存在」 |
| `core/state/database_mpt.go:82-90` | NoTries 下跳过 trie reader，account 只能来自 snapshot |
| `core/state/snapshot/disklayer.go:121` | `ErrNotCoveredYet` |
| `params/protocol_params.go:30,31,38,39,40,41,100,101,103` | `PayBidTxGasLimit=25000`、`MaxTxGas=1<<24`、`CallNewAccountGas=25000`、`TxGas=21000`、`SystemTxsGasHardLimit=20M`、`SystemTxsGasSoftLimit=1M`、`TxTokenPerNonZeroByte=4`、`TxCostFloorPerToken=10`、`TxAccessListAddressGas=2400` |
| `params/config.go:248-249` | 主网 Pascal/Prague = 1742436600（2025-03-20）→ **7702 与 7623 都已激活** |
| `params/config.go:253-254` | 主网 Osaka/Mendel = 1777343400（2026-04-28）→ **`MaxTxGas` 已生效** |
| `params/config.go:258,312` | `AmsterdamTime = nil`（两网） |
| `core/rawdb/ancient_scheme.go:47-55` | header/body/receipt 表均 `prunable: true` |
| `core/blockchain.go:885-890` | BSC 的 `HistoryPruningCutoff()` 永远返回 `(0, genesisHash)` |
| `eth/downloader/downloader.go:1694-1709`、`core/blockchain.go:1370-1405` | snap sync 的 `commitPivotBlock` / `SnapSyncComplete` |
| `core/types/receipt.go:388-415` | 逐笔 `GasUsed` 由 `CumulativeGasUsed` 差分重建 |
| `core/vm/contracts.go:405-412` | BSC 自有预编译 `0x64`–`0x69` |
| `core/types/tx_setcode.go:34,199`、`tx_blob.go:296` | `DelegationPrefix`；SetCodeTx/BlobTx 的 `To` 恒非 nil |
| `miner/bid_block.go:265,278,293` | `mux.Post`（签名广播）在 `InsertChain` **之前**；revoke 路径 |

---

*本报告的所有代码锚点基于 `payment-lane` 分支 `ece8248c4`，由六个独立角度的并行代码尽调交叉核对；BEP-696 的状态与内容经 GitHub API 核实。量化数字（EIP-7623 floor、`MaxTxGas`、breathe 块系统 gas、治理延迟）均可由附录 B 的只读锚点直接复核。*
