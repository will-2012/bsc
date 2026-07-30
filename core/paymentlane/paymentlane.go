// Package paymentlane 实现 BEP-703 的 gas 记账规则。
//
// 规则本体是每个区块一条不等式：
//
//	systemGasUsed + generalGasUsed + max(paymentGasUsed, laneSize) <= GasLimit
//
// 配额是地板不是天花板：超出配额的 payment gas 与 general 流量在同等条件下
// 竞争剩余空间。未被用掉的配额空转而不回收 —— 若缺额回流给 general，排除
// payment 交易对出块者就零成本，地板恰在最需要它的拥堵时刻变成虚设。
//
// 配额与两个 gas 桶承诺进 header.UncleHash。Parlia 不允许
// uncle，该字段恒为 EmptyUncleHash、从不被读取；BEP-696（已 MERGED）正是
// 「复用 UncleHash 作通用元数据」并已批准配套的兼容性声明：客户端必须接受
// 该字段的任意 32 字节值，uncle 为空改由 body 校验、不得从这个 hash 反推。
//
// 承诺进 header 把递推深度压到 1 —— 节点只需要父 header，于是 snap sync、
// reorg、重启、历史裁剪四个问题一次性消失；而且 UncleHash 本来就在 Parlia
// 的 seal hash 里，认证是白送的。
//
// # UncleHash 复用还需要放开的地方（本包与出块流程之外，尚未改）
//
// 凡是「从 body 的 uncle 列表反推 header.UncleHash」的代码，激活后都必须
// fork-gate，否则带承诺的块无法导入、无法传播、无法同步：
//
//	core/block_validator.go   ValidateBody 的 CalcUncleHash 相等检查（已改）
//	eth/protocols/eth/handlers.go   handleNewBlock 会静默丢弃广播来的块
//	eth/fetcher/block_fetcher.go    body 永远配不上 header
//	eth/downloader/queue.go         full sync 每块 errInvalidBody 并掉 peer
//
// 「uncle 必须为空」这条语义本身由 body 侧独立强制（Parlia 的 VerifyUncles
// 检查 len(block.Uncles()) > 0），所以放开哈希相等检查不会放开 uncle。
//
// 本文件里的 Enabled / Classify / Quota 三个是 mock，见下方注释。
package paymentlane

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"math/bits"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
)

// Class 是一笔交易的车道类别。
//
// Parlia 系统交易不走这套分类：它们被 core/state_processor.go 从用户交易
// 循环里剥离，gas 由 Finalize 直接累加进 header.GasUsed，在不等式里单列为
// systemGasUsed。
//
// 只有两个取值。「尚未分类」不是一个状态 —— 调用方在进入记账之前必须已经
// 定好类别，所以 Account 没有失败模式。
type Class uint8

const (
	// ClassGeneral 必须是零值：Class 的默认值落到这里才是安全的。
	ClassGeneral Class = iota
	ClassPayment
)

var (
	ErrViolated           = errors.New("payment lane inequality violated")
	ErrBucketMismatch     = errors.New("payment lane buckets do not sum to gas used")
	ErrBucketOverflow     = errors.New("payment lane buckets exceed header gas used")
	ErrQuotaMismatch      = errors.New("payment lane quota does not match parent derivation")
	ErrBadCommitment      = errors.New("payment lane commitment is malformed")
	ErrCommitmentUntruthy = errors.New("payment lane commitment does not match replayed buckets")
)

// ---------------------------------------------------------------------------
// mock 区。
//
// 下面三个是本地版本的接缝，实现代码全部按它们的签名写成。做成包级 var 是
// 为了让测试能替换。真实实现的归属：
//
//   - Enabled  -> params.ChainConfig.IsPaymentLane(number, time)，一个新的
//     时间戳硬分叉门。这里默认返回 false，所以 mock 不动时全部车道路径都是
//     死代码，行为与改动前逐字一致（这是零回归的依据）。
//
//   - Classify -> §3.1 的分类器。三个要点：
//     (a) 必须用绑定 parent.Root 的只读 state reader（真实签名要多收一个
//     reader 参数）—— 用正在推进的 statedb 会让「同块内先部署 code 再转账
//     过去」漂移，出块者可以用一笔便宜交易决定哪些用户的转账进车道；
//     (b) 「无 code」判据是 codeHash == keccak256("")，且不存在的账户视为
//     无 code（statedb.GetCodeHash 对不存在账户返回零 hash 而不是
//     EmptyCodeHash，写错会把每一笔转向全新地址的转账判成 general —— 恰好
//     是车道最核心的用例）；
//     (c) 廉价判据必须排在 state 读之前（tx type 白名单、accessList 为空、
//     To != nil、len(Data) == 0），否则打包尾部逐个淘汰 general 账户会退化
//     成每账户一次 trie 查询。tx type 白名单只放行 0x00/0x01/0x02，所以
//     BlobTx 与 SetCodeTx 天然是 general，出块侧不需要特判。
//
//   - Quota    -> §3.3/§3.4 的配额推进：读父 header 的承诺拿到 laneSize(n-1)
//     与 signal，走一步得出本块配额。
//     EXPAND_STEP/SHRINK_STEP 只是每块的增量，给不出「现在在哪」—— 配额是从
//     激活块起所有 ±step 的累积和，不是 signal 的函数，所以它必须被显式承诺。
//     这个累加器不是实现产物，是 §3.3 三分支结构逼出来的：滞后带那一支要求
//     配额「保持不变」，不变于什么？不变于上一块的值 —— 这就是记忆。想彻底
//     去掉它只能把配额改成 signal 的无记忆函数，那要放弃 §3.5 的滞后带与
//     快涨慢跌，而 §4 称这两条是 hard requirement。
//
//     递推跑在 gas 空间上（而 BEP §3.3 的字面表述是对 ratio ±一步）。两者在
//     GasLimit 恒定时完全等价，前提是**步长必须按当前 GasLimit 折算**：
//     stepGas = EXPAND_STEP * header.GasLimit / 10000。写成固定 gas 步长会让
//     扩张的相对速度随链容量增长而悄悄变慢，那正是 §4 说 ratio 设计要避免的。
//     GasLimit 变动时两者会短暂分歧（gas 空间要多花约一个块爬回去），但边界
//     钳制会把它拉回同一稳态。这是一处对 BEP 文本的偏离，需要与作者确认。
//
//     Quota 必须自己完成全部钳制（比例边界与绝对边界的交集），且只能用
//     (parent, header) 里的共识可见量 —— 出块侧绝不能再钳一次，见 Budget。
// ---------------------------------------------------------------------------

// Enabled 报告车道规则是否对该区块生效。
var Enabled = func(config *params.ChainConfig, number *big.Int, time uint64) bool {
	return false
}

// Classify 返回一笔交易的车道类别。
var Classify = func(tx *types.Transaction) Class {
	return ClassGeneral
}

// Quota 返回 header 这一块的车道配额。它同时是打包用的预算和要承诺进 header
// 的递推态 —— 一个值两个用途，所以不存在「两个派生值必须一致」这类隐含关系。
var Quota = func(parent, header *types.Header) uint64 {
	return 0
}

// ---------------------------------------------------------------------------
// header 承诺，承载于 UncleHash。
//
//	[0:8]   laneSize        uint64 big-endian
//	[8:16]  generalGasUsed  uint64 big-endian
//	[16:24] paymentGasUsed  uint64 big-endian
//	[24]    版本，恒为 commitVersion
//	[25:32] 保留，必须为零
//
// 承诺的是配额本身（gas），不是它的 ratio。这样递推就直接跑在 gas 空间上：
// laneSize(n) = clamp(laneSize(n-1) ± step)。好处是承诺的值恰好就是矿工本来
// 就要用来打包的那个量 —— 不需要在 environment 上多存一个只为承诺而存在的
// 字段，也不存在「配额与 ratio 两个派生值必须一致」这条要人守的关系。
//
// systemGasUsed 由 header.GasUsed - general - payment 派生（走 DeriveSystemGas）。
// 存两个显式桶而不是「一个桶 + 派生另一个」，是为了让 §3.3 的 signal 分子
// 干净：Parlia 的 updateValidatorSetV2 在每个 breathe 块烧约 12.16M gas
// （70M 块的 17.4pp），若并入 general 会每天误触发一次配额扩张。
// ---------------------------------------------------------------------------

// commitVersion 让 Encode 的值域永不包含全零 hash。
//
// 这不是为了将来扩展，而是一条正确性要求：core/types/block.go 的 NewBlock 用
// 「UncleHash 是零值」判断调用方有没有写过该字段。而 Commitment{0,0,0} 恰好
// 编码成全零 —— 配额为 0 的空块，分叉激活初期最常见的形状。
// 没有版本位时它会被 NewBlock 覆写成 EmptyUncleHash，再被导入侧 Decode 拒掉，
// 表现为间歇性 BAD_BLOCK。
const commitVersion byte = 1

type Commitment struct {
	LaneSize       uint64
	GeneralGasUsed uint64
	PaymentGasUsed uint64
}

func Encode(c Commitment) common.Hash {
	var h common.Hash
	binary.BigEndian.PutUint64(h[0:8], c.LaneSize)
	binary.BigEndian.PutUint64(h[8:16], c.GeneralGasUsed)
	binary.BigEndian.PutUint64(h[16:24], c.PaymentGasUsed)
	h[24] = commitVersion
	return h
}

// Decode 校验版本位与保留位。缺了这两条，EmptyUncleHash 或将来 BEP-696 的
// 元数据都会被静默当成车道记账读出来。
func Decode(h common.Hash) (Commitment, error) {
	if h[24] != commitVersion {
		return Commitment{}, fmt.Errorf("%w: version %d", ErrBadCommitment, h[24])
	}
	for _, b := range h[25:32] {
		if b != 0 {
			return Commitment{}, fmt.Errorf("%w: reserved bits set", ErrBadCommitment)
		}
	}
	return Commitment{
		LaneSize:       binary.BigEndian.Uint64(h[0:8]),
		GeneralGasUsed: binary.BigEndian.Uint64(h[8:16]),
		PaymentGasUsed: binary.BigEndian.Uint64(h[16:24]),
	}, nil
}

// ---------------------------------------------------------------------------
// 规则本体。
// ---------------------------------------------------------------------------

// CheckInequality 是 §3.2 的区块有效性规则，也是唯一的判据来源：出块前自检、
// bid block 准入门、区块导入校验全部调它，这样三处不可能走偏。
//
// 三项相加必须防溢出。出块侧的输入都受 GasLimit 约束（约 1e8，够不着 2^64），
// 但 bid block 准入侧的两个桶直接来自 header.UncleHash 的 32 个字节 —— 对
// builder 而言那是完全可控的输入。溢出会让和绕回一个小数值从而**通过**检查，
// 把「签名前拒掉违规 bid」这道门整个废掉。
func CheckInequality(gasLimit, systemGasUsed, generalGasUsed, paymentGasUsed, laneSize uint64) error {
	sum, carry := bits.Add64(systemGasUsed, generalGasUsed, 0)
	if carry == 0 {
		sum, carry = bits.Add64(sum, max(paymentGasUsed, laneSize), 0)
	}
	if carry != 0 || sum > gasLimit {
		return fmt.Errorf("%w: system %d general %d payment %d lane %d limit %d",
			ErrViolated, systemGasUsed, generalGasUsed, paymentGasUsed, laneSize, gasLimit)
	}
	return nil
}

// DeriveSystemGas 从 header.GasUsed 与两个承诺桶反推 systemGasUsed。
//
// 溢出在这里是可达的攻击面：general = 2^64-1 且 payment = 1 时两者之和绕回 0，
// 通过朴素的 `general+payment > headerGasUsed` 检查，随后的减法又把 system
// 反推成 headerGasUsed 本身，三道检查一起被绕过。所有从 header 反推桶的调用方
// 都必须走这个函数，不要自己写减法。
func DeriveSystemGas(headerGasUsed, generalGasUsed, paymentGasUsed uint64) (uint64, error) {
	sum, carry := bits.Add64(generalGasUsed, paymentGasUsed, 0)
	if carry != 0 || sum > headerGasUsed {
		return 0, fmt.Errorf("%w: general %d payment %d header gas used %d",
			ErrBucketOverflow, generalGasUsed, paymentGasUsed, headerGasUsed)
	}
	return headerGasUsed - sum, nil
}

// ---------------------------------------------------------------------------
// 打包侧的准入代数。
// ---------------------------------------------------------------------------

// Budget 携带两个类别桶，并把 §3.2 的不等式表达成准入谓词。
//
// 为什么不用「双 gas pool」：把预算静态切成 general = C-L 与 payment = L 两个
// 池，等价于装箱问题，而首次适应不是最优 —— 会拒绝规则本来允许的区块。反例见
// TestInvariantAdmissionBeatsStaticPools（bid 场景下交易集顺序由 builder 给定
// 不可重排，那个差别直接表现为整个 bid 被拒）。
//
// 这里刻意**不存容量**。容量的唯一权威是 gasPool 自己（调用方把
// gasPool.Gas() 作为 shared 传进来），存第二份只会引入一条必须靠人维护的
// 相等关系。同理，出块侧也不做任何 laneSize 的钳制：LaneSize 必须直接是
// Size(parent, header) 的返回值。钳制看着像保护，实则是共识分歧 —— 出块侧
// 唯一能用来钳的量是 gasReserved，而它是矿工本地的启发式上界（普通块 1M、
// breathe 块 20M），验证方看不到。矿工钳了验证方没钳 ⇒ IdleLane 偏小 ⇒
// general 超打包 ⇒ 无效块，而自检用的是同一个被钳的 L，抓不到。
// 配额确实大于可用预算时，Headroom 会把 general 压到 0（只能打包 payment），
// Verify 再决定这个块能不能出 —— 全程无分歧。
type Budget struct {
	LaneSize    uint64 // 本块配额，即 Size(parent, header)
	PaymentUsed uint64
	GeneralUsed uint64
}

// IdleLane 是配额中尚未被 payment 流量填掉的部分。general 交易必须让出它，
// payment 交易不必。
//
// 它的物理意义由这条恒等式给出（对任意无符号 p、L 成立）：
//
//	max(paymentUsed, laneSize) ≡ paymentUsed + IdleLane
//
// 代进 §3.2 的不等式，规则就是
//
//	system + general + payment + IdleLane <= GasLimit
//
// 也就是说 IdleLane 是一笔虚拟交易的 gas 消耗：它像真交易一样占据区块空间、
// 参与容量竞争，却不产生任何手续费，存在的唯一目的是替还没到来的支付流量把门
// 顶住。BEP 里的 max() 只是这件事的另一种写法，而 Headroom 里的减法就是让
// general 交易和这笔虚拟交易挤在同一个块里。
//
// 打包过程中它是「预留此刻还剩多少约束力」：每来一笔 payment 交易真正花掉车道
// 里的 gas，被扣着的量就一对一缩小 —— 那部分已从「为支付预留」变成「被支付
// 用掉」。封块时它是这个块浪费掉的容量上界（§3.2 明定未用配额空转不回收），
// 说上界是因为只有确实有 general 交易在等这块空间时损失才真实发生。
func (b Budget) IdleLane() uint64 { return satSub(b.LaneSize, b.PaymentUsed) }

// Headroom 返回某类别交易当前允许的最大 gas 上限。shared 是共享余量，出块侧
// 传 gasPool.Gas() —— 这样 bid 路径上 SubGas(PayBidTxGasLimit) 那 25000 的
// 临时预留天然被尊重。
//
// 正确性（记 gu/pu 为两桶实耗、C 为池容量、L 为 LaneSize）：
//
//	general 准入 ⟺ g <= shared - max(L-pu, 0)
//	payment 准入 ⟺ p <= shared
//
//	情形 pu_final >= L：每次准入都要求该笔 gas 上限 <= shared，即准入后
//	  gu+pu <= C；实耗 <= 上限，故 gu+max(pu,L) = gu+pu <= C ✓
//	情形 pu_final <  L：全程 pu <= pu_final < L，故每次 general 准入展开为
//	  g <= C-(gu+pu)-(L-pu) = C-gu-L，即准入后 gu+L <= C；payment 准入不改
//	  变 gu，取最后一次 general 准入即得 gu_final+L <= C ✓
//
// 两个 Headroom 都单调不增（gu、pu、max(pu,L) 均单调不减），由此得到三条：
//
//	(1) 每个前缀都是合法区块 —— commitTransactions 可以在任意一轮被
//	    interruptCh 打断并把半成品交给共识引擎，半成品必须有效；
//	(2) 「现在装不下」⟺「永远装不下」—— 这是 transactionsByPriceAndNonce.Pop()
//	    （永久丢弃该账户）的正确性根据；
//	(3) L <= C 时 shared >= IdleLane 全程成立，即 general 偷不走车道空间，
//	    配额到打包结束时一定还在。
//
// 另外 general 谓词严格更严（只多减一个非负量），所以「两类都装不下 TxGas」
// ⟺「shared < TxGas」—— 出块侧的循环终止判据因此一个字都不用改。
func (b Budget) Headroom(shared uint64, class Class) uint64 {
	if class == ClassPayment {
		return shared
	}
	return satSub(shared, b.IdleLane())
}

func (b Budget) Admits(shared uint64, class Class, gasLimit uint64) bool {
	return gasLimit <= b.Headroom(shared, class)
}

// Account 把一笔交易的 gas 记到对应的桶。
//
// delta 必须由调用方从 gasPool.Used() 差分得出，而不是取 receipt.GasUsed。
// 理由是三个口径今天恰好相等、测试全绿，但 core/state_transition.go 的
// IsAmsterdam 分支会让它们漂移（receipt.GasUsed 扣退款、MaxUsedGas 不扣、
// gasPool.Used() 视分叉而定）。差分法让 PaymentUsed+GeneralUsed ≡ Used()
// 成为构造性事实而非人工约定；而且执行失败时 gasPool.Set(snapshot) 已把池
// 恢复，delta 自然为 0，两个桶不需要单独的回滚路径。
func (b *Budget) Account(class Class, delta uint64) {
	if class == ClassPayment {
		b.PaymentUsed += delta
		return
	}
	b.GeneralUsed += delta
}

// Verify 是打包结束后的自检，判据与 CheckInequality 一致。
//
// 两条检查的价值不一样。不等式那条只在「配额本身大于本块可用预算」时才可能
// 失败（Size 给出了一个这个块装不下的配额），此时放弃出块是唯一正确的响应 ——
// 无论如何都不存在合法块，而参数得靠治理去修。桶之和那条才是这个断言的主要
// 目标：它守的是「每次 apply 之后都记了账」这条纪律，而纪律是靠人维护的。
//
// systemGasUsed 传 gasReserved（矿工的上界估计）：真实系统交易 gas <= 它，
// 所以这里过了导入侧一定过。
func (b Budget) Verify(gasLimit, systemGasUsed, poolUsed uint64) error {
	if b.PaymentUsed+b.GeneralUsed != poolUsed {
		return fmt.Errorf("%w: payment %d general %d pool %d",
			ErrBucketMismatch, b.PaymentUsed, b.GeneralUsed, poolUsed)
	}
	return CheckInequality(gasLimit, systemGasUsed, b.GeneralUsed, b.PaymentUsed, b.LaneSize)
}

// VerifyCommitment 是导入侧的校验，也是整套规则**唯一的权威强制点**。
//
// 出块侧的 Verify 只能自证：矿工用自己算的桶对自己的承诺，作恶者换一组自洽的
// 假数就能通过（bid block 准入门同理，见 miner/bid_simulator.go）。这里的桶是
// 本地重放执行得出的，与 header 承诺逐字比对之后，说谎的块拿不到 canonical
// 地位 —— 这条检查一旦缺失，「验证块符合规则」就完全不成立，无论出块侧写得
// 多严密。
//
// 参数口径必须与出块侧同源，否则诚实的块会被拒：
//
//	poolUsed      = gp.Used()，只含用户交易；两侧都用同一个 GasPool 类型的
//	                同一个方法，池的初值不同（矿工扣了 gasReserved）但差分相消。
//	systemGasUsed = Finalize 之后的 header.GasUsed 减去上面那个 poolUsed，
//	                因为 Parlia 系统交易不走 gasPool、由 Finalize 直接累加。
//	                导入侧拿到的是**真实值**，不需要 DeriveSystemGas 那个从
//	                builder 可控字节反推的减法。
//
// 不比较 LaneSize：它是 header 字段，真实性由 parlia.verifyCascadingFields 对
// Quota(parent, header) 核，而 Process 从不重复校验任何 header 字段。
func (b Budget) VerifyCommitment(gasLimit, systemGasUsed, poolUsed uint64, c Commitment) error {
	if b.GeneralUsed != c.GeneralGasUsed || b.PaymentUsed != c.PaymentGasUsed {
		return fmt.Errorf("%w: committed general %d payment %d, replayed general %d payment %d",
			ErrCommitmentUntruthy, c.GeneralGasUsed, c.PaymentGasUsed, b.GeneralUsed, b.PaymentUsed)
	}
	return b.Verify(gasLimit, systemGasUsed, poolUsed)
}

// satSub 是饱和减法；车道算术全是无符号的。
func satSub(a, b uint64) uint64 {
	if a < b {
		return 0
	}
	return a - b
}
