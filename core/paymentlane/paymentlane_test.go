package paymentlane

import (
	"errors"
	"math"
	"math/rand"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// ---------------------------------------------------------------------------
// 共享脚手架：用一个裸预算复刻出块侧的打包循环。
//
// miner 侧的 parlia 打包链路在单测里跑不起来（worker_test.go 的引擎 switch 只认
// clique/ethash，唯一的 parlia 测试是 t.Skip），所以准入代数与不变式全部在这里守。
// ---------------------------------------------------------------------------

type txSpec struct {
	class  Class
	limit  uint64 // 准入用 gas 上限
	actual uint64 // 记账用实耗（<= limit，模拟退款）
}

// laneRun 按顺序把 seq 喂给准入谓词，装不下就跳过（对应 txs.Pop()）。
// reserve 复刻 bid 路径的 SubGas(PayBidTxGasLimit)：一份不属于任何桶的外部预留。
// 每一步都断言四条不变式，返回被接受的下标与终态。
func laneRun(t *testing.T, capacity, laneSize, reserve uint64, seq []txSpec) ([]int, Budget) {
	t.Helper()
	b := Budget{LaneSize: laneSize}
	shared := func() uint64 { return satSub(capacity, b.PaymentUsed+b.GeneralUsed+reserve) }

	prev := map[Class]uint64{ClassGeneral: math.MaxUint64, ClassPayment: math.MaxUint64}
	var taken []int
	for i, tx := range seq {
		if !b.Admits(shared(), tx.class, tx.limit) {
			continue
		}
		b.Account(tx.class, tx.actual)
		taken = append(taken, i)

		// (1) 桶之和 == 池消耗
		if b.PaymentUsed+b.GeneralUsed != capacity-reserve-shared() {
			t.Fatalf("第 %d 笔后桶与池不一致: p=%d g=%d shared=%d",
				i, b.PaymentUsed, b.GeneralUsed, shared())
		}
		// (2) 每个前缀都是合法区块 —— commitTransactions 可在任意一轮被
		//     interruptCh 打断并把半成品交给共识引擎。
		//
		// 前提是配额本身装得下：L > capacity 时不存在任何合法块（连空块都
		// 不合法），这条不变式在那种配置下是空的，由 sealLane 的自检兜底。
		if laneSize+reserve <= capacity {
			if err := CheckInequality(capacity, 0, b.GeneralUsed, b.PaymentUsed, laneSize); err != nil {
				t.Fatalf("第 %d 笔后前缀非法: %v", i, err)
			}
		}
		// (3) 两个 headroom 单调不增 —— Pop() 永久丢弃账户的正确性根据
		for _, c := range []Class{ClassGeneral, ClassPayment} {
			if h := b.Headroom(shared(), c); h > prev[c] {
				t.Fatalf("第 %d 笔后 class=%d 的 headroom 回升 %d -> %d，Pop() 不再成立",
					i, c, prev[c], h)
			}
			prev[c] = b.Headroom(shared(), c)
		}
		// (4) L+reserve <= capacity 时 general 偷不走车道空间
		if laneSize+reserve <= capacity && shared() < b.IdleLane() {
			t.Fatalf("第 %d 笔后 shared(%d) < IdleLane(%d)，车道空间被侵占",
				i, shared(), b.IdleLane())
		}
	}
	return taken, b
}

// ---------------------------------------------------------------------------
// 准入代数
// ---------------------------------------------------------------------------

// TestAdmissionInvariants 是打包循环全部安全性论证的落地：随机序列 + 逐步断言。
func TestAdmissionInvariants(t *testing.T) {
	const capacity = 1000
	for seed := int64(0); seed < 3000; seed++ {
		rng := rand.New(rand.NewSource(seed))
		// 覆盖 laneSize 的两个退化端点 0 与 capacity
		laneSize := uint64(rng.Intn(capacity + 1))
		seq := make([]txSpec, 200)
		for i := range seq {
			limit := uint64(1 + rng.Intn(300))
			seq[i] = txSpec{
				class:  Class(rng.Intn(2)),
				limit:  limit,
				actual: uint64(rng.Intn(int(limit) + 1)),
			}
		}
		laneRun(t, capacity, laneSize, 0, seq)
	}
}

// TestAdmissionIsExactlyTight 穷举证明 Admits 与「这一笔烧满上限后区块仍合法」
// 逐位等价。
//
// TestAdmissionInvariants 只证明了没有假接受（会产出无效块）；这里补的是没有
// 假拒收 —— 而假拒收不报任何错，只表现为 validator 收入下降，是更难发现的那半。
func TestAdmissionIsExactlyTight(t *testing.T) {
	const capacity = 40
	for laneSize := uint64(0); laneSize <= capacity; laneSize += 7 {
		for pu := uint64(0); pu <= capacity; pu += 3 {
			for gu := uint64(0); gu+pu <= capacity; gu += 3 {
				b := Budget{LaneSize: laneSize, PaymentUsed: pu, GeneralUsed: gu}
				// 只枚举可达状态：前置状态本身非法时谈「谓词是否精确」没有意义。
				// 例如 L=7 时 gu=36 根本达不到 —— general 准入把 gu 卡在 C-L=33。
				if CheckInequality(capacity, 0, gu, pu, laneSize) != nil {
					continue
				}
				shared := capacity - pu - gu
				for _, class := range []Class{ClassGeneral, ClassPayment} {
					for g := uint64(0); g <= capacity; g++ {
						after := b
						after.Account(class, g)
						legal := CheckInequality(capacity, 0,
							after.GeneralUsed, after.PaymentUsed, laneSize) == nil
						if got := b.Admits(shared, class, g); got != legal {
							t.Fatalf("L=%d pu=%d gu=%d class=%d g=%d: Admits=%v 但烧满后合法=%v",
								laneSize, pu, gu, class, g, got, legal)
						}
					}
				}
			}
		}
	}
}

// TestGeneralHeadroomFlatBelowLane 守住「配额是地板」在准入代数上的体现：
// payment 在配额内增长完全不挤压 general，越过配额后才开始逐 gas 竞争。
func TestGeneralHeadroomFlatBelowLane(t *testing.T) {
	const capacity, laneSize = 1000, 300
	for _, pu := range []uint64{0, 1, laneSize / 2, laneSize - 1, laneSize} {
		b := Budget{LaneSize: laneSize, PaymentUsed: pu}
		if got, want := b.Headroom(capacity-pu, ClassGeneral), uint64(capacity-laneSize); got != want {
			t.Errorf("pu=%d（配额内）general headroom = %d, 期望恒为 %d", pu, got, want)
		}
	}
	// 越过配额之后，每多烧 1 gas 就从 general 身上拿走 1 gas
	for _, over := range []uint64{1, 2, 100} {
		pu := uint64(laneSize) + over
		b := Budget{LaneSize: laneSize, PaymentUsed: pu}
		if got, want := b.Headroom(capacity-pu, ClassGeneral), capacity-pu; got != want {
			t.Errorf("pu=L+%d general headroom = %d, 期望 %d", over, got, want)
		}
	}
}

// TestIdleLaneBoundaries 覆盖 IdleLane 的端点，特别是 IdleLane > shared ——
// 那时饱和减法必须把 headroom 兜到 0，裸减法会下溢到 2^64 附近并让谓词失效。
func TestIdleLaneBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		laneSize, pu, shared  uint64
		wantIdle, wantGeneral uint64
	}{
		{"配额恰好填满，general 拿回全部共享余量", 100, 100, 500, 0, 500},
		{"配额只差 1 gas", 100, 99, 500, 1, 499},
		{"超填不会让 IdleLane 环绕", 100, 150, 500, 0, 500},
		{"IdleLane 恰好等于共享余量，general 一笔进不来", 100, 0, 100, 100, 0},
		{"IdleLane 大于共享余量，饱和到 0", 900, 0, 100, 900, 0},
		{"配额为 0，车道退化", 0, 0, 500, 0, 500},
	} {
		b := Budget{LaneSize: tc.laneSize, PaymentUsed: tc.pu}
		if got := b.IdleLane(); got != tc.wantIdle {
			t.Errorf("%s: IdleLane = %d, 期望 %d", tc.name, got, tc.wantIdle)
		}
		if got := b.Headroom(tc.shared, ClassGeneral); got != tc.wantGeneral {
			t.Errorf("%s: general headroom = %d, 期望 %d", tc.name, got, tc.wantGeneral)
		}
		if got := b.Headroom(tc.shared, ClassPayment); got != tc.shared {
			t.Errorf("%s: payment headroom = %d, 期望等于共享余量 %d", tc.name, got, tc.shared)
		}
	}
}

// TestPaymentPredicateIsTheLooserOne 守住出块侧那条**没有改动**的循环终止判据。
//
// worker.go 沿用上游的 `gasPool.Gas() < params.TxGas`，其正确性依赖
// Headroom(general) <= Headroom(payment) 恒成立 —— 于是「两类都装不下 TxGas」
// 等价于「共享余量 < TxGas」。这条一旦被破坏，终止判据就必须跟着改。
func TestPaymentPredicateIsTheLooserOne(t *testing.T) {
	for laneSize := uint64(0); laneSize <= 200; laneSize += 13 {
		for pu := uint64(0); pu <= 200; pu += 11 {
			for shared := uint64(0); shared <= 200; shared += 7 {
				b := Budget{LaneSize: laneSize, PaymentUsed: pu}
				if b.Headroom(shared, ClassGeneral) > b.Headroom(shared, ClassPayment) {
					t.Fatalf("L=%d pu=%d shared=%d: general headroom 反而更宽",
						laneSize, pu, shared)
				}
			}
		}
	}
}

// TestLaneSizeExceedsCapacity 覆盖「配额大于本块可用预算」。
//
// 出块侧刻意不做钳制：唯一可用来钳的量是矿工本地的 gasReserved，验证方看不到
// 它，钳了就是共识分歧。所以这里必须成立两件事 —— general 被完全挤出，
// 且自检能判定这个块到底能不能出。
func TestLaneSizeExceedsCapacity(t *testing.T) {
	const capacity = 1000
	taken, b := laneRun(t, capacity, capacity+1, 0, []txSpec{
		{ClassGeneral, 1, 1},
		{ClassPayment, 500, 500},
		{ClassGeneral, 1, 1},
	})
	if len(taken) != 1 || taken[0] != 1 {
		t.Fatalf("期望只有 payment 交易被接受，实际接受下标 %v", taken)
	}
	// 自检把容量当 gasLimit：配额装不下 ⇒ 拒绝出块（放弃槽位而不是出坏块）
	if err := b.Verify(capacity, 0, b.PaymentUsed+b.GeneralUsed); !errors.Is(err, ErrViolated) {
		t.Fatalf("配额大于容量时自检应报 ErrViolated，实际 %v", err)
	}
	// 配额恰好等于容量：空块仍然合法
	if err := (Budget{LaneSize: capacity}).Verify(capacity, 0, 0); err != nil {
		t.Fatalf("配额恰好等于容量时空块应合法: %v", err)
	}
}

// TestPayBidTxAlwaysFitsAfterLaneAdmission 守住 bid 路径的代数闭合：循环期间
// SubGas(PayBidTxGasLimit) 的预留保证 payBidTx 在 AddGas 之后一定装得下。
//
// payBidTx 被强制归 general（否则 MEV 回扣会搭上保障普通转账的车道），
// 所以它要的是 general headroom。
func TestPayBidTxAlwaysFitsAfterLaneAdmission(t *testing.T) {
	const capacity, payBidTxGas = 1000, 25
	for seed := int64(0); seed < 2000; seed++ {
		rng := rand.New(rand.NewSource(seed))
		// Size 必须自己给 payBidTx 留出这一份，见下方阈值断言
		laneSize := uint64(rng.Intn(capacity - payBidTxGas + 1))
		seq := make([]txSpec, 100)
		for i := range seq {
			limit := uint64(1 + rng.Intn(200))
			seq[i] = txSpec{Class(rng.Intn(2)), limit, uint64(rng.Intn(int(limit) + 1))}
		}
		_, b := laneRun(t, capacity, laneSize, payBidTxGas, seq)

		// AddGas 归还预留之后
		shared := capacity - b.PaymentUsed - b.GeneralUsed
		if h := b.Headroom(shared, ClassGeneral); h < payBidTxGas {
			t.Fatalf("seed %d: L=%d 时 payBidTx 装不下，general headroom=%d", seed, laneSize, h)
		}
	}
	// 阈值精确落在 capacity - payBidTxGas
	b := Budget{LaneSize: capacity - payBidTxGas + 1}
	if b.Headroom(capacity, ClassGeneral) >= payBidTxGas {
		t.Fatal("配额越过 capacity-payBidTxGas 时 payBidTx 本应装不下")
	}
}

// TestPackingIsOrderSensitive 记录一个事实：同一批交易的不同到达顺序，能打包的
// 总量不同。这不是 bug（bid 顺序由 builder 给定、本地顺序由 tip 排序给定），
// 但任何「顺序无关所以可以换序计算」的推理都是错的。
func TestPackingIsOrderSensitive(t *testing.T) {
	const capacity, laneSize = 100, 50
	g := txSpec{ClassGeneral, 50, 50}
	p := txSpec{ClassPayment, 60, 60}

	_, paymentFirst := laneRun(t, capacity, laneSize, 0, []txSpec{p, g})
	_, generalFirst := laneRun(t, capacity, laneSize, 0, []txSpec{g, p})

	if got := paymentFirst.PaymentUsed + paymentFirst.GeneralUsed; got != 60 {
		t.Errorf("payment 先：总量 %d，期望 60", got)
	}
	if got := generalFirst.PaymentUsed + generalFirst.GeneralUsed; got != 50 {
		t.Errorf("general 先：总量 %d，期望 50", got)
	}
}

// TestInvariantAdmissionBeatsStaticPools 钉住选择「单池 + 不等式准入」而不是
// 「双 gas pool」的那个反例：静态分池等价装箱，首次适应会拒绝规则允许的区块。
func TestInvariantAdmissionBeatsStaticPools(t *testing.T) {
	const capacity, laneSize = 200, 100
	seq := []txSpec{
		{ClassPayment, 60, 60}, {ClassPayment, 50, 50},
		{ClassPayment, 50, 50}, {ClassGeneral, 40, 40},
	} // 总和 200，恰好等于容量

	// 双池贪心：60→payment(剩40)、50 装不下 payment→general(剩50)、
	//           50→general(剩0)、40 无处可去 → 失败
	paymentPool, generalPool := uint64(laneSize), uint64(capacity-laneSize)
	staticOK := true
	for _, tx := range seq {
		switch {
		case tx.class == ClassPayment && paymentPool >= tx.limit:
			paymentPool -= tx.limit
		case generalPool >= tx.limit:
			generalPool -= tx.limit
		default:
			staticOK = false
		}
	}
	if staticOK {
		t.Fatal("反例失效：双池贪心居然接受了这个序列，需要重新构造")
	}

	if taken, _ := laneRun(t, capacity, laneSize, 0, seq); len(taken) != len(seq) {
		t.Fatalf("不等式准入应全部接受，实际只接受了 %v", taken)
	}
}

// ---------------------------------------------------------------------------
// 规则本体
// ---------------------------------------------------------------------------

// TestLaneIsFloorNotCeiling 覆盖 §3.2 两个 regime 的边界，逐条对应规则文本。
func TestLaneIsFloorNotCeiling(t *testing.T) {
	const limit, lane = 100, 20
	for _, tc := range []struct {
		name             string
		general, payment uint64
		wantErr          bool
	}{
		{"payment 恰好填满配额（三项之和恰好等于 GasLimit）", 80, 20, false},
		{"payment 超出 1 gas，general 少 1 gas", 79, 21, false},
		{"payment 差 1 gas 并不会把空出的配额让给 general", 81, 19, true},
		{"没有支付需求时配额空转", 80, 0, false},
		{"general 拿不到空转的配额", 81, 0, true},
		{"配额是地板不是天花板，payment 可以占满整块", 0, 100, false},
	} {
		err := CheckInequality(limit, 0, tc.general, tc.payment, lane)
		if tc.wantErr && !errors.Is(err, ErrViolated) {
			t.Errorf("%s: 期望 ErrViolated，实际 %v", tc.name, err)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("%s: 期望合法，实际 %v", tc.name, err)
		}
	}
}

// TestOverflowIsNotAWayIn 守住 bid 准入门的溢出攻击面。
//
// bid block 的两个桶直接来自 header.UncleHash 的 32 字节，对 builder 而言完全
// 可控。朴素加减法会让 general=2^64-1 / payment=1 这一对绕回小数值从而**通过**
// 检查，把「签名前拒掉违规 bid」这道门整个废掉，攻击者由此恢复「零成本让指定
// validator 丢一个槽位」的能力。
func TestOverflowIsNotAWayIn(t *testing.T) {
	const gasLimit = 70_000_000
	maxU := uint64(math.MaxUint64)

	for _, tc := range []struct{ system, general, payment uint64 }{
		{maxU, 1, 0},
		{1, maxU, 0},
		{gasLimit, maxU, 1},
		{0, maxU/2 + 1, maxU/2 + 1},
	} {
		if err := CheckInequality(gasLimit, tc.system, tc.general, tc.payment, 0); !errors.Is(err, ErrViolated) {
			t.Errorf("CheckInequality(system=%d general=%d payment=%d) 应判违规，实际 %v",
				tc.system, tc.general, tc.payment, err)
		}
	}

	for _, tc := range []struct{ general, payment uint64 }{
		{maxU, 1},
		{maxU/2 + 1, maxU/2 + 1},
		{gasLimit + 1, 0},
	} {
		if _, err := DeriveSystemGas(gasLimit, tc.general, tc.payment); !errors.Is(err, ErrBucketOverflow) {
			t.Errorf("DeriveSystemGas(general=%d payment=%d) 应判桶溢出，实际 %v",
				tc.general, tc.payment, err)
		}
	}

	// 正常路径仍要能反推出正确的 system gas
	if got, err := DeriveSystemGas(1000, 600, 300); err != nil || got != 100 {
		t.Fatalf("DeriveSystemGas 正常路径 = (%d, %v)，期望 (100, nil)", got, err)
	}
}

// TestVerifyFailureTriggers 把「哪种编码错误 → 哪个 error」固定下来。
// 出块侧真踩到时日志里只有这一个 error，它必须能指向病因。
func TestVerifyFailureTriggers(t *testing.T) {
	for _, tc := range []struct {
		name             string
		b                Budget
		poolUsed, system uint64
		wantErr          error
	}{
		{"一致且合法", Budget{LaneSize: 20, PaymentUsed: 20, GeneralUsed: 60}, 80, 0, nil},
		{"桶之和小于池：漏记了一笔", Budget{LaneSize: 20, PaymentUsed: 20, GeneralUsed: 50}, 80, 0, ErrBucketMismatch},
		{"桶之和大于池：把外部预留算进了某一笔", Budget{LaneSize: 20, PaymentUsed: 20, GeneralUsed: 70}, 80, 0, ErrBucketMismatch},
		{"记账一致但配额装不下这个块", Budget{LaneSize: 200}, 0, 0, ErrViolated},
		{"系统交易预留把块挤爆", Budget{LaneSize: 20, PaymentUsed: 20, GeneralUsed: 60}, 80, 21, ErrViolated},
		{"两条都违反时先报桶之和（它才指向病因）", Budget{LaneSize: 200, PaymentUsed: 5}, 99, 0, ErrBucketMismatch},
	} {
		err := tc.b.Verify(100, tc.system, tc.poolUsed)
		switch {
		case tc.wantErr == nil && err != nil:
			t.Errorf("%s: 期望通过，实际 %v", tc.name, err)
		case tc.wantErr != nil && !errors.Is(err, tc.wantErr):
			t.Errorf("%s: 期望 %v，实际 %v", tc.name, tc.wantErr, err)
		}
	}
}

// ---------------------------------------------------------------------------
// header 承诺
// ---------------------------------------------------------------------------

// TestCommitmentEncoding 钉住 UncleHash 的字节布局。
func TestCommitmentEncoding(t *testing.T) {
	maxU := uint64(math.MaxUint64)
	for _, c := range []Commitment{
		{},
		{LaneSize: 5_600_000, GeneralGasUsed: 61_234_567, PaymentGasUsed: 5_600_000},
		{LaneSize: maxU, GeneralGasUsed: maxU, PaymentGasUsed: maxU},
	} {
		got, err := Decode(Encode(c))
		if err != nil {
			t.Fatalf("%+v 解码失败: %v", c, err)
		}
		if got != c {
			t.Fatalf("round trip 不一致: got %+v want %+v", got, c)
		}
	}

	// Encode 的值域绝不能包含全零 hash：core/types/block.go 的 NewBlock 用
	// 「UncleHash 是零值」判断调用方有没有写过该字段，而 Commitment{} 恰好是
	// 「ratio 还没起来的空块」—— 分叉激活初期最常见的形状。没有版本位时它会被
	// NewBlock 覆写成 EmptyUncleHash 再被导入侧拒掉，表现为间歇性 BAD_BLOCK。
	if Encode(Commitment{}) == (common.Hash{}) {
		t.Fatal("零值承诺编码成了全零 hash，会撞上 NewBlock 的零值哨兵")
	}
}

// TestDecodeRejectsMalformed 逐字节验证版本位与保留位。
// 只翻一个字节的弱断言挡不住「检查写成 h[31] != 0」这类差一错误。
func TestDecodeRejectsMalformed(t *testing.T) {
	good := Encode(Commitment{LaneSize: 800, GeneralGasUsed: 1, PaymentGasUsed: 2})

	for _, v := range []byte{0, 2, 0xff} {
		bad := good
		bad[24] = v
		if _, err := Decode(bad); !errors.Is(err, ErrBadCommitment) {
			t.Errorf("版本位 = %d 应被拒，实际 %v", v, err)
		}
	}
	for i := 25; i < 32; i++ {
		for _, v := range []byte{1, 0x80, 0xff} {
			bad := good
			bad[i] = v
			if _, err := Decode(bad); !errors.Is(err, ErrBadCommitment) {
				t.Errorf("保留位 h[%d] = %d 应被拒，实际 %v", i, v, err)
			}
		}
	}
	// h[23] 属于 payment 桶的最低字节，不是保留位（差一错误的另一个方向）
	ok := good
	ok[23] = 0xff
	if _, err := Decode(ok); err != nil {
		t.Errorf("h[23] 属数据区，不该被拒: %v", err)
	}
	// EmptyUncleHash 必须被拒 —— 这让「parlia 的 EmptyUncleHash 覆写回归」
	// 变成一个确定性可检测的故障，而不是诡异的记账偏差
	if _, err := Decode(types.EmptyUncleHash); !errors.Is(err, ErrBadCommitment) {
		t.Errorf("EmptyUncleHash 应被拒，实际 %v", err)
	}
}
