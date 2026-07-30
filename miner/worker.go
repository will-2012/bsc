// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package miner

import (
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/holiman/uint256"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc/eip1559"
	"github.com/ethereum/go-ethereum/consensus/misc/eip4844"
	"github.com/ethereum/go-ethereum/consensus/parlia"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/paymentlane"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/stateless"
	"github.com/ethereum/go-ethereum/core/systemcontracts"
	"github.com/ethereum/go-ethereum/core/txpool"
	"github.com/ethereum/go-ethereum/core/types"
	buildertypes "github.com/ethereum/go-ethereum/core/types/builder"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/miner/minerconfig"
	"github.com/ethereum/go-ethereum/params"
)

const (
	// resultQueueSize is the size of channel listening to sealing result.
	resultQueueSize = 10

	// txChanSize is the size of channel listening to NewTxsEvent.
	// The number is referenced from the size of tx pool.
	txChanSize = 4096

	// chainHeadChanSize is the size of channel listening to ChainHeadEvent.
	chainHeadChanSize = 10

	// minRecommitInterval is the minimal time interval to recreate the sealing block with
	// any newly arrived transactions.
	minRecommitInterval = 1 * time.Second

	// staleThreshold is the maximum depth of the acceptable stale block.
	staleThreshold = 11

	// the current mining loops could have asynchronous risk of mining block with
	// same height, keep recently mined blocks to avoid double sign for safety,
	recentMinedCacheLimit = 20
)

var (
	bidExistGauge        = metrics.NewRegisteredGauge("worker/bidExist", nil)
	bidWinGauge          = metrics.NewRegisteredGauge("worker/bidWin", nil)
	inturnBlocksGauge    = metrics.NewRegisteredGauge("worker/inturnBlocks", nil)
	bestBidGasUsedGauge  = metrics.NewRegisteredGauge("worker/bestBidGasUsed", nil)  // MGas
	bestWorkGasUsedGauge = metrics.NewRegisteredGauge("worker/bestWorkGasUsed", nil) // MGas
	bidBlockExistGauge   = metrics.NewRegisteredGauge("worker/bidBlockExist", nil)
	bidBlockWinGauge     = metrics.NewRegisteredGauge("worker/bidBlockWin", nil)
	bidBlockCommitGauge  = metrics.NewRegisteredGauge("worker/bidBlockCommit", nil)
	bidBlockGasUsedGauge = metrics.NewRegisteredGauge("worker/bidBlockGasUsed", nil) // MGas
	bidBlockRevokeGauge  = metrics.NewRegisteredGauge("worker/bidBlockRevoke", nil)  // cumulative revoke count
	// bidBlockVerifyFailedGauge counts sealed BidBlocks that failed async InsertChain verification (cumulative).
	bidBlockVerifyFailedGauge = metrics.NewRegisteredGauge("worker/bidBlockVerifyFailed", nil)
	// bidBlockRevokedBuildersGauge snapshots how many builders are revoked, taken at each revoke.
	bidBlockRevokedBuildersGauge = metrics.NewRegisteredGauge("worker/bidBlockRevokedBuilders", nil)

	writeBlockTimer      = metrics.NewRegisteredTimer("worker/writeblock", nil)
	finalizeBlockTimer   = metrics.NewRegisteredTimer("worker/finalizeblock", nil)
	pendingPlainTxsTimer = metrics.NewRegisteredTimer("worker/pendingPlainTxs", nil)
	pendingBlobTxsTimer  = metrics.NewRegisteredTimer("worker/pendingBlobTxs", nil)

	errBlockInterruptedByNewHead   = errors.New("new head arrived while building block")
	errBlockInterruptedByRecommit  = errors.New("recommit interrupt while building block")
	errBlockInterruptedByTimeout   = errors.New("timeout while building block")
	errBlockInterruptedByOutOfGas  = errors.New("out of gas while building block")
	errBlockInterruptedByBetterBid = errors.New("better bid arrived while building block")
)

// maxBlobsPerBlock returns the maximum number of blobs per block.
// Users can specify the maximum number of blobs per block if necessary.
func (w *worker) maxBlobsPerBlock(time uint64) int {
	maxBlobs := eip4844.MaxBlobsPerBlock(w.chainConfig, time)
	if w.config.MaxBlobsPerBlock != 0 && w.config.MaxBlobsPerBlock < maxBlobs {
		maxBlobs = w.config.MaxBlobsPerBlock
	}
	return maxBlobs
}

// environment is the worker's current environment and holds all
// information of the sealing block generation.
type environment struct {
	signer   types.Signer
	state    *state.StateDB // apply state changes here
	tcount   int            // tx count in cycle
	size     uint64         // size of the block we are building
	gasPool  *core.GasPool  // available gas used to pack transactions
	coinbase common.Address
	evm      *vm.EVM

	header   *types.Header
	txs      []*types.Transaction
	receipts []*types.Receipt
	sidecars types.BlobSidecars
	blobs    int

	witness *stateless.Witness

	committed bool

	// fromBid marks an env owned by the bid simulator (retained in bidsToSim,
	// discarded by its clearLoop). The worker must not discard it even when a
	// winning bid's env becomes w.current, or the EVM arena is released twice.
	fromBid bool

	// BEP-703 车道。laneOn 是硬分叉门的快照，避免打包热路径每笔交易都重新
	// 求值；它为 false 时下面所有车道分支都是死代码，打包行为与分叉前逐字一致。
	//
	// gasPool 仍然是唯一的池，容量仍是 GasLimit-gasReserved。车道不是第二个池：
	// 静态切分预算等价于装箱问题，会拒绝规则本来允许的区块（见 paymentlane.Budget）。
	// 取而代之的是分别累计两类交易已消耗的 gas，再把 §3.2 的不等式直接当准入
	// 谓词用 —— laneBudget 就是这两个累计量加上本块配额。
	laneOn      bool
	laneBudget  paymentlane.Budget
	gasReserved uint64 // Parlia 系统交易预留，出块前自检要用
	// laneOn 时的不变式：laneBudget.PaymentUsed+GeneralUsed == gasPool.Used()
}

// laneAdmits 报告某类别的交易能否装进本块。
//
// 共享余量取 gasPool.Gas() 而不是自己重算，这样 bid 路径上
// SubGas(PayBidTxGasLimit) 那 25000 的临时预留天然被尊重。
//
// 车道关闭时 laneBudget 是零值 ⇒ IdleLane 为 0 ⇒ 两类的 headroom 都等于共享
// 余量，谓词退化成上游的 `gasPool.Gas() < tx.Gas()`。零回归是零值给的，不需要
// 特判。
func (env *environment) laneAdmits(class paymentlane.Class, gasLimit uint64) bool {
	return env.laneBudget.Admits(env.gasPool.Gas(), class, gasLimit)
}

// laneClassOf 给一笔外来交易定类别。车道未激活时一律 general —— 这样「未分类」
// 就不再是一个可能的状态，Account 因此不需要失败模式。
func (env *environment) laneClassOf(tx *types.Transaction) paymentlane.Class {
	if !env.laneOn {
		return paymentlane.ClassGeneral
	}
	return paymentlane.Classify(tx)
}

// sealLane 把车道记账写进 header 承诺，并用与导入侧相同的判据自检。
//
// 失败即放弃出块。丢一个槽位远好过广播一个全网必然拒收的块 —— 后者会让
// validator 持续出坏块直到被 jail，而 ValidateBody 与 VerifyHeader 都指不出
// 原因，日志里只有 BAD_BLOCK。
func (env *environment) sealLane() error {
	if !env.laneOn {
		return nil
	}
	// 承诺的三个值全部来自 laneBudget —— 配额既是本块的打包预算，也是下一块
	// 递推的起点，一个值两个用途，不需要额外字段。
	env.header.UncleHash = paymentlane.Encode(paymentlane.Commitment{
		LaneSize:       env.laneBudget.LaneSize,
		GeneralGasUsed: env.laneBudget.GeneralUsed,
		PaymentGasUsed: env.laneBudget.PaymentUsed,
	})
	return env.laneBudget.Verify(env.header.GasLimit, env.gasReserved, env.gasPool.Used())
}

// discard terminates the background prefetcher go-routine. It should
// always be called for all created environment instances otherwise
// the go-routine leak can happen.
func (env *environment) discard() {
	if env.state != nil {
		env.state.StopPrefetcher()
	}
	if env.evm != nil {
		env.evm.Release()
		env.evm = nil
	}
}

// task contains all information for consensus engine sealing and result submitting.
type task struct {
	receipts []*types.Receipt
	state    *state.StateDB
	block    *types.Block

	bidBlockInfo *bidBlockTaskInfo

	createdAt     time.Time
	miningStartAt time.Time
}

// txFitsSize reports whether the transaction fits into the block size limit.
func (env *environment) txFitsSize(tx *types.Transaction) bool {
	return env.size+tx.Size() < params.MaxBlockSize-maxBlockSizeBufferZone
}

const (
	commitInterruptNone int32 = iota
	commitInterruptNewHead
	commitInterruptResubmit
	commitInterruptTimeout
	commitInterruptOutOfGas
	commitInterruptBetterBid
)

// Block size is capped by the protocol at params.MaxBlockSize. When producing blocks, we
// try to say below the size including a buffer zone, this is to avoid going over the
// maximum size with auxiliary data added into the block.
const maxBlockSizeBufferZone = 1_000_000

// newWorkReq represents a request for new sealing work submitting with relative interrupt notifier.
type newWorkReq struct {
	interruptCh chan int32
	timestamp   int64
}

// newPayloadResult is the result of payload generation.
type newPayloadResult struct {
	err      error
	block    *types.Block
	fees     *big.Int               // total block fees
	sidecars []*types.BlobTxSidecar // collected blobs of blob transactions
	stateDB  *state.StateDB         // StateDB after executing the transactions
	receipts []*types.Receipt       // Receipts collected during construction
	requests [][]byte               // Consensus layer requests collected during block construction
	witness  *stateless.Witness     // Witness is an optional stateless proof
}

// getWorkReq represents a request for getting a new sealing work with provided parameters.
type getWorkReq struct {
	params *generateParams
	result chan *newPayloadResult // non-blocking channel
}

type bidFetcher interface {
	GetBestBid(parentHash common.Hash) *BidRuntime
	GetSimulatingBid(prevBlockHash common.Hash) *BidRuntime
	GetBestBidBlock(parentHash common.Hash) *buildertypes.DecodedBidBlock
}

// worker is the main object which takes care of submitting new work to consensus engine
// and gathering the sealing result.
type worker struct {
	bidFetcher  bidFetcher
	prefetcher  core.Prefetcher
	config      *minerconfig.Config
	chainConfig *params.ChainConfig
	engine      consensus.Engine
	eth         Backend
	permMgr     *BidBlockPermissionManager
	prio        []common.Address // A list of senders to prioritize
	chain       *core.BlockChain

	// Subscriptions
	mux          *event.TypeMux
	chainHeadCh  chan core.ChainHeadEvent
	chainHeadSub event.Subscription

	// Channels
	newWorkCh          chan *newWorkReq
	getWorkCh          chan *getWorkReq
	taskCh             chan *task
	resultCh           chan *types.Block
	startCh            chan struct{}
	exitCh             chan struct{}
	resubmitIntervalCh chan time.Duration

	wg sync.WaitGroup

	current *environment // An environment for current running cycle.

	confMu   sync.RWMutex // The lock used to protect the config fields: GasCeil, GasTip and Extradata
	coinbase common.Address
	extra    []byte
	tip      *uint256.Int // Minimum tip needed for non-local transaction to include them

	pendingMu    sync.RWMutex
	pendingTasks map[common.Hash]*task

	// atomic status counters
	running atomic.Bool // The indicator whether the consensus engine is running or not.
	syncing atomic.Bool // The indicator whether the node is still syncing.

	// recommit is the time interval to re-create sealing work or to re-build
	// payload in proof-of-stake stage.
	recommit          time.Duration
	recentMinedBlocks *lru.Cache[uint64, []common.Hash]

	// Test hooks
	newTaskHook  func(*task)                        // Method to call upon receiving a new sealing task.
	skipSealHook func(*task) bool                   // Method to decide whether skipping the sealing.
	fullTaskHook func()                             // Method to call before pushing the full sealing task.
	resubmitHook func(time.Duration, time.Duration) // Method to call upon updating resubmitting interval.
}

func newWorker(config *minerconfig.Config, engine consensus.Engine, eth Backend, mux *event.TypeMux, permMgr *BidBlockPermissionManager) *worker {
	if permMgr == nil {
		permMgr = NewBidBlockPermissionManager()
	}
	chainConfig := eth.BlockChain().Config()
	prefetcher := core.NewStatePrefetcher(chainConfig, eth.BlockChain().HeadChain())
	if config.Mev.Enabled != nil && *config.Mev.Enabled {
		prefetcher.EnableMevMode()
	}
	worker := &worker{
		prefetcher:         prefetcher,
		config:             config,
		chainConfig:        chainConfig,
		engine:             engine,
		eth:                eth,
		permMgr:            permMgr,
		chain:              eth.BlockChain(),
		mux:                mux,
		coinbase:           config.Etherbase,
		extra:              config.ExtraData,
		tip:                uint256.MustFromBig(config.GasPrice),
		pendingTasks:       make(map[common.Hash]*task),
		chainHeadCh:        make(chan core.ChainHeadEvent, chainHeadChanSize),
		newWorkCh:          make(chan *newWorkReq),
		getWorkCh:          make(chan *getWorkReq),
		taskCh:             make(chan *task),
		resultCh:           make(chan *types.Block, resultQueueSize),
		startCh:            make(chan struct{}, 1),
		exitCh:             make(chan struct{}),
		resubmitIntervalCh: make(chan time.Duration),
		recentMinedBlocks:  lru.NewCache[uint64, []common.Hash](recentMinedCacheLimit),
	}
	// Subscribe events for blockchain
	worker.chainHeadSub = eth.BlockChain().SubscribeChainHeadEvent(worker.chainHeadCh)

	// Sanitize recommit interval if the user-specified one is too short.
	recommit := minRecommitInterval
	if worker.config.Recommit != nil && *worker.config.Recommit > minRecommitInterval {
		recommit = *worker.config.Recommit
	}
	worker.recommit = recommit

	worker.wg.Add(4)
	go worker.mainLoop()
	go worker.newWorkLoop(recommit)
	go worker.resultLoop()
	go worker.taskLoop()

	return worker
}

func (w *worker) setBestBidFetcher(fetcher bidFetcher) {
	w.bidFetcher = fetcher
}

func (w *worker) getPrefetcher() core.Prefetcher {
	return w.prefetcher
}

// setEtherbase sets the etherbase used to initialize the block coinbase field.
func (w *worker) setEtherbase(addr common.Address) {
	w.confMu.Lock()
	defer w.confMu.Unlock()
	w.coinbase = addr
}

// etherbase retrieves the configured etherbase address.
func (w *worker) etherbase() common.Address {
	w.confMu.RLock()
	defer w.confMu.RUnlock()
	return w.coinbase
}

func (w *worker) setGasCeil(ceil uint64) {
	w.confMu.Lock()
	defer w.confMu.Unlock()
	w.config.GasCeil = ceil
}

func (w *worker) getGasCeil() uint64 {
	w.confMu.Lock()
	defer w.confMu.Unlock()
	return w.config.GasCeil
}

// setExtra sets the content used to initialize the block extra field.
func (w *worker) setExtra(extra []byte) {
	w.confMu.Lock()
	defer w.confMu.Unlock()
	w.extra = extra
}

// setGasTip sets the minimum miner tip needed to include a non-local transaction.
func (w *worker) setGasTip(tip *big.Int) {
	w.confMu.Lock()
	defer w.confMu.Unlock()
	w.tip = uint256.MustFromBig(tip)
}

// setRecommitInterval updates the interval for miner sealing work recommitting.
func (w *worker) setRecommitInterval(interval time.Duration) {
	select {
	case w.resubmitIntervalCh <- interval:
	case <-w.exitCh:
	}
}

// setPrioAddresses sets a list of addresses to prioritize for transaction inclusion.
func (w *worker) setPrioAddresses(prio []common.Address) {
	w.confMu.Lock()
	defer w.confMu.Unlock()
	w.prio = prio
}

// start sets the running status as 1 and triggers new work submitting.
func (w *worker) start() {
	w.running.Store(true)
	w.startCh <- struct{}{}
}

// stop sets the running status as 0.
func (w *worker) stop() {
	w.running.Store(false)
}

// isRunning returns an indicator whether worker is running or not.
func (w *worker) isRunning() bool {
	return w.running.Load()
}

// close terminates all background threads maintained by the worker.
// Note the worker does not support being closed multiple times.
func (w *worker) close() {
	w.running.Store(false)
	close(w.exitCh)
	w.wg.Wait()
}

// newWorkLoop is a standalone goroutine to submit new sealing work upon received events.
func (w *worker) newWorkLoop(recommit time.Duration) {
	defer w.wg.Done()
	var (
		interruptCh chan int32
		minRecommit = recommit // minimal resubmit interval specified by user.
		timestamp   int64      // timestamp for each round of sealing.
	)

	timer := time.NewTimer(0)
	defer timer.Stop()
	<-timer.C // discard the initial tick

	// commit aborts in-flight transaction execution with given signal and resubmits a new one.
	commit := func(reason int32) {
		if interruptCh != nil {
			// each commit work will have its own interruptCh to stop work with a reason
			interruptCh <- reason
			close(interruptCh)
		}
		interruptCh = make(chan int32, 1)
		select {
		case w.newWorkCh <- &newWorkReq{interruptCh: interruptCh, timestamp: timestamp}:
		case <-w.exitCh:
			return
		}
		timer.Reset(recommit)
	}
	// clearPending cleans the stale pending tasks.
	clearPending := func(number uint64) {
		w.pendingMu.Lock()
		for h, t := range w.pendingTasks {
			if t.block.NumberU64()+staleThreshold <= number {
				delete(w.pendingTasks, h)
			}
		}
		w.pendingMu.Unlock()
	}

	for {
		select {
		case <-w.startCh:
			clearPending(w.chain.CurrentBlock().Number.Uint64())
			timestamp = time.Now().Unix()
			commit(commitInterruptNewHead)

		case head := <-w.chainHeadCh:
			if !w.isRunning() {
				continue
			}
			if interruptCh != nil {
				interruptCh <- commitInterruptNewHead
				close(interruptCh)
				interruptCh = nil
			}
			clearPending(head.Header.Number.Uint64())
			timestamp = time.Now().Unix()
			if p, ok := w.engine.(*parlia.Parlia); ok {
				signedRecent, err := p.SignRecently(w.chain, head.Header)
				if err != nil {
					timer.Reset(recommit)
					log.Debug("Not allowed to propose block", "err", err)
					continue
				}
				if signedRecent {
					timer.Reset(recommit)
					log.Info("Signed recently, must wait")
					continue
				}
			}
			commit(commitInterruptNewHead)

		case <-timer.C:
			// If sealing is running resubmit a new work cycle periodically to pull in
			// higher priced transactions. Disable this overhead for pending blocks.
			if w.isRunning() && ((w.chainConfig.Clique != nil &&
				w.chainConfig.Clique.Period > 0) || (w.chainConfig.IsInBSC())) {
				// Short circuit if no new transaction arrives.
				commit(commitInterruptResubmit)
			}

		case interval := <-w.resubmitIntervalCh:
			// Adjust resubmit interval explicitly by user.
			if interval < minRecommitInterval {
				log.Warn("Sanitizing miner recommit interval", "provided", interval, "updated", minRecommitInterval)
				interval = minRecommitInterval
			}
			log.Info("Miner recommit interval update", "from", minRecommit, "to", interval)
			minRecommit, recommit = interval, interval

			if w.resubmitHook != nil {
				w.resubmitHook(minRecommit, recommit)
			}

		case <-w.exitCh:
			return
		}
	}
}

// mainLoop is responsible for generating and submitting sealing work based on
// the received event. It can support two modes: automatically generate task and
// submit it or return task according to given parameters for various proposes.
func (w *worker) mainLoop() {
	defer w.wg.Done()
	defer w.chainHeadSub.Unsubscribe()
	defer func() {
		if w.current != nil && !w.current.fromBid {
			w.current.discard()
		}
	}()

	for {
		select {
		case req := <-w.newWorkCh:
			w.commitWork(req.interruptCh, req.timestamp)

		case req := <-w.getWorkCh:
			req.result <- w.generateWork(req.params, false)

		// System stopped
		case <-w.exitCh:
			return
		case <-w.chainHeadSub.Err():
			return
		}
	}
}

// taskLoop is a standalone goroutine to fetch sealing task from the generator and
// push them to consensus engine.
func (w *worker) taskLoop() {
	defer w.wg.Done()
	var (
		stopCh chan struct{}
		prev   common.Hash
	)

	// interrupt aborts the in-flight sealing task.
	interrupt := func() {
		if stopCh != nil {
			close(stopCh)
			stopCh = nil
		}
	}
	for {
		select {
		case task := <-w.taskCh:
			if w.newTaskHook != nil {
				w.newTaskHook(task)
			}
			// Reject duplicate sealing work due to resubmitting.
			sealHash := w.engine.SealHash(task.block.Header())
			if sealHash == prev {
				continue
			}
			// Interrupt previous sealing operation
			interrupt()
			stopCh, prev = make(chan struct{}), sealHash

			if w.skipSealHook != nil && w.skipSealHook(task) {
				continue
			}
			w.pendingMu.Lock()
			w.pendingTasks[sealHash] = task
			w.pendingMu.Unlock()

			if err := w.engine.Seal(w.chain, task.block, w.resultCh, stopCh); err != nil {
				log.Warn("Block sealing failed", "err", err)
				w.pendingMu.Lock()
				delete(w.pendingTasks, sealHash)
				w.pendingMu.Unlock()
			}
		case <-w.exitCh:
			interrupt()
			return
		}
	}
}

// resultLoop is a standalone goroutine to handle sealing result submitting
// and flush relative data to the database.
func (w *worker) resultLoop() {
	defer w.wg.Done()
	for {
		select {
		case block := <-w.resultCh:
			// Short circuit when receiving empty result.
			if block == nil {
				continue
			}
			// Short circuit when receiving duplicate result caused by resubmitting.
			if w.chain.HasBlock(block.Hash(), block.NumberU64()) {
				continue
			}
			var (
				sealhash = w.engine.SealHash(block.Header())
				hash     = block.Hash()
			)
			w.pendingMu.RLock()
			task, exist := w.pendingTasks[sealhash]
			w.pendingMu.RUnlock()
			if !exist {
				log.Error("Block found but no relative pending task", "number", block.Number(), "sealhash", sealhash, "hash", hash)
				continue
			}

			if !w.recordMinedBlock(block) {
				continue
			}

			// BidBlock path: broadcast first, then InsertChain for async verification
			if task.bidBlockInfo != nil {
				w.handleBidBlockResult(block, task)
				continue
			}

			// Different block could share same sealhash, deep copy here to prevent write-write conflict.
			var (
				receipts = make([]*types.Receipt, len(task.receipts))
				logs     []*types.Log
			)
			for i, taskReceipt := range task.receipts {
				receipt := new(types.Receipt)
				receipts[i] = receipt
				*receipt = *taskReceipt

				// add block location fields
				receipt.BlockHash = hash
				receipt.BlockNumber = block.Number()
				receipt.TransactionIndex = uint(i)

				// Update the block hash in all logs since it is now available and not when the
				// receipt/log of individual transactions were created.
				receipt.Logs = make([]*types.Log, len(taskReceipt.Logs))
				for i, taskLog := range taskReceipt.Logs {
					log := new(types.Log)
					receipt.Logs[i] = log
					*log = *taskLog
					log.BlockHash = hash
				}
				logs = append(logs, receipt.Logs...)
			}

			// Commit block and state to database.
			start := time.Now()
			status, err := w.chain.WriteBlockAndSetHead(block, receipts, logs, task.state, w.mux)
			if status != core.CanonStatTy {
				if err != nil {
					log.Error("Failed writing block to chain", "err", err, "status", status)
				} else {
					log.Info("Written block as SideChain and avoid broadcasting", "status", status)
				}
				continue
			}
			writeBlockTimer.UpdateSince(start)
			stats := w.chain.GetBlockStats(block.Hash())
			stats.SendBlockTime.Store(time.Now().UnixMilli())
			stats.StartMiningTime.Store(task.miningStartAt.UnixMilli())
			log.Info("Successfully seal and write new block", "number", block.Number(), "hash", hash, "time", block.Header().MilliTimestamp(), "sealhash", sealhash,
				"block size", block.Size(), "elapsed", common.PrettyDuration(time.Since(task.createdAt)))
			w.mux.Post(core.NewMinedBlockEvent{Block: block})

		case <-w.exitCh:
			return
		}
	}
}

// recordMinedBlock records the mined parent for this height and rejects repeat signing.
func (w *worker) recordMinedBlock(block *types.Block) bool {
	if prev, ok := w.recentMinedBlocks.Get(block.NumberU64()); ok {
		if slices.Contains(prev, block.ParentHash()) {
			log.Error("Reject Double Sign!!", "block", block.NumberU64(),
				"hash", block.Hash(),
				"root", block.Root(),
				"ParentHash", block.ParentHash())
			return false
		}
		prevParents := append(prev, block.ParentHash())
		w.recentMinedBlocks.Add(block.NumberU64(), prevParents)
	} else {
		// Add() will call removeOldest internally to remove the oldest element
		// if the LRU Cache is full.
		w.recentMinedBlocks.Add(block.NumberU64(), []common.Hash{block.ParentHash()})
	}
	return true
}

// makeEnv creates a new environment for the sealing block.
func (w *worker) makeEnv(parent *types.Header, header *types.Header, coinbase common.Address,
	prevEnv *environment, witness bool) (*environment, error) {
	// Retrieve the parent state to execute on top and start a prefetcher for
	// the miner to speed block sealing up a bit
	state, err := w.chain.StateWithCacheAt(parent.Root)
	if err != nil {
		return nil, err
	}
	var bundle *stateless.Witness
	if witness {
		bundle, err = stateless.NewWitness(header, w.chain, false)
		if err != nil {
			return nil, err
		}
	}
	state.StartPrefetcher("miner", bundle)
	// Parlia reserves gas for the system txs it applies in FinalizeAndAssemble,
	// so user txs must leave room for them. Initialise the gas pool at
	// GasLimit-gasReserved (rather than reserving via SubGas afterwards) so
	// GasPool.Used() stays equal to the real user-tx consumption; header.GasUsed
	// then matches the reservation-free gas pool used on block import
	// (core.StateProcessor) instead of being inflated by the reservation.
	var gasReserved uint64
	if p, ok := w.engine.(*parlia.Parlia); ok {
		gasReserved = p.EstimateGasReservedForSystemTxs(w.chain, header)
		if gasReserved > header.GasLimit {
			gasReserved = header.GasLimit
		}
		log.Debug("makeEnv", "number", header.Number.Uint64(), "time", header.Time, "EstimateGasReservedForSystemTxs", gasReserved)
	}
	// Note the passed coinbase may be different with header.Coinbase.
	env := &environment{
		signer:      types.MakeSigner(w.chainConfig, header.Number, header.Time),
		state:       state,
		size:        uint64(header.Size()),
		coinbase:    coinbase,
		gasPool:     core.NewGasPool(header.GasLimit - gasReserved),
		gasReserved: gasReserved,
		header:      header,
		witness:     state.Witness(),
		evm:         vm.NewEVM(core.NewEVMBlockContext(header, w.chain, &coinbase), state, w.chainConfig, vm.Config{}),
	}
	// BEP-703。配额只依赖 (parent, header)，所以块内是常量；commitWork 的多轮
	// 重试每轮重建 env 但看到同一个值，轮间因此可比较。
	//
	// 这里刻意不对 laneSize 做任何钳制 —— 出块侧唯一能用来钳的量是
	// gasReserved，而它是矿工本地的启发式上界，验证方看不到，钳了就是共识
	// 分歧（详见 paymentlane.Budget 的注释）。配额真的大于本块可用预算时，
	// Headroom 会把 general 压到 0、sealLane 的自检再决定这个块能不能出。
	if paymentlane.Enabled(w.chainConfig, header.Number, header.Time) {
		env.laneOn = true
		env.laneBudget = paymentlane.Budget{LaneSize: paymentlane.Quota(parent, header)}
	}
	// Keep track of transactions which return errors so they can be removed
	env.tcount = 0
	return env, nil
}

func (w *worker) commitTransaction(env *environment, tx *types.Transaction, receiptProcessors ...core.ReceiptProcessor) ([]*types.Log, error) {
	if tx.Type() == types.BlobTxType {
		return w.commitBlobTransaction(env, tx, receiptProcessors...)
	}

	receipt, err := w.applyTransaction(env, tx, receiptProcessors...)
	if err != nil {
		return nil, err
	}
	env.txs = append(env.txs, tx)
	env.receipts = append(env.receipts, receipt)
	env.size += tx.Size()
	env.tcount++
	return receipt.Logs, nil
}

func (w *worker) commitBlobTransaction(env *environment, tx *types.Transaction, receiptProcessors ...core.ReceiptProcessor) ([]*types.Log, error) {
	sc := types.NewBlobSidecarFromTx(tx)
	if sc == nil {
		panic("blob transaction without blobs in miner")
	}
	// Checking against blob gas limit: It's kind of ugly to perform this check here, but there
	// isn't really a better place right now. The blob gas limit is checked at block validation time
	// and not during execution. This means core.ApplyTransaction will not return an error if the
	// tx has too many blobs. So we have to explicitly check it here.
	maxBlobs := w.maxBlobsPerBlock(env.header.Time)
	if env.blobs+len(sc.Blobs) > maxBlobs {
		return nil, errors.New("max data blobs reached")
	}

	receipt, err := w.applyTransaction(env, tx, receiptProcessors...)
	if err != nil {
		return nil, err
	}
	sc.TxIndex = uint64(len(env.txs))
	txNoBlob := tx.WithoutBlobTxSidecar()
	env.txs = append(env.txs, txNoBlob)
	env.receipts = append(env.receipts, receipt)
	env.sidecars = append(env.sidecars, sc)
	env.blobs += len(sc.Blobs)
	env.size += txNoBlob.Size()
	env.tcount++
	*env.header.BlobGasUsed += receipt.BlobGasUsed
	return receipt.Logs, nil
}

// applyTransaction runs the transaction. If execution fails, state and gas pool are reverted.
func (w *worker) applyTransaction(env *environment, tx *types.Transaction, receiptProcessors ...core.ReceiptProcessor) (*types.Receipt, error) {
	var (
		snap = env.state.Snapshot()
		gp   = env.gasPool.Snapshot()
	)

	receipt, err := core.ApplyTransaction(env.evm, env.gasPool, env.state, env.header, tx, receiptProcessors...)
	if err != nil {
		env.state.RevertToSnapshot(snap)
		env.gasPool.Set(gp)
	}
	env.header.GasUsed = env.gasPool.Used()
	return receipt, err
}

func (w *worker) commitTransactions(env *environment, plainTxs, blobTxs *transactionsByPriceAndNonce,
	interruptCh chan int32, stopTimer *time.Timer) error {
	isCancun := w.chainConfig.IsCancun(env.header.Number, env.header.Time)

	// initialize bloom processors
	processorCapacity := 100
	if plainTxs.CurrentSize() < processorCapacity {
		processorCapacity = plainTxs.CurrentSize()
	}
	bloomProcessors := core.NewAsyncReceiptBloomGenerator(processorCapacity)
	defer bloomProcessors.Close()

	stopPrefetchCh := make(chan struct{})
	defer close(stopPrefetchCh)
	// prefetch plainTxs txs, don't bother to prefetch a few blobTxs
	txsPrefetch := plainTxs.Copy()
	// prefetchCurr marks the tx the main loop is on; the prefetch feeder reads
	// it to rate-limit itself (see Forward).
	var prefetchCurr atomic.Pointer[types.Transaction]
	if prefetchHead := txsPrefetch.PeekWithUnwrap(); prefetchHead != nil {
		prefetchCurr.Store(prefetchHead)
		w.prefetcher.PrefetchMining(txsPrefetch, env.header, env.gasPool.Gas(), env.state.StateForPrefetch(), *w.chain.GetVMConfig(), stopPrefetchCh, &prefetchCurr)
	}

	signal := commitInterruptNone
LOOP:
	for {
		// In the following three cases, we will interrupt the execution of the transaction.
		// (1) new head block event arrival, the reason is 1
		// (2) worker start or restart, the reason is 1
		// (3) worker recreate the sealing block with any newly arrived transactions, the reason is 2.
		// For the first two cases, the semi-finished work will be discarded.
		// For the third case, the semi-finished work will be submitted to the consensus engine.
		if interruptCh != nil {
			select {
			case signal, ok := <-interruptCh:
				if !ok {
					// should never be here, since interruptCh should not be read before
					log.Warn("commit transactions stopped unknown")
				}
				return signalToErr(signal)
			default:
			}
		}
		// If we don't have enough gas for any further transactions then we're done.
		// BEP-703 不改这一行：payment 谓词更松，故「两类都装不下 TxGas」⟺
		// 「共享余量 < TxGas」，与原判据等价（TestPaymentPredicateIsTheLooserOne）。
		if env.gasPool.Gas() < params.TxGas {
			log.Trace("Not enough gas for further transactions", "have", env.gasPool, "want", params.TxGas)
			signal = commitInterruptOutOfGas
			break
		}
		if stopTimer != nil {
			select {
			case <-stopTimer.C:
				log.Info("Not enough time for further transactions", "txs", len(env.txs))
				stopTimer.Reset(0) // re-active the timer, in case it will be used later.
				signal = commitInterruptTimeout
				break LOOP
			default:
			}
		}

		// If we don't have enough blob space for any further blob transactions,
		// skip that list altogether
		if !blobTxs.Empty() && env.blobs >= w.maxBlobsPerBlock(env.header.Time) {
			log.Trace("Not enough blob space for further blob transactions")
			blobTxs.Clear()
			// Fall though to pick up any plain txs
		}
		// Retrieve the next transaction and abort if all done.
		var (
			ltx *txpool.LazyTransaction
			txs *transactionsByPriceAndNonce
		)
		pltx, ptip := plainTxs.Peek()
		bltx, btip := blobTxs.Peek()

		switch {
		case pltx == nil:
			txs, ltx = blobTxs, bltx
		case bltx == nil:
			txs, ltx = plainTxs, pltx
		default:
			if ptip.Lt(btip) {
				txs, ltx = blobTxs, bltx
			} else {
				txs, ltx = plainTxs, pltx
			}
		}
		if ltx == nil {
			break
		}

		// Most of the blob gas logic here is agnostic as to if the chain supports
		// blobs or not, however the max check panics when called on a chain without
		// a defined schedule, so we need to verify it's safe to call.
		if isCancun {
			left := w.maxBlobsPerBlock(env.header.Time) - env.blobs
			if left < int(ltx.BlobGas/params.BlobTxBlobGasPerBlob) {
				log.Trace("Not enough blob space left for transaction", "hash", ltx.Hash, "left", left, "needed", ltx.BlobGas/params.BlobTxBlobGasPerBlob)
				txs.Pop()
				continue
			}
		}

		// Transaction seems to fit, pull it up from the pool
		tx := ltx.Resolve()
		if tx == nil {
			log.Trace("Ignoring evicted transaction", "hash", ltx.Hash)
			txs.Pop()
			continue
		}
		prefetchCurr.Store(tx)

		// If we don't have enough space for the next transaction, skip the account.
		//
		// 判断要先知道类别（两类的可用空间不同），而类别要看 To/data/tx type ——
		// LazyTransaction 上没有，所以上游那句 gasPool.Gas() < ltx.Gas 挪到了
		// Resolve 之后。Pop 掉整个账户的正确性见 paymentlane.Budget.Headroom：
		// 两类的 headroom 都单调不增，「现在装不下」就是「永远装不下」。
		class := env.laneClassOf(tx)
		if !env.laneAdmits(class, tx.Gas()) {
			log.Trace("Not enough gas left for transaction", "hash", ltx.Hash,
				"class", class, "left", env.gasPool.Gas(), "needed", tx.Gas())
			txs.Pop()
			continue
		}

		// if inclusion of the transaction would put the block size over the
		// maximum we allow, don't add any more txs to the payload.
		if !env.txFitsSize(tx) {
			// 上游在这里 break 整个循环，车道打开后这会饿死配额：payment 交易都是
			// 小体积转账，本来装得下，却被一笔 calldata 大户连坐，而空转配额按
			// §3.2 不回收。配额还有空间时改成丢账户继续扫；env.size 单调递增，
			// 所以 Pop 是正确的。
			if env.laneOn && env.laneBudget.IdleLane() >= params.TxGas {
				txs.Pop()
				continue
			}
			break
		}
		// Error may be ignored here. The error has already been checked
		// during transaction acceptance in the transaction pool.
		from, _ := types.Sender(env.signer, tx)

		// Check whether the tx is replay protected. If we're not in the EIP155 hf
		// phase, start ignoring the sender until we do.
		if tx.Protected() && !w.chainConfig.IsEIP155(env.header.Number) {
			log.Trace("Ignoring replay protected transaction", "hash", ltx.Hash, "eip155", w.chainConfig.EIP155Block)
			txs.Pop()
			continue
		}
		// Start executing the transaction
		env.state.SetTxContext(tx.Hash(), env.tcount)

		// BEP-703：在这里取快照、返回后差分记账，class 因此不用跨函数边界传递，
		// commitTransaction 及其下游保持上游原样。前提是这里到 core.ApplyTransaction
		// 之间不能有任何代码改动 gasPool —— 中间插入这类调用会静默算错承诺。
		// 用差分而不用 receipt.GasUsed 的理由见 paymentlane.Budget.Account。
		usedBefore := env.gasPool.Used()
		_, err := w.commitTransaction(env, tx, bloomProcessors)
		if env.laneOn {
			env.laneBudget.Account(class, env.gasPool.Used()-usedBefore)
		}
		switch {
		case errors.Is(err, core.ErrNonceTooLow):
			// New head notification data race between the transaction pool and miner, shift
			log.Trace("Skipping transaction with low nonce", "hash", ltx.Hash, "sender", from, "nonce", tx.Nonce())
			txs.Shift()

		case errors.Is(err, nil):
			// Everything ok, shift in the next transaction from the same account
			txs.Shift()

		default:
			// Transaction is regarded as invalid, drop all consecutive transactions from
			// the same sender because of `nonce-too-high` clause.
			log.Debug("Transaction failed, account skipped", "hash", ltx.Hash, "err", err)
			txs.Pop()
		}
	}

	return signalToErr(signal)
}

// generateParams wraps various of settings for generating sealing task.
type generateParams struct {
	timestamp   uint64            // The timestamp for sealing task
	forceTime   bool              // Flag whether the given timestamp is immutable or not
	parentHash  common.Hash       // Parent block hash, empty means the latest chain head
	coinbase    common.Address    // The fee recipient address for including transaction
	random      common.Hash       // The randomness generated by beacon chain, empty before the merge
	withdrawals types.Withdrawals // List of withdrawals to include in block.
	prevWork    *environment
	beaconRoot  *common.Hash // The beacon root (cancun field).
	slotNum     *uint64      // The slot number (amsterdam field).
	noTxs       bool         // Flag whether an empty block without any transaction is expected
}

// prepareWork constructs the sealing task according to the given parameters,
// either based on the last chain head or specified parent. In this function
// the pending transactions are not filled yet, only the empty task returned.
func (w *worker) prepareWork(genParams *generateParams, witness bool) (*environment, error) {
	w.confMu.RLock()
	defer w.confMu.RUnlock()

	// Find the parent block for sealing task
	parent := w.chain.CurrentBlock()
	if genParams.parentHash != (common.Hash{}) {
		block := w.chain.GetBlockByHash(genParams.parentHash)
		if block == nil {
			return nil, errors.New("missing parent")
		}
		parent = block.Header()
	}
	// Sanity check the timestamp correctness, recap the timestamp
	// to parent+1 if the mutation is allowed.
	timestamp := genParams.timestamp
	if parent.Time >= timestamp {
		if genParams.forceTime {
			return nil, fmt.Errorf("invalid timestamp, parent %d given %d", parent.Time, timestamp)
		}
		timestamp = parent.Time + 1
	}
	// Construct the sealing block header.
	header := &types.Header{
		ParentHash: parent.Hash(),
		Number:     new(big.Int).Add(parent.Number, common.Big1),
		GasLimit:   core.CalcGasLimit(parent.GasLimit, w.config.GasCeil),
		Time:       timestamp,
		Coinbase:   genParams.coinbase,
	}
	// Set the extra field.
	if len(w.extra) != 0 {
		header.Extra = w.extra
	}
	// Set the randomness field from the beacon chain if it's available.
	if genParams.random != (common.Hash{}) {
		header.MixDigest = genParams.random
	}
	// Set baseFee and GasLimit if we are on an EIP-1559 chain
	if w.chainConfig.IsLondon(header.Number) {
		header.BaseFee = eip1559.CalcBaseFee(w.chainConfig, parent)
		if w.chainConfig.IsNotInBSC() && !w.chainConfig.IsLondon(parent.Number) {
			parentGasLimit := parent.GasLimit * w.chainConfig.ElasticityMultiplier()
			header.GasLimit = core.CalcGasLimit(parentGasLimit, w.config.GasCeil)
		}
	}
	// Run the consensus preparation with the default or customized consensus engine.
	// Note that the `header.Time` may be changed.
	if err := w.engine.Prepare(w.chain, header); err != nil {
		log.Error("Failed to prepare header for sealing", "err", err)
		return nil, err
	}
	// Apply EIP-4844, EIP-4788.
	if w.chainConfig.IsCancun(header.Number, header.Time) {
		var excessBlobGas uint64
		if w.chainConfig.IsCancun(parent.Number, parent.Time) {
			excessBlobGas = eip4844.CalcExcessBlobGas(w.chainConfig, parent, header.Time)
		}
		header.BlobGasUsed = new(uint64)
		header.ExcessBlobGas = &excessBlobGas
		if w.chainConfig.IsNotInBSC() {
			header.ParentBeaconRoot = genParams.beaconRoot
		} else {
			header.WithdrawalsHash = &types.EmptyWithdrawalsHash
			if w.chainConfig.IsBohr(header.Number, header.Time) {
				header.ParentBeaconRoot = new(common.Hash)
			}
			if w.chainConfig.IsPrague(header.Number, header.Time) {
				header.RequestsHash = &types.EmptyRequestsHash
			}
		}
	}
	// Apply EIP-7843.
	if w.chainConfig.IsAmsterdam(header.Number, header.Time) {
		uint64SlotNum := header.Number.Uint64()
		header.SlotNumber = &uint64SlotNum
	}
	// Could potentially happen if starting to mine in an odd state.
	// Note genParams.coinbase can be different with header.Coinbase
	// since clique algorithm can modify the coinbase field in header.
	env, err := w.makeEnv(parent, header, genParams.coinbase, genParams.prevWork, witness)
	if err != nil {
		log.Error("Failed to create sealing context", "err", err)
		return nil, err
	}

	// Handle upgrade built-in system contract code
	systemcontracts.TryUpdateBuildInSystemContract(w.chainConfig, header.Number, parent.Time, header.Time, env.state, true)

	if header.ParentBeaconRoot != nil {
		core.ProcessBeaconBlockRoot(*header.ParentBeaconRoot, env.evm)
	}

	if w.chainConfig.IsPrague(header.Number, header.Time) {
		core.ProcessParentBlockHash(header.ParentHash, env.evm)
	}
	return env, nil
}

// fillTransactions retrieves the pending transactions from the txpool and fills them
// into the given sealing block. The transaction selection and ordering strategy can
// be customized with the plugin in the future.
func (w *worker) fillTransactions(interruptCh chan int32, env *environment, stopTimer *time.Timer, bidTxs mapset.Set[common.Hash]) (err error) {
	w.confMu.RLock()
	tip := w.tip
	prio := w.prio
	w.confMu.RUnlock()

	// Retrieve the pending transactions pre-filtered by the 1559/4844 dynamic fees
	filter := txpool.PendingFilter{
		MinTip: tip,
	}
	if env.header.BaseFee != nil {
		filter.BaseFee = uint256.MustFromBig(env.header.BaseFee)
	}
	if env.header.ExcessBlobGas != nil {
		filter.BlobFee = uint256.MustFromBig(eip4844.CalcBlobFee(w.chainConfig, env.header))
	}

	if w.chainConfig.IsOsaka(env.header.Number, env.header.Time) && !w.chainConfig.IsAmsterdam(env.header.Number, env.header.Time) {
		filter.GasLimitCap = params.MaxTxGas
	}
	filter.BlobTxs = false
	plainTxsStart := time.Now()
	pendingPlainTxs, _ := w.eth.TxPool().Pending(filter)
	pendingPlainTxsTimer.UpdateSince(plainTxsStart)

	var pendingBlobTxs map[common.Address][]*txpool.LazyTransaction
	if env.header.Number.Uint64()%params.BlobEligibleBlockInterval == 0 {
		filter.BlobTxs = true
		filter.BlobVersion = types.BlobSidecarVersion0

		blobTxsStart := time.Now()
		pendingBlobTxs, _ = w.eth.TxPool().Pending(filter)
		pendingBlobTxsTimer.UpdateSince(blobTxsStart)
	} else {
		pendingBlobTxs = make(map[common.Address][]*txpool.LazyTransaction)
	}

	if bidTxs != nil {
		filterBidTxs := func(commonTxs map[common.Address][]*txpool.LazyTransaction) {
			for acc, txs := range commonTxs {
				for i := len(txs) - 1; i >= 0; i-- {
					if bidTxs.Contains(txs[i].Hash) {
						if i == len(txs)-1 {
							delete(commonTxs, acc)
						} else {
							commonTxs[acc] = txs[i+1:]
						}
						break
					}
				}
			}
		}

		filterBidTxs(pendingPlainTxs)
		filterBidTxs(pendingBlobTxs)
	}

	// Split the pending transactions into locals and remotes.
	prioPlainTxs, normalPlainTxs := make(map[common.Address][]*txpool.LazyTransaction), pendingPlainTxs
	prioBlobTxs, normalBlobTxs := make(map[common.Address][]*txpool.LazyTransaction), pendingBlobTxs

	for _, account := range prio {
		if txs := normalPlainTxs[account]; len(txs) > 0 {
			delete(normalPlainTxs, account)
			prioPlainTxs[account] = txs
		}
		if txs := normalBlobTxs[account]; len(txs) > 0 {
			delete(normalBlobTxs, account)
			prioBlobTxs[account] = txs
		}
	}

	// Fill the block with all available pending transactions.
	if len(prioPlainTxs) > 0 || len(prioBlobTxs) > 0 {
		plainTxs := newTransactionsByPriceAndNonce(env.signer, prioPlainTxs, env.header.BaseFee)
		blobTxs := newTransactionsByPriceAndNonce(env.signer, prioBlobTxs, env.header.BaseFee)

		if err := w.commitTransactions(env, plainTxs, blobTxs, interruptCh, stopTimer); err != nil {
			return err
		}
	}
	if len(normalPlainTxs) > 0 || len(normalBlobTxs) > 0 {
		plainTxs := newTransactionsByPriceAndNonce(env.signer, normalPlainTxs, env.header.BaseFee)
		blobTxs := newTransactionsByPriceAndNonce(env.signer, normalBlobTxs, env.header.BaseFee)

		if err := w.commitTransactions(env, plainTxs, blobTxs, interruptCh, stopTimer); err != nil {
			return err
		}
	}

	// BEP-703 不需要单独的「车道补填轮」：general headroom 耗尽后 payment 交易仍
	// 按共享余量准入，所以上面两轮会把堆走到空，配额自然被填满。
	//
	// 别加第三轮「MinTip=nil 重查池」：payment 交易并不会因 tip 低而进不了候选集
	// （StartMining 把 miner.gasprice 推进 txpool，加上 ErrTipAboveFeeCap 与 BSC
	// 的 baseFee≡0，legacypool.Pending 的 MinTip 截断永不触发），而那一轮会把
	// MinTip 以下的 general 交易也打包进来，等于悄悄取消矿工的 tip 底线。
	return nil
}

// generateWork generates a sealing block based on the given parameters.
func (w *worker) generateWork(genParam *generateParams, witness bool) *newPayloadResult {
	work, err := w.prepareWork(genParam, witness)
	if err != nil {
		return &newPayloadResult{err: err}
	}
	defer work.discard()

	// Check withdrawals fit max block size.
	// Due to the cap on withdrawal count, this can actually never happen, but we still need to
	// check to ensure the CL notices there's a problem if the withdrawal cap is ever lifted.
	maxBlockSize := params.MaxBlockSize - maxBlockSizeBufferZone
	if genParam.withdrawals.Size() > maxBlockSize {
		return &newPayloadResult{err: errors.New("withdrawals exceed max block size")}
	}
	// Also add size of withdrawals to work block size.
	work.size += uint64(genParam.withdrawals.Size())

	if !genParam.noTxs {
		interrupt := new(atomic.Int32)
		timer := time.AfterFunc(*w.config.Recommit, func() {
			interrupt.Store(commitInterruptTimeout)
		})
		defer timer.Stop()

		err := w.fillTransactions(nil, work, nil, nil)
		if errors.Is(err, errBlockInterruptedByTimeout) {
			log.Warn("Block building is interrupted", "allowance", common.PrettyDuration(w.recommit))
		}
	}
	body := types.Body{Transactions: work.txs, Withdrawals: genParam.withdrawals}
	if w.chainConfig.IsNotInBSC() {
		if !w.chainConfig.IsShanghai(work.header.Number, work.header.Time) {
			if body.Withdrawals != nil {
				return &newPayloadResult{err: errors.New("unexpected withdrawals before shanghai")}
			}
		} else {
			if body.Withdrawals == nil {
				body.Withdrawals = make([]*types.Withdrawal, 0)
			}
		}
	}
	allLogs := make([]*types.Log, 0)
	for _, r := range work.receipts {
		allLogs = append(allLogs, r.Logs...)
	}
	// Collect consensus-layer requests if Prague is enabled.
	var requests [][]byte
	if w.chainConfig.IsPrague(work.header.Number, work.header.Time) && w.chainConfig.IsNotInBSC() {
		requests = [][]byte{}
		// EIP-6110 deposits
		if err := core.ParseDepositLogs(&requests, allLogs, w.chainConfig); err != nil {
			return &newPayloadResult{err: err}
		}
		// EIP-7002
		if err := core.ProcessWithdrawalQueue(&requests, work.evm); err != nil {
			return &newPayloadResult{err: err}
		}
		// EIP-7251 consolidations
		if err := core.ProcessConsolidationQueue(&requests, work.evm); err != nil {
			return &newPayloadResult{err: err}
		}
	}
	if requests != nil {
		reqHash := types.CalcRequestsHash(requests)
		work.header.RequestsHash = &reqHash
	}

	fees := work.state.GetBalance(consensus.SystemAddress)
	// 车道承诺必须在 AssembleBlock 之前写入 header（dev / Engine API 路径）。
	if err := work.sealLane(); err != nil {
		return &newPayloadResult{err: err}
	}
	block, receipts, err := core.AssembleBlock(w.engine, w.chain, work.header, work.state, &body, work.receipts)
	if err != nil {
		return &newPayloadResult{err: err}
	}

	return &newPayloadResult{
		block:    block,
		fees:     fees.ToBig(),
		sidecars: work.sidecars.BlobTxSidecarList(),
		stateDB:  work.state,
		receipts: receipts,
		requests: requests,
		witness:  work.witness,
	}
}

// delay returns the time budget for local block building.
// For the last block in a validator's turn it caps the budget to BlockInterval/4,
// giving the next validator enough lead time to avoid producing an empty block.
// (post-Fermi: 450ms interval, ~120ms one-way p2p delay)
func (w *worker) delay(header *types.Header, leftOver *time.Duration) *time.Duration {
	d := w.engine.Delay(w.chain, header, leftOver)
	if d == nil {
		return nil
	}
	if p, ok := w.engine.(*parlia.Parlia); ok && p.IsLastBlockInTurn(w.chain, header) {
		blockInterval, err := p.BlockInterval(w.chain, header)
		if err == nil {
			if cap := time.Duration(blockInterval) * time.Millisecond / 4; *d > cap {
				return &cap
			}
		}
	}
	return d
}

// commitWork generates several new sealing tasks based on the parent block
// and submit them to the sealer.
func (w *worker) commitWork(interruptCh chan int32, timestamp int64) {
	// Abort committing if node is still syncing
	if w.syncing.Load() {
		return
	}
	start := time.Now()

	// Set the coinbase if the worker is running or it's required
	var coinbase common.Address
	if w.isRunning() {
		coinbase = w.etherbase()
		if coinbase == (common.Address{}) {
			log.Error("Refusing to mine without etherbase")
			return
		}
	}

	stopTimer := time.NewTimer(0)
	defer stopTimer.Stop()
	<-stopTimer.C // discard the initial tick

	stopWaitTimer := time.NewTimer(0)
	defer stopWaitTimer.Stop()
	<-stopWaitTimer.C // discard the initial tick

	// validator can try several times to get the most profitable block,
	// as long as the timestamp is not reached.
	workList := make([]*environment, 0, 10)
	parentHash := w.chain.CurrentBlock().Hash()
	var prevWork *environment
	// workList clean up
	defer func() {
		for _, wk := range workList {
			// only keep the best work, discard others.
			if wk == w.current {
				continue
			}
			wk.discard()
		}
	}()

LOOP:
	for {
		work, err := w.prepareWork(&generateParams{
			timestamp:  uint64(timestamp),
			parentHash: parentHash,
			coinbase:   coinbase,
			prevWork:   prevWork,
		}, false)
		if err != nil {
			return
		}
		prevWork = work
		workList = append(workList, work)

		delay := w.delay(work.header, w.config.DelayLeftOver)
		if delay == nil {
			log.Warn("commitWork delay is nil, something is wrong")
			stopTimer = nil
		} else if *delay <= 0 {
			log.Debug("Not enough time for commitWork")
			break
		} else {
			if !w.inTurn() && len(workList) == 1 {
				if parliaEngine, ok := w.engine.(*parlia.Parlia); ok {
					// When mining out of turn, continuous access to the txpool and trie database
					// may cause lock contention, slowing down transaction insertion and block importing.
					// Applying a backoff delay mitigates this issue and significantly reduces CPU usage.
					if blockInterval, err := parliaEngine.BlockInterval(w.chain, w.chain.CurrentBlock()); err == nil {
						beforeSealing := time.Until(time.UnixMilli(int64(work.header.MilliTimestamp())))
						if wait := beforeSealing - time.Duration(blockInterval)*time.Millisecond; wait > 0 {
							log.Debug("Applying backoff before mining", "block", work.header.Number, "waiting(ms)", wait.Milliseconds())
							select {
							case <-time.After(wait):
							case <-interruptCh:
								log.Debug("CommitWork interrupted: new block imported or resubmission triggered", "block", work.header.Number)
								return
							}
						}
					}
				}
			}
			log.Debug("commitWork stopTimer", "block", work.header.Number,
				"header time", time.UnixMilli(int64(work.header.MilliTimestamp())),
				"commit delay", *delay, "DelayLeftOver", w.config.DelayLeftOver)
			stopTimer.Reset(*delay)
		}

		// subscribe before fillTransactions
		txsCh := make(chan core.NewTxsEvent, txChanSize)
		// Subscribe for transaction insertion events (whether from network or resurrects)
		sub := w.eth.TxPool().SubscribeTransactions(txsCh, true)
		// if TxPool has been stopped, `sub` would be nil, it could happen on shutdown.
		if sub == nil {
			log.Info("commitWork SubscribeTransactions return nil")
		} else {
			defer sub.Unsubscribe()
		}

		// Fill pending transactions from the txpool into the block.
		fillStart := time.Now()
		err = w.fillTransactions(interruptCh, work, stopTimer, nil)
		fillDuration := time.Since(fillStart)
		switch {
		case errors.Is(err, errBlockInterruptedByNewHead):
			// work.discard()
			log.Debug("commitWork abort", "err", err)
			return
		case errors.Is(err, errBlockInterruptedByRecommit):
			fallthrough
		case errors.Is(err, errBlockInterruptedByTimeout):
			fallthrough
		case errors.Is(err, errBlockInterruptedByOutOfGas):
			// break the loop to get the best work
			log.Debug("commitWork finish", "reason", err)
			break LOOP
		}

		if interruptCh == nil || stopTimer == nil {
			// it is single commit work, no need to try several time.
			log.Info("commitWork interruptCh or stopTimer is nil")
			break
		}

		newTxsNum := 0
		// stopTimer was the maximum delay for each fillTransactions
		// but now it is used to wait until (head.Time - DelayLeftOver) is reached.
		stopTimer.Reset(time.Until(time.UnixMilli(int64(work.header.MilliTimestamp()))) - *w.config.DelayLeftOver)
	LOOP_WAIT:
		for {
			select {
			case <-stopTimer.C:
				log.Debug("commitWork stopTimer expired")
				break LOOP
			case <-interruptCh:
				log.Debug("commitWork interruptCh closed, new block imported or resubmit triggered")
				return
			case ev := <-txsCh:
				delay := w.delay(work.header, w.config.DelayLeftOver)
				log.Debug("commitWork txsCh arrived", "fillDuration", fillDuration.String(),
					"delay", delay.String(), "work.tcount", work.tcount,
					"newTxsNum", newTxsNum, "len(ev.Txs)", len(ev.Txs))
				if *delay < fillDuration {
					// There may not have enough time for another fillTransactions.
					break LOOP
				} else if *delay < fillDuration*2 {
					// We can schedule another fillTransactions, but the time is limited,
					// probably it is the last chance, schedule it immediately.
					break LOOP_WAIT
				} else {
					// There is still plenty of time left.
					// We can wait a while to collect more transactions before
					// schedule another fillTransaction to reduce CPU cost.
					// There will be 2 cases to schedule another fillTransactions:
					//   1.newTxsNum >= work.tcount
					//   2.no much time left, have to schedule it immediately.
					newTxsNum = newTxsNum + len(ev.Txs)
					if newTxsNum >= work.tcount {
						break LOOP_WAIT
					}
					stopWaitTimer.Reset(*delay - fillDuration*2)
				}
			case <-stopWaitTimer.C:
				if newTxsNum > 0 {
					break LOOP_WAIT
				}
			}
		}
		// if sub's channel if full, it will block other NewTxsEvent subscribers,
		// so unsubscribe ASAP and Unsubscribe() is re-enterable, safe to call several time.
		if sub != nil {
			sub.Unsubscribe()
		}
	}
	// get the most profitable work
	bestWork := workList[0]
	bestReward := new(uint256.Int)
	for i, wk := range workList {
		balance := wk.state.GetBalance(consensus.SystemAddress)
		log.Debug("Get the most profitable work", "index", i, "balance", balance, "bestReward", bestReward)
		if balance.Cmp(bestReward) > 0 {
			bestWork = wk
			bestReward = balance
		}
	}

	// when out-turn, use bestWork to prevent bundle leakage.
	// when in-turn, compare with remote work.
	var bestBid *BidRuntime
	var bestBidBlock *buildertypes.DecodedBidBlock
	var bidBlockCommitted bool
	var bidBlockFallback bool
	var simBidBlockReward *uint256.Int
	var simBidValidatorReward *uint256.Int
	var localValidatorReward *uint256.Int
	if w.bidFetcher != nil && bestWork.header.Difficulty.Cmp(diffInTurn) == 0 {
		inturnBlocksGauge.Inc(1)
		localValidatorReward = new(uint256.Int).Mul(bestReward, uint256.NewInt(*w.config.Mev.ValidatorCommission))
		localValidatorReward.Div(localValidatorReward, uint256.NewInt(10000))

		// We want to start sealing the block as late as possible here if mev is enabled, so we could give builder the chance to send their final bid.
		// Time left till sealing the block.
		tillSealingTime := time.Until(time.UnixMilli(int64(bestWork.header.MilliTimestamp()))) - *w.config.DelayLeftOver
		if tillSealingTime > 0 {
			// Still some time left, wait for the best bid.
			// This happens during the peak time of the network, the local block building LOOP would break earlier than
			// the final sealing time by meeting the errBlockInterruptedByOutOfGas criteria.

			log.Info("commitWork local building finished, wait for the best bid", "tillSealingTime", common.PrettyDuration(tillSealingTime))
			stopTimer.Reset(tillSealingTime)
			select {
			case <-stopTimer.C:
			case <-interruptCh:
				log.Debug("commitWork interruptCh closed, new block imported or resubmit triggered")
				return
			}
		}

		// Stage 1 candidate A — legacy SendBid (simBid).
		bestBid = w.bidFetcher.GetBestBid(bestWork.header.ParentHash)
		if bestBid != nil {
			bidExistGauge.Inc(1)
			bestBidGasUsedGauge.Update(int64(bestBid.bid.GasUsed) / 1_000_000)
			bestWorkGasUsedGauge.Update(int64(bestWork.header.GasUsed) / 1_000_000)
			simBidBlockReward = uint256.MustFromBig(bestBid.packedBlockReward)
			simBidValidatorReward = uint256.MustFromBig(bestBid.packedValidatorReward)
		}

		// Stage 1 candidate B — SendBidBlock.
		bestBidBlock = w.bidFetcher.GetBestBidBlock(parentHash)
		if bestBidBlock != nil {
			bidBlockExistGauge.Inc(1)
		}
	}

	if bestBidBlock != nil && w.selectBidBlock(bestBidBlock, simBidBlockReward, simBidValidatorReward, bestReward) {
		bidBlockWinGauge.Inc(1)
		task, err := w.prepareBidBlockTask(bestBidBlock, start)
		if err != nil {
			log.Error("Failed to prepare bid block, fallback",
				"builder", bestBidBlock.Builder,
				"err", err)
			bidBlockFallback = true
		} else {
			systemTxCount := len(bestBidBlock.Txs) - bestBidBlock.SystemTxStart
			w.enqueueBidBlockTask(task, systemTxCount)
			bidBlockCommitted = true
			bidBlockCommitGauge.Inc(1)
			bidBlockGasUsedGauge.Update(int64(bestBidBlock.Header.GasUsed) / 1_000_000)
			bestWorkGasUsedGauge.Update(int64(bestWork.header.GasUsed) / 1_000_000)
		}
	}

	if bidBlockCommitted {
		if w.current != nil && !w.current.fromBid {
			w.current.discard()
		}
		w.current = nil
		return
	}

	// simBid fallback. Re-runs the legacy dual-threshold gate against simBid
	// whenever no BidBlock is being committed.
	if bestBid != nil {
		if bestReward.Cmp(simBidBlockReward) < 0 &&
			localValidatorReward.Cmp(simBidValidatorReward) < 0 {
			bidWinGauge.Inc(1)
			if bestBid.greedyMerged {
				greedyMergeOnchainCounter.Inc(1)
			}
			bestWork = bestBid.env
			// Record MEV v1 (bid path) source and builder address.
			setBidMevInfo(bestWork.header, bestBid.bid.Builder, false)
			logMsg := "[BUILDER BLOCK]"
			if bidBlockFallback {
				logMsg = "[BUILDER BLOCK] (simBid fallback)"
			}
			log.Info(logMsg,
				"block", bestWork.header.Number.Uint64(),
				"builder", bestBid.bid.Builder,
				"blockReward", weiToEtherStringF6(simBidBlockReward.ToBig()),
				"validatorReward", weiToEtherStringF6(simBidValidatorReward.ToBig()),
				"bid", bestBid.bid.Hash().TerminalString(),
			)
		}
	}

	w.commit(bestWork, w.fullTaskHook, start)

	// Swap out the old work with the new one, terminating any leftover
	// prefetcher processes in the mean time and starting a new one.
	if w.current != nil && !w.current.fromBid {
		w.current.discard()
	}
	w.current = bestWork
}

// inTurn return true if the current worker is in turn.
func (w *worker) inTurn() bool {
	validator, _ := w.engine.NextInTurnValidator(w.chain, w.chain.CurrentBlock())
	return validator != common.Address{} && validator == w.etherbase()
}

// commit runs any post-transaction state modifications, assembles the final block
// and commits new work if consensus engine is running.
func (w *worker) commit(env *environment, interval func(), start time.Time) error {
	if w.isRunning() {
		if env.committed {
			log.Warn("Invalid work commit: already committed", "number", env.header.Number.Uint64())
			return nil
		}
		if interval != nil {
			interval()
		}
		fees := env.state.GetBalance(consensus.SystemAddress).ToBig()
		feesInEther := new(big.Float).Quo(new(big.Float).SetInt(fees), big.NewFloat(params.Ether))
		// Withdrawals are set to nil here, because this is only called in PoW.
		finalizeStart := time.Now()
		body := types.Body{Transactions: env.txs}
		if env.header.EmptyWithdrawalsHash() {
			body.Withdrawals = make([]*types.Withdrawal, 0)
		}
		// 在 header 被拷贝并封装之前写入车道承诺。失败有两种：记账漏了一笔
		// （编码 bug），或者 Quota 给出的配额大于本块装得下的量（治理参数误配，
		// 此时不存在任何合法块）。两种都只能放弃这个槽位。
		if err := env.sealLane(); err != nil {
			log.Error("Payment lane invariant violated while sealing, abort",
				"number", env.header.Number, "err", err)
			return err
		}
		block, receipts, err := core.AssembleBlock(w.engine, w.chain, types.CopyHeader(env.header), env.state, &body, env.receipts)
		env.committed = true
		if err != nil {
			return err
		}
		env.txs = body.Transactions
		env.receipts = receipts
		finalizeBlockTimer.UpdateSince(finalizeStart)

		// If Cancun enabled, sidecars can't be nil then.
		if w.chainConfig.IsCancun(env.header.Number, env.header.Time) && env.sidecars == nil {
			env.sidecars = make(types.BlobSidecars, 0)
		}
		block = block.WithSidecars(env.sidecars)

		select {
		case w.taskCh <- &task{receipts: receipts, state: env.state, block: block, createdAt: time.Now(), miningStartAt: start}:
			log.Info("Commit new sealing work", "number", block.Number(), "sealhash", w.engine.SealHash(block.Header()),
				"txs", len(env.txs), "blobs", env.blobs, "gas", block.GasUsed(), "fees", feesInEther, "elapsed", common.PrettyDuration(time.Since(start)))

		case <-w.exitCh:
			log.Info("Worker has exited")
		}
	}
	return nil
}

// getSealingBlock generates the sealing block based on the given parameters.
// The generation result will be passed back via the given channel no matter
// the generation itself succeeds or not.
func (w *worker) getSealingBlock(params *generateParams) *newPayloadResult {
	req := &getWorkReq{
		params: params,
		result: make(chan *newPayloadResult, 1),
	}
	select {
	case w.getWorkCh <- req:
		return <-req.result
	case <-w.exitCh:
		return &newPayloadResult{err: errors.New("miner closed")}
	}
}

func (w *worker) tryWaitProposalDoneWhenStopping() {
	parlia, ok := w.engine.(*parlia.Parlia)
	// if the consensus is not parlia, just skip waiting
	if !ok {
		return
	}

	currentHeader := w.chain.CurrentBlock()
	currentBlock := currentHeader.Number.Uint64()
	startBlock, endBlock, err := parlia.NextProposalBlock(w.chain, currentHeader, w.coinbase)
	if err != nil {
		log.Warn("Failed to get next proposal block, skip waiting", "err", err)
		return
	}

	log.Info("Checking miner's next proposal block", "current", currentBlock,
		"proposalStart", startBlock, "proposalEnd", endBlock, "maxWait", *w.config.MaxWaitProposalInSecs)
	if endBlock <= currentBlock {
		log.Warn("next proposal end block has passed, ignore")
		return
	}
	blockInterval, err := parlia.BlockInterval(w.chain, currentHeader)
	if err != nil {
		log.Debug("failed to get BlockInterval when tryWaitProposalDoneWhenStopping")
	}
	if startBlock > currentBlock && ((startBlock-currentBlock)*blockInterval/1000) > *w.config.MaxWaitProposalInSecs {
		log.Warn("the next proposal start block is too far, just skip waiting")
		return
	}

	// wait one more block for safety
	waitSecs := (endBlock - currentBlock + 1) * blockInterval / 1000
	log.Info("The miner will propose in later, waiting for the proposal to be done",
		"currentBlock", currentBlock, "nextProposalStart", startBlock, "nextProposalEnd", endBlock, "waitTime", waitSecs)
	time.Sleep(time.Duration(waitSecs) * time.Second)
}

// signalToErr converts the interruption signal to a concrete error type for return.
// The given signal must be a valid interruption signal.
func signalToErr(signal int32) error {
	switch signal {
	case commitInterruptNone:
		return nil
	case commitInterruptNewHead:
		return errBlockInterruptedByNewHead
	case commitInterruptResubmit:
		return errBlockInterruptedByRecommit
	case commitInterruptTimeout:
		return errBlockInterruptedByTimeout
	case commitInterruptOutOfGas:
		return errBlockInterruptedByOutOfGas
	case commitInterruptBetterBid:
		return errBlockInterruptedByBetterBid
	default:
		panic(fmt.Errorf("undefined signal %d", signal))
	}
}
