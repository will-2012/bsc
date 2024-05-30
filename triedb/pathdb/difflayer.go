// Copyright 2022 The go-ethereum Authors
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

package pathdb

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/triestate"
	bloomfilter "github.com/holiman/bloomfilter/v2"
)

var (
	// aggregatorMemoryLimit is the maximum size of the bottom-most diff layer
	// that aggregates the writes from above until it's flushed into the disk
	// layer.
	//
	// Note, bumping this up might drastically increase the size of the bloom
	// filters that's stored in every diff layer. Don't do that without fully
	// understanding all the implications.
	aggregatorMemoryLimit = uint64(4 * 1024 * 1024)

	// aggregatorItemLimit is an approximate number of items that will end up
	// in the aggregator layer before it's flushed out to disk. A plain account
	// weighs around 14B (+hash), a storage slot 32B (+hash), a deleted slot
	// 0B (+hash). Slots are mostly set/unset in lockstep, so that average at
	// 16B (+hash). All in all, the average entry seems to be 15+32=47B. Use a
	// smaller number to be on the safe side.
	aggregatorItemLimit = aggregatorMemoryLimit / 42

	// bloomTargetError is the target false positive rate when the aggregator
	// layer is at its fullest. The actual value will probably move around up
	// and down from this number, it's mostly a ballpark figure.
	//
	// Note, dropping this down might drastically increase the size of the bloom
	// filters that's stored in every diff layer. Don't do that without fully
	// understanding all the implications.
	bloomTargetError = 0.02

	// bloomSize is the ideal bloom filter size given the maximum number of items
	// it's expected to hold and the target false positive error rate.
	bloomSize = math.Ceil(float64(aggregatorItemLimit) * math.Log(bloomTargetError) / math.Log(1/math.Pow(2, math.Log(2))))

	// bloomFuncs is the ideal number of bits a single entry should set in the
	// bloom filter to keep its size to a minimum (given it's size and maximum
	// entry count).
	bloomFuncs = math.Round((bloomSize / float64(aggregatorItemLimit)) * math.Log(2))

	// the bloom offsets are runtime constants which determines which part of the
	// account/storage hash the hasher functions looks at, to determine the
	// bloom key for an account/slot. This is randomized at init(), so that the
	// global population of nodes do not all display the exact same behaviour with
	// regards to bloom content
	bloomNodeHasherOffset = 0
)

func init() {
	// Init the bloom offsets in the range [0:24] (requires 8 bytes)
	bloomNodeHasherOffset = rand.Intn(25)
}

func nodeBloomHash(h common.Hash, p []byte) uint64 {
	return binary.BigEndian.Uint64(h[bloomNodeHasherOffset:bloomNodeHasherOffset+8]) ^ pathBloomHash(p)
}

//func pathBloomHash(p []byte) uint64 {
//	if len(p) > 16 {
//		panic("invalid path")
//	}
//	var result uint64
//	for _, nibble := range p {
//		if nibble > 0x0F {
//			panic("invalid path nibble value")
//		}
//		result = (result << 4) | uint64(nibble)
//	}
//
//	return uint64(len(p))<<32 + result
//}

func pathBloomHash(p []byte) uint64 {
	hasher := fnv.New64a()
	_, err := hasher.Write(p)
	if err != nil {
		panic(err)
	}
	return hasher.Sum64()
}

// diffLayer represents a collection of modifications made to the in-memory tries
// along with associated state changes after running a block on top.
//
// The goal of a diff layer is to act as a journal, tracking recent modifications
// made to the state, that have not yet graduated into a semi-immutable state.
type diffLayer struct {
	// Immutables
	root   common.Hash                               // Root hash to which this layer diff belongs to
	id     uint64                                    // Corresponding state id
	block  uint64                                    // Associated block number
	nodes  map[common.Hash]map[string]*trienode.Node // Cached trie nodes indexed by owner and path
	states *triestate.Set                            // Associated state change set for building history
	memory uint64                                    // Approximate guess as to how much memory we use

	parent layer        // Parent layer modified by this one, never nil, **can be changed**
	lock   sync.RWMutex // Lock used to protect parent

	origin     *diskLayer
	diffed     *bloomfilter.Filter // Bloom filter tracking all the diffed items up to the disk layer
	selfDiffed *bloomfilter.Filter // Bloom filter tracking all the diffed items of its own
}

// newDiffLayer creates a new diff layer on top of an existing layer.
func newDiffLayer(parent layer, root common.Hash, id uint64, block uint64, nodes map[common.Hash]map[string]*trienode.Node, states *triestate.Set) *diffLayer {
	var (
		size  int64
		count int
	)
	dl := &diffLayer{
		root:   root,
		id:     id,
		block:  block,
		nodes:  nodes,
		states: states,
		parent: parent,
	}
	switch l := parent.(type) {
	case *diskLayer:
		dl.rebloom(l)
	case *diffLayer:
		dl.rebloom(l.origin)
	default:
		panic("unknown parent type")
	}

	for _, subset := range nodes {
		for path, n := range subset {
			dl.memory += uint64(n.Size() + len(path))
			size += int64(len(n.Blob) + len(path))
		}
		count += len(subset)
	}
	if states != nil {
		dl.memory += uint64(states.Size())
	}
	dirtyWriteMeter.Mark(size)
	diffLayerNodesMeter.Mark(int64(count))
	diffLayerBytesMeter.Mark(int64(dl.memory))
	log.Debug("Created new diff layer", "id", id, "block", block, "nodes", count, "size", common.StorageSize(dl.memory), "root", dl.root)
	return dl
}

// rebloom discards the layer's current bloom and rebuilds it from scratch based
// on the parent's and the local diffs.
func (dl *diffLayer) rebloom(origin *diskLayer) {
	dl.lock.Lock()
	defer dl.lock.Unlock()

	defer func(start time.Time) {
		bloomIndexTimer.Update(time.Since(start))
	}(time.Now())

	// Inject the new origin that triggered the rebloom
	dl.origin = origin

	// Retrieve the parent bloom or create a fresh empty one
	if parent, ok := dl.parent.(*diffLayer); ok {
		parent.lock.RLock()
		dl.diffed, _ = parent.diffed.Copy()
		parent.lock.RUnlock()
	} else {
		if dl.selfDiffed == nil {
			dl.diffed, _ = bloomfilter.New(uint64(bloomSize), uint64(bloomFuncs))
		} else {
			dl.diffed, _ = dl.selfDiffed.NewCompatible()
		}
	}

	if dl.selfDiffed == nil {
		dl.selfDiffed, _ = dl.diffed.NewCompatible()
		for owner, subset := range dl.nodes {
			for path, _ := range subset {
				dl.selfDiffed.AddHash(nodeBloomHash(owner, []byte(path)))
			}
		}
	}
	err := dl.diffed.UnionInPlace(dl.selfDiffed)
	if err != nil {
		log.Error("diff layer bloom filter failed to union in place", "id", dl.id, "err", err)
	}
	// Calculate the current false positive rate and update the error rate meter.
	// This is a bit cheating because subsequent layers will overwrite it, but it
	// should be fine, we're only interested in ballpark figures.
	k := float64(dl.diffed.K())
	n := float64(dl.diffed.N())
	m := float64(dl.diffed.M())
	bloomErrorGauge.Update(math.Pow(1.0-math.Exp((-k)*(n+0.5)/(m-1)), k))
}

// rootHash implements the layer interface, returning the root hash of
// corresponding state.
func (dl *diffLayer) rootHash() common.Hash {
	return dl.root
}

// stateID implements the layer interface, returning the state id of the layer.
func (dl *diffLayer) stateID() uint64 {
	return dl.id
}

// parentLayer implements the layer interface, returning the subsequent
// layer of the diff layer.
func (dl *diffLayer) parentLayer() layer {
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	return dl.parent
}

// node retrieves the node with provided node information. It's the internal
// version of Node function with additional accessed layer tracked. No error
// will be returned if node is not found.
func (dl *diffLayer) node(owner common.Hash, path []byte, hash common.Hash, depth int, args *[]interface{}) ([]byte, error) {
	var (
		step1Start  time.Time
		step1End    time.Time
		step2Start  time.Time
		step2End    time.Time
		contractLen int64
		step3Start  time.Time
		step3End    time.Time
		trieLen     int64
		step4Start  time.Time
		step4End    time.Time
		step5End    time.Time
		step6Start  time.Time
		step6End    time.Time
		step7Start  time.Time
		step7End    time.Time
	)
	startNode := time.Now()
	defer func() {

		cost := common.PrettyDuration(time.Now().Sub(startNode))
		keyStr := fmt.Sprintf("%d_depth_difflayer_node", depth)
		*args = append(*args, []interface{}{keyStr, cost}...)
		var total_cost time.Duration
		if step5End.Unix() != 0 {
			total_cost = step5End.Sub(startNode)
		} else {
			total_cost = step6End.Sub(startNode)
		}
		if total_cost > 1*time.Millisecond {
			*args = append(*args, []interface{}{"inner_diff_total_cost", common.PrettyDuration(step5End.Sub(startNode))}...)
			*args = append(*args, []interface{}{"inner_lock_cost", common.PrettyDuration(step1End.Sub(step1Start))}...)
			*args = append(*args, []interface{}{"inner_query_contract_map_cost", common.PrettyDuration(step2End.Sub(step2Start))}...)
			*args = append(*args, []interface{}{"contract_map_len", contractLen}...)
			*args = append(*args, []interface{}{"inner_query_trie_map_cost", common.PrettyDuration(step3End.Sub(step3Start))}...)
			*args = append(*args, []interface{}{"trie_map_len", trieLen}...)
			*args = append(*args, []interface{}{"inner_update_metrics_cost1", common.PrettyDuration(step6End.Sub(step6Start))}...)
			if step7End.Unix() != 0 {
				*args = append(*args, []interface{}{"inner_update_metrics_cost2", common.PrettyDuration(step7End.Sub(step7Start))}...)
			}
			*args = append(*args, []interface{}{"inner_unlock_cost", common.PrettyDuration(step4End.Sub(step4Start))}...)
		}
	}()

	// Hold the lock, ensure the parent won't be changed during the
	// state accessing.
	step1Start = time.Now()
	dl.lock.RLock()
	step1End = time.Now()

	defer func() {
		step4Start = time.Now()
		dl.lock.RUnlock()
		step4End = time.Now()
	}()

	step2Start = time.Now()
	// If the trie node is known locally, return it
	subset, ok := dl.nodes[owner]
	step2End = time.Now()

	step6Start = time.Now()
	pathGetContractDiffLayerTimer.Update(step2End.Sub(step2Start))
	contractLen = int64(len(dl.nodes))
	pathDiffLayerContractLenGauge.Update(contractLen)
	trieLen = int64(len(subset))
	pathDiffLayerEOALenGauge.Update(trieLen)
	if ok {
		step3Start = time.Now()
		n, ok := subset[string(path)]
		step3End = time.Now()
		pathGetEOADiffLayerTimer.Update(step3End.Sub(step3Start))
		step6End = time.Now()
		if ok {
			// If the trie node is not hash matched, or marked as removed,
			// bubble up an error here. It shouldn't happen at all.
			if n.Hash != hash {
				dirtyFalseMeter.Mark(1)
				log.Error("Unexpected trie node in diff layer", "owner", owner, "path", path, "expect", hash, "got", n.Hash)
				return nil, newUnexpectedNodeError("diff", hash, n.Hash, owner, path, n.Blob)
			}
			step7Start = time.Now()
			dirtyHitMeter.Mark(1)
			dirtyNodeHitDepthHist.Update(int64(depth))
			dirtyReadMeter.Mark(int64(len(n.Blob)))
			step7End = time.Now()
			return n.Blob, nil
		}
	}
	step5End = time.Now()

	// Trie node unknown to this layer, resolve from parent
	if diff, ok := dl.parent.(*diffLayer); ok {
		return diff.node(owner, path, hash, depth+1, args)
	}
	// Failed to resolve through diff layers, fallback to disk layer
	return dl.parent.Node(owner, path, hash, args)
}

// Node implements the layer interface, retrieving the trie node blob with the
// provided node information. No error will be returned if the node is not found.
func (dl *diffLayer) Node(owner common.Hash, path []byte, hash common.Hash, args *[]interface{}) ([]byte, error) {
	var depth int
	start := time.Now()
	defer func() {
		cost := common.PrettyDuration(time.Now().Sub(start))
		keyStr := fmt.Sprintf("%d_depth_difflayer_Node", depth)
		*args = append(*args, []interface{}{keyStr, cost}...)
	}()

	dl.lock.RLock()
	defer dl.lock.RUnlock()

	var origin *diskLayer

	startQueryFilter := time.Now()
	hit := dl.diffed.ContainsHash(nodeBloomHash(owner, path))
	queryBloomIndexTimer.UpdateSince(startQueryFilter)
	if !hit {
		missBloomMeter.Mark(1)
		origin = dl.origin // extract origin while holding the lock
	} else {
		hitBloomMeter.Mark(1)
	}

	if origin != nil {
		return origin.Node(owner, path, hash, args)
	}
	return dl.node(owner, path, hash, depth, args)
}

// update implements the layer interface, creating a new layer on top of the
// existing layer tree with the specified data items.
func (dl *diffLayer) update(root common.Hash, id uint64, block uint64, nodes map[common.Hash]map[string]*trienode.Node, states *triestate.Set) *diffLayer {
	return newDiffLayer(dl, root, id, block, nodes, states)
}

// persist flushes the diff layer and all its parent layers to disk layer.
func (dl *diffLayer) persist(force bool) (layer, error) {
	if parent, ok := dl.parentLayer().(*diffLayer); ok {
		// Hold the lock to prevent any read operation until the new
		// parent is linked correctly.
		dl.lock.Lock()

		// The merging of diff layers starts at the bottom-most layer,
		// therefore we recurse down here, flattening on the way up
		// (diffToDisk).
		result, err := parent.persist(force)
		if err != nil {
			dl.lock.Unlock()
			return nil, err
		}
		dl.parent = result
		dl.lock.Unlock()
	}
	return diffToDisk(dl, force)
}

// diffToDisk merges a bottom-most diff into the persistent disk layer underneath
// it. The method will panic if called onto a non-bottom-most diff layer.
func diffToDisk(layer *diffLayer, force bool) (layer, error) {
	disk, ok := layer.parentLayer().(*diskLayer)
	if !ok {
		panic(fmt.Sprintf("unknown layer type: %T", layer.parentLayer()))
	}
	return disk.commit(layer, force)
}
