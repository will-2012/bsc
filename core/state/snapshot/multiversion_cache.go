package snapshot

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

type destructCacheItem struct {
	version uint64
	root    common.Hash
}

type accountCacheItem struct {
	version uint64
	root    common.Hash
	data    []byte
}

type storageCacheItem struct {
	version uint64
	root    common.Hash
	data    []byte
}

func cloneParentMap(parentMap map[common.Hash]struct{}) map[common.Hash]struct{} {
	cloneMap := make(map[common.Hash]struct{})
	for k := range parentMap {
		cloneMap[k] = struct{}{}
	}
	return cloneMap
}

type MultiVersionSnapshotCache struct {
	lock             sync.RWMutex
	destructCache    map[common.Hash][]*destructCacheItem
	accountDataCache map[common.Hash][]*accountCacheItem
	storageDataCache map[common.Hash]map[common.Hash][]*storageCacheItem
	minVersion       uint64 // bottom version
	diffLayerParent  map[common.Hash]map[common.Hash]struct{}
	cacheItemNumber  int64
}

func NewMultiVersionSnapshotCache() *MultiVersionSnapshotCache {
	return &MultiVersionSnapshotCache{
		destructCache:    make(map[common.Hash][]*destructCacheItem),
		accountDataCache: make(map[common.Hash][]*accountCacheItem),
		storageDataCache: make(map[common.Hash]map[common.Hash][]*storageCacheItem),
		minVersion:       0,
		diffLayerParent:  make(map[common.Hash]map[common.Hash]struct{}),
		cacheItemNumber:  0,
	}
}

func (c *MultiVersionSnapshotCache) checkParent(childRoot common.Hash, parentRoot common.Hash) bool {
	if c == nil {
		return false
	}
	c.lock.RLock()
	defer c.lock.RUnlock()
	if _, exist := c.diffLayerParent[childRoot]; !exist {
		return false
	}
	if _, exist := c.diffLayerParent[childRoot][parentRoot]; !exist {
		return false
	}
	return true
}

func (c *MultiVersionSnapshotCache) ResetParentMap(newDiffLayerParent map[common.Hash]map[common.Hash]struct{}) {
	if c == nil {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	c.diffLayerParent = newDiffLayerParent
	log.Info("Reset parent map")
}

func (c *MultiVersionSnapshotCache) AddDiffLayer(ly *diffLayer) {
	if c == nil || ly == nil {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	log.Info("Add difflayer to snapshot multiversion cache", "root", ly.root, "version_id", ly.diffLayerID, "current_cache_item_number", c.cacheItemNumber)

	for hash := range ly.destructSet {
		if multiVersionItems, exist := c.destructCache[hash]; exist {
			multiVersionItems = append(multiVersionItems, &destructCacheItem{version: ly.diffLayerID, root: ly.root})
			c.destructCache[hash] = multiVersionItems
		} else {
			c.destructCache[hash] = []*destructCacheItem{&destructCacheItem{version: ly.diffLayerID, root: ly.root}}
		}
		c.cacheItemNumber++
		log.Info("Add destruct to cache",
			"cache_account_hash", hash, "cache_version", ly.diffLayerID, "cache_root", ly.root)
	}
	for hash, aData := range ly.accountData {
		if multiVersionItems, exist := c.accountDataCache[hash]; exist {
			multiVersionItems = append(multiVersionItems, &accountCacheItem{version: ly.diffLayerID, root: ly.root, data: aData})
			c.accountDataCache[hash] = multiVersionItems
		} else {
			c.accountDataCache[hash] = []*accountCacheItem{&accountCacheItem{version: ly.diffLayerID, root: ly.root, data: aData}}
		}
		c.cacheItemNumber++
		log.Info("Add account to cache",
			"cache_account_hash", hash, "cache_version", ly.diffLayerID, "cache_root", ly.root)
	}
	for accountHash, slots := range ly.storageData {
		if _, exist := c.storageDataCache[accountHash]; !exist {
			c.storageDataCache[accountHash] = make(map[common.Hash][]*storageCacheItem)
		}
		for storageHash, sData := range slots {
			if multiVersionItems, exist := c.storageDataCache[accountHash][storageHash]; exist {
				multiVersionItems = append(multiVersionItems, &storageCacheItem{version: ly.diffLayerID, root: ly.root, data: sData})
				c.storageDataCache[accountHash][storageHash] = multiVersionItems
			} else {
				c.storageDataCache[accountHash][storageHash] = []*storageCacheItem{&storageCacheItem{version: ly.diffLayerID, root: ly.root, data: sData}}
			}
			c.cacheItemNumber++
			log.Info("Add storage to cache",
				"cache_account_hash", accountHash, "cache_storage_hash", storageHash, "cache_version", ly.diffLayerID, "cache_root", ly.root)
		}
	}

	if parentDiffLayer, ok := ly.parent.(*diffLayer); ok {
		if parentLayerParent, exist := c.diffLayerParent[parentDiffLayer.root]; exist {
			clonedParentLayerParent := cloneParentMap(parentLayerParent)
			clonedParentLayerParent[ly.root] = struct{}{}
			c.diffLayerParent[ly.root] = clonedParentLayerParent
		} else {
			log.Warn("Impossible branch, maybe there is a bug.")
		}
	} else {
		c.diffLayerParent[ly.root] = make(map[common.Hash]struct{})
		c.diffLayerParent[ly.root][ly.root] = struct{}{}
	}
	diffMultiVersionCacheLengthGauge.Update(c.cacheItemNumber)
}

func (c *MultiVersionSnapshotCache) RemoveDiffLayer(ly *diffLayer) {
	if c == nil || ly == nil {
		return
	}
	c.lock.Lock()
	if c.minVersion < ly.diffLayerID {
		c.minVersion = ly.diffLayerID
	}
	c.lock.Unlock()
	log.Info("Remove difflayer from snapshot multiversion cache", "root", ly.root, "version_id", ly.diffLayerID, "current_cache_item_number", c.cacheItemNumber)

	go func() {
		c.lock.Lock()
		defer c.lock.Unlock()

		for aHash, multiVersionDestructList := range c.destructCache {
			for i := 0; i < len(c.destructCache); i++ {
				if multiVersionDestructList[i].version <= c.minVersion {
					multiVersionDestructList = append(multiVersionDestructList[:i], multiVersionDestructList[i+1:]...)
					i--
					c.cacheItemNumber--
				}
			}
			if len(multiVersionDestructList) == 0 {
				delete(c.destructCache, aHash)
			}
		}

		for aHash, multiVersionAccoutList := range c.accountDataCache {
			for i := 0; i < len(c.accountDataCache); i++ {
				if multiVersionAccoutList[i].version <= c.minVersion {
					multiVersionAccoutList = append(multiVersionAccoutList[:i], multiVersionAccoutList[i+1:]...)
					i--
					c.cacheItemNumber--
				}
			}
			if len(multiVersionAccoutList) == 0 {
				delete(c.accountDataCache, aHash)
			}
		}
		for aHash := range c.storageDataCache {
			for sHash, multiVersionStorageList := range c.storageDataCache[aHash] {
				for i := 0; i < len(multiVersionStorageList); i++ {
					if multiVersionStorageList[i].version <= c.minVersion {
						multiVersionStorageList = append(multiVersionStorageList[:i], multiVersionStorageList[i+1:]...)
						i--
						c.cacheItemNumber--
					}
				}
				if len(multiVersionStorageList) == 0 {
					delete(c.storageDataCache[aHash], sHash)
				}
			}
			if len(c.storageDataCache[aHash]) == 0 {
				delete(c.storageDataCache, aHash)
			}
		}

		delete(c.diffLayerParent, ly.root)
		for _, v := range c.diffLayerParent {
			delete(v, ly.root)
		}
		diffMultiVersionCacheLengthGauge.Update(c.cacheItemNumber)
	}()
}

// QueryAccount return tuple(data-slice, need-try-disklayer, error)
func (c *MultiVersionSnapshotCache) QueryAccount(version uint64, rootHash common.Hash, ahash common.Hash) ([]byte, bool, error) {
	if c == nil {
		return nil, false, fmt.Errorf("not found, need try difflayer")
	}
	c.lock.RLock()
	defer c.lock.RUnlock()

	var (
		queryAccountItem  *accountCacheItem
		queryDestructItem *destructCacheItem
	)

	{
		if multiVersionItems, exist := c.accountDataCache[ahash]; exist && len(multiVersionItems) != 0 {
			log.Info("Try query account cache",
				"query_version", version,
				"query_root_hash", rootHash,
				"query_account_hash", ahash,
				"multi_version_cache_len", len(multiVersionItems))
			for i := len(multiVersionItems) - 1; i >= 0; i-- {
				if multiVersionItems[i].version <= version &&
					multiVersionItems[i].version > c.minVersion &&
					c.checkParent(rootHash, multiVersionItems[i].root) {
					queryAccountItem = multiVersionItems[i]
					log.Info("Account hit account cache",
						"query_version", version,
						"query_root_hash", rootHash,
						"query_account_hash", ahash,
						"hit_version", queryAccountItem.version,
						"hit_root_hash", queryAccountItem.root)
					break
				}
				log.Info("Try hit account cache",
					"query_version", version,
					"query_root_hash", rootHash,
					"query_account_hash", ahash,
					"try_hit_version", multiVersionItems[i].version,
					"try_hit_root_hash", multiVersionItems[i].root)
			}
		}
	}

	{
		if multiVersionItems, exist := c.destructCache[ahash]; exist && len(multiVersionItems) != 0 {
			log.Info("Try query destruct cache",
				"query_version", version,
				"query_root_hash", rootHash,
				"query_account_hash", ahash,
				"multi_version_cache_len", len(multiVersionItems))
			for i := len(multiVersionItems) - 1; i >= 0; i-- {
				if multiVersionItems[i].version <= version &&
					multiVersionItems[i].version > c.minVersion &&
					c.checkParent(rootHash, multiVersionItems[i].root) {
					queryDestructItem = multiVersionItems[i]
					log.Info("Account hit destruct cache",
						"query_version", version,
						"query_root_hash", rootHash,
						"query_account_hash", ahash,
						"hit_version", queryDestructItem.version,
						"hit_root_hash", queryDestructItem.root)
					break
				}
				log.Info("Try hit destruct cache",
					"query_version", version,
					"query_root_hash", rootHash,
					"query_account_hash", ahash,
					"hit_version", multiVersionItems[i].version,
					"hit_root_hash", multiVersionItems[i].root)
			}
		}
	}
	if queryAccountItem != nil && queryDestructItem == nil {
		return queryAccountItem.data, false, nil // founded
	}

	if queryAccountItem == nil && queryDestructItem != nil {
		return nil, false, nil // deleted
	}

	if queryAccountItem == nil && queryDestructItem == nil {
		return nil, true, nil
	}

	//if queryAccountItem != nil && queryDestructItem != nil {
	if queryAccountItem.version >= queryDestructItem.version {
		return queryAccountItem.data, false, nil // founded
	} else {
		return nil, false, nil // deleted
	}
}

// QueryStorage return tuple(data-slice, need-try-disklayer, error)
func (c *MultiVersionSnapshotCache) QueryStorage(version uint64, rootHash common.Hash, ahash common.Hash, shash common.Hash) ([]byte, bool, error) {
	if c == nil {
		return nil, false, fmt.Errorf("not found, need try difflayer")
	}

	c.lock.RLock()
	defer c.lock.RUnlock()

	var (
		queryStorageItem  *storageCacheItem
		queryDestructItem *destructCacheItem
	)

	{
		if _, exist := c.storageDataCache[ahash]; exist {
			if multiVersionItems, exist2 := c.storageDataCache[ahash][shash]; exist2 && len(multiVersionItems) != 0 {
				log.Info("Try query storage cache",
					"query_version", version,
					"query_root_hash", rootHash,
					"query_account_hash", ahash,
					"query_storage_hash", shash,
					"multi_version_cache_len", len(multiVersionItems))
				for i := len(multiVersionItems) - 1; i >= 0; i-- {
					if multiVersionItems[i].version <= version &&
						multiVersionItems[i].version > c.minVersion &&
						c.checkParent(rootHash, multiVersionItems[i].root) {
						queryStorageItem = multiVersionItems[i]
						log.Info("Account hit storage cache",
							"query_version", version,
							"query_root_hash", rootHash,
							"query_account_hash", ahash,
							"query_storage_hash", shash,
							"hit_version", queryStorageItem.version,
							"hit_root_hash", queryStorageItem.root)
						break
					}
					log.Info("Try hit storage cache",
						"query_version", version,
						"query_root_hash", rootHash,
						"query_account_hash", ahash,
						"query_storage_hash", shash,
						"hit_version", multiVersionItems[i].version,
						"hit_root_hash", multiVersionItems[i].root)
				}
			}
		}
	}

	{
		if multiVersionItems, exist := c.destructCache[ahash]; exist && len(multiVersionItems) != 0 {
			log.Info("Try query destruct cache",
				"query_version", version,
				"query_root_hash", rootHash,
				"query_account_hash", ahash,
				"query_storage_hash", shash,
				"multi_version_cache_len", len(multiVersionItems))
			for i := len(multiVersionItems) - 1; i >= 0; i-- {
				if multiVersionItems[i].version <= version &&
					multiVersionItems[i].version > c.minVersion &&
					c.checkParent(rootHash, multiVersionItems[i].root) {
					queryDestructItem = multiVersionItems[i]
					log.Info("Account hit destruct cache",
						"query_version", version,
						"query_root_hash", rootHash,
						"query_account_hash", ahash,
						"query_storage_hash", shash,
						"hit_version", queryDestructItem.version,
						"hit_root_hash", queryDestructItem.root)
					break
				}
				log.Info("Try hit destruct cache",
					"query_version", version,
					"query_root_hash", rootHash,
					"query_account_hash", ahash,
					"query_storage_hash", shash,
					"hit_version", multiVersionItems[i].version,
					"hit_root_hash", multiVersionItems[i].root)
			}
		}
	}

	if queryStorageItem != nil && queryDestructItem == nil {
		return queryStorageItem.data, false, nil // founded
	}

	if queryStorageItem == nil && queryDestructItem != nil {
		return nil, false, nil // deleted
	}

	if queryStorageItem == nil && queryDestructItem == nil {
		return nil, true, nil // not founded and need try disklayer
	}

	// if queryStorageItem != nil && queryDestructItem != nil {
	if queryStorageItem.version >= queryDestructItem.version {
		return queryStorageItem.data, false, nil // founded
	} else {
		return nil, false, nil // deleted
	}
}
