package snapshot

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
)

type destructCacheItem struct {
	version uint64
}

type accountCacheItem struct {
	version uint64
	data    []byte
}

type storageCacheItem struct {
	version uint64
	data    []byte
}

type MultiVersionSnapshotCache struct {
	lock             sync.RWMutex
	destructCache    map[common.Hash][]*destructCacheItem
	accountDataCache map[common.Hash][]*accountCacheItem
	storageDataCache map[common.Hash]map[common.Hash][]*storageCacheItem
	minVersion       uint64 // bottom version
}

func NewMultiVersionSnapshotCache() *MultiVersionSnapshotCache {
	return &MultiVersionSnapshotCache{
		destructCache:    make(map[common.Hash][]*destructCacheItem),
		accountDataCache: make(map[common.Hash][]*accountCacheItem),
		storageDataCache: make(map[common.Hash]map[common.Hash][]*storageCacheItem),
		minVersion:       math.MaxUint64,
	}
}

func (c *MultiVersionSnapshotCache) AddDiffLayer(ly *diffLayer) {
	if c == nil || ly == nil {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()

	for hash := range ly.destructSet {
		if multiVersionItems, exist := c.destructCache[hash]; exist {
			multiVersionItems = append(multiVersionItems, &destructCacheItem{version: ly.diffLayerID})
			c.destructCache[hash] = multiVersionItems
		} else {
			c.destructCache[hash] = []*destructCacheItem{&destructCacheItem{version: ly.diffLayerID}}
		}
	}
	for hash, aData := range ly.accountData {
		if multiVersionItems, exist := c.accountDataCache[hash]; exist {
			multiVersionItems = append(multiVersionItems, &accountCacheItem{version: ly.diffLayerID, data: aData})
			c.accountDataCache[hash] = multiVersionItems
		} else {
			c.accountDataCache[hash] = []*accountCacheItem{&accountCacheItem{version: ly.diffLayerID, data: aData}}
		}
	}
	for accountHash, slots := range ly.storageData {
		if _, exist := c.storageDataCache[accountHash]; !exist {
			c.storageDataCache[accountHash] = make(map[common.Hash][]*storageCacheItem)
		}
		for storageHash, sData := range slots {
			if multiVersionItems, exist := c.storageDataCache[accountHash][storageHash]; exist {
				multiVersionItems = append(multiVersionItems, &storageCacheItem{version: ly.diffLayerID, data: sData})
				c.storageDataCache[accountHash][storageHash] = multiVersionItems
			} else {
				c.storageDataCache[accountHash][storageHash] = []*storageCacheItem{&storageCacheItem{version: ly.diffLayerID, data: sData}}
			}
		}
	}
}

func (c *MultiVersionSnapshotCache) RemoveDiffLayer(ly *diffLayer) {
	if c == nil || ly == nil {
		return
	}
	c.lock.Lock()
	if c.minVersion > ly.diffLayerID {
		c.minVersion = ly.diffLayerID
	}
	c.lock.Unlock()

	go func() {
		c.lock.Lock()
		defer c.lock.Unlock()

		for aHash, multiVersionDestructList := range c.destructCache {
			for i := 0; i < len(c.destructCache); i++ {
				if multiVersionDestructList[i].version <= c.minVersion {
					multiVersionDestructList = append(multiVersionDestructList[:i], multiVersionDestructList[i+1:]...)
					i--
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
				}
			}
			if len(multiVersionAccoutList) == 0 {
				delete(c.accountDataCache, aHash)
			}
		}
		for aHash, _ := range c.storageDataCache {
			for sHash, multiVersionStorageList := range c.storageDataCache[aHash] {
				for i := 0; i < len(multiVersionStorageList); i++ {
					if multiVersionStorageList[i].version <= c.minVersion {
						multiVersionStorageList = append(multiVersionStorageList[:i], multiVersionStorageList[i+1:]...)
						i--
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
			for i := len(multiVersionItems) - 1; i >= 0; i-- {
				// TODO: check parent.
				if multiVersionItems[i].version <= version && multiVersionItems[i].version > c.minVersion {
					//hit = true
					queryAccountItem = multiVersionItems[i]
				}
			}
		}
	}

	{
		if multiVersionItems, exist := c.destructCache[ahash]; exist && len(multiVersionItems) != 0 {
			for i := len(multiVersionItems) - 1; i >= 0; i-- {
				// TODO: check parent.
				if multiVersionItems[i].version <= version && multiVersionItems[i].version > c.minVersion {
					//hit = true
					queryDestructItem = multiVersionItems[i]
				}
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
				for i := len(multiVersionItems) - 1; i >= 0; i-- {
					// TODO: check parent.
					if multiVersionItems[i].version <= version && multiVersionItems[i].version > c.minVersion {
						//hit = true
						queryStorageItem = multiVersionItems[i]
					}
				}
			}
		}
	}

	{
		if multiVersionItems, exist := c.destructCache[ahash]; exist && len(multiVersionItems) != 0 {
			for i := len(multiVersionItems) - 1; i >= 0; i-- {
				// TODO: check parent.
				if multiVersionItems[i].version <= version && multiVersionItems[i].version > c.minVersion {
					//hit = true
					queryDestructItem = multiVersionItems[i]
				}
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
