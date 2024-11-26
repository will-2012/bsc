package snapshot

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func collectDiffLayerAncestors(layer Snapshot) map[common.Hash]struct{} {
	set := make(map[common.Hash]struct{})
	for {
		parent := layer.Parent()
		if parent == nil {
			break // finished
		}
		if _, ok := parent.(*diskLayer); ok {
			break // finished
		}
		set[parent.Root()] = struct{}{}
		layer = parent
	}
	return set
}

// lookup is an internal help structure to quickly identify
type lookup struct {
	// todo: add lock?? or in layer tree lock??
	state2LayerRoots map[string][]common.Hash
	descendants      map[common.Hash]map[common.Hash]struct{}
}

// newLookup initializes the lookup structure.
func newLookup(head Snapshot) *lookup {
	l := new(lookup)

	{ // setup state mapping
		var (
			current = head
			layers  []Snapshot
		)
		for current != nil {
			layers = append(layers, current)
			current = current.Parent()
		}
		l.state2LayerRoots = make(map[string][]common.Hash)

		// Apply the layers from bottom to top
		for i := len(layers) - 1; i >= 0; i-- {
			switch diff := layers[i].(type) {
			case *diskLayer:
				continue
			case *diffLayer:
				l.addLayer(diff)
			}
		}
	}

	{ // setup descendant mapping
		var (
			current     = head
			layers      = make(map[common.Hash]Snapshot)
			descendants = make(map[common.Hash]map[common.Hash]struct{})
		)
		for {
			hash := current.Root()
			layers[hash] = current

			// Traverse the ancestors (diff only) of the current layer and link them
			for h := range collectDiffLayerAncestors(current) {
				subset := descendants[h]
				if subset == nil {
					subset = make(map[common.Hash]struct{})
					descendants[h] = subset
				}
				subset[hash] = struct{}{}
			}
			parent := current.Parent()
			if parent == nil {
				break
			}
			current = parent
		}
		l.descendants = descendants
	}

	return l
}

func (l *lookup) isDescendant(state common.Hash, ancestor common.Hash) bool {
	subset := l.descendants[ancestor]
	if subset == nil {
		return false
	}
	_, ok := subset[state]
	return ok
}

// addLayer traverses all the dirty state within the given diff layer and links
// them into the lookup set.
func (l *lookup) addLayer(diff *diffLayer) {
	defer func(now time.Time) {
		lookupAddLayerTimer.UpdateSince(now)
	}(time.Now())

	// TODO(rjl493456442) theoretically the code below could be parallelized,
	// but it will slow down the other parts of system (e.g., EVM execution)
	// with unknown reasons.
	diffRoot := diff.Root()
	for accountHash, _ := range diff.accountData {
		l.state2LayerRoots[accountHash.String()] = append(l.state2LayerRoots[accountHash.String()], diffRoot)
	}

	for accountHash, slots := range diff.storageData {
		for storageHash := range slots {
			l.state2LayerRoots[accountHash.String()+storageHash.String()] = append(l.state2LayerRoots[accountHash.String()+storageHash.String()], diffRoot)
		}
	}
	// TODO: update descendant mapping
}

// removeLayer traverses all the dirty state within the given diff layer and
// unlinks them from the lookup set.
func (l *lookup) removeLayer(diff *diffLayer) error {
	defer func(now time.Time) {
		lookupRemoveLayerTimer.UpdateSince(now)
	}(time.Now())

	// TODO(rjl493456442) theoretically the code below could be parallelized,
	// but it will slow down the other parts of system (e.g., EVM execution)
	// with unknown reasons.
	diffRoot := diff.Root()
	for accountHash, _ := range diff.accountData {
		stateKey := accountHash.String()

		subset := l.state2LayerRoots[stateKey]
		if subset == nil {
			return fmt.Errorf("unknown account addr hash %s", stateKey)
		}
		var found bool
		for j := 0; j < len(subset); j++ {
			if subset[j] == diffRoot {
				if j == 0 {
					subset = subset[1:] // TODO what if the underlying slice is held forever?
				} else {
					subset = append(subset[:j], subset[j+1:]...)
				}
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("failed to delete lookup %s", stateKey)
		}
		if len(subset) == 0 {
			delete(l.state2LayerRoots, stateKey)
		} else {
			l.state2LayerRoots[stateKey] = subset
		}
	}

	for accountHash, slots := range diff.storageData {
		for storageHash := range slots {
			stateKey := accountHash.String() + storageHash.String()

			subset := l.state2LayerRoots[stateKey]
			if subset == nil {
				return fmt.Errorf("unknown account addr hash %s", stateKey)
			}
			var found bool
			for j := 0; j < len(subset); j++ {
				if subset[j] == diffRoot {
					if j == 0 {
						subset = subset[1:] // TODO what if the underlying slice is held forever?
					} else {
						subset = append(subset[:j], subset[j+1:]...)
					}
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("failed to delete lookup %s", stateKey)
			}
			if len(subset) == 0 {
				delete(l.state2LayerRoots, stateKey)
			} else {
				l.state2LayerRoots[stateKey] = subset
			}
		}
	}

	// TODO: update descendant mapping
	return nil
}

func (l *lookup) lookupAccount(accountAddrHash common.Hash, head common.Hash) common.Hash {
	list, exists := l.state2LayerRoots[accountAddrHash.String()]
	if !exists {
		return common.Hash{}
	}

	// Traverse the list in reverse order to find the first entry that either
	// matches the specified head or is a descendant of it.
	for i := len(list) - 1; i >= 0; i-- {
		if list[i] == head || l.isDescendant(head, list[i]) {
			return list[i]
		}
	}
	return common.Hash{}
}

func (l *lookup) lookupStorage(accountAddrHash common.Hash, slot common.Hash, head common.Hash) common.Hash {
	list, exists := l.state2LayerRoots[accountAddrHash.String()+slot.String()]
	if !exists {
		return common.Hash{}
	}

	// Traverse the list in reverse order to find the first entry that either
	// matches the specified head or is a descendant of it.
	for i := len(list) - 1; i >= 0; i-- {
		if list[i] == head || l.isDescendant(head, list[i]) {
			return list[i]
		}
	}
	return common.Hash{}
}
