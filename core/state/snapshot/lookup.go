package snapshot

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// lookup is an internal help structure to quickly identify
type lookup struct {
	// todo: lock??
	state2LayerRoots map[string][]common.Hash
}

// newLookup initializes the lookup structure.
func newLookup(head Snapshot) *lookup {
	var (
		current = head
		layers  []Snapshot
	)
	for current != nil {
		layers = append(layers, current)
		current = current.Parent()
	}
	l := new(lookup)
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
	return l
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
		//if list[i] == head || l.descendant(head, list[i]) {
		if list[i] == head {
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
		//if list[i] == head || l.descendant(head, list[i]) {
		if list[i] == head {
			return list[i]
		}
	}
	return common.Hash{}
}
