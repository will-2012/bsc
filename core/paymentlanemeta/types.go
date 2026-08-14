package paymentlanemeta

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/paymentlane"
)

const pageSize uint64 = 128

// Meta is the parent-derived lane metadata needed before the block executes.
type Meta struct {
	Params paymentlane.Params
	Listed map[common.Address]struct{}
}
