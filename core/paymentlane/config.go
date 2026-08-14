package paymentlane

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

// ContractAddress is the PaymentLane system contract, installed by the Gauss fork. Spelled out
// rather than taken from systemcontracts, which would stop this package being a leaf.
var ContractAddress = common.HexToAddress("0x0000000000000000000000000000000000002007")

// These defaults mirror PaymentLane's `DEFAULT_*` constants. Getter-based reads see them
// after the contract's own fallback logic has run, but the values still matter here for
// rules tests and for keeping the shipped tuple legible in one place.
const (
	defaultMinRatio      = 200       // 2%
	defaultMaxRatio      = 800       // 8%
	defaultExpandTrigger = 8_000     // 80%
	defaultShrinkTrigger = 7_000     // 70%
	defaultExpandStep    = 200       // 2%
	defaultShrinkStep    = 50        // 0.5%
	defaultMinGas        = 2_000_000 // gas
	defaultMaxGas        = 8_000_000 // gas
)

// Params is the eight governable values of BEP-703 section 3.6 as one decoded tuple.
// The first six are parts per RatioDenom, MinGas and MaxGas absolute gas.
type Params struct {
	MinRatio      uint64
	MaxRatio      uint64
	ExpandTrigger uint64
	ShrinkTrigger uint64
	ExpandStep    uint64
	ShrinkStep    uint64
	MinGas        uint64
	MaxGas        uint64
}

func (p Params) String() string {
	return fmt.Sprintf("minRatio %d maxRatio %d expandTrigger %d shrinkTrigger %d expandStep %d shrinkStep %d minGas %d maxGas %d",
		p.MinRatio, p.MaxRatio, p.ExpandTrigger, p.ShrinkTrigger, p.ExpandStep, p.ShrinkStep, p.MinGas, p.MaxGas)
}
