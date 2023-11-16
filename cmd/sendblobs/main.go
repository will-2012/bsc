package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"log"
	"math/big"
	"os"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/crypto/kzg4844"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/params"

	"github.com/consensys/gnark-crypto/ecc/bls12-381/fr"
	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
	"github.com/holiman/uint256"
	"github.com/pkg/errors"
)

const prefix = "SEND_BLOBS"
const BytesPerBlob = 131072

var (
	emptyBlob          = kzg4844.Blob{}
	emptyBlobCommit, _ = kzg4844.BlobToCommitment(emptyBlob)
	emptyBlobProof, _  = kzg4844.ComputeBlobProof(emptyBlob, emptyBlobCommit)
	emptyBlobVHash     = blobHash(emptyBlobCommit)
)

func blobHash(commit kzg4844.Commitment) common.Hash {
	hasher := sha256.New()
	hasher.Write(commit[:])
	hash := hasher.Sum(nil)

	var vhash common.Hash
	vhash[0] = params.BlobTxHashVersion
	copy(vhash[1:], hash[1:])

	return vhash
}

// send-blobs <url-without-auth> <transactions-send-formula 10x1,4x2,3x6> <secret-key> <receiver-address>
// send-blobs http://localhost:8545 5 0x0000000000000000000000000000000000000000000000000000000000000000 0x000000000000000000000000000000000000f1c1 100 100
// sendblobs http://localhost:8545 5 9b28f36fbd67381120752d6172ecdcf10e06ab2d9a1367aac00cdcd6ac7855d3 0x000000000000000000000000000000000000f1c1 100 100
// ./sendblobs http://localhost:8545 1 9b28f36fbd67381120752d6172ecdcf10e06ab2d9a1367aac00cdcd6ac7855d3 0x000000000000000000000000000000000000f1c1 1 1

// ./sendblobs http://localhost:8545 1 d6adea2a444b376821d6e8dd5c7f2a665e8b15a5ffb3d346ab1b0d2133eb9caa 0x000000000000000000000000000000000000f1c1 1 1

func main() {
	logger := log.New(os.Stdout, prefix, log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
	if err := run(logger); err != nil {
		log.Fatalf(err.Error())
	}
}

func run(logger *log.Logger) error {
	fmt.Println(os.Args)
	rpcURL := os.Args[1]
	blobTxCounts := parseBlobTxCounts(os.Args[2])
	fmt.Println(blobTxCounts)
	privateKeyString := os.Args[3]
	receiver := common.HexToAddress(os.Args[4])

	maxFeePerDataGas := uint64(1)

	if len(os.Args) > 4 {
		var err error
		maxFeePerDataGas, err = strconv.ParseUint(os.Args[5], 10, 64)
		if err != nil {
			return errors.Wrap(err, "parsing maxFeePerDataGas on argument pos 5")
		}
	}

	feeMultiplier := uint64(1)
	if len(os.Args) > 5 {
		var err error
		feeMultiplier, err = strconv.ParseUint(os.Args[6], 10, 64)
		if err != nil {
			return errors.Wrap(err, "parsing maxFeePerDataGas on argument pos 6")
		}
	}

	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return errors.Wrap(err, "connecting to eth client")
	}

	privateKeyECDSA, err := crypto.HexToECDSA(privateKeyString)
	if err != nil {
		return errors.Wrap(err, "parsing private key")
	}

	ctx := context.Background()

	nonce, err := client.PendingNonceAt(ctx, crypto.PubkeyToAddress(privateKeyECDSA.PublicKey))
	if err != nil {
		return errors.Wrap(err, "getting nonce")
	}
	//nonce++

	chainID, err := client.ChainID(ctx)
	if err != nil {
		return errors.Wrap(err, "retreiving chain id")
	}

	for _, btxc := range blobTxCounts {
		txCount, blobCount := btxc.count, btxc.perTx
		fmt.Println("txCount, blobCount ", txCount, blobCount)

		for txCount > 0 {

			gasPrice, err := client.SuggestGasPrice(ctx)
			if err != nil {
				return errors.Wrap(err, "retrieving gas price")
			}

			maxPriorityFeePerGas, err := client.SuggestGasTipCap(ctx)
			if err != nil {
				return errors.Wrap(err, "retrieving gas tip cap")
			}

			txCount--
			unsignedTx := &types.BlobTx{
				ChainID:    uint256.MustFromBig(chainID),
				Nonce:      nonce,
				GasTipCap:  uint256.NewInt(maxPriorityFeePerGas.Uint64()),
				GasFeeCap:  uint256.NewInt(gasPrice.Mul(gasPrice, new(big.Int).SetUint64(feeMultiplier)).Uint64()),
				Gas:        21000,
				BlobFeeCap: uint256.NewInt(maxFeePerDataGas),
				BlobHashes: []common.Hash{emptyBlobVHash},
				Value:      uint256.NewInt(100),
				Sidecar: &types.BlobTxSidecar{
					Blobs:       []kzg4844.Blob{emptyBlob},
					Commitments: []kzg4844.Commitment{emptyBlobCommit},
					Proofs:      []kzg4844.Proof{emptyBlobProof},
				},
				To: receiver,
			}
			typeTx := types.NewTx(unsignedTx)
			// todo convert unsigned to signed -> done
			signer := types.LatestSignerForChainID(chainID)
			signedTx, err := types.SignTx(typeTx, signer, privateKeyECDSA)
			if err != nil {
				return errors.Wrapf(err, "could not sign tx: %+v", signedTx)
			}
			err = client.SendTransaction(ctx, signedTx)
			if err != nil {
				return errors.Wrapf(err, "sending signed tx: %+v", signedTx)
			}

			nonce++
		}
	}

	return nil
}

func parseBlobTxCounts(blobTxCountsStr string) []blobTxCount {
	blobTxCountsStrArr := strings.Split(blobTxCountsStr, ",")
	blobTxCounts := make([]blobTxCount, len(blobTxCountsStrArr))

	for i, btxcStr := range blobTxCountsStrArr {
		if strings.Contains(btxcStr, "x") {
			parts := strings.Split(btxcStr, "x")
			count, _ := strconv.Atoi(parts[0])
			perTx, _ := strconv.Atoi(parts[1])
			blobTxCounts[i] = blobTxCount{count, perTx}
		} else {
			count, _ := strconv.Atoi(btxcStr)
			blobTxCounts[i] = blobTxCount{count, 1}
		}
	}

	return blobTxCounts
}

type blobTxCount struct {
	count int
	perTx int
}

func randBlob() kzg4844.Blob {
	var blob kzg4844.Blob
	for i := 0; i < len(blob); i += gokzg4844.SerializedScalarSize {
		fieldElementBytes := randFieldElement()
		copy(blob[i:i+gokzg4844.SerializedScalarSize], fieldElementBytes[:])
	}
	return blob
}

func randFieldElement() [32]byte {
	bytes := make([]byte, 32)
	_, err := rand.Read(bytes)
	if err != nil {
		panic("failed to get random field element")
	}
	var r fr.Element
	r.SetBytes(bytes)

	return gokzg4844.SerializeScalar(r)
}
