package surfstore

import (
	context "context"
	"fmt"
	"sync"
)

type BlockStore struct {
	lock     sync.Mutex
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	bs.lock.Lock()
	defer bs.lock.Unlock()
	block := new(Block)
	store := bs.BlockMap[blockHash.Hash]
	if store != nil {
		block.BlockData = store.BlockData
		block.BlockSize = store.BlockSize
	}
	return block, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	bs.lock.Lock()
	defer bs.lock.Unlock()
	hash := GetBlockHashString(block.BlockData)
	success := new(Success)
	success.Flag = true
	if bs.BlockMap[hash] != nil {
		fmt.Println("Hash conflict")
	}
	bs.BlockMap[hash] = block
	return success, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	blockHash := new(BlockHashes)
	for _, hash := range blockHashesIn.Hashes {
		if bs.BlockMap[hash] != nil {
			blockHash.Hashes = append(blockHash.Hashes, hash)
		}
	}
	return blockHash, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
