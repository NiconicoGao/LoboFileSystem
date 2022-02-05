package surfstore

import (
	context "context"
	"fmt"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddr string
	BaseDir       string
	BlockSize     int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Println(err)
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		fmt.Println(err)
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string) (bool, error) {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Println(err)
		return false, err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.PutBlock(ctx, block)
	if err != nil {
		fmt.Println(err)
		conn.Close()
		return false, err
	}

	// close the connection
	return b.Flag, conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string) ([]string, error) {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		fmt.Println(err)
		conn.Close()
		return nil, err
	}

	// close the connection
	return b.Hashes, conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap() (map[string]*FileMetaData, error) {
	// connect to the server
	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	c := NewMetaStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetFileInfoMap(ctx, new(emptypb.Empty))
	if err != nil {
		fmt.Println(err)
		conn.Close()
		return nil, err
	}

	// close the connection
	return b.FileInfoMap, conn.Close()
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData) (int32, error) {
	// connect to the server
	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	c := NewMetaStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.UpdateFile(ctx, fileMetaData)
	if err != nil && strings.Contains(err.Error(), "Version") {
		conn.Close()
		return 0, err
	}

	if err != nil {
		fmt.Println(err)
		conn.Close()
		return 0, err
	}

	// close the connection
	return b.Version, conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreAddr() (string, error) {
	// connect to the server
	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Println(err)
		return "", err
	}
	c := NewMetaStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlockStoreAddr(ctx, new(emptypb.Empty))
	if err != nil {
		fmt.Println(err)
		conn.Close()
		return "", err
	}

	// close the connection
	return b.Addr, conn.Close()
}

// This line guarantees all method for RPCClient are implemented
// var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(hostPort, baseDir string, blockSize int) RPCClient {
	if blockSize == 0 {
		panic("Blocksize cannot be 0")
	}

	return RPCClient{
		MetaStoreAddr: hostPort,
		BaseDir:       baseDir,
		BlockSize:     blockSize,
	}
}
