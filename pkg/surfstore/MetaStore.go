package surfstore

import (
	context "context"
	"errors"
	"fmt"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	lock           sync.Mutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	fileInfos := new(FileInfoMap)
	fileInfoMap := make(map[string]*FileMetaData)
	m.lock.Lock()
	for k, v := range m.FileMetaMap {
		fileInfoMap[k] = v
	}
	m.lock.Unlock()
	fileInfos.FileInfoMap = fileInfoMap
	// fmt.Printf("Current file map : %v", fileInfoMap)
	return fileInfos, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	version := new(Version)
	version.Version = fileMetaData.Version
	m.lock.Lock()
	defer m.lock.Unlock()

	oldInfo := m.FileMetaMap[fileMetaData.Filename]
	if oldInfo == nil {
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		return version, nil
	}

	if oldInfo.Version+1 != fileMetaData.Version {
		return version, errors.New(fmt.Sprintf("Version Error. Should %v but got %v", oldInfo.Version+1, fileMetaData.Version))
	}

	m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	return version, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	resp := new(BlockStoreAddr)
	resp.Addr = m.BlockStoreAddr
	return resp, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
