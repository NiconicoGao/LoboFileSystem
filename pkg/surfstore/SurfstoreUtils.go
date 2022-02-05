package surfstore

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

type FileChopInfo struct {
	Filename string
	FileData map[string][]byte
	FileHash []string
	Version  int32
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// Get FileInfo
	realFileInfo, err := NewIndexFile(client.BaseDir, client.BlockSize)
	if err != nil {
		panic(err)
	}

	localFileInfo, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		fmt.Println("Index not found, using generated data")
	}

	remoteFileInfo, err := client.GetFileInfoMap()
	if err != nil {
		panic(err)
	}

	// Delete file
	realFilenameMap := make(map[string]bool)
	for _, realInfo := range realFileInfo {
		realFilenameMap[realInfo.Filename] = true
	}
	deleteTask := make([]*FileMetaData, 0)
	for _, localInfo := range localFileInfo {
		if !realFilenameMap[localInfo.Filename] && len(localInfo.BlockHashList) > 0 && localInfo.BlockHashList[0] != "0" {
			fileMeta := new(FileMetaData)
			fileMeta.Filename = localInfo.Filename
			fileMeta.BlockHashList = []string{"0"}
			fileMeta.Version = localInfo.Version + 1
			deleteTask = append(deleteTask, fileMeta)
		}
	}

	deleteList, _ := deleteData(client, deleteTask)
	for _, fileInfo := range deleteList {
		localFileInfo[fileInfo.Filename] = fileInfo
	}

	// New file
	localFilenameMap := make(map[string]bool)
	for _, realInfo := range localFileInfo {
		localFilenameMap[realInfo.Filename] = true
	}
	newTask := make([]*FileChopInfo, 0)
	for _, realInfo := range realFileInfo {
		if !localFilenameMap[realInfo.Filename] {
			fileChopInfo := new(FileChopInfo)
			fileChopInfo.Filename = realInfo.Filename
			fileChopInfo.Version = 1
			fileChopInfo.FileHash, fileChopInfo.FileData = GetBlockFromFilename(client.BaseDir, realInfo.Filename, client.BlockSize)
			newTask = append(newTask, fileChopInfo)
		}
	}

	successList, err := uploadData(client, newTask)
	if err != nil {
		fmt.Println(err)
	}

	for _, fileMeta := range successList {
		localFileInfo[fileMeta.Filename] = fileMeta
		fmt.Printf("File %v uploaded\n", fileMeta.Filename)
	}

	// Upload
	uploadTask := make([]*FileChopInfo, 0)
	for filename, fileInfo := range localFileInfo {
		if !realFilenameMap[fileInfo.Filename] {
			continue
		}

		fileHash, fileChopInfo := GetBlockFromFilename(client.BaseDir, filename, client.BlockSize)
		hashEqual := testEq(fileHash, fileInfo.BlockHashList)
		remoteInfo := remoteFileInfo[filename]
		if hashEqual || (remoteInfo != nil && remoteInfo.Version != fileInfo.Version) {
			continue
		}

		uploadTask = append(uploadTask, &FileChopInfo{
			Filename: filename,
			FileData: fileChopInfo,
			Version:  fileInfo.Version + 1,
			FileHash: fileHash,
		})

	}

	successList, err = uploadData(client, uploadTask)
	if err != nil {
		fmt.Println(err)
	}

	for _, fileMeta := range successList {
		localFileInfo[fileMeta.Filename] = fileMeta
		fmt.Printf("File %v uploaded\n", fileMeta.Filename)
	}

	// Get FileInfo Again
	remoteFileInfo, err = client.GetFileInfoMap()
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	// download
	downloadTask := make([]*FileMetaData, 0)
	deleteTask = make([]*FileMetaData, 0)
	for filename, fileInfo := range remoteFileInfo {
		localFile := localFileInfo[filename]
		if localFile != nil && localFile.Version >= fileInfo.Version {
			continue
		}

		if len(fileInfo.BlockHashList) == 1 && fileInfo.BlockHashList[0] == "0" {
			deleteTask = append(deleteTask, fileInfo)
			continue
		}

		downloadTask = append(downloadTask, fileInfo)
	}

	downloadList, err := downloadData(client, downloadTask)
	if err != nil {
		fmt.Println(err)
	}

	fileMetas := fileChopToFileMeta(downloadList)
	for _, fileInfo := range fileMetas {
		localFileInfo[fileInfo.Filename] = fileInfo
		fmt.Printf("File %v download\n", fileInfo.Filename)
	}

	deleteLocalList, _ := deleteLocalData(client, deleteTask)
	for _, deleteInfo := range deleteLocalList {
		localFileInfo[deleteInfo.Filename] = deleteInfo
		fmt.Printf("File %v deleted\n", deleteInfo.Filename)
	}

	err = WriteMetaFile(localFileInfo, client.BaseDir)
	if err != nil {
		panic(err)
	}
}

func GetBlockFromFilename(baseDir, filename string, blocksize int) ([]string, map[string][]byte) {
	filePath, _ := filepath.Abs(ConcatPath(baseDir, filename))
	file, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Println(err)
	}
	data := make(map[string][]byte)
	hashList := make([]string, 0)
	start := 0
	for ; start < len(file)-blocksize; start += blocksize {
		s := file[start : start+blocksize]
		h := GetBlockHashString(s)
		data[h] = s
		hashList = append(hashList, h)
	}

	s := file[start:]
	h := GetBlockHashString(s)
	data[h] = s
	hashList = append(hashList, h)
	return hashList, data
}

func uploadData(client RPCClient, fileChopInfo []*FileChopInfo) ([]*FileMetaData, error) {
	fileInfos := fileChopToFileMeta(fileChopInfo)
	for _, fileInfo := range fileInfos {
		fmt.Printf("Modified file get %v %v %v\n", fileInfo.Filename, fileInfo.Version, len(fileInfo.BlockHashList))
	}

	successInfo := make([]*FileMetaData, 0)
	if len(fileChopInfo) == 0 {
		return successInfo, nil
	}

	chop := make(map[string][]byte)
	for _, fileInfo := range fileChopInfo {
		for k, v := range fileInfo.FileData {
			chop[k] = v
		}
	}

	hashSet := make([]string, 0)
	for k := range chop {
		hashSet = append(hashSet, k)
	}

	blockAddr, err := client.GetBlockStoreAddr()
	if err != nil {
		return nil, err
	}

	existChop, err := client.HasBlocks(hashSet, blockAddr)
	if err != nil {
		return nil, err
	}

	modifiedChop := sliceSub(hashSet, existChop)

	for _, hash := range modifiedChop {
		block := new(Block)
		block.BlockData = chop[hash]
		block.BlockSize = int32(len(chop[hash]))
		_, err := client.PutBlock(block, blockAddr)
		if err != nil {
			return nil, err
		}

	}

	for _, fileInfo := range fileInfos {
		version, err := client.UpdateFile(fileInfo)
		if err != nil {
			fmt.Printf("Error in upload data %v", err.Error())
			continue
		}

		fileInfo.Version = version
		successInfo = append(successInfo, fileInfo)
	}
	return successInfo, nil
}

func deleteData(client RPCClient, deleteTask []*FileMetaData) ([]*FileMetaData, error) {
	successList := make([]*FileMetaData, 0)
	for _, fileInfo := range deleteTask {
		fmt.Printf("Delete file get %v %v\n", fileInfo.Filename, fileInfo.Version)
		version, err := client.UpdateFile(fileInfo)
		if err != nil {
			fmt.Printf("Error in delete data %v", err.Error())
			continue
		}
		fileInfo.Version = version
		successList = append(successList, fileInfo)
	}

	return successList, nil
}

func deleteLocalData(client RPCClient, deleteTask []*FileMetaData) ([]*FileMetaData, error) {
	for _, fileInfo := range deleteTask {
		fmt.Printf("Deleted file get %v %v\n", fileInfo.Filename, fileInfo.Version)
		filepath, _ := filepath.Abs(ConcatPath(client.BaseDir, fileInfo.Filename))
		if fileExists(filepath) {
			os.Remove(filepath)
		}
	}
	return deleteTask, nil

}

func downloadData(client RPCClient, downloadTask []*FileMetaData) ([]*FileChopInfo, error) {
	successInfo := make([]*FileChopInfo, 0)
	if len(downloadTask) == 0 {
		return successInfo, nil
	}

	blockAddr, err := client.GetBlockStoreAddr()
	if err != nil {
		return nil, err
	}

	for _, fileMeta := range downloadTask {
		fmt.Printf("Download file get %v %v\n", fileMeta.Filename, fileMeta.Version)
		fileChop := new(FileChopInfo)
		fileChop.Filename = fileMeta.Filename
		fileChop.Version = fileMeta.Version
		fileChop.FileHash = fileMeta.BlockHashList
		fileChop.FileData = make(map[string][]byte)
		for _, hash := range fileMeta.BlockHashList {
			block := new(Block)
			err := client.GetBlock(hash, blockAddr, block)
			if err != nil {
				panic(err)
			}
			fileChop.FileData[hash] = block.BlockData
		}

		filepath, _ := filepath.Abs(ConcatPath(client.BaseDir, fileChop.Filename))
		f, err := os.Create(filepath)
		if err != nil {
			panic(err)
		}
		for _, hash := range fileChop.FileHash {
			f.Write(fileChop.FileData[hash])
		}
		f.Close()

		successInfo = append(successInfo, fileChop)

	}

	return successInfo, nil
}

func testEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func fileChopToFileMeta(a []*FileChopInfo) []*FileMetaData {
	fileMetaList := make([]*FileMetaData, 0)
	for _, fileChop := range a {
		fileInfo := new(FileMetaData)
		fileInfo.Filename = fileChop.Filename
		fileInfo.Version = fileChop.Version
		fileInfo.BlockHashList = fileChop.FileHash
		fileMetaList = append(fileMetaList, fileInfo)
	}
	return fileMetaList
}

func NewIndexFile(baseDir string, blocksize int) (map[string]*FileMetaData, error) {
	fileMetaMap := make(map[string]*FileMetaData)
	metaFilePath, _ := filepath.Abs(baseDir)
	files, err := ioutil.ReadDir(metaFilePath)
	if err != nil {
		return fileMetaMap, err
	}

	for _, info := range files {
		if !info.IsDir() && info.Name() != "index.txt" {
			file := new(FileMetaData)
			file.Filename = info.Name()
			file.Version = 1
			file.BlockHashList, _ = GetBlockFromFilename(baseDir, file.Filename, blocksize)
			fileMetaMap[file.Filename] = file
		}
	}
	return fileMetaMap, nil
}

func sliceSub(all, sub []string) []string {
	ans := make([]string, 0)
	for _, s := range all {
		exist := false
		for _, no := range sub {
			if s == no {
				exist = true
			}
		}

		if !exist {
			ans = append(ans, s)
		}

	}
	return ans
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
