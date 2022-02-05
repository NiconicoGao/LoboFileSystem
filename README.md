# Lobo Online Filesystem
Lobo filesystem is an online file system where user upload and download their personal files. The system contains two RPC servers and a client. Meta server is responsable for file description storage and block server for file content storage, so that the client can either upload and download their files. 


## Feature
- Reliable and scaleable meta server by **RAFT**
- Break file into blocks and store by hash. 

## Usage
We also provide a make file for you to run the BlockStore and MetaStore servers.
1. Run both block and meta servers (**listens to localhost on port 8081**):
```shell
make run-both
```

2. Run block server (**listens to localhost on port 8081**):
```shell
make run-blockstore
```

3. Run meta server (**listens to localhost on port 8080**):
```shell
make run-metastore
```

4. Run Client
```shell
go run cmd/SurfstoreClientExec/main.go -d <meta_addr:port> <base_dir> <block_size>
```
The client upload all files in base_dir and download all files on server. Files on server will overwrite files in local. 

## Autuor
Lobo bunny

