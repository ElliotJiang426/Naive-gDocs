package main

import (
	"errors"
	"fmt"
	common "gfs/Common"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

const (
	dataNodeNum = 3
)

type Master struct {
	sync.Mutex

	Conn       *zk.Conn
	FileInfos  map[string](map[string](*common.ChunkInfo))
	WorkerLoad map[string]int
}

/**
 * connect
 * Master Node 连接到 zookeeper
 */
func MasterConnect() (*zk.Conn, error) {
	// 创建监听的option，用于初始化zk
	// eventCallbackOption := zk.WithEventCallback(callback)
	// 创建zk连接地址
	hosts := common.MasterZkHosts
	hostPro := new(zk.DNSHostProvider)
	err := hostPro.Init(hosts)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	server, retryStart := hostPro.Next() //获得host
	fmt.Println(server, retryStart)

	//连接成功后会调用
	hostPro.Connected()

	// 连接zk
	conn, _, err := zk.Connect(hosts, time.Second*5)
	if err != nil {
		fmt.Printf("MASTER: 连接 zookeeper 失败，error: %v\n", err)
		return nil, err
	}
	return conn, err
}

/**
 * GetNodeList
 * 获取 chunk 对应的 nodelist 列表
 * IN: 	path -- 文件名
 *		chunk -- chunk 名
 * OUT: chunkInfo -- chunk 的 version 和 nodelist 信息
 *		error -- 报错信息
 */
func (ms *Master) GetNodeList(path string, chunk string) (*common.ChunkInfo, error) {
	if chunks, ok := ms.FileInfos[path]; ok {
		if chunkInfo, ok := chunks[chunk]; ok {
			return chunkInfo, nil
		} else {
			err := errors.New("GetNodeList: chunk doesn't exist")
			return nil, err
		}
	} else {
		err := errors.New("GetNodeList: file doesn't exist")
		return nil, err
	}
}

/**
 * GetHeartbeat
 * 获取当前 Worker 存储 chunk 的信息
 * IN:
 * OUT:
 */
func (ms *Master) GetHeartbeat() {
	ms.FileInfos = make(map[string](map[string](*common.ChunkInfo)))
	var wg sync.WaitGroup

	for ip, _ := range ms.WorkerLoad {
		wg.Add(1)
		go func(ip string) {
			defer wg.Done()

			var res common.HeartbeatReply
			var args common.RPCArgs
			err := common.Call(ip, "Worker.Heartbeat", &args, &res)
			if err != nil {
				// 可能无法连接到对应的 worker，需要从可用的 worker 列表中去除
				fmt.Printf("MASTER: 获取 %s 状态失败, ERROR: %v\n", ip, err)
				ms.Lock()

				delete(ms.WorkerLoad, ip)

				ms.Unlock()
				return
			}
			fmt.Printf("MASTER: 获取 %s 状态成功\n", ip)

			// 记录负载
			ms.Lock()
			ms.WorkerLoad[ip] = res.ChunkLoad
			ms.Unlock()
			fmt.Printf("MASTER: %s 的负载为 %d 个 chunk\n", ip, res.ChunkLoad)

			// 记录存储的节点
			for file, chunks := range res.FileInfos {
				ms.Lock()
				if ms.FileInfos[file] == nil {
					ms.FileInfos[file] = make(map[string](*common.ChunkInfo))
				}
				ms.Unlock()

				for chunk, version := range chunks {
					ms.Lock()
					// 如果没有对应的记录，先初始化结构体
					if ms.FileInfos[file][chunk] == nil {
						ms.FileInfos[file][chunk] = new(common.ChunkInfo)
						ms.FileInfos[file][chunk].Version = -1
					}
					// 如果 version 等于当前的 version 加入路径
					if version == ms.FileInfos[file][chunk].Version {
						ms.FileInfos[file][chunk].NodeList = append(ms.FileInfos[file][chunk].NodeList, ip)
					} else if version > ms.FileInfos[file][chunk].Version {
						// 如果发现更大的 version，更改当前记录的 ip
						ms.FileInfos[file][chunk].NodeList = []string{ip}
						ms.FileInfos[file][chunk].Version = version
					}
					ms.Unlock()
				}
			}
		}(ip)
	}

	wg.Wait()

	for file, chunks := range ms.FileInfos {
		for chunk, chunkInfo := range chunks {
			fmt.Printf("FILE %s CHUNK %s \n", file, chunk)
			fmt.Printf("VERSION: %s \n", strconv.Itoa(chunkInfo.Version))
			fmt.Println("NODELIST: ", chunkInfo.NodeList)
			fmt.Printf("\n")
		}
	}

	fmt.Println("MASTER: nodeList refreshed")
}

/**
 * HeartbeatTicker
 * 用来定时监听 heartbeat
 * IN: ms -- 指向 Master 的指针
 * OUT:
 */
func HeartbeatTicker(ms *Master) {
	ticker := time.NewTicker(common.HeartbeatTime * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		ms.GetHeartbeat()
	}
}

/**
 * startMaster
 * 启动 Master Node 的进程
 */
func StartMaster(ip string) {
	ms := new(Master)

	// 初始化负载记录
	ms.WorkerLoad = make(map[string]int)
	for _, ip := range common.WorkerRPCHosts {
		ms.WorkerLoad[ip] = 0
	}

	// 初始化 nodelist
	ms.GetHeartbeat()
	// 启动 heartbeat 监听
	go HeartbeatTicker(ms)
	var err error
	// 连接到 zookeeper server
	ms.Conn, err = MasterConnect()
	if err != nil {
		fmt.Printf("MASTER: Master Node 启动失败: %v\n", err)
		return
	}

	// rpc := rpc.NewServer()
	rpc.Register(ms) // 注册rpc服务
	rpc.HandleHTTP()

	lis, err := net.Listen("tcp", ip)
	if err != nil {
		log.Fatalln("Fatal error: ", err)
	}

	fmt.Fprintf(os.Stdout, "%s", "Start connection\n")

	http.Serve(lis, nil)
}

/**
 * Register
 * Worker 节点注册
 * IN: ip -- worker 的 ip 地址
 * OUT:
 */
func (ms *Master) Register(args *common.RegisterArgs, _ *struct{}) error {
	ms.Lock()
	ip := args.IP
	load := args.Load

	// 检查节点是否已经注册
	// FIXME: 可能是 map 本身的问题，此处无论是否存在，读到的 ok 都是 true，如果本身不存在，对应会是默认值 0
	_, ok := ms.WorkerLoad[ip]
	if !ok {
		ms.WorkerLoad[ip] = load
		ms.Unlock()
		return errors.New("该 worker 节点已注册")
	}

	// 添加节点
	ms.WorkerLoad[ip] = load
	fmt.Printf("MASTER: 节点 %s 注册成功\n", ip)

	ms.Unlock()
	return nil
}

/**
 * AssignWorker
 * Master 为新创建的 chunk 指定 worker
 * IN:
 * OUT: nodeList -- 指定的 worker 节点 ip 列表
 */
func (ms *Master) AssignWorker() []string {
	nodeList := make([]string, common.NodeForChunk)
	minLoad := 10000
	minLoadWorker := ""

	for i := 0; i < common.NodeForChunk; i++ {
		if i == len(ms.WorkerLoad) {
			break
		}
		for ip, load := range ms.WorkerLoad {
			if load < minLoad {
				exist := false
				// 先检查是否已经选取了
				for _, workerSelected := range nodeList {
					if workerSelected == ip {
						exist = true
						break
					}
				}
				if !exist {
					minLoad = load
					minLoadWorker = ip
				}
			}
		}
		// 添加到备选中
		nodeList[i] = minLoadWorker
		ms.WorkerLoad[minLoadWorker] = ms.WorkerLoad[minLoadWorker] + 1
		minLoad = 10000
		minLoadWorker = ""
	}
	fmt.Println("MASTER: 分配节点成功：", nodeList)

	return nodeList
}

/**
 * GetChunks
 * 获取对应路径的文件的 Chunks 的列表
 * IN: path -- 文件的路径（文件名）
 * OUT: chunks -- chunk 名和 Data node 地址列表的映射
 */
func (ms *Master) GetChunks(args *common.RPCArgs, chunks *([]common.ChunkInfo)) error {
	path := args.Path

	chunkList, _, err := ms.Conn.Children(path)
	if err != nil {
		fmt.Printf("MASTER: Chunk 获取失败, err: %v\n", err)
		return err
	}

	(*chunks) = make([]common.ChunkInfo, len(chunkList)-2) // 剔除 lock 子节点

	i := 0
	for _, chunk := range chunkList {
		if chunk == "lock" || chunk == "auth" {
			continue
		}
		chunkInfo := new(common.ChunkInfo)

		// fill nodeList
		chunkInfo, err := ms.GetNodeList(path, chunk)
		if err != nil {
			fmt.Printf("MASTER: Chunk 获取失败, err: %v\n", err)
			return err
		}
		// fill name
		chunkInfo.Chunk = chunk

		(*chunks)[i] = (*chunkInfo)
		i = i + 1
		fmt.Println("MASTER: chunk ", chunk, " stores in node: ", chunkInfo.NodeList)
	}
	return nil
}

/**
 * GetChunk
 * 获取指定的 chunk 的 nodeList
 * IN: 	path -- 文件的路径
 *		chunk -- chunk 名
 * OUT: chunkInfo -- chunk 的 version 和 nodeList 信息
 */
func (ms *Master) GetChunk(args *common.RPCArgs, chunkInfo *common.ChunkInfo) error {
	path := args.Path
	chunk := args.Data
	// 先获取对应的 nodelist
	masterChunkInfo := new(common.ChunkInfo)
	masterChunkInfo, err := ms.GetNodeList(path, chunk)
	if err != nil {
		return err
	}
	masterChunkInfo.Chunk = chunk
	(*chunkInfo) = (*masterChunkInfo)
	return nil
}

/**
 * OpenFile
 * 打开文件并作权限控制
 * IN: 	path -- 文件的路径
 *		user -- 用户的 IP
 *		flag -- 权限
 * OUT:
 */
 // 
 // Now it's only for httpServer to check whether file exists
func (ms *Master) OpenFile(args *common.OpenCloseArgs, _ *struct{}) error {
	path := args.Path
	// user := args.User
	// flag := args.Flag

	// 先查找文件
	_, err := common.Get(ms.Conn, path)
	if err != nil {
		fmt.Printf("MASTER: 打开的文件 %s 不存在\n", path)
		return err
	}

	// // 检查是否已经打开
	// _, err = common.Get(ms.Conn, path+"/auth/"+user)
	// if err == nil {
	// 	fmt.Printf("MASTER: file %s 已经打开\n", path)
	// 	return err
	// }

	// // 获取 file 锁
	// fileLock, err := common.Lock(ms.Conn, path)
	// if err != nil {
	// 	fmt.Printf("MASTER: 获取锁失败, err: %v\n", err)
	// 	return err
	// }

	// // 设置权限
	// err = common.Add(ms.Conn, path+"/auth/"+user, strconv.Itoa(flag))
	// if err != nil {
	// 	fmt.Printf("MASTER: file %s 打开失败, err: %v\n", path, err)
	// 	return err
	// }

	// // 释放 file 锁
	// err = common.Unlock(ms.Conn, fileLock)
	// if err != nil {
	// 	fmt.Printf("MASTER: 释放锁失败, err: %v\n", err)
	// 	return err
	// }

	return nil
}

/**
 * CloseFile
 * 关闭文件并删除权限
 * IN: 	path -- 文件的路径
 *		user -- 用户的 IP
 * OUT:
 */
func (ms *Master) CloseFile(args *common.OpenCloseArgs, _ *struct{}) error {
	path := args.Path
	user := args.User

	// 先查找文件
	_, err := common.Get(ms.Conn, path)
	if err != nil {
		fmt.Printf("MASTER: 打开的文件 %s 不存在\n", path)
		return err
	}

	// 检查是否已经打开
	_, err = common.Get(ms.Conn, path+"/auth/"+user)
	if err != nil {
		fmt.Printf("MASTER: file %s 没有打开\n", path)
		return err
	}

	// 获取 file 锁
	fileLock, err := common.Lock(ms.Conn, path)
	if err != nil {
		fmt.Printf("MASTER: 获取锁失败, err: %v\n", err)
		return err
	}

	// 设置权限
	common.Del(ms.Conn, path+"/auth/"+user)

	// 释放 file 锁
	err = common.Unlock(ms.Conn, fileLock)
	if err != nil {
		fmt.Printf("MASTER: 释放锁失败, err: %v\n", err)
		return err
	}

	return nil
}

/**
 * GetAuth
 * 检查用户的权限
 * IN: 	path -- 文件的路径
 *		user -- 用户的 IP
 * OUT:
 */
func (ms *Master) GetAuth(args *common.OpenCloseArgs, res *common.AuthReply) error {
	path := args.Path
	user := args.User
	res.Flag = -1

	// 先查找文件
	_, err := common.Get(ms.Conn, path)
	if err != nil {
		fmt.Printf("MASTER: 打开的文件 %s 不存在\n", path)
		return err
	}

	// 检查是否已经打开
	_, err = common.Get(ms.Conn, path+"/auth/"+user)
	if err != nil {
		fmt.Printf("MASTER: file %s 没有权限\n", path)
		return err
	}

	// 获取 file 锁
	fileLock, err := common.Lock(ms.Conn, path)
	if err != nil {
		fmt.Printf("MASTER: 获取锁失败, err: %v\n", err)
		return err
	}

	// 设置权限
	flag, err := common.Get(ms.Conn, path+"/auth/"+user)
	if err != nil {
		fmt.Printf("MASTER: file %s 没有权限\n", path)
		return err
	}
	res.Flag, _ = strconv.Atoi(string(flag))

	// 释放 file 锁
	err = common.Unlock(ms.Conn, fileLock)
	if err != nil {
		fmt.Printf("MASTER: 释放锁失败, err: %v\n", err)
		return err
	}

	return nil
}

/**
 * IsExist
 * 检查文件是否存在
 * IN:	path -- 文件目录
 * OUT:
 */
func (ms *Master) IsExist(args *common.RPCArgs, _ *struct{}) error {
	path := args.Path

	_, err := common.Get(ms.Conn, path)
	if err != nil {
		fmt.Printf("MASTER: 文件 %s 不存在\n", path)
		return err
	}
	fmt.Printf("MASTER: 文件 %s 存在\n", path)
	return nil
}

/**
 * CreateFile
 * 创建文件
 * IN: path -- 文件的路径
 */
func (ms *Master) CreateFile(args *common.RPCArgs, _ *struct{}) error {
	path := args.Path
	// 创建文件 node
	err := common.Add(ms.Conn, path, "")
	if err != nil {
		fmt.Printf("MASTER: file %s 创建失败, err: %v\n", path, err)
		return err
	}

	// 创建 file 的权限子节点
	err = common.Add(ms.Conn, path+"/auth", "")
	if err != nil {
		fmt.Printf("MASTER: file auth 创建失败, err: %v\n", err)
		return err
	}

	// 创建 file 对应的的锁
	err = common.Add(ms.Conn, path+"/lock", "")
	if err != nil {
		fmt.Printf("MASTER: file lock 创建失败, err: %v\n", err)
		return err
	}

	fmt.Printf("MASTER: 文件 %s 创建成功\n", path)
	return nil
}

/**
 * CreateChunk
 * 创建 chunk
 * IN: 	path -- 文件路径
 * 		chunk -- chunk 的名称
 * OUT: nodeList -- chunk 对应的 node 列表
 * 		version -- chunk 的最新版本号
 */
func (ms *Master) CreateChunk(args *common.RPCArgs, res *common.ChunkReply) error {
	path := args.Path
	chunk := args.Data
	// 需要改成实际的 IP:Port
	nodeList := ms.AssignWorker()

	// 获取 file 的锁
	fileLock, err := common.Lock(ms.Conn, path)
	if err != nil {
		fmt.Printf("MASTER: 获取 %s 锁失败, err: %v\n", path, err)
		return err
	}

	// 创建 chunk 对应的的子节点
	err = common.Add(ms.Conn, path+"/"+chunk, "")
	if err != nil {
		fmt.Printf("MASTER: chunk 创建失败, err: %v\n", err)
		res.Version = 0
		res.NodeList = nil
		return err
	}

	// 创建 chunk 对应的的版本
	err = common.Add(ms.Conn, path+"/"+chunk+"/version", "0")
	if err != nil {
		fmt.Printf("MASTER: chunk 创建失败, err: %v\n", err)
		res.Version = 0
		res.NodeList = nil
		return err
	}

	// 创建 chunk 对应的的锁
	err = common.Add(ms.Conn, path+"/"+chunk+"/lock", "")
	if err != nil {
		fmt.Printf("MASTER: chunk lock 创建失败, err: %v\n", err)
		res.Version = 0
		res.NodeList = nil
		return err
	}

	// 创建文件对应的数据结构
	_, ok := ms.FileInfos[path]
	if !ok {
		ms.FileInfos[path] = make(map[string](*common.ChunkInfo))
	}

	chunkInfo := new(common.ChunkInfo)
	chunkInfo.Version = 0
	chunkInfo.NodeList = nodeList
	ms.FileInfos[path][chunk] = chunkInfo

	err = common.Unlock(ms.Conn, fileLock)
	if err != nil {
		fmt.Printf("MASTER: 释放锁失败, err: %v\n", err)
		return err
	}

	fmt.Println("MASTER: chunk 创建成功")
	res.Version = 0
	res.NodeList = nodeList
	return nil
}

/**
 * WriteChunk
 * 获取要写的 chunk 对应的 data node 的 IP:Port
 * IN:  path -- 文件路径
 *		chunk -- chunk 的名字
 * OUT: nodeList -- chunk 对应的 node 列表
 *		version -- chunk 的最新版本号
 */
func (ms *Master) WriteChunk(args *common.RPCArgs, res *common.ChunkReply) error {
	path := args.Path
	chunk := args.Data

	// 先查找文件
	_, err := common.Get(ms.Conn, path)
	if err != nil {
		fmt.Printf("MASTER: write 操作的文件 %s 不存在\n", path)
		return err
	}

	// 先获取对应的 nodelist
	chunkInfo, ok := ms.FileInfos[path][chunk]
	if !ok {
		err := ms.CreateChunk(args, res)
		return err
	}

	// 更新 version，仍然从 zk 获取且更新到 zk
	chunkLock, err := common.Lock(ms.Conn, path+"/"+chunk)
	if err != nil {
		fmt.Printf("MASTER: 获取锁失败, err: %v\n", err)
		return err
	}

	versionStr, err := common.Get(ms.Conn, path+"/"+chunk+"/version")
	if err != nil {
		common.Unlock(ms.Conn, chunkLock)
		fmt.Printf("MASTER: 更新版本失败, err: %v\n", err)
		return err
	}
	version, _ := strconv.Atoi(string(versionStr))
	versionStr = common.Modify(ms.Conn, path+"/"+chunk+"/version", strconv.Itoa(version+1))

	err = common.Unlock(ms.Conn, chunkLock)
	if err != nil {
		fmt.Printf("MASTER: 释放锁失败, err: %v\n", err)
		return err
	}

	chunkInfo.Version, _ = strconv.Atoi(string(versionStr))
	res.Version, _ = strconv.Atoi(string(versionStr))
	res.NodeList = chunkInfo.NodeList
	return nil
}

/**
 * DeleteFile
 * 删除文件及 chunk
 * IN:	path -- 文件路径
 * OUT:
 */
func (ms *Master) DeleteFile(args *common.RPCArgs, _ *struct{}) error {
	path := args.Path

	// 先查找文件，删除内存的映射
	// 先查找文件
	_, err := common.Get(ms.Conn, path)
	if err != nil {
		fmt.Printf("MASTER: write 操作的文件 %s 不存在\n", path)
		return err
	}
	ms.Lock()
	_, ok := ms.FileInfos[path]
	if ok {
		delete(ms.FileInfos, path)
	}
	ms.Unlock()

	_, err = common.Lock(ms.Conn, path)
	if err != nil {
		fmt.Printf("MASTER: 获取锁失败, err: %v\n", err)
		return err
	}

	chunkList, _, err := ms.Conn.Children(path)
	if err != nil {
		fmt.Printf("MASTER: 文件 %s 删除失败, err: %v\n", path, err)
		return err
	}

	for _, chunk := range chunkList {
		if chunk == "lock" || chunk == "auth" {
			continue
		}
		_, err := common.Lock(ms.Conn, path+"/"+chunk)
		if err != nil {
			fmt.Printf("MASTER: 获取锁失败, err: %v\n", err)
			return err
		}

		common.Del(ms.Conn, path+"/"+chunk+"/version")

		common.CancelLock(ms.Conn, path+"/"+chunk+"/lock")
		common.Del(ms.Conn, path+"/"+chunk)
		delete(ms.FileInfos[path], chunk)

		fmt.Printf("MASTER: 文件 %s: %s 删除成功\n", path, chunk)
	}

	// 删除权限
	userList, _, err := ms.Conn.Children(path + "/auth")
	if err != nil {
		fmt.Printf("MASTER: 文件 %s 删除失败, err: %v\n", path, err)
		return err
	}
	for _, user := range userList {
		common.Del(ms.Conn, path+"/auth/"+user)
	}
	common.Del(ms.Conn, path+"/auth")

	common.CancelLock(ms.Conn, path+"/lock")
	common.Del(ms.Conn, path)

	fmt.Printf("MASTER: 文件 %s 删除成功\n", path)
	return nil
}

/**
 * zk watch 回调函数
 */
func callback(event zk.Event) {
	// zk.EventNodeCreated
	// zk.EventNodeDeleted
	fmt.Println("###########################")
	fmt.Println("path: ", event.Path)
	fmt.Println("type: ", event.Type.String())
	fmt.Println("state: ", event.State.String())
	fmt.Println("---------------------------")
}

func main() {
	// 创建zk连接
	// 这个地址是 Master 本机的 RPC 地址
	go StartMaster("127.0.0.1:8095")

	var operation string
	for {
		fmt.Println("输入 quit 退出")
		fmt.Scanln(&operation)
		if operation == "quit" {
			break
		}
	}
}
