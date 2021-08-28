package main

import (
	"bufio"
	"errors"
	"fmt"
	common "gfs/Common"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	bufferSize = 10 // writeBuffer 的大小

)

type Worker struct {
	sync.Mutex
	// Conn         *zk.Conn
	NodeIP       string
	NodePort     string
	IsRecovering bool
	FileInfos    map[string](map[string]int)
	ChunkLoad    int
}

var (
	writeBuffer   map[int](*common.WriteChunkArgs) // 2PC 第一阶段缓存
	nextBufferIdx = 0                              // 下一个可用的 buffer 的下标
	quitCh        chan int
)

/**
 * workerConnect
 * Worker Node 连接到 zookeeper
 */
// func WorkerConnect() (*zk.Conn, error) {
// 	// 创建监听的option，用于初始化zk
// 	// eventCallbackOption := zk.WithEventCallback(callback)
// 	// 创建zk连接地址
// 	host := []string{os.Args[2]}
// 	hostPro := new(zk.DNSHostProvider)
// 	err := hostPro.Init(host)
// 	if err != nil {
// 		fmt.Println(err)
// 		return nil, err
// 	}
// 	server, retryStart := hostPro.Next() //获得host
// 	fmt.Println(server, retryStart)

// 	//连接成功后会调用
// 	hostPro.Connected()

// 	// 连接zk
// 	conn, _, err := zk.Connect(host, time.Second*5)
// 	if err != nil {
// 		fmt.Printf("WORKER: 连接 zookeeper 失败，error: %v\n", err)
// 		return nil, err
// 	}
// 	return conn, err
// }

/**
 * startWorker
 * 启动 worker node
 * IN: ip -- worker node 的 RPC 地址
 */
func StartWorker(ip string) {
	wk := new(Worker)

	// 节点 IP:Port
	colon := strings.Index(ip, ":")
	wk.NodeIP = ip[:colon]
	wk.NodePort = ip[colon+1:]
	wk.IsRecovering = false
	wk.FileInfos = make(map[string](map[string]int))
	wk.ChunkLoad = 0

	// var err error
	// // 连接到 zookeeper server
	// wk.Conn, err = WorkerConnect()
	// if err != nil {
	// 	fmt.Printf("WORKER: Worker Node 启动失败: %v\n", err)
	// 	return
	// }

	wk.Recover()

	// 等到所有 master 都刷新了 node 列表
	time.Sleep(time.Duration(common.HeartbeatTime) * time.Second)
	go CheckpointTicker(wk)

	// rpcs := rpc.NewServer()
	rpc.Register(wk) // 注册rpc服务
	rpc.HandleHTTP()

	lis, err := net.Listen("tcp", ip)
	if err != nil {
		log.Fatalln("Fatal error: ", err)
	}
	fmt.Fprintf(os.Stdout, "%s", "Start connection\n")

	// 向 Master 注册此 worker
	wk.Register()

	http.Serve(lis, nil)
}

/**
 * checkVersion
 * 检查 version 是否匹配
 * IN:  path -- 文件的路径
 * 		chunk -- chunk 的名称
 *		version -- (要操作的)chunk 的版本
 * OUT: 0 -- 版本号相同
 * 		1 -- 大于目标版本
 *		-1 -- 小于目标版本
 */
func (wk *Worker) CheckVersion(path string, chunk string, version int) int {

	var r int
	wk.Lock()
	_, ok := wk.FileInfos[path]
	wk.Unlock()
	if !ok {
		r = 1
	} else {
		wk.Lock()
		oldVersion, ok := wk.FileInfos[path][chunk]
		wk.Unlock()
		if !ok {
			r = 1
		} else {
			r = version - oldVersion
		}
	}
	return r
}

/**
 * Register
 * worker 向 master 发送 RPC 进行注册
 * IN:
 * OUT:
 */
func (wk *Worker) Register() {
	var args common.RegisterArgs
	args.IP = wk.NodeIP + ":" + wk.NodePort
	args.Load = wk.ChunkLoad

	for _, ip := range common.MasterRPCHosts {
		err := common.Call(ip, "Master.Register", &args, nil)
		if err != nil {
			// 可能无法连接到对应的 Master
			fmt.Printf("Worker: 注册到 %s 失败, ERROR: %v\n", ip, err)
		}
	}
}

/**
 * Heartbeat
 * 获取 Worker 储存的 chunk 信息
 * IN:
 * OUT:
 */
func (wk *Worker) Heartbeat(_ *struct{}, res *common.HeartbeatReply) error {
	// res.FileInfos = make(map[string](map[string]int))
	// fileList, _, err := wk.Conn.Children("/")
	// if err != nil {
	// 	fmt.Printf("WORKER: File 获取失败, err: %v\n", err)
	// 	return err
	// }

	// for _, file := range fileList {
	// 	if file == "zookeeper" {
	// 		continue
	// 	}
	// 	path := "/" + file
	// 	chunkList, _, err := wk.Conn.Children(path)
	// 	if err != nil {
	// 		fmt.Printf("WORKER: Chunk 获取失败, err: %v\n", err)
	// 		return err
	// 	}

	// 	(res.FileInfos)[path] = make(map[string]int)
	// 	for _, chunk := range chunkList {
	// 		// fill nodeList
	// 		version, err := common.Get(wk.Conn, path+"/"+chunk+"/version")
	// 		if err != nil {
	// 			fmt.Printf("WOEKER: version 获取失败, err: %v\n", err)
	// 			return err
	// 		}
	// 		(res.FileInfos)[path][chunk], _ = strconv.Atoi(string(version))
	// 	}
	// }
	res.FileInfos = wk.FileInfos
	res.ChunkLoad = wk.ChunkLoad
	fmt.Println("WOEKER: Heartbeat 获取成功")

	return nil
}

/**
 * TrackLog
 * 记录操作的 Log
 * IN:	logText -- 需要记录的 log 文本
 * OUT:
 */
func (wk *Worker) TrackLog(logText string) error {
	if wk.IsRecovering == true {
		return nil
	}
	// 打开文件
	file, err := os.OpenFile("../log/log_"+wk.NodePort+".txt", os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf("WORKER: 打开 Log 文件错误, err: %v\n", err)
		// 创建文件
		file, err = os.Create("../log/log_" + wk.NodePort + ".txt")
		// 判断是否出错
		if err != nil {
			fmt.Printf("WORKER: 创建 Log 文件错误 err: %v\n", err)
			return err
		}
	}
	defer file.Close()

	_, err = file.Seek(0, io.SeekEnd)
	if err != nil {
		fmt.Printf("WORKER: 文件 seek 错误 err: %v\n", err)
		return err
	}

	// 写入文件
	writer := bufio.NewWriter(file)
	writer.WriteString(logText)
	// bufio.NewWriter 是带缓冲的写,如果不调用 Flush 方法, 那么数据不会写入文件
	writer.Flush()

	return nil
}

/**
 * TrackMetaData
 * 将 chunk 的 metadata 记录到内存
 * IN: 	path -- 文件路径
 * 		chunk -- chunk 名
 * 		version -- chunk 版本
 * OUT:
 */
func (wk *Worker) TrackMetaData(path string, chunk string, version int) {
	wk.Lock()
	_, ok := wk.FileInfos[path]
	if !ok {
		wk.FileInfos[path] = make(map[string]int)

		err := os.MkdirAll("../data/"+wk.NodePort+path, os.ModePerm)
		// 判断是否出错
		if err != nil {
			fmt.Printf("WORKER: 创建文件错误 err: %v\n", err)
			return
		}
		fmt.Printf("WORKER: 文件 %s 创建成功\n", path)
	}

	_, ok = wk.FileInfos[path][chunk]
	if !ok {
		wk.ChunkLoad = wk.ChunkLoad + 1
	}
	wk.FileInfos[path][chunk] = version
	wk.Unlock()
}

/**
 * Checkpoint
 * 检查点，用于压缩 Log
 * IN:
 * OUT:
 */
func (wk *Worker) Checkpoint() {
	wk.Lock()
	data := "CHECKPOINT|"
	for file, chunks := range wk.FileInfos {
		for chunk, version := range chunks {
			fmt.Printf("FILE %s CHUNK %s \n", file, chunk)
			fmt.Printf("VERSION: %s \n", strconv.Itoa(version))
			fmt.Printf("\n")
			data = data + file + "|" + chunk + "|" + strconv.Itoa(version) + "|"
		}
	}
	data = data + "END\n"

	// 创建文件
	file, err := os.Create("../log/log_" + wk.NodePort + "_new.txt")
	// 判断是否出错
	if err != nil {
		fmt.Printf("WORKER: 创建 Log 文件错误 err: %v\n", err)
		// 删除旧 Log
		err = os.Remove("../log/log_" + wk.NodePort + "_new.txt")
		// 判断是否出错
		if err != nil {
			fmt.Printf("WORKER: Log 文件删除错误 err: %v\n", err)
			return
		}
		file, err = os.Create("../log/log_" + wk.NodePort + "_new.txt")
	}

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		fmt.Printf("WORKER: 文件 seek 错误 err: %v\n", err)
		return
	}

	// 写入文件
	writer := bufio.NewWriter(file)
	writer.WriteString(data)
	// bufio.NewWriter 是带缓冲的写,如果不调用 Flush 方法, 那么数据不会写入文件
	writer.Flush()
	file.Close()

	// 更名
	err = os.Rename("../log/log_"+wk.NodePort+".txt", "../log/log_"+wk.NodePort+"_old.txt")
	// 判断是否出错
	if err != nil {
		fmt.Printf("WORKER: old Log 文件重命名错误 err: %v\n", err)
	}
	err = os.Rename("../log/log_"+wk.NodePort+"_new.txt", "../log/log_"+wk.NodePort+".txt")
	// 判断是否出错
	if err != nil {
		fmt.Printf("WORKER: new Log 文件重命名错误 err: %v\n", err)
	}

	// 删除旧 Log
	err = os.Remove("../log/log_" + wk.NodePort + "_old.txt")
	// 判断是否出错
	if err != nil {
		fmt.Printf("WORKER: Log 文件删除错误 err: %v\n", err)
	}

	fmt.Println("WORKER: Checkpoint 记录成功")

	wk.Unlock()
}

/**
 * CheckpointTicker
 * 用来定时记录 checkpoint
 * IN: wk -- 指向 Worker 的指针
 * OUT:
 */
func CheckpointTicker(wk *Worker) {
	ticker := time.NewTicker(common.CheckpointTime * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		wk.Checkpoint()
	}
}

/**
 * CreateFile
 * 创建文件(已废弃)
 * IN: path -- 文件的路径
 */
// func (wk *Worker) CreateFile(args *common.CreateFileArgs, _ *struct{}) error {
// 	path := args.Path

// 	wk.Lock()
// 	_, ok := wk.FileInfos[path]
// 	if !ok {
// 		// 写入 log
// 		var logText string
// 		logText = "CREATE|" + path + "|COMMIT\n"
// 		err := wk.TrackLog(logText)
// 		if err != nil {
// 			fmt.Printf("WORKER: 写入 Log 失败, err: %v\n", err)
// 			return err
// 		}

// 		wk.FileInfos[path] = make(map[string]int)
// 		err = os.MkdirAll("../data/"+wk.NodePort+path, os.ModePerm)
// 		// 判断是否出错
// 		if err != nil {
// 			fmt.Printf("WORKER: 创建文件错误 err: %v\n", err)
// 			wk.Unlock()
// 			return err
// 		}

// 		fmt.Printf("WORKER: 文件 %s 创建成功\n", path)
// 		wk.Unlock()
// 		return err
// 	}

// 	err := errors.New("文件已存在")
// 	fmt.Printf("WORKER: 创建文件错误 err: %v\n", err)
// 	wk.Unlock()
// 	return err
// }

/**
 * CreateChunk
 * 新建一个 chunk
 * IN:	path -- 文件的路径
 * 		chunk -- chunk 的名称
 */
// func (wk *Worker) CreateChunk(args *common.WriteChunkArgs, _ *struct{}) error {
// 	path := args.Path
// 	chunk := args.Chunk

// 	// 创建 chunk 对应的的子节点
// 	err := common.Add(wk.Conn, path+"/"+chunk, "")
// 	if err != nil {
// 		err = wk.CreateFile(args, nil)
// 		if err != nil {
// 			fmt.Printf("WORKER: chunk 创建失败, err: %v\n", err)
// 			return err
// 		}
// 		err := common.Add(wk.Conn, path+"/"+chunk, "")
// 		if err != nil {
// 			fmt.Printf("WORKER: chunk 创建失败, err: %v\n", err)
// 			return err
// 		}
// 	}

// 	// 创建 chunk 对应的的版本
// 	err = common.Add(wk.Conn, path+"/"+chunk+"/version", "0")
// 	if err != nil {
// 		fmt.Printf("WORKER: chunk 创建失败, err: %v\n", err)
// 		return err
// 	}

// 	fmt.Println("WORKER: chunk 创建成功")

// 	return nil
// }

/**
 * WritePrepare
 * 2PC 第一阶段，将数据暂时存入内存
 * IN: 	path -- 文件的路径
 *		data -- 写入的数据
 *		version -- 写入数据的版本号
 *		offset -- 写入数据的偏移量
 */
func (wk *Worker) WritePrepare(args *common.WriteChunkArgs, res *common.WritePrepareReply) error {
	writeCache := new(common.WriteChunkArgs)
	writeCache.Path = args.Path
	writeCache.Chunk = args.Chunk
	writeCache.Version = args.Version
	writeCache.Data = args.Data
	writeCache.Offset = args.Offset

	// FIXME: 需要加锁
	wk.Lock()
	idx := nextBufferIdx
	writeBuffer[idx] = writeCache
	nextBufferIdx = (nextBufferIdx + 1) % 10
	wk.Unlock()

	res.Index = idx
	fmt.Println("WORKER: 数据准备完成")
	return nil
}

/**
 * WriteAbort
 * 2PC 第一阶段失败后调用
 * IN:	path -- 文件的路径
 *		data -- 写入的数据
 *		version -- 写入数据的版本号
 *		offset -- 写入数据的偏移量
 */
func (wk *Worker) WriteAbort(args *common.WriteChunkArgs, res *struct{}) error {
	path := args.Path
	chunk := args.Chunk
	version := args.Version

	// 检查 version
	versionCheck := wk.CheckVersion(path, chunk, version)
	if versionCheck <= 0 {
		// 写入版本过旧，写入失败
		return errors.New("目标版本已更新，写入错误")
	} else if versionCheck == 1 {
		// 写入 log
		var logText string
		logText = "WRITE|" + path + "|" + chunk + "|0|" + strconv.Itoa(version) + "|0||" + "COMMIT\n"
		err := wk.TrackLog(logText)
		if err != nil {
			fmt.Printf("WORKER: 写入 Log 失败, err: %v\n", err)
			return err
		}
	} else {
		// 写入版本过新，需要等到上一个版本写入，重试
		return errors.New("版本不匹配，请重试")
	}
	// 检查 version
	wk.TrackMetaData(path, chunk, version)
	return nil
}

/**
 * WriteChunk
 * 写入指定 chunk 的元数据和数据
 * IN: 	path -- 文件的路径
 *		data -- 写入的数据
 *		version -- 写入数据的版本号
 *		offset -- 写入数据的偏移量
 * OUT:
 */
func (wk *Worker) WriteChunk(args *common.WriteCommitArgs, _ *struct{}) error {
	wk.Lock()
	writeChunkArgs := writeBuffer[args.Index]
	wk.Unlock()

	path := writeChunkArgs.Path
	chunk := writeChunkArgs.Chunk
	version := writeChunkArgs.Version
	data := writeChunkArgs.Data
	offset := writeChunkArgs.Offset

	// 检查 version
	versionCheck := wk.CheckVersion(path, chunk, version)
	if versionCheck <= 0 {
		// 写入版本过旧，写入失败
		return errors.New("目标版本已更新，写入错误")
	} else if versionCheck == 1 {
		// 写入 log
		var logText string
		logText = "WRITE|" + path + "|" + chunk + "|" + strconv.Itoa(int(offset)) + "|" + strconv.Itoa(version) + "|" + strconv.Itoa(len(data)) + "|" + data + "|" + "COMMIT\n"
		err := wk.TrackLog(logText)
		if err != nil {
			fmt.Printf("WORKER: 写入 Log 失败, err: %v\n", err)
			return err
		}
	} else {
		// 写入版本过新，需要等到上一个版本写入，重试
		return errors.New("版本不匹配，请重试")
	}

	// 打开文件，如果不存在则创建
	file, err := os.OpenFile("../data/"+wk.NodePort+path+"/"+chunk+".txt", os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf("WORKER: 打开文件错误, err: %v\n", err)

		// 创建 file 文件
		err = os.MkdirAll("../data/"+wk.NodePort+path, os.ModePerm)
		// 判断是否出错
		if err != nil {
			fmt.Printf("WORKER: 创建文件错误 err: %v\n", err)
			return err
		}

		// 创建 chunk 文件
		file, err = os.Create("../data/" + wk.NodePort + path + "/" + chunk + ".txt")
		// 判断是否出错
		if err != nil {
			fmt.Printf("WORKER: 创建文件错误 err: %v\n", err)
			return err
		}
	}
	defer file.Close()

	// 定位 offset
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		fmt.Printf("WORKER: 文件 seek 错误 err: %v\n", err)
	}

	// 写入文件
	writer := bufio.NewWriter(file)
	writer.WriteString(data)
	// bufio.NewWriter 是带缓冲的写,如果不调用 Flush 方法, 那么数据不会写入文件
	writer.Flush()

	// 记录 metadata 到内存
	wk.TrackMetaData(path, chunk, version)
	fmt.Println("WORKER: 写入 chunk 成功")

	return nil
}

/**
 * AppendChunk
 * 追加 chunk 内容
 * IN: 	path -- 文件的路径
 *		data -- 写入的数据
 *		version -- 写入数据的版本号
 * OUT:
 */
func (wk *Worker) AppendChunk(args *common.WriteCommitArgs, res *common.AppendChunkReply) error {
	wk.Lock()
	writeChunkArgs := writeBuffer[args.Index]
	wk.Unlock()

	path := writeChunkArgs.Path
	chunk := writeChunkArgs.Chunk
	version := writeChunkArgs.Version
	data := writeChunkArgs.Data

	// 检查 version
	versionCheck := wk.CheckVersion(path, chunk, version)
	if versionCheck <= 0 {
		return errors.New("目标版本已更新，写入错误")
	} else if versionCheck == 1 {
		// 写入 log
		var logText string
		logText = "APPEND|" + path + "|" + chunk + "|" + strconv.Itoa(version) + "|" + strconv.Itoa(len(data)) + "|" + data + "|" + "COMMIT\n"
		err := wk.TrackLog(logText)
		if err != nil {
			fmt.Printf("WORKER: 写入 Log 失败, err: %v\n", err)
			return err
		}
	} else {
		return errors.New("版本不匹配，请重试")
	}

	// 打开文件，如果不存在则创建
	file, err := os.OpenFile("../data/"+wk.NodePort+path+"/"+chunk+".txt", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("WORKER: 打开文件错误, err: %v\n", err)

		// 创建 file 文件
		err = os.MkdirAll("../data/"+wk.NodePort+path, os.ModePerm)
		// 判断是否出错
		if err != nil {
			fmt.Printf("WORKER: 创建文件错误 err: %v\n", err)
			return err
		}

		// 创建文件
		file, err = os.Create("../data/" + wk.NodePort + path + "/" + chunk + ".txt")
		// 判断是否出错
		if err != nil {
			fmt.Printf("WORKER: 创建文件错误 err: %v\n", err)
			return err
		}
	}
	defer file.Close()

	// 定位 offset
	// _, err = file.Seek(0, io.SeekEnd)
	// if err != nil {
	// 	fmt.Printf("WORKER: 文件 seek 错误 err: %v\n", err)
	// }

	// 获取文件大小
	fi, err := os.Stat("../data/" + wk.NodePort + path + "/" + chunk + ".txt")
	if err != nil {
		fmt.Printf("WORKER: 文件大小获取错误 err: %v\n", err)
		return err
	}
	size := fi.Size()
	writeSize := int(common.MaxChunkSize - size)
	if writeSize > len(data) {
		writeSize = len(data)
	}
	res.WriteSize = writeSize

	// 写入文件
	writer := bufio.NewWriter(file)
	writer.WriteString(data[:writeSize])
	// bufio.NewWriter 是带缓冲的写,如果不调用 Flush 方法, 那么数据不会写入文件
	writer.Flush()

	// 记录 metadata 到内存
	wk.TrackMetaData(path, chunk, version)
	fmt.Println("WORKER: 写入 chunk 成功")

	return nil
}

/**
 * ReadChunk
 * 读取指定 chunk 的数据
 * IN:	path -- 文件路径
 *		chunk -- chunk 名称
 *		version -- chunk 版本
 * OUT: data -- 读取的内容
 */
func (wk *Worker) ReadChunk(args *common.ReadChunkArgs, res *common.ReadChunkReply) error {
	path := args.Path
	chunk := args.Chunk
	version := args.Version

	// 获取 chunk
	wk.Lock()
	_, ok := wk.FileInfos[path]
	wk.Unlock()
	if !ok {
		err := errors.New("file 不存在")
		fmt.Printf("WORKER: 读取 chunk 错误, err: %v\n", err)
		return err
	}
	wk.Lock()
	_, ok = wk.FileInfos[path][chunk]
	wk.Unlock()
	if !ok {
		err := errors.New("chunk 不存在")
		fmt.Printf("WORKER: 读取 chunk 错误, err: %v\n", err)
		return err
	}

	// 比较 version，如果存储的版本小于要读取的版本，说明还没有写入，应该 retry
	if wk.CheckVersion(path, chunk, version) > 0 {
		fmt.Println("WORKER: 读取 Chunk 失败, err: Chunk Version 不匹配")
		return errors.New("ReadChunk: version check failed")
	}
	// TODO: 需要处理 version 不匹配的情况

	// 打开文件
	file, err := os.OpenFile("../data/"+wk.NodePort+path+"/"+chunk+".txt", os.O_RDONLY, 0666)
	if err != nil {
		fmt.Printf("WORKER: 打开文件错误, err: %v\n", err)
		return err
	}
	defer file.Close()

	// 读取数据
	reader := bufio.NewReader(file)
	var build strings.Builder
	for {
		str, err := reader.ReadString('\n')
		fmt.Printf("WORKER: 读取 %s \n", str)
		build.WriteString(str)
		if err == io.EOF {
			break
		}
	}
	res.Data = build.String()

	fmt.Printf("WORKER: 读取 %s 成功\n", chunk)

	return nil
}

/**
 * DeleteFile
 * 删除文件
 * IN: path -- 文件路径
 */
func (wk *Worker) DeleteFile(args *common.DeleteFileArgs, _ *struct{}) error {
	path := args.Path

	// 写入 log
	var logText string
	logText = "DELETE|" + path + " COMMIT\n"
	err := wk.TrackLog(logText)
	if err != nil {
		fmt.Printf("WORKER: 写入 Log 失败, err: %v\n", err)
		return err
	}

	wk.Lock()
	_, ok := wk.FileInfos[path]
	if ok {
		delete(wk.FileInfos, path)
	}

	pattern := "../data/" + wk.NodePort + path + "/*.txt"
	matches, err := filepath.Glob(pattern)
	if err != nil {
		if err != nil {
			fmt.Printf("WORKER: 文件 %s 删除失败, err: %v\n", pattern, err)
			return err
		}
	}
	fmt.Printf("WORKER: 共找到文件 %s 的 %d 个 chunk\n", path, len(matches))

	wk.ChunkLoad = wk.ChunkLoad - len(matches)

	err = os.RemoveAll("../data/" + wk.NodePort + path)
	if err != nil {
		fmt.Printf("WORKER: 文件 %s 删除失败, err: %v\n", path, err)
		return err
	}

	wk.Unlock()
	fmt.Printf("WORKER: 文件 %s 删除成功\n", path)
	return nil
}

/**
 * DecodeLog
 * 根据 Log 执行指令
 * IN: str -- Log 中一行的内容
 * OUT:
 */
func (wk *Worker) DecodeLog(str string) error {
	if len(str) == 0 {
		return nil
	}
	blank := strings.Index(str, "|")
	operation := str[:blank]
	str = str[blank+1:]
	switch operation {
	case "CHECKPOINT":
		for {
			if str == "END\n" {
				break
			}
			blank = strings.Index(str, "|")
			path := str[:blank]
			str = str[blank+1:]

			blank = strings.Index(str, "|")
			chunk := str[:blank]
			str = str[blank+1:]

			blank = strings.Index(str, "|")
			version, _ := strconv.Atoi(str[:blank])
			str = str[blank+1:]

			wk.TrackMetaData(path, chunk, version)
		}
		break
	case "WRITE":
		blank = strings.Index(str, "|")
		path := str[:blank]
		str = str[blank+1:]

		blank = strings.Index(str, "|")
		chunk := str[:blank]
		str = str[blank+1:]

		blank = strings.Index(str, "|")
		offset := str[:blank]
		str = str[blank+1:]

		blank = strings.Index(str, "|")
		version := str[:blank]
		str = str[blank+1:]

		blank = strings.Index(str, "|")
		size, _ := strconv.Atoi(str[:blank])
		str = str[blank+1:]

		data := str[:size]
		str = str[size+1:]
		if str == "COMMIT\n" {
			var args common.WriteChunkArgs
			args.Path = path
			args.Chunk = chunk
			off, _ := strconv.Atoi(offset)
			args.Offset = int64(off)
			args.Version, _ = strconv.Atoi(version)
			args.Data = data

			var res common.WritePrepareReply
			var args2 common.WriteCommitArgs
			err := wk.WritePrepare(&args, &res)
			if err != nil {
				fmt.Printf("WORKER: 恢复失败，ERROR: %v\n", err)
				return err
			}
			args2.Index = res.Index
			err = wk.WriteChunk(&args2, nil)
			if err != nil {
				fmt.Printf("WORKER: 恢复失败，ERROR: %v\n", err)
				return err
			}
		} else {
			return errors.New("WORKER: 日志不完整")
		}
		fmt.Println("WORKER: 恢复成功")
		break
	case "APPEND":
		blank = strings.Index(str, "|")
		path := str[:blank]
		str = str[blank+1:]

		blank = strings.Index(str, "|")
		chunk := str[:blank]
		str = str[blank+1:]

		blank = strings.Index(str, "|")
		version := str[:blank]
		str = str[blank+1:]

		blank = strings.Index(str, "|")
		size, _ := strconv.Atoi(str[:blank])
		str = str[blank+1:]

		data := str[:size]
		str = str[size+1:]
		if str == "COMMIT\n" {
			var args common.WriteChunkArgs
			args.Path = path
			args.Chunk = chunk
			args.Offset = 0
			args.Version, _ = strconv.Atoi(version)
			args.Data = data

			var res common.WritePrepareReply
			var args2 common.WriteCommitArgs
			err := wk.WritePrepare(&args, &res)
			if err != nil {
				fmt.Printf("WORKER: 恢复失败，ERROR: %v\n", err)
				return err
			}
			args2.Index = res.Index
			err = wk.AppendChunk(&args2, nil)
			if err != nil {
				fmt.Printf("WORKER: 恢复失败，ERROR: %v\n", err)
				return err
			}
		} else {
			return errors.New("WORKER: 日志不完整")
		}
		fmt.Println("WORKER: 恢复成功")
		break
	// case "CREATE":
	// 	blank = strings.Index(str, "|")
	// 	path := str[:blank]
	// 	str = str[blank+1:]

	// 	if str == "COMMIT\n" {
	// 		var args common.CreateFileArgs
	// 		args.Path = path
	// 		err := wk.CreateFile(&args, nil)
	// 		if err != nil {
	// 			fmt.Printf("WORKER: 恢复失败，ERROR: %v\n", err)
	// 			return err
	// 		}
	// 	} else {
	// 		return errors.New("WORKER: 日志不完整")
	// 	}
	// 	fmt.Println("WORKER: 恢复成功")
	// 	break
	case "DELETE":
		blank = strings.Index(str, "|")
		path := str[:blank]
		str = str[blank+1:]

		if str == "COMMIT\n" {
			var args common.DeleteFileArgs
			args.Path = path
			err := wk.DeleteFile(&args, nil)
			if err != nil {
				fmt.Printf("WORKER: 恢复失败，ERROR: %v\n", err)
				return err
			}
		} else {
			return errors.New("WORKER: 日志不完整")
		}
		fmt.Println("WORKER: 恢复成功")
		break
	default:
		break
	}

	return nil
}

/**
 * Recover
 * 根据 Log 恢复数据
 * IN:
 * OUT:
 */
func (wk *Worker) Recover() error {
	wk.IsRecovering = true
	// 打开文件
	file, err := os.OpenFile("../log/log_"+wk.NodePort+".txt", os.O_RDONLY, 0666)
	if err != nil {
		file, err = os.OpenFile("../log/log_"+wk.NodePort+"_old.txt", os.O_RDONLY, 0666)
		if err != nil {
			fmt.Printf("WORKER: 打开 LOG 文件错误, err: %v\n", err)
			return err
		}
	}
	defer file.Close()

	// 读取数据
	reader := bufio.NewReader(file)
	for {
		str, err := reader.ReadString('\n')
		fmt.Printf("WORKER: 读取 %s \n", str)
		wk.DecodeLog(str)
		if err == io.EOF {
			break
		}
	}

	fmt.Println("WORKER: 恢复 LOG 成功")
	wk.IsRecovering = false
	return nil
}

// 触发异常退出
func quit() {
	quitCh <- 1
}

// 启动的时候的形式：
// go run worker_server.go 127.0.0.1:PORT(本地的 RPC 地址)
func main() {
	// hosts := []string{
	// 	"192.168.152.128:2184",
	// 	"192.168.152.128:2185",
	// 	"192.168.152.128:2186"}

	// for test
	// quitCh = make(chan int)

	// 初始化 write buffer, 大小暂定为 10
	writeBuffer = make(map[int](*common.WriteChunkArgs), bufferSize)

	// 创建zk连接
	go StartWorker(os.Args[1])

	// for test
	// <-quitCh
	// return

	var operation string
	for {
		fmt.Println("输入 quit 退出")
		fmt.Scanln(&operation)
		if operation == "quit" {
			break
		}
	}
}
