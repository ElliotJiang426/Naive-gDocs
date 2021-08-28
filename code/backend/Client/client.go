package client

import (
	"fmt"
	common "gfs/Common"
	"math/rand"

	//	"net/rpc"
	"regexp"
	"strconv"
	"sync"

	mapset "github.com/deckarep/golang-set"
)

type Client struct {
	sync.Mutex
	master    string
	fileTable *common.FileTable
}

/**
 * @description Return a client
 * @date 16:18 2021/7/4
 * @param
 * @return
 **/
func NewClient(master string) *Client {
	return &Client{
		master:    master,
		fileTable: common.NewFileTable(),
	}
}

/**
 * @description Open the file
 * @date 13:40 2021/7/12
 * @param
 *		path	string
 *		auth	int
 * @return file descriptor
 **/
func (c *Client) Open(path string, auth int) (*common.FileDescriptor, error) {
	// for http Server
	// check file exists
	var args common.RPCArgs
	args.Path = path
	err := common.Call(c.master, "Master.IsExist", args, nil)
	if err != nil {
		return nil, err
	}

	return c.fileTable.GetAndSet(path, auth)
}

func (c *Client) Close(fd *common.FileDescriptor) error {
	return c.fileTable.Delete(fd)
}

/**
 * @description Create a file
 * @date 16:20 2021/7/4z
 * @param
 * 		path
 * @return
 **/
func (c *Client) Create(path string) (*common.FileDescriptor, error) {
	var args common.RPCArgs

	args.Path = path

	// 默认是读写权限
	fd, err := c.fileTable.GetAndSet(path, common.O_RDWR)
	if err != nil {
		fmt.Printf("CLIENT: Create File Failed at Get FD\n \t ERROR :%v ", err)
		return nil, err
	}

	err = common.Call(c.master, "Master.CreateFile", &args, nil)
	if err != nil {
		fmt.Printf("CLIENT: Create File Failed at Master.CreateFile\n \t ERROR: %v\n", err)
		// 删除fd
		c.fileTable.Delete(fd)
		return nil, err
	}
	return fd, nil
}

/**
 * @description Delete a file
 * @date 16:50 2021/7/4
 * @param
 *		fd
 * @return
 **/
func (c *Client) Delete(path string) error {
	var chunks []common.ChunkInfo
	var args common.RPCArgs
	var masterDeleteReply common.DeleteReply
	var wg sync.WaitGroup

	args.Path = path
	err := common.Call(c.master, "Master.GetChunks", &args, &chunks)

	if err != nil {
		fmt.Printf("CLIENT: Delete failed at Master.GetChunks\n \t ERROR: %v\n", err)
		return err
	}

	err = common.Call(c.master, "Master.DeleteFile", &args, &masterDeleteReply)
	if err != nil {
		fmt.Printf("CLIENT: Delete failed at Master.DeleteFile \n \t ERROR: %v\n", err)
		return err
	}

	// 此处的设计是由 client 直接去跟所有副本协调删除
	// 使用 goroutine 需要 wg 来协调
	nodeSet := mapset.NewSet()
	for _, chunk := range chunks {
		nodeList := chunk.NodeList
		for _, node := range nodeList {
			nodeSet.Add(node)
		}
	}

	for node := range nodeSet.Iterator().C {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			err = common.Call(node, "Worker.DeleteFile", &args, nil)
			if err != nil {
				fmt.Printf("CLIENT: Delete failed at Worker.DeleteFile \n \t ERROR: %v\n", err)
			}
		}(node.(string))
	}
	wg.Wait()

	if err != nil {
		return err
	}
	return nil
}

/**
 * @description Read a file at specific offset,it reads up to len(data) bytes from the file
 * @date 17:40 2021/7/4
 * @param
 *		path
 *		offset
 *		data	len(data) gives size, bytes read should be stored in data
 * @return size of bytes read
 **/
func (c *Client) Read(fd *common.FileDescriptor, offset int64, data []byte) (n int, err error) {
	path, err := c.fileTable.GetPath(fd)
	if err != nil {
		return 0, err
	}

	if readAuth, err := c.fileTable.HaveReadAuth(fd); err != nil {
		return 0, err
	} else {
		if !readAuth {
			return 0, fmt.Errorf("CLIENT: READ FAILED \n \t ERROR: have no read auth\n")
		}
	}

	pos := 0
	for pos < len(data) {
		index := offset / common.MaxChunkSize
		chunkOffset := offset % common.MaxChunkSize
		cnt, err := c.ReadChunk(path, int(index), chunkOffset, data[pos:])

		if err != nil && err.(common.Error).Code != common.EOF {
			fmt.Printf("CLIENT: READ FAILED \n \t ERROR: %v \n", err)
			break
		}

		offset += int64(cnt)
		pos += cnt
		if err != nil {
			if err.(common.Error).Code != common.EOF {
				fmt.Printf("CLIENT: READ FAILED \n \t ERROR: %v \n", err)
			}
			break
		}
	}
	return pos, err
}

/**
 * @description Read data from the chunk at specific offset
 * @date 19:34 2021/7/4
 * @param
 * @return size of bytes read
 **/
func (c *Client) ReadChunk(path string, index int, offset int64, data []byte) (int, error) {
	var readLen int
	var args4GetChunk common.RPCArgs
	var chunkInfo common.ChunkInfo
	var reply common.ReadChunkReply
	var args4ReadChunk common.ReadChunkArgs

	chunkHandle := "chunk" + strconv.Itoa(index)
	args4GetChunk.Path = path
	args4GetChunk.Data = chunkHandle
	// get ChunkInfo
	err := common.Call(c.master, "Master.GetChunk", &args4GetChunk, &chunkInfo)
	// Chunk Not Exists
	if err != nil {
		fmt.Println("chunkhandle is " + chunkHandle)
		return 0, common.Error{Code: common.WCN, Err: "Wrong Chunk Number"}
	}
	if common.MaxChunkSize-offset > int64(len(data)) {
		readLen = len(data)
	} else {
		readLen = int(common.MaxChunkSize - offset)
	}

	nodesNum := len(chunkInfo.NodeList)

	// check replica
	if nodesNum == 0 {
		return 0, fmt.Errorf("no replica")
	}

	// prepare to read chunk
	args4ReadChunk.Path = path
	args4ReadChunk.Chunk = chunkHandle
	args4ReadChunk.Version = chunkInfo.Version
	var errCnt int
	for _, randomIndex := range rand.Perm(nodesNum) {
		// select replica
		nodeSelected := chunkInfo.NodeList[randomIndex]
		err = common.Call(nodeSelected, "Worker.ReadChunk", &args4ReadChunk, &reply)
		if err == nil {
			break
		}
		errCnt++
	}
	if errCnt == nodesNum {
		return 0, common.Error{Code: common.UNKNOWN, Err: "Read Unknown Error"}
	}

	chunkContentLen := len(reply.Data)

	if offset > int64(chunkContentLen) {
		return 0, fmt.Errorf("read exceeds chunks EOF")
	}

	err = nil
	// 注意：正常地跨Chunk读是不会抛出 EOF error的
	// 因为此时readLen = MaxChunkSize - offset = chunkContentLen - offset,不会触发条件
	if readLen > chunkContentLen-int(offset) {
		readLen = chunkContentLen - int(offset)
		err = common.Error{Code: common.EOF, Err: "EOF"}
	}

	copy(data, []byte(reply.Data[offset:offset+int64(readLen)]))
	return readLen, err
}

/**
 * @description Write a file at specific offset
 * @date 23:19 2021/7/9
 * @param
 *		path
 *		offset
 * 		data
 * @return
 **/
func (c *Client) Write(fd *common.FileDescriptor, offset int64, data []byte) error {
	path, err := c.fileTable.GetPath(fd)
	if err != nil {
		return err
	}

	if writeAuth, err := c.fileTable.HaveWriteAuth(fd); err != nil {
		return err
	} else {
		if !writeAuth {
			return fmt.Errorf("CLIENT: WIRTE FAILED \n \t ERROR: have no write auth\n")
		}
	}
	// pos标记已经写入的位置
	pos := 0
	for {
		index := offset / common.MaxChunkSize
		chunkOffset := offset % common.MaxChunkSize

		// 每次chunk写的最大限制
		writeMax := int(common.MaxChunkSize - chunkOffset)
		var writeLen int
		// 如果按最大写计算已经超出数据大小
		if pos+writeMax > len(data) {
			// 截断多余的数据
			writeLen = len(data) - pos
		} else {
			// 否则按最大写计算
			writeLen = writeMax
		}
		err = c.WriteChunk(path, int(index), chunkOffset, data[pos:pos+writeLen])
		if err != nil {
			fmt.Printf("CLIENT: Write failed \n \t ERROR: \n")
			break
		}
		// 修改全局offset以及pos
		offset += int64(writeLen)
		pos += writeLen

		if pos == len(data) {
			fmt.Println("CLIENT: finish write!")
			break
		}
	}
	return err
}

func (c *Client) WriteChunk(path string, index int, offset int64, data []byte) error {
	var args common.RPCArgs
	var chunkReply common.ChunkReply
	var wg sync.WaitGroup

	chunkHandle := "chunk" + strconv.Itoa(index)
	args.Path = path
	args.Data = chunkHandle
	fmt.Println(chunkHandle)
	// get chunkInfo
	err := common.Call(c.master, "Master.WriteChunk", &args, &chunkReply)
	if err != nil {
		fmt.Printf("CLIENT: WriteChunk failed at Mastser.WriteChunk \t \n Error: %v \n", err)
		return err
	}

	nodesNum := len(chunkReply.NodeList)
	// check replica
	if nodesNum == 0 {
		return fmt.Errorf("no replica")
	}
	nodeReply := make(map[string]*common.WritePrepareReply, nodesNum)
	prepareSucc := 1
	wg.Add(nodesNum)
	fmt.Println(chunkReply.NodeList)

	for _, node := range chunkReply.NodeList {
		// 2PC phase 1
		go func(node string) {
			defer wg.Done()
			var args common.WriteChunkArgs
			res := new(common.WritePrepareReply)

			// 准备参数
			args.Path = path
			args.Chunk = chunkHandle
			args.Version = chunkReply.Version
			args.Offset = offset
			args.Data = string(data)
			//// FIXME: test data
			//args.Offset = 10
			//args.Data = "this is test data Ver." + strconv.Itoa(chunkInfo.Version)
			fmt.Printf("WritePrepare %s", node)
			err = common.Call(node, "Worker.WritePrepare", &args, res)
			if err != nil {
				fmt.Printf("CLIENT: Worker prepare 失败, ERROR: %v\n", err)
				res.Index = -1
				c.Lock()
				prepareSucc = 0
				nodeReply[node] = res
				c.Unlock()
				return
			}
			c.Lock()
			nodeReply[node] = res
			c.Unlock()
			fmt.Printf("CLIENT: Worker prepare node 成功\n")
		}(node)
	}

	wg.Wait()

	wg.Add(nodesNum)
	if prepareSucc != 1 {
		fmt.Printf("CLIENT: Worker write prepare 阶段失败\n")
		return nil
	} else {
		for _, node := range chunkReply.NodeList {
			// 2PC phase 2
			go func(node string) {
				defer wg.Done()
				var args common.WriteCommitArgs
				// 准备参数
				args.Index = nodeReply[node].Index
				for i := 0; i < common.RetryTimes; i++ {
					err = common.Call(node, "Worker.WriteChunk", &args, nil)
					if err == nil {
						break
					}
					// FIXME: 还需要再斟酌一下
					fmt.Printf("CLIENT: Worker write 失败，正在重试, ERROR: %v\n", err)
				}
				fmt.Printf("CLIENT: Worker write node 成功\n")
			}(node)
		}
	}
	wg.Wait()
	return nil
}

func (c *Client) Append(fd *common.FileDescriptor, data []byte) error {
	var args4GetChunks common.RPCArgs
	var chunksInfo []common.ChunkInfo
	var maxChunkIndex int

	toWriteSize := len(data)
	// check append size
	if toWriteSize > common.MaxAppendSize {
		return fmt.Errorf("CLIENT: APPEND FAILED\n\t ERROR: append size should be less than MaxAppendSize\n")
	}

	path, err := c.fileTable.GetPath(fd)
	if err != nil {
		return err
	}

	if writeAuth, err := c.fileTable.HaveWriteAuth(fd); err != nil {
		return err
	} else {
		if !writeAuth {
			return fmt.Errorf("CLIENT: WIRTE FAILED \n \t ERROR: have no write auth\n")
		}
	}

	maxChunkIndex = 0
	regExp := regexp.MustCompile(`^chunk([\d]+)$`)
	args4GetChunks.Path = path
	// get chunksInfo
	err = common.Call(c.master, "Master.GetChunks", &args4GetChunks, &chunksInfo)

	if err != nil {
		return err
	}

	for _, chunkInfo := range chunksInfo {
		//fmt.Println(chunkInfo.Chunk)
		chunkIndex := regExp.FindStringSubmatch(chunkInfo.Chunk)[1]
		if number, _ := strconv.Atoi(chunkIndex); number > maxChunkIndex {
			maxChunkIndex = number
		}
	}
	//if maxChunkIndex == 0{
	//	return fmt.Errorf("no chunk")
	//}
	err = c.AppendHelper(path, maxChunkIndex, data, true)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) AppendHelper(path string, chunkIndex int, data []byte, flag bool) error {
	var wg sync.WaitGroup
	var args4WriteChunk common.RPCArgs
	var reply4WriteChunk common.ChunkReply

	args4WriteChunk.Path = path
	args4WriteChunk.Data = "chunk" + strconv.Itoa(chunkIndex)

	err := common.Call(c.master, "Master.WriteChunk", &args4WriteChunk, &reply4WriteChunk)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return err
	}
	nodesNum := len(reply4WriteChunk.NodeList)
	// check replica
	if nodesNum == 0 {
		return fmt.Errorf("no replica")
	}

	nodeReply := make(map[string]*common.WritePrepareReply, nodesNum)
	prepareSucc := 1
	wg.Add(nodesNum)
	fmt.Println(reply4WriteChunk.NodeList)

	for _, node := range reply4WriteChunk.NodeList {
		go func(node string) {
			defer wg.Done()

			var args common.WriteChunkArgs
			res := new(common.WritePrepareReply)

			// 准备参数
			args.Path = path
			args.Chunk = args4WriteChunk.Data
			args.Version = reply4WriteChunk.Version
			args.Data = string(data)
			err = common.Call(node, "Worker.WritePrepare", &args, &res)
			if err != nil {
				fmt.Printf("CLIENT: Worker prepare 失败, ERROR: %v\n", err)
				res.Index = -1
				c.Lock()
				prepareSucc = 0
				nodeReply[node] = res
				c.Unlock()
				return
			}
			c.Lock()
			nodeReply[node] = res
			c.Unlock()
			fmt.Printf("CLIENT: Worker prepare node 成功\n")
		}(node)
	}

	wg.Wait()
	// time.Sleep(time.Duration(5) * time.Second)

	wg.Add(nodesNum)
	haveAppendChan := make(chan int, nodesNum)
	if prepareSucc != 1 {
		fmt.Printf("CLIENT: Worker append prepare 阶段失败\n")
		return nil
	} else {
		for _, node := range reply4WriteChunk.NodeList {
			// 2PC phase 2
			go func(node string) {
				defer wg.Done()
				var args common.WriteCommitArgs
				var reply common.AppendChunkReply

				// 准备参数
				args.Index = nodeReply[node].Index
				for i := 0; i < common.RetryTimes; i++ {
					err = common.Call(node, "Worker.AppendChunk", &args, &reply)
					if err == nil {
						haveAppendChan <- reply.WriteSize
						break
					}
					fmt.Printf("CLIENT: Worker append 失败，正在重试, ERROR: %v\n", err)
				}
				fmt.Printf("CLIENT: Worker append node 成功\n")
			}(node)
		}
	}
	wg.Wait()

	// if flag = false, don't check it
	haveAppendSize := <-haveAppendChan
	if haveAppendSize < len(data) && flag {
		err = c.AppendHelper(path, chunkIndex+1, data[haveAppendSize:], false)
		if err != nil {
			return err
		}
	}
	return nil
}

// ONLY FOR TEST
// func main() {
// 	var (
// 		ip        string
// 		operation string
// 		args      common.RPCArgs
// 		conn      *rpc.Client
// 		theThirdArg string
// 	)

// 	// 创建 client 端
// 	//fmt.Print("CLIENT: 选择要连接的 Master：")
// 	//fmt.Scanln(&ip)
// 	//client := NewClient(ip)
// 	client := NewClient("127.0.0.1:8095")

// 	/* 增删改查 */
// 	for {
// 		fmt.Println("CLIENT: 选择要执行的操作：create/write/delete/get/read/disconnect/quit")
// 		fmt.Scanln(&operation, &args.Path, &args.Data, &theThirdArg)
// 		if operation == "create" {
// 			// 创建文件 输入格式：create /filename
// 			client.Create(args.Path)
// 		} else if operation == "write" {
// 			offset, _ := strconv.ParseInt(args.Data,10,64)
// 			fd, _ := client.Open(args.Path, common.O_RDWR)
// 			client.Write(fd, offset, []byte(theThirdArg))
// 			client.Close(fd)
// 		} else if operation == "append" {
// 			fd, _ := client.Open(args.Path, common.O_RDWR)
// 			client.Append(fd,[]byte(args.Data))
// 			client.Close(fd)
// 		} else if operation == "delete" {
// 			// 删除文件 输入格式：delete /filename
// 			client.Delete(args.Path)
// 		} else if operation == "read" {
// 			// 读取文件 输入格式：read /filename chunkname
// 			fd, _ := client.Open(args.Path, common.O_RDWR)
// 			offset, _ := strconv.ParseInt(args.Data, 10, 64)
// 			size, _ := strconv.ParseInt(theThirdArg,10,64)
// 			data := make([]byte, size)
// 			client.Read(fd, offset, data[:])
// 			fmt.Println(string(data))
// 			client.Close(fd)
// 		} else if operation == "disconnect" {
// 			// 输入格式：disconnect
// 			// 重新选择客户端
// 			fmt.Print("CLIENT: 选择要连接的 Master：")
// 			fmt.Scanln(&ip)
// 			client = NewClient(ip)
// 		} else if operation == "quit" {
// 			// 退出 输入格式：quit
// 			break
// 		} else {
// 			fmt.Println("CLIENT: 请输入有效的命令")
// 		}
// 	}

// 	conn.Close()
// }
