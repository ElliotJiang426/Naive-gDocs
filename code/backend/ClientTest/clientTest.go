package main

import (
	"fmt"
	common "gfs/Common"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

type Client struct {
	sync.Mutex
	master string
}

var (
	client *Client
)

/**
 * @description Return a client
 * @date 16:18 2021/7/4
 * @param
 * @return
 **/
func NewClient(master string) *Client {
	return &Client{
		master: master,
	}
}

func test_getchunks() {
	// 建立连接
	ip := "127.0.0.1:8095"

	var args common.RPCArgs
	var res []common.ChunkInfo
	args.Path = "/fileTmp"

	err := common.Call(ip, "Master.GetChunks", &args, &res)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}
	for _, chunk := range res {
		fmt.Printf("Chunk: %s, Version:%d\n", chunk.Chunk, chunk.Version)
		fmt.Println("Nodelist: ", chunk.NodeList)
	}

	fmt.Println("TEST: GetChunks test pass")
}

func test_createfile() {
	ip := "127.0.0.1:8095"
	var args common.RPCArgs
	args.Path = "/fileTmp"

	err := common.Call(ip, "Master.CreateFile", &args, nil)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}
	fmt.Println("TEST: CreateFile test pass")
}

func test_deletefile() {
	ip := "127.0.0.1:8095"
	var chunks []common.ChunkInfo
	var args common.RPCArgs
	var masterDeleteReply common.DeleteReply
	var wg sync.WaitGroup

	args.Path = "/fileTmp"

	err := common.Call(ip, "Master.GetChunks", &args, &chunks)

	if err != nil {
		fmt.Println("TEST: DeleteFile test failed")
		return
	}

	err = common.Call(ip, "Master.DeleteFile", &args, &masterDeleteReply)
	if err != nil {
		fmt.Println("TEST: DeleteFile test failed")
		return
	}

	for _, chunk := range chunks {
		nodesList := chunk.NodeList
		for _, node := range nodesList {
			wg.Add(1)
			go func(node string) {
				defer wg.Done()
				err = common.Call(node, "Worker.DeleteFile", &args, nil)
				if err != nil {
					fmt.Println("TEST: DeleteFile test failed")
					return
				}
			}(node)
		}
	}
	wg.Wait()

	fmt.Println("TEST: DeleteFile test pass")
}

func test_writechunk(testId string, chunk string) {
	ip := "127.0.0.1:8095"
	var args1 common.RPCArgs
	var res1 common.ChunkReply
	args1.Path = "/fileTmp"
	args1.Data = chunk

	err := common.Call(ip, "Master.WriteChunk", &args1, &res1)
	if err != nil {
		fmt.Printf("%s ERROR: %v\n", testId, err)
		return
	}
	fmt.Printf("%s CLIENT: Write to master 成功\n", testId)

	// ***************************** //

	var wg sync.WaitGroup
	nodeReply := make(map[string](*common.WritePrepareReply), len(res1.NodeList))
	prepareSucc := 1
	wg.Add(len(res1.NodeList))
	for _, node := range res1.NodeList {
		// 2PC phase 1
		go func(node string) {
			defer wg.Done()

			var args2 common.WriteChunkArgs
			res2 := new(common.WritePrepareReply)

			// 准备参数
			args2.Path = args1.Path
			args2.Chunk = args1.Data
			args2.Version = res1.Version
			// FIXME: test data
			args2.Offset = 0
			args2.Data = "this is test data Ver." + strconv.Itoa(res1.Version)
			err = common.Call(node, "Worker.WritePrepare", &args2, res2)
			if err != nil {
				fmt.Printf("%s CLIENT: Worker prepare 失败, ERROR: %v\n", testId, err)
				res2.Index = -1
				client.Lock()
				prepareSucc = 0
				nodeReply[node] = res2
				client.Unlock()
				return
			}
			client.Lock()
			nodeReply[node] = res2
			client.Unlock()
			fmt.Printf("%s CLIENT: Worker prepare node 成功\n", testId)
		}(node)
	}

	wg.Wait()

	if prepareSucc != 1 {
		fmt.Printf("%s CLIENT: Worker write prepare 阶段失败\n", testId)
		return
	} else {
		wg.Add(len(res1.NodeList))
		for _, node := range res1.NodeList {
			// 2PC phase 2
			go func(node string) {
				defer wg.Done()
				var args3 common.WriteCommitArgs

				// 准备参数
				args3.Index = nodeReply[node].Index
				for i := 0; i < 3; i++ {
					err = common.Call(node, "Worker.WriteChunk", &args3, nil)
					if err == nil {
						break
					}
					time.Sleep(time.Duration(5) * time.Second)
					fmt.Printf("%s CLIENT: Worker write 失败，正在重试, ERROR: %v\n", testId, err)
				}
				fmt.Printf("%s CLIENT: Worker write node 成功\n", testId)
			}(node)
		}
	}
	wg.Wait()
	fmt.Printf("%s TEST: WriteChunk test pass\n", testId)
}

func test_appendchunk(testId string, chunk string) {
	ip := "127.0.0.1:8095"
	var args1 common.RPCArgs
	var res1 common.ChunkReply
	args1.Path = "/fileTmp"
	args1.Data = chunk

	err := common.Call(ip, "Master.WriteChunk", &args1, &res1)
	if err != nil {
		fmt.Printf("%s ERROR: %v\n", testId, err)
		return
	}
	fmt.Printf("%s CLIENT: Write to master 成功\n", testId)

	// ***************************** //

	var wg sync.WaitGroup
	nodeReply := make(map[string](*common.WritePrepareReply), len(res1.NodeList))
	prepareSucc := 1
	wg.Add(len(res1.NodeList))
	for _, node := range res1.NodeList {
		// 2PC phase 1
		go func(node string) {
			defer wg.Done()

			var args2 common.WriteChunkArgs
			res2 := new(common.WritePrepareReply)

			// 准备参数
			args2.Path = args1.Path
			args2.Chunk = args1.Data
			args2.Version = res1.Version
			args2.Data = "this is test data Ver." + strconv.Itoa(res1.Version)
			err = common.Call(node, "Worker.WritePrepare", &args2, res2)
			if err != nil {
				fmt.Printf("%s CLIENT: Worker prepare 失败, ERROR: %v\n", testId, err)
				res2.Index = -1
				client.Lock()
				prepareSucc = 0
				nodeReply[node] = res2
				client.Unlock()
				return
			}
			client.Lock()
			nodeReply[node] = res2
			client.Unlock()
			fmt.Printf("%s CLIENT: Worker prepare node 成功\n", testId)
		}(node)
	}

	wg.Wait()

	if prepareSucc != 1 {
		fmt.Printf("%s CLIENT: Worker write prepare 阶段失败\n", testId)
		return
	} else {
		wg.Add(len(res1.NodeList))
		for _, node := range res1.NodeList {
			// 2PC phase 2
			go func(node string) {
				defer wg.Done()
				var args3 common.WriteCommitArgs
				var res3 common.AppendChunkReply

				// 准备参数
				args3.Index = nodeReply[node].Index
				for i := 0; i < 3; i++ {
					err = common.Call(node, "Worker.AppendChunk", &args3, &res3)
					if err == nil {
						break
					}
					time.Sleep(time.Duration(5) * time.Second)
					fmt.Printf("%s CLIENT: Worker write 失败，正在重试, ERROR: %v\n", testId, err)
				}
				fmt.Printf("%s CLIENT: Worker write node 成功, 追加 %d Bytes\n", testId, res3.WriteSize)
			}(node)
		}
	}

	wg.Wait()

	fmt.Printf("%s TEST: AppendChunk test pass", testId)
}

func test_writeabort() {
	ip := "127.0.0.1:8095"
	var args1 common.RPCArgs
	var res1 common.ChunkReply
	args1.Path = "/fileTmp"
	args1.Data = "chunk1"

	err := common.Call(ip, "Master.WriteChunk", &args1, &res1)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}

	// ***************************** //

	var wg sync.WaitGroup
	nodeReply := make(map[string](*common.WritePrepareReply), len(res1.NodeList))
	prepareSucc := 1
	wg.Add(len(res1.NodeList))
	for _, node := range res1.NodeList {
		// 2PC phase 1
		go func(node string) {
			defer wg.Done()

			var args2 common.WriteChunkArgs
			res2 := new(common.WritePrepareReply)

			// 准备参数
			args2.Path = args1.Path
			args2.Chunk = args1.Data
			args2.Version = res1.Version
			// FIXME: test data
			args2.Offset = 0
			args2.Data = "this is test data Ver." + strconv.Itoa(res1.Version)
			err = common.Call(node, "Worker.WritePrepare", &args2, res2)
			if err != nil {
				fmt.Printf("CLIENT: Worker prepare 失败, ERROR: %v\n", err)
				res2.Index = -1
				client.Lock()
				prepareSucc = 0
				nodeReply[node] = res2
				client.Unlock()
				return
			}
			client.Lock()
			nodeReply[node] = res2
			client.Unlock()
			fmt.Printf("CLIENT: Worker prepare node 成功\n")
		}(node)
	}

	wg.Wait()

	if prepareSucc != 1 {
		fmt.Printf("CLIENT: Worker write prepare 阶段失败\n")
		return
	} else {
		for _, node := range res1.NodeList {
			// 2PC phase 2
			// go func(node string) {
			var args3 common.WriteChunkArgs

			// 准备参数
			args3.Path = args1.Path
			args3.Chunk = args1.Data
			args3.Version = res1.Version
			for {
				err = common.Call(node, "Worker.WriteAbort", &args3, nil)
				if err == nil {
					break
				}
				fmt.Printf("CLIENT: Worker write abort 失败，正在重试, ERROR: %v\n", err)
			}
			fmt.Printf("CLIENT: Worker write abort 成功\n")
			// }(node)
		}
	}

	fmt.Println("TEST: WriteAbort test pass")
}

func test_getchunk() {
	ip := "127.0.0.1:8095"
	var args common.RPCArgs
	var res common.ChunkInfo
	args.Path = "/fileTmp"
	args.Data = "chunk1"

	err := common.Call(ip, "Master.GetChunk", &args, &res)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}

	fmt.Printf("Chunk: %s, Version:%d\n", res.Chunk, res.Version)
	fmt.Println("Nodelist: ")
	for _, node := range res.NodeList {

		fmt.Printf("%s\n", node)
	}

	fmt.Println("TEST: GetChunk test pass")
}

func test_readchunk(testId string, chunk string) {
	ip := "127.0.0.1:8095"
	var args1 common.RPCArgs
	var res1 common.ChunkInfo
	args1.Path = "/fileTmp"
	args1.Data = chunk

	err := common.Call(ip, "Master.GetChunk", &args1, &res1)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}

	fmt.Printf("Chunk: %s, Version:%d\n", res1.Chunk, res1.Version)
	fmt.Println("Nodelist: ", res1.NodeList)
	for _, node := range res1.NodeList {
		var args2 common.ReadChunkArgs
		var res2 common.ReadChunkReply
		args2.Path = args1.Path
		args2.Chunk = args1.Data
		args2.Version = res1.Version

		err = common.Call(node, "Worker.ReadChunk", &args2, &res2)
		if err == nil {
			fmt.Printf("%s 读取：%s\n", testId, res2.Data)
			break
		}
	}

	fmt.Printf("%s TEST: ReadChunk test pass\n", testId)
}

func test_getchunk_sync() {
	users := 5

	var wg sync.WaitGroup
	wg.Add(users)
	for i := 0; i < users; i++ {
		go func() {
			defer wg.Done()
			test_getchunk()
		}()
	}
	wg.Wait()

	fmt.Println("TEST: GetChunkSync test pass")
}

func test_getchunks_sync() {
	users := 5

	var wg sync.WaitGroup
	wg.Add(users)
	for i := 0; i < users; i++ {
		go func() {
			defer wg.Done()
			test_getchunks()
		}()
	}
	wg.Wait()

	fmt.Println("TEST: GetChunksSync test pass")
}

func test_readchunk_sync() {
	users := 5

	var wg sync.WaitGroup
	wg.Add(users)
	for i := 0; i < users; i++ {
		go func(i int) {
			defer wg.Done()
			testId := strconv.Itoa(i)
			test_readchunk(testId, "chunk1")
		}(i)
	}
	wg.Wait()

	fmt.Println("TEST: ReadChunkSync test pass")
}

func test_readchunk_diff_sync() {
	users := 5
	chunks := []string{"chunk1", "chunk2", "chunk3"}

	var wg sync.WaitGroup
	wg.Add(users)
	for i := 0; i < users; i++ {
		go func(i int) {
			defer wg.Done()
			testId := strconv.Itoa(i)
			test_readchunk(testId, chunks[i%3])
		}(i)
	}
	wg.Wait()

	fmt.Println("TEST: ReadChunkDiffSync test pass")
}

func test_read_write_diff_sync() {
	users := 5

	var wg sync.WaitGroup
	wg.Add(users)
	for i := 0; i < users; i++ {
		go func(i int) {
			defer wg.Done()
			testId := strconv.Itoa(i)
			if i == 3 {
				test_writechunk(testId, "chunk1")
			} else {
				test_readchunk(testId, "chunk2")
			}
		}(i)
	}
	wg.Wait()

	fmt.Println("TEST: ReadWriteDiffSync test pass")
}

func test_writechunk_diff_sync() {
	users := 3
	chunks := []string{"chunk1", "chunk2", "chunk3"}

	var wg sync.WaitGroup
	wg.Add(users)
	for i := 0; i < users; i++ {
		go func(i int) {
			defer wg.Done()
			testId := strconv.Itoa(i)
			test_writechunk(testId, chunks[i%3])
		}(i)
	}
	wg.Wait()

	fmt.Println("TEST: WrtieChunkDiffSync test pass")
}

func test_writechunk_sync() {
	users := 30
	chunks := []string{"chunk1", "chunk2", "chunk3"}

	var wg sync.WaitGroup
	wg.Add(users)
	for i := 0; i < users; i++ {
		go func(i int) {
			defer wg.Done()
			testId := strconv.Itoa(i)
			test_writechunk(testId, chunks[i%3])
		}(i)
	}
	wg.Wait()

	fmt.Println("TEST: WriteChunkSync test pass")
}

func test_read_write_sync() {
	users := 10

	var wg sync.WaitGroup
	wg.Add(users)
	for i := 0; i < users; i++ {
		go func(i int) {
			defer wg.Done()
			testId := strconv.Itoa(i)
			if i == 3 {
				test_writechunk(testId, "chunk3")
			} else {
				r := rand.Intn(100)
				time.Sleep(time.Duration(r) * time.Millisecond)
				test_readchunk(testId, "chunk3")
			}
		}(i)
	}
	wg.Wait()

	fmt.Println("TEST: ReadWriteSync test pass")
}

func test_write_failed(testId string, chunk string) {
	ip := "127.0.0.1:8095"
	var args1 common.RPCArgs
	var res1 common.ChunkReply
	args1.Path = "/fileTmp"
	args1.Data = chunk

	err := common.Call(ip, "Master.WriteChunk", &args1, &res1)
	if err != nil {
		fmt.Printf("%s ERROR: %v\n", testId, err)
		return
	}
	fmt.Printf("%s CLIENT: Write to master 成功\n", testId)

	// ***************************** //

	var wg sync.WaitGroup
	nodeReply := make(map[string](*common.WritePrepareReply), len(res1.NodeList))
	prepareSucc := 1
	wg.Add(len(res1.NodeList))
	for _, node := range res1.NodeList {
		// 2PC phase 1
		go func(node string) {
			defer wg.Done()

			var args2 common.WriteChunkArgs
			res2 := new(common.WritePrepareReply)

			// 准备参数
			args2.Path = args1.Path
			args2.Chunk = args1.Data
			args2.Version = res1.Version
			// FIXME: test data
			args2.Offset = 0
			args2.Data = "this is test data Ver." + strconv.Itoa(res1.Version)
			err = common.Call(node, "Worker.WritePrepare", &args2, res2)
			if err != nil {
				fmt.Printf("%s CLIENT: Worker prepare 失败, ERROR: %v\n", testId, err)
				res2.Index = -1
				client.Lock()
				prepareSucc = 0
				nodeReply[node] = res2
				client.Unlock()
				return
			}
			client.Lock()
			nodeReply[node] = res2
			client.Unlock()
			fmt.Printf("%s CLIENT: Worker prepare node 成功\n", testId)
		}(node)
	}

	wg.Wait()

	if prepareSucc != 1 {
		fmt.Printf("%s CLIENT: Worker write prepare 阶段失败\n", testId)
		return
	} else {
		wg.Add(len(res1.NodeList))
		for _, node := range res1.NodeList {
			// 2PC phase 2
			go func(node string) {
				defer wg.Done()
				var args3 common.WriteCommitArgs

				// 准备参数
				args3.Index = nodeReply[node].Index
				for i := 0; i < 3; i++ {
					err = common.Call(node, "Worker.WriteChunk", &args3, nil)
					if err == nil {
						fmt.Printf("%s CLIENT: Worker write node 成功\n", testId)
						break
					}
					fmt.Printf("%s CLIENT: Worker write 失败，正在重试, ERROR: %v\n", testId, err)
				}
				fmt.Printf("%s CLIENT: Worker write node 失败\n", testId)
			}(node)
		}
	}

	wg.Wait()
	fmt.Printf("%s TEST: WriteFailed test pass\n", testId)
}

func test_high_load_read() {
	users := 100
	chunks := []string{"chunk1", "chunk2", "chunk3"}

	var wg sync.WaitGroup
	wg.Add(users)
	for i := 0; i < users; i++ {
		go func(i int) {
			defer wg.Done()
			testId := strconv.Itoa(i)
			r := rand.Intn(100)
			chunk := chunks[r%3]
			test_readchunk(testId, chunk)
		}(i)
	}
	wg.Wait()

	fmt.Println("TEST: HighLoadRead test pass")
}

func test_high_load_write() {
	users := 30
	chunks := []string{"chunk1", "chunk2", "chunk3"}

	var wg sync.WaitGroup
	wg.Add(users)
	for i := 0; i < users; i++ {
		go func(i int) {
			defer wg.Done()
			testId := strconv.Itoa(i)
			chunk := chunks[i%3]
			test_writechunk(testId, chunk)
		}(i)
	}
	wg.Wait()

	fmt.Println("TEST: HighLoadWrite test pass")
}

func test_high_load_append() {
	users := 30
	chunks := []string{"chunk1", "chunk2", "chunk3"}

	var wg sync.WaitGroup
	wg.Add(users)
	for i := 0; i < users; i++ {
		go func(i int) {
			defer wg.Done()
			testId := strconv.Itoa(i)
			chunk := chunks[i%3]
			test_appendchunk(testId, chunk)
		}(i)
	}
	wg.Wait()

	fmt.Println("TEST: HighLoadAppend test pass")
}

func test_file_open_close() {
	ip := "127.0.0.1:8095"
	users := 5
	flags := []int{common.O_RDONLY, common.O_WRONLY, common.O_RDWR}

	var wg sync.WaitGroup
	wg.Add(users)
	for i := 0; i < users; i++ {
		go func(i int) {
			defer wg.Done()
			var args common.OpenCloseArgs
			var res common.AuthReply

			args.Path = "/fileTmp"
			args.User = strconv.Itoa(i)
			args.Flag = flags[i%3]

			err := common.Call(ip, "Master.OpenFile", &args, nil)
			if err != nil {
				fmt.Printf("%s ERROR: %v\n", args.User, err)
				return
			}
			fmt.Printf("%s CLIENT: File Opened\n", args.User)

			err = common.Call(ip, "Master.GetAuth", &args, &res)
			if err != nil {
				fmt.Printf("%s ERROR: %v\n", args.User, err)
				return
			}

			if res.Flag == args.Flag {
				fmt.Printf("%s CLIENT: Auth check pass\n", args.User)
			} else {
				fmt.Printf("%s CLIENT: Auth check failed\n", args.User)
			}

			err = common.Call(ip, "Master.CloseFile", &args, nil)
			if err != nil {
				fmt.Printf("%s ERROR: %v\n", args.User, err)
				return
			}
			fmt.Printf("%s CLIENT: File Closed\n", args.User)
		}(i)
	}
	wg.Wait()

	fmt.Println("TEST: FileOpenClose test pass")
}

func test_isexist() {
	ip := "127.0.0.1:8095"
	var args common.RPCArgs
	args.Path = "/fileTmp"

	err := common.Call(ip, "Master.IsExist", &args, nil)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}
	fmt.Println("TEST: IsExist test pass")
}

func main() {
	rand.Seed(time.Now().UnixNano())
	client = NewClient("127.0.0.1:8095")
	// test_createfile()
	test_writechunk("1", "chunk1")
	// test_getchunk()
	// test_getchunks()
	// test_deletefile()
	// test_appendchunk("1", "chunk2")
	// test_readchunk("1", "chunk1")
	// test_writeabort()
	// test_isexist()

	// test_getchunk_sync()
	// test_getchunks_sync()
	// test_readchunk_sync()
	// test_readchunk_diff_sync()
	// test_read_write_diff_sync()
	// test_writechunk_diff_sync()
	// test_writechunk_sync()
	// test_read_write_sync()
	// test_write_failed("1", "chunk1")
	// test_high_load_read()
	// test_high_load_write()
	// test_file_open_close()

}
