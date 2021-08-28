# Naïve gDocs Testing Report

### PART 1 Overview

对整个 gDocs 的测试根据系统的架构，分成了对不同层次、不同部分的测试，主要包括对 DFS Server（Master、Worker 节点）的测试、对 DFS Client 的测试、对 Web Server 的测试以及对前端的测试。每一个测试都是在下层测试的基础上进行的。

### PART 2 DFS

DFS 的 Server 主要是指 Master 节点和 Worker 节点，需要根据设计文档中，对 DFS 的诸多特性的实现进行相应的测试，判断实现的 RPC 调用是否能够达到预期效果。通过编写测试的代码模拟了 Client 调用 Master 和 Worker RPC 时的各种可能情况的运行逻辑，利用 goroutine 模拟了一定的并发访问，并在合适的地方添加一些代码来触发服务器进程的退出模拟 Server Failure 的事件。这部分的测试详细代码请见 `code/backend/ClientTest/clientTest.go`

##### Client API

Client 主要提供了 Open, Close, Create, Delete, Read, Write, Append 接口，该部分主要测试 Client 是否能够正常地与 Master 以及 Worker 节点交互并完成用户指定的操作，在`code/backend/ClientAPITest/clientAPITest.go`中主要通过 RPC 返回是否出现错误或超时以及 查看 worker 的数据记录是否与预期一致来进行验证。

| 测试函数     | 测试结果 |
| ------------ | -------- |
| `testCreate` | *pass*   |
| `testOpen`   | *pass*   |
| `testAppend` | *pass*   |
| `testRead`   | *pass*   |
| `testWrite`  | *pass*   |
| `testDelete` | *pass*   |

##### Basic Operation

测试了单用户访问 DFS 时 Master 和 Worker 节点的基本的 RPC 调用是否能够正常执行，通过打印 RPC 的返回结果和查看 Zookeeper 或本地文件的数据来判断测试是否通过。测试的大致内容如下表：

| 测试函数           | RPC 调用（重复的已省略）                                     | 测试结果 |
| ------------------ | ------------------------------------------------------------ | -------- |
| `test_getchunks`   | `Master.GetChunks`                                           | *pass*   |
| `test_getchunk`    | `Master.GetChunk`                                            | *pass*   |
| `test_createfile`  | `Master.CreateFile`                                          | *pass*   |
| `test_deletefile`  | `Master.DeleteFile` , `Worker.DeleteFile`                    | *pass*   |
| `test_writechunk`  | `Master.WriteChunk` , `Worker.WritePrepare` , `Worker.WriteChunk` | *pass*   |
| `test_appendchunk` | `Worker.AppendChunk`                                         | *pass*   |
| `test_writeabort`  | `Worker.WriteAbort`                                          | *pass*   |
| `test_readchunk`   | `Master.ReadChunk`                                           | *pass*   |
| `test_isexist`     | `Master.IsExist`                                             | *pass*   |

基本的 RPC 调用都可以按照预期的基本操作的逻辑执行，得到预期的结果，当控制合适的测试用例时也能获得正确的报错信息。

通过这些基本操作的组合，可以模拟出多种场景的测试用例，根据报错信息和 Zookeeper 及本地文件的状态来判断执行是否符合逻辑。（下表中加黑的为非正常的执行情况，应当报错）

| 测试序号                      | 测试逻辑                                    | 测试结果 |
| ----------------------------- | ------------------------------------------- | -------- |
| 1：create_on_not_exist        | 创建一个不存在的文件                        | *pass*   |
| 2：create_on_exist            | **创建一个已经存在的文件**                  | *pass*   |
| 3：is_exist_on_exist          | 判断一个已经存在的文件是否存在              | *pass*   |
| 4：is_exist_on_not_exist      | 判断一个不存在的文件是否存在                | *pass*   |
| 5：delete_on_empty            | 删除一个已存在的空文件                      | *pass*   |
| 6：delete_on_not_exist        | **删除一个不存在的文件**                    | *pass*   |
| 7：delete_on_not_empty        | 删除一个有内容的文件                        | *pass*   |
| 8：write_on_exist             | 写入一个已存在的文件已存在的 chunk          | *pass*   |
| 9：write_on_exist_new_chunk   | 写入一个已存在的文件不存在的 chunk          | *pass*   |
| 10：write_on_not_exist        | **写入一个不存在的文件**                    | *pass*   |
| 11：get_chunks_on_not_exist   | **获取不存在文件的 chunks 列表**            | *pass*   |
| 12：get_chunks_empty          | **获取为空的文件的 chunks 列表**            | *pass*   |
| 13：get_chunks_not_empty      | 获取不为空的文件的 chunks 列表              | *pass*   |
| 14：get_chunk_on_not_exist    | **获取不存在的文件的指定 chunk**            | *pass*   |
| 15：get_chunk_not_exist       | **获取存在文件的不存在的 chunk**            | *pass*   |
| 16：get_chunk_exist           | 获取存在文件的存在的 chunk                  | *pass*   |
| 17：append_on_exist           | 在已存在的文件的已存在的 chunk 追加内容     | *pass*   |
| 18：append_on_not_exist       | **在不存在的文件追加内容**                  | *pass*   |
| 19：append_on_exist_new_chunk | 在已存在的文件的不存在的 chunk 追加内容     | *pass*   |
| 20：read_on_not_exist_file    | **读取不存在的文件的内容**                  | *pass*   |
| 21：read_on_not_exist_chunk   | **读取已存在的文件的不存在的 chunk 的内容** | *pass*   |
| 22：read_on_exist             | 读取已存在的文件已存在的 chunk 的内容       | *pass*   |

通过以上的测试用例可以看出，Master 和 Worker 对于大部分正常或者异常的执行情况都能够正确处理。

##### Consistency

一致性的测试主要是指两个方面，一是 metadata 对于不同的 Master 的一致性，二是不同 Worker 保存相同的 chunk 内容的一致性。前者的一致性主要由 Zookeeper 保证，后者的一致性通过 2PC 和版本检查来保证。

具体的测试用例同样是通过组合上述基本操作进行。

针对 Metadata 在 Master 之间的一致性所使用的测试用例和测试结果如下：

| 测试序号                     | 测试逻辑                                           | 测试结果 |
| ---------------------------- | -------------------------------------------------- | -------- |
| 1：write_read_diff_master    | 在 Master A 写入 chunk，在 Master B 读取对应 chunk | *pass*   |
| 2：create_delete_diff_master | 在 Master A 创建文件，在 Master B 删除对应文件     | *pass*   |
| 3：create_write_diff_master  | 在 Master A 创建文件，在 Master B 写入文件         | *pass*   |
| 4：delete_read_diff_master   | 在 Master A 删除文件，在 Master B 读取文件         | *pass*   |

因为 Zookeeper 已经可以提供很好的一致性，因此这里仅做了一部分比较有代表性的测试，结果都符合预期。

针对 chunk 内容在不同 Worker 之间的一致性，使用 2PC 的方法来保证，2PC 中写操作的部分代码大致逻辑如下：

```go
...
err := common.Call(ip, "Master.WriteChunk", &args1, &res1)
...
var wg sync.WaitGroup
wg.Add(len(res1.NodeList))
// Phase 1
for _, node := range res1.NodeList {
 	go func(node string) {
        defer wg.Done()
    	...
        err = common.Call(node, "Worker.WritePrepare", &args2, res2)
		...
    }(node)
}
wg.Wait()
// Phase 2
for _, node := range res1.NodeList {
	go func(node string) {
        ...
        err = common.Call(node, "Worker.WriteChunk", &args3, nil)
        ...
    }(node)
}
...
```

2PC 的逻辑保证了在 Worker 可用的情况下，同一个 chunk 的数据会全部写到对应的 Worker 上，如果没有任何 Worker 可用则后续会调用 `WriteAbort` RPC 放弃写入。

由于之前对 Write 以及 Append 操作的测试本身就已经使用了 2PC 的逻辑，因此没有单独设计测试用例。从各个 Worker 都能读到对应的相同的数据，说明 Worker 间的数据一致性得到了保证，通过了测试。

##### Concurrency

利用 Go 的 goroutine 机制开启了多个协程，并行地执行某些逻辑，模拟测试并发情况下 DFS 的运行情况，通过对测试返回结果、打印信息、一致性方面的考察判断测试是否符合预期。

主要的测试用例及测试结果如下：

| 测试序号                | 测试逻辑                                                  | 测试结果 |
| ----------------------- | --------------------------------------------------------- | -------- |
| 1：getchunk_sync        | 多个用户同时获取相同的 chunk 所在的节点列表               | *pass*   |
| 2：getchunks_sync       | 多个用户同时获取相同文件的 chunk 列表                     | *pass*   |
| 3：readchunk_sync       | 多个用户同时读取相同 chunk 的内容                         | *pass*   |
| 4：readchunk_diff_sync  | 多个用户同时读取不同的 chunk 的内容                       | *pass*   |
| 5：read_write_diff_sync | 在一个用户写 Chunk A 时，多个用户以随机的时间读取 Chunk B | *pass*   |
| 6：writechunk_diff_sync | 多个用户同时写各不相同的 chunk                            | *pass*   |
| 7：writechunk_sync      | 多个用户同时写相同的 chunk                                | *pass*   |
| 8：read_write_sync      | 在一个用户写 Chunk A 时，多个用户以随机的时间读取 Chunk A | *pass*   |

其中的并发包括了不同程度的并发量（5/10/50/100）的条件下对应操作的多次测试，每个测试的结果都符合预期，说明并发性满足了要求。

##### Log & Recovery

为了测试 Log 和 Recovery 的实现，在 Master 的代码中加入了部分 trap 代码段，会触发 Master 进程退出，再次启动 Master 节点后通过观察本地日志和数据是否根据日志恢复，判断是否正常执行了 Log 和 Recover 的逻辑。

以 Write 操作为例，测试用例及测试结果如下：

| 测试序号                        | 测试逻辑                                          | 测试结果 |
| ------------------------------- | ------------------------------------------------- | -------- |
| 1：crash_before_log             | 在将操作写入 Log 之前触发 trap 退出 Worker 进程   | *pass*   |
| 2：crash_after_log_before_data  | 在将操作记入 Log 之后，实际写入磁盘之前触发 trap  | *pass*   |
| 3：crash_after_data_before_meta | 在将数据写入磁盘之后，更新内存元数据之前触发 trap | *pass*   |
| 4：crash_after_meta             | 在完成所有操作返回之前触发 trap                   | *pass*   |

##### Scalability, Heartbeat & Load Balance

由于 Worker 节点的可用性体现在 Master 是否接收到其心跳，且是否在内存中将其视为可用节点并为其分配 chunk，因此对可扩展性、心跳机制和负载均衡的测试通过人为的关闭、启动 Worker 进程，并根据 Master 进程心跳机制获取的信息来判断是否满足要求。

测试用例及测试结果如下：

| 测试序号 | 测试逻辑                                                     | 测试结果                                                     |
| -------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1        | 手动关闭 Worker 进程，观察 Master 返回的 Worker 节点列表是否还有此节点 | 下一次心跳后，关闭的 Worker 节点不再出现在 Master 返回 Client  的记录当中 |
| 2        | 手动启动 Worker 进程，观察 Master 返回的 Worker 节点列表是否增加了此节点 | 当 Worker 节点 Register 之后，此节点出现在了 Master 的记录中，且能正常为该节点指派 chunk，而由于负载均衡的机制，Master 会将新建的 chunk 优先保存到此节点上 |

### PART 3 Web Server

Web Server 主要提供了getRecycleBin, isFileAlive, create, load, falseDelete, trueDelete, restore, ws 接口，该部分主要测试 Web Server 是否能够正常地与前端和DFS这两部分交互并完成用户指定的操作，在`code/backend/httpserver/httpserver_test.go`中主要通过 "net/http/httptest"模拟前端发送相应请求并返回结果，并通过”testing“和相应逻辑来验证reponse status、返回结果和DFS中文件是否成功操作。在完成测试之后，删除DFS中产生的测试文件。

主要的测试用例及测试结果如下：

| 测试函数                     | 测试逻辑                                                     | 测试结果 |
| ---------------------------- | ------------------------------------------------------------ | -------- |
| `Test_getRecycleBin`         | 发送getRecycleBin请求，检查返回的status是否正确和返回的回收站列表是否为当前文件系统中所存的内容 | *pass*   |
| `Test_isFileAlive_notExist`  | 发送参数为一个不存在文件的isFileAlive请求，检查返回的status是否正确和返回结果是否为该文件不存在 | *pass*   |
| `Test_isFileAlive_exist`     | 发送参数为一个存在文件的isFileAlive请求，检查返回的status是否正确和返回结果是否为该文件存在 | *pass*   |
| `Test_create`                | 若DFS中Log不存在先创建Log，发送参数带文件名的create请求，检查返回的status是否正确和返回结果是否为创建成功，并在DFS中检查相应的文件和Log是否创建成功 | *pass*   |
| `Test_load`                  | 在DFS创建一个新的test文件，发送参数为test的load请求，检查返回的status是否正确和返回结果是否为期待的结构和内容 | *pass*   |
| `Test_falseDeleteAndRestore` | 若DFS中copy、RecycleBin不存在，先创建copy、RecycleBin，并新增一个test文件。发送参数为test的falseDelete请求，检查返回的status是否正确和返回结果是否为删除成功，并在DFS中检查copy里有无新增test文件和RecycleBin里是否新增了”test"内容；发送参数为test的restore请求，检查返回的status是否正确和返回结果是否为恢复成功，并在DFS中检查test文件是否恢复 | *pass*   |
| `Test_trueDelete`            | 先调用falseDelete，创造出DFS中的copy里有test、RecycleBin内容中有”test“的测试环境。发送参数为test的trueDelete请求，检查返回的status是否正确和返回结果是否为删除成功，并在DFS中检查copy里的test文件是否被删除 | *pass*   |
| `Test_websocket`             | 创建两个对同一个test文件的ws请求，通过WebSocket在线测试工具分别发送内容，检查DFS中文件保存的内容是否正确 | *pass*   |

