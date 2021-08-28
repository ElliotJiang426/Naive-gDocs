package common

import (
	"errors"
	"fmt"
	"net/rpc"
	"sort"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

// HeartbeatReply 信息
type HeartbeatReply struct {
	FileInfos map[string](map[string]int)
	ChunkLoad int
}

// Master
// chunk 元数据信息
type ChunkInfo struct {
	Chunk    string
	Version  int
	NodeList []string
}

// RPC 参数结构体
type RPCArgs struct {
	Path string
	Data string
}

// RPC 返回结构体
type ChunkReply struct {
	Version  int
	NodeList []string
}

// Register 参数结构体
type RegisterArgs struct {
	IP   string
	Load int
}

type OpenCloseArgs struct {
	Path string
	User string
	Flag int
}

type AuthReply struct {
	Flag int
}

// Worker
type WriteChunkArgs struct {
	Path    string
	Chunk   string
	Version int
	Offset  int64
	Data    string
}

type WritePrepareReply struct {
	Index int
}

type WriteCommitArgs struct {
	Index int
}

type AppendChunkReply struct {
	WriteSize int
}

type ReadChunkArgs struct {
	Path    string
	Chunk   string
	Version int
}

type ReadChunkReply struct {
	Data string
}

type DeleteFileArgs struct {
	Path string
}

// Client
type CreateReply struct {
}
type DeleteReply struct {
}

const (
	//Client
	RetryTimes = 3
	// chunk
	MaxChunkSize   = 1 << 20 // 1 MB
	MaxAppendSize  = MaxChunkSize / 4
	HeartbeatTime  = 10
	CheckpointTime = 30
	NodeForChunk   = 3 // 每个 chunk 存储到多少 chunkserver 上
	O_RDONLY       = 0
	O_WRONLY       = 1
	O_RDWR         = 2
)

// Master 节点的 zk server 的地址
var MasterZkHosts = []string{
	"192.168.152.128:2181",
	"192.168.152.128:2182",
	"192.168.152.128:2183"}

// var MasterZkHosts = []string{
// 	"127.0.0.1:2181",
// 	"127.0.0.1:2182",
// 	"127.0.0.1:2183"}

// Master 节点的 RPC 地址
var MasterRPCHosts = []string{
	"127.0.0.1:8095",
	"127.0.0.1:8096",
	"127.0.0.1:8097"}

// Worker 节点的 RPC 地址
var WorkerRPCHosts = []string{
	"127.0.0.1:8098",
	"127.0.0.1:8099",
	"127.0.0.1:8100"}

/**
 * 增
 */
func Add(conn *zk.Conn, path string, datastr string) error {
	var data = []byte(datastr)
	// flags有4种取值：
	// 0: 永久，除非手动删除
	// zk.FlagEphemeral = 1: 短暂，session断开则该节点也被删除
	// zk.FlagSequence  = 2: 会自动在节点后面添加序号
	// 3:Ephemeral和Sequence，即，短暂且自动添加序号
	var flags int32 = 0
	// 获取访问控制权限
	acls := zk.WorldACL(zk.PermAll)
	s, err := conn.Create(path, data, flags, acls)
	if err != nil {
		fmt.Printf("Add: 创建 %s 失败, err: %v\n", path, err)
		return err
	}
	fmt.Printf("Add: 创建 %s 成功\n", s)
	return nil
}

/**
 * 获取 Ticket, 监视前一个
 */
func Lock(conn *zk.Conn, path string) (string, error) {
	var flags int32 = 3
	acls := zk.WorldACL(zk.PermAll)

	myTicket, err := conn.Create(path+"/lock/temp_lock", nil, flags, acls)
	if err != nil {
		fmt.Printf("Lock: 创建 %s 失败, err: %v\n", path, err)
		return myTicket, err
	}

	lockList, _, err := conn.Children(path + "/lock")
	if err != nil {
		fmt.Printf("Lock: 获取锁列表失败, err: %v\n", err)
		return myTicket, err
	}

	if len(lockList) == 1 {
		// 没有其他锁
		return myTicket, err
	} else {
		// 有其他锁
		sort.Strings(lockList)
		minTicket := path + "/lock/" + lockList[0]
		if myTicket == minTicket {
			return myTicket, err
		}

		s := strings.Index(myTicket, "temp_lock")
		myOrder := myTicket[s:]

		// 找到上一个锁的持有者
		for i, ticket := range lockList {
			if myOrder == ticket {
				lastTicket := lockList[i-1]
				ok, _, ch, err := conn.ExistsW(path + "/lock/" + lastTicket)
				if err != nil {
					fmt.Printf("Lock: 监听失败, err: %v\n", err)
					return myTicket, err
				}

				// 开始监听
				if ok {
					fmt.Printf("Lock: 开始监听, err: %v\n", err)
					c := <-ch
					if c.Type == zk.EventNodeDeleted {
						return myTicket, err
					} else if c.Type == zk.EventNodeDataChanged {
						Modify(conn, myTicket, "CANCELED")
						Unlock(conn, myTicket)
						err = errors.New("锁的状态发生改变，获取锁失败")
						return myTicket, err
					}
				} else {
					return myTicket, err
				}
				break
			}
		}
	}

	return myTicket, err
}

/**
 * 放锁
 */
func Unlock(conn *zk.Conn, lock string) error {
	_, sate, err := conn.Get(lock)
	if err != nil {
		fmt.Printf("Unlock: 删除 %s 失败, err: %v\n", lock, err)
		return err
	}
	err = conn.Delete(lock, sate.Version)
	if err != nil {
		fmt.Printf("Unlock: 删除 %s 失败, err: %v\n", lock, err)
		return err
	}
	return nil
}

/**
 * 注销锁
 */
func CancelLock(conn *zk.Conn, path string) error {
	lockList, _, err := conn.Children(path)
	for _, lock := range lockList {
		_, sate, err := conn.Get(path + "/" + lock)
		if err == nil {
			err = conn.Delete(path+"/"+lock, sate.Version)
		}
	}
	_, sate, err := conn.Get(path)
	err = conn.Delete(path, sate.Version)
	if err != nil {
		fmt.Printf("CancelLock: 删除 lock 失败, err: %v\n", err)
		return err
	}
	fmt.Println("CancelLock: 删除 lock 成功")
	return err
}

/**
 * 查
 */
func Get(conn *zk.Conn, path string) ([]byte, error) {
	data, _, err := conn.Get(path)
	if err != nil {
		fmt.Printf("Get: 查询 %s 失败, err: %v\n", path, err)
		return data, err
	}
	fmt.Printf("Get: %s 的值为 %s\n", path, string(data))
	return data, err
}

// 删改与增不同在于其函数中的version参数,其中version是用于 CAS支持
// 可以通过此种方式保证原子性
/**
 * 改
 */
func Modify(conn *zk.Conn, path string, newDataStr string) []byte {
	newData := []byte(newDataStr)
	oldData, sate, err := conn.Get(path)
	if err != nil {
		fmt.Printf("Modify: 修改 %s 失败, err: %v\n", path, err)
		return nil
	}
	_, err = conn.Set(path, newData, sate.Version)
	if err != nil {
		fmt.Printf("Modify: 修改 %s 失败, err: %v\n", path, err)
		return nil
	}
	fmt.Printf("Modify: 修改 %s 成功 %s -> %s\n", path, string(oldData), newDataStr)
	return newData
}

/**
 * 删
 */
func Del(conn *zk.Conn, path string) {
	_, sate, err := conn.Get(path)
	if err != nil {
		fmt.Printf("Del: 删除 %s 失败, err: %v\n", path, err)
		return
	}
	err = conn.Delete(path, sate.Version)
	if err != nil {
		fmt.Printf("Del: 删除 %s 失败, err: %v\n", path, err)
		return
	}
	fmt.Printf("Del: 删除 %s 成功\n", path)
}

/**
 * @description Call is a RPC helper function
 * @date 16:03 2021/7/4
 * @param
 * @return
 **/
func Call(addr string, rpcName string, args interface{}, reply interface{}) error {
	// it seems that it may cause the leak of goroutines
	conn, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	timeout := time.Duration(5 * time.Second)
	done := make(chan error, 1)
	go func() {
		done <- conn.Call(rpcName, args, reply)
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("rpc timeout")
	}
}

/**
 * @description Sample randomly choose k elements from {0, 1, ..., n-1}
 * @date 19:47 2021/7/4
 * @param
 *		n  number of elements
 *		k  number of elements selected, k should be less than or equal to n
 * @return
 **/
//
//func Sample(n, k int) ([]int, error) {
//	if n < k {
//		return nil, fmt.Errorf("population is not enough for sampling (n = %d, k = %d)", n, k)
//	}
//	return rand.Perm(n)[:k], nil
//}

type Error struct {
	Code int
	Err  string
}

const (
	EOF     = iota
	WCN     // WrongChunkNumber
	UNKNOWN // UnknownError
)

func (e Error) Error() string {
	return e.Err
}
