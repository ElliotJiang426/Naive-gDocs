package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	common "gfs/Common"
	gfs "gfs/client"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"github.com/gorilla/websocket"
	"golang.org/x/text/encoding/charmap"
)

var client *gfs.Client

type V struct {
	V *int `json:"v"`
}

type Celldata struct {
	T string      `json:"t"`
	R int         `json:"r"`
	C int         `json:"c"`
	V interface{} `json:"v"`
}

type Image struct {
	V map[string]interface{} `json:"v"`
}

type AutoGenerated struct {
	Name     string        `json:"name"`
	Index    string        `json:"index"`
	Order    int           `json:"order"`
	Status   int           `json:"status"`
	Celldata []interface{} `json:"celldata"`
	Images   []interface{} `json:"images"`
}

type RecycleBin struct {
	Name string `json:"name"`
}

func isFileAlive(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	query := request.URL.Query()
	filename := query.Get("filename")

	log.Printf("GET: filename=%s\n", filename)
	fd, err := client.Open("/"+filename, common.O_RDWR)
	fmt.Println(err)
	if err == nil {
		client.Close(fd)
		fmt.Fprintf(writer, `true`)
	} else {
		fmt.Fprintf(writer, `false`)
	}
}

func create(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	query := request.URL.Query()
	filename := query.Get("filename")

	log.Printf("GET: filename=%s\n", filename)
	_, err := client.Create("/" + filename)
	if err == nil {
		fmt.Fprintf(writer, `create sucess`)
	} else {
		fmt.Fprintf(writer, `create fail`)
	}
	_, err = client.Create("/Log/" + filename)
	if err == nil {
		fmt.Fprintf(writer, `create log sucess`)
	} else {
		fmt.Fprintf(writer, `create log fail`)
	}

}

func load(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	query := request.URL.Query()
	filename := query.Get("filename")

	fd, _ := client.Open("/"+filename, common.O_RDWR)
	buffer := make([]byte, common.MaxChunkSize)
	pos, _ := client.Read(fd, 0, buffer)
	// fmt.Println(string(buffer[0:pos]))

	arr := strings.Split(string(buffer[0:pos]), "||")

	var celldatalist = []interface{}{}
	var imageList = []interface{}{}
	for i := 0; i < len(arr); i++ {
		if arr[i] == "" {
			continue
		}
		fmt.Println(arr[i])
		var celldata Celldata
		json.Unmarshal([]byte(arr[i]), &celldata)

		if celldata.T == "all" {
			var image Image
			var imageProperty interface{}
			json.Unmarshal([]byte(arr[i]), &image)
			for k := range image.V {
				imageProperty = image.V[k]
			}
			imageList = append(imageList, imageProperty)
		} else {
			celldatalist = append(celldatalist, celldata)
		}

	}

	data := new(AutoGenerated)
	data.Name = filename
	data.Index = "sheet_01"
	data.Order = 1
	data.Status = 1
	data.Celldata = celldatalist
	data.Images = imageList
	var datalist = []AutoGenerated{*data}

	json, err := json.Marshal(datalist)
	if err != nil {
		fmt.Println("json error")
	}
	fmt.Println(string(json))
	writer.Write(json)
}

func getRecycleBin(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Access-Control-Allow-Origin", "*")

	fd, _ := client.Open("/RecycleBin", common.O_RDWR)
	buffer := make([]byte, 10000)
	pos, _ := client.Read(fd, 0, buffer)

	// arr := strings.Fields(string(buffer[0:pos]))
	arr := strings.Split(string(buffer[0:pos]), "||")
	fmt.Println(arr)

	datalist := []RecycleBin{}
	for i := 0; i < len(arr); i++ {
		if arr[i] == "" {
			continue
		}
		var data RecycleBin
		data.Name = arr[i]
		datalist = append(datalist, data)
	}

	json, err := json.Marshal(datalist)
	if err != nil {
		fmt.Println("json error")
	}
	writer.Write(json)
}

func falseDelete(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	query := request.URL.Query()
	filename := query.Get("filename")

	fd, _ := client.Open("/RecycleBin", common.O_RDWR)
	client.Append(fd, []byte("||"))
	client.Append(fd, []byte(filename))
	client.Close(fd)

	fd, _ = client.Open("/"+filename, common.O_RDWR)
	buffer := make([]byte, 10000)
	pos, _ := client.Read(fd, 0, buffer)
	data := string(buffer[0:pos])
	client.Delete("/" + filename)
	client.Close(fd)

	fd, _ = client.Create("/copy/" + filename)
	client.Write(fd, 0, []byte(data))
	client.Close(fd)

	fmt.Fprintf(writer, `delete sucess`)
}

func trueDelete(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	query := request.URL.Query()
	filename := query.Get("filename")

	fd, _ := client.Open("/RecycleBin", common.O_RDWR)
	buffer := make([]byte, 10000)
	pos, _ := client.Read(fd, 0, buffer)
	arr := strings.Split(string(buffer[0:pos]), "||")

	client.Delete("/RecycleBin")
	fd, _ = client.Create("/RecycleBin")

	for i := 0; i < len(arr); i++ {
		if arr[i] != filename && arr[i] != "" {
			client.Append(fd, []byte(arr[i]))
			client.Append(fd, []byte("||"))
		}
	}

	client.Delete("/copy/" + filename)
	client.Close(fd)
	fmt.Fprintf(writer, `delete sucess`)
}

func restore(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	query := request.URL.Query()
	filename := query.Get("filename")

	fd, _ := client.Open("/RecycleBin", common.O_RDWR)
	buffer := make([]byte, 10000)
	pos, _ := client.Read(fd, 0, buffer)
	arr := strings.Split(string(buffer[0:pos]), "||")

	client.Delete("/RecycleBin")
	fd, _ = client.Create("/RecycleBin")

	for i := 0; i < len(arr); i++ {
		if arr[i] != filename && arr[i] != "" {
			client.Append(fd, []byte(arr[i]))
			client.Append(fd, []byte("||"))
		}
	}

	fd, _ = client.Open("/copy/"+filename, common.O_RDWR)
	buffer = make([]byte, 10000)
	pos, _ = client.Read(fd, 0, buffer)
	data := string(buffer[0:pos])
	client.Delete("/copy/" + filename)

	fd, _ = client.Create("/" + filename)
	client.Write(fd, 0, []byte(data))
	client.Close(fd)
	fmt.Fprintf(writer, `restore sucess`)
}

//客户端管理
type ClientManager struct {
	//客户端 map 储存并管理所有的长连接client，在线的为true，不在的为false
	clients map[*WsClient]bool
	//web端发送来的的message我们用broadcast来接收，并最后分发给所有的client
	broadcast chan []byte
	//谁发来的信息
	cid string
	//新创建的长连接client
	register chan *WsClient
	//新注销的长连接client
	unregister chan *WsClient
}

// 客户端 Client
type WsClient struct {
	username string          // 用户名
	filename string          // 文件名
	id       string          // 用户id
	socket   *websocket.Conn // 连接的socket
	send     chan []byte     // 发送的消息
}

// 会把Message格式化成json
type Message struct {
	//消息struct
	Id       string `json:"id,omitempty"`       //websocket的id
	Type     string `json:"type"`               //类型
	Data     string `json:"data"`               //内容
	Username string `json:"username,omitempty"` //用户名
}

// 创建客户端管理者
// var manager = ClientManager{
// 	broadcast:  make(chan []byte),
// 	register:   make(chan *WsClient),
// 	unregister: make(chan *WsClient),
// 	clients:    make(map[*WsClient]bool),
// 	cid:        "",
// }

// socket 设置
var (
	upgrader = websocket.Upgrader{
		//
		ReadBufferSize: 1023,
		//
		WriteBufferSize: 1023,
		//允许跨域
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

// manager table
var managerTable = make(map[string](*ClientManager))

func (manager *ClientManager) start() {
	fmt.Println("start")
	for {
		select {
		//如果有新的连接接入,就通过channel把连接传递给conn
		case conn := <-manager.register:
			//把客户端的连接设置为true
			manager.clients[conn] = true
			//把返回连接成功的消息json格式化
			//jsonMessage, _ := json.Marshal(&Message{Data: "id:" + manager.cid + " has connected."})
			//调用客户端的send方法，发送消息
			//manager.send(jsonMessage, conn)
			//如果连接断开了
		case conn := <-manager.unregister:
			//判断连接的状态，如果是true,就关闭send，删除连接client的值
			if _, ok := manager.clients[conn]; ok {
				close(conn.send)
				//todo delete有问题
				delete(manager.clients, conn)
				//jsonMessage, _ := json.Marshal(&Message{Data: "id:" + manager.cid + " has disconnected."})
				//manager.send(jsonMessage, conn)
			}
			//广播
		case message := <-manager.broadcast:
			//遍历已经连接的客户端，把消息发送给他们
			//fmt.Println(message)
			for conn := range manager.clients {
				//判断发送给谁（不发送自己）
				//接收者id： conn.id
				//发送者id： manager.cid
				fmt.Println(conn.id)
				fmt.Println(manager.cid)
				if conn.id == manager.cid {
					continue
				}
				select {
				case conn.send <- message:
					// default:
					// 	close(conn.send)
					// 	delete(manager.clients, conn)
				}
			}
		}
	}
}

//定义客户端管理的send方法
func (manager *ClientManager) send(message []byte, ignore *WsClient) {
	for conn := range manager.clients {
		//不给屏蔽的连接发送消息
		if conn != ignore {
			conn.send <- message
		}
	}
}

//定义客户端结构体的read方法
func (c *WsClient) read() {
	manager, _ := managerTable[c.filename]
	defer func() {
		//结构体cid赋值
		manager.cid = c.id
		//触发关闭
		manager.unregister <- c
		c.socket.Close()
	}()

	for {
		//读取消息
		_, message, err := c.socket.ReadMessage()
		//如果有错误信息，就注销这个连接然后关闭
		if err != nil {
			//fmt.Println(err)
			manager.unregister <- c
			c.socket.Close()
			break
		}
		//如果没有错误信息就把信息放入broadcast
		//fmt.Println(string(message))
		unmessage, err := ungzip(message)
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println(string(unmessage))
		fmt.Println(len(string(unmessage)))
		var msg struct {
			T string `json:"t"`
			R int    `json:"r"`
			C int    `json:"c"`
		}
		json.Unmarshal(unmessage, &msg)

		var tp string
		switch msg.T {
		case "v", "rv", "rv_end", "cg", "fc", "drc", "all", "arc", "f", "fsc", "fsr", "sha", "shc", "shd", "shr", "shre", "sh", "c", "na":
			tp = "2"
			fd, _ := client.Open("/"+c.filename, common.O_RDWR)
			client.Append(fd, []byte(string(unmessage)))
			client.Append(fd, []byte("||"))
			client.Close(fd)

			// 记录 log
			timeObj := time.Now()
			var str = timeObj.Format("2006-01-02 03:04:05")
			fd, _ = client.Open("/Log/"+c.filename, common.O_RDWR)
			client.Append(fd, []byte(str+" "))
			client.Append(fd, []byte(c.username+" "))
			client.Append(fd, []byte("modify ROW: "+strconv.Itoa(msg.R)+" Col: "+strconv.Itoa(msg.C)+"\n"))
			client.Close(fd)
		// case "all":
		// 	tp = "2"

		case "mv":
			tp = "3"
		case "":
			tp = "4"
		default:
			tp = "1"
		}

		jsonMessage, _ := json.Marshal(&Message{Id: c.id, Data: string(unmessage), Type: tp, Username: c.username})

		//结构体cid赋值
		manager.cid = c.id
		//触发消息发送
		manager.broadcast <- jsonMessage

	}
}

func ungzip(gzipmsg []byte) (reqmsg []byte, err error) {
	if len(gzipmsg) == 0 {
		return
	}
	if string(gzipmsg) == "rub" {
		reqmsg = gzipmsg
		return
	}
	e := charmap.ISO8859_1.NewEncoder()
	encodeMsg, err := e.Bytes(gzipmsg)
	if err != nil {
		return
	}
	b := bytes.NewReader(encodeMsg)
	r, err := gzip.NewReader(b)
	if err != nil {
		return
	}
	defer r.Close()
	reqmsg, err = ioutil.ReadAll(r)
	if err != nil {
		return
	}
	reqstr, err := url.QueryUnescape(string(reqmsg))
	if err != nil {
		return
	}
	reqmsg = []byte(reqstr)
	return
}

func (c *WsClient) write() {
	defer func() {
		c.socket.Close()
	}()

	for {
		select {
		//从send里读消息
		case message, ok := <-c.send:
			//如果没有消息
			if !ok {
				c.socket.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			//有消息就写入，发送给web端
			fmt.Println(string(message))
			c.socket.WriteMessage(websocket.TextMessage, message)
		}
	}
}

func wsHandler(res http.ResponseWriter, req *http.Request) {
	//将http协议升级成websocket协议
	conn, err := (&websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}).Upgrade(res, req, nil)
	if err != nil {
		http.NotFound(res, req)
		return
	}
	//可以用传来的参数识别身份
	query := req.URL.Query()
	filename := query.Get("filename")
	id := (uuid.Must(uuid.NewV4())).String()

	// 获取/创建对应的 manager
	manager, ok := managerTable[filename]
	if !ok {
		manager = new(ClientManager)
		manager.broadcast = make(chan []byte)
		manager.register = make(chan *WsClient)
		manager.unregister = make(chan *WsClient)
		manager.clients = make(map[*WsClient]bool)
		manager.cid = ""
		go manager.start()
		managerTable[filename] = manager
	}

	//这里是随机生成id
	//每一次连接都会新开一个client，client.id通过uuid生成保证每次都是不同的
	usernames := []string{"Alice", "Bob", "Carol", "Dave", "Elliot", "Frank", "Grace", "Helen", "Iva", "Joshua"}
	r := rand.Intn(10)
	wsclient := &WsClient{username: usernames[r], filename: filename, id: id, socket: conn, send: make(chan []byte)}
	//注册一个新的链接
	manager.cid = wsclient.id
	manager.register <- wsclient

	//启动协程收web端传过来的消息
	go wsclient.read()
	//启动协程把消息返回给web端
	go wsclient.write()
}

func main() {
	client = gfs.NewClient("127.0.0.1:8095")
	rand.Seed(time.Now().UnixNano())

	//check if file copy exists
	fd, err1 := client.Open("/copy", common.O_RDWR)
	if err1 != nil {
		fd, _ = client.Create("/copy")
	}
	client.Close(fd)
	//check if RecycleBin exists
	fd, err1 = client.Open("/RecycleBin", common.O_RDWR)
	if err1 != nil {
		fd, _ = client.Create("/RecycleBin")
	}
	client.Close(fd)
	//check if RecycleBin exists
	fd, err1 = client.Open("/Log", common.O_RDWR)
	if err1 != nil {
		fd, _ = client.Create("/Log")
	}
	client.Close(fd)

	// go manager.start()

	http.HandleFunc("/ws", wsHandler)
	http.HandleFunc("/isFileAlive", isFileAlive)
	http.HandleFunc("/create", create)
	http.HandleFunc("/load", load)
	http.HandleFunc("/getRecycleBin", getRecycleBin)
	http.HandleFunc("/falseDelete", falseDelete)
	http.HandleFunc("/delete", trueDelete)
	http.HandleFunc("/restore", restore)

	err := http.ListenAndServe(":8088", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err.Error())
	}
}
