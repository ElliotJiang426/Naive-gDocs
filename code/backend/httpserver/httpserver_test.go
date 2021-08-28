package main
 
import (
    //"fmt"
    "strings"
    "testing"
	"net/http"
	gfs "gfs/client"
	common "gfs/Common"
	"net/http/httptest"
)
 
func Test_getRecycleBin(t *testing.T) {
	client = gfs.NewClient("127.0.0.1:8095")
    //新建file RecycleBin
	fd, err1 := client.Open("/RecycleBin", common.O_RDWR)
	if err1 != nil {
		fd, _ = client.Create("/RecycleBin")
	}
	client.Close(fd)

	req, err := http.NewRequest("GET", "/getRecycleBin", nil)
    if err != nil {
        t.Fatal(err)
    }
    rr := httptest.NewRecorder()
    getRecycleBin(rr, req)
    if status := rr.Code; status != http.StatusOK {
        t.Errorf("handler returned wrong status code: got %v want %v",
            status, http.StatusOK)
    }
    expected := `[{"name":""}]`
    if rr.Body.String() != expected {
        t.Errorf("handler returned unexpected body: got %v want %v",
            rr.Body.String(), expected)
    }

    //删除file
	client.Delete("/RecycleBin")
}

func Test_isFileAlive_notExist(t *testing.T) {
	req, err := http.NewRequest("GET", "/isFileAlive?filename=test", nil)
    if err != nil {
        t.Fatal(err)
    }
    rr := httptest.NewRecorder()
    isFileAlive(rr, req)
    if status := rr.Code; status != http.StatusOK {
        t.Errorf("handler returned wrong status code: got %v want %v",
            status, http.StatusOK)
    }
    expected := `false`
    if rr.Body.String() != expected {
        t.Errorf("handler returned unexpected body: got %v want %v",
            rr.Body.String(), expected)
    }
}

func Test_isFileAlive_exist(t *testing.T) {
	//新建file test
	fd, err1 := client.Open("/test", common.O_RDWR)
	if err1 != nil {
		fd, _ = client.Create("/test")
	}
	client.Close(fd)

	req, err := http.NewRequest("GET", "/isFileAlive?filename=test", nil)
    if err != nil {
        t.Fatal(err)
    }
    rr := httptest.NewRecorder()
    isFileAlive(rr, req)
    if status := rr.Code; status != http.StatusOK {
        t.Errorf("handler returned wrong status code: got %v want %v",
            status, http.StatusOK)
    }
    expected := `true`
    if rr.Body.String() != expected {
        t.Errorf("handler returned unexpected body: got %v want %v",
            rr.Body.String(), expected)
    }

	//删除file test
	client.Delete("/test")
}

func Test_create(t *testing.T) {
	//新建log
	fd, err1 := client.Open("/Log", common.O_RDWR)
	if err1 != nil {
		fd, _ = client.Create("/Log")
	}
	client.Close(fd)

	req, err := http.NewRequest("GET", "/create?filename=test", nil)
    if err != nil {
        t.Fatal(err)
    }
    rr := httptest.NewRecorder()
    create(rr, req)
    if status := rr.Code; status != http.StatusOK {
        t.Errorf("handler returned wrong status code: got %v want %v",
            status, http.StatusOK)
    }
    expected := `create sucesscreate log sucess`
    if rr.Body.String() != expected {
        t.Errorf("handler returned unexpected body: got %v want %v",
            rr.Body.String(), expected)
    }

	//删除file test和log
	client.Delete("/test")
	client.Delete("/Log/test")
	client.Delete("/Log")
}

func Test_load(t *testing.T) {
	//新建test并写入"test"
	fd, err1 := client.Open("/test", common.O_RDWR)
	if err1 != nil {
		fd, _ = client.Create("/test")
		client.Write(fd, 0, []byte("test"))
	}
	client.Close(fd)

	req, err := http.NewRequest("POST", "/load?filename=test", nil)
    if err != nil {
        t.Fatal(err)
    }
    rr := httptest.NewRecorder()
    load(rr, req)
    if status := rr.Code; status != http.StatusOK {
        t.Errorf("handler returned wrong status code: got %v want %v",
            status, http.StatusOK)
    }
    expected := `[{"name":"test","index":"sheet_01","order":1,"status":1,"celldata":[{"t":"","r":0,"c":0,"v":null}],"image":[]}]`
    if rr.Body.String() != expected {
        t.Errorf("handler returned unexpected body: got %v want %v",
            rr.Body.String(), expected)
    }

	//删除file test
	client.Delete("/test")
}

func Test_falseDeleteAndRestore(t *testing.T) {
	//新建RecycleBin
	fd, err1 := client.Open("/RecycleBin", common.O_RDWR)
	if err1 != nil {
		fd, _ = client.Create("/RecycleBin")
	}
	client.Close(fd)
    //新建copy
    fd, err1 = client.Open("/copy", common.O_RDWR)
	if err1 != nil {
		fd, _ = client.Create("/copy")
	}
	client.Close(fd)
	//新建test并写入"test"
	fd, err1 = client.Open("/test", common.O_RDWR)
	if err1 != nil {
		fd, _ = client.Create("/test")
		client.Write(fd, 0, []byte("test"))
	}
	client.Close(fd)

	req, err := http.NewRequest("GET", "/falseDelete?filename=test", nil)
    if err != nil {
        t.Fatal(err)
    }
    rr := httptest.NewRecorder()
    falseDelete(rr, req)
    if status := rr.Code; status != http.StatusOK {
        t.Errorf("handler returned wrong status code: got %v want %v",
            status, http.StatusOK)
    }
    expected := `delete sucess`
    if rr.Body.String() != expected {
        t.Errorf("handler returned unexpected body: got %v want %v",
            rr.Body.String(), expected)
    }
    //检查RecycleBin中是否有删除的文件名
    fd, _ = client.Open("/RecycleBin", common.O_RDWR)
	buffer := make([]byte, 10000)
	pos, _ := client.Read(fd, 0, buffer)
	arr := strings.Split(string(buffer[0:pos]), "||")
    for i := 0; i < len(arr); i++ {
		if arr[i] == "test" {
			break
		}
        if i == len(arr) - 1{
            if arr[i] != "test"{
                t.Errorf("RecylceBin doesn't have test")
            }   
        }
	}
    client.Close(fd)
    //检查copy/test中是否存储着删除的数据
    fd, _ = client.Open("/copy/test", common.O_RDWR)
	buffer = make([]byte, 10000)
	pos, _ = client.Read(fd, 0, buffer)
    data := string(buffer[0:pos])
    expectedData := "test"
    if data != expectedData{
        t.Errorf("handler returned unexpected body: got %v want %v",
           data, expectedData)
    }
    client.Close(fd)

    //restore
    req, err = http.NewRequest("GET", "/restore?filename=test", nil)
    if err != nil {
        t.Fatal(err)
    }
    rr = httptest.NewRecorder()
    restore(rr, req)
    if status := rr.Code; status != http.StatusOK {
        t.Errorf("handler returned wrong status code: got %v want %v",
            status, http.StatusOK)
    }
    expected = `restore sucess`
    if rr.Body.String() != expected {
        t.Errorf("handler returned unexpected body: got %v want %v",
            rr.Body.String(), expected)
    }
    //检查test文件是否成功恢复
    fd, _ = client.Open("/test", common.O_RDWR)
	buffer = make([]byte, 10000)
	pos, _ = client.Read(fd, 0, buffer)
    data = string(buffer[0:pos])
    expectedData = "test"
    if data != expectedData{
        t.Errorf("handler returned unexpected body: got %v want %v",
           data, expectedData)
    }
    client.Close(fd)

    //删除file
    client.Delete("/test")
    client.Delete("/copy")
    client.Delete("/RecycleBin")
}

func Test_trueDelete(t *testing.T) {
	//新建RecycleBin
	fd, err1 := client.Open("/RecycleBin", common.O_RDWR)
	if err1 != nil {
		fd, _ = client.Create("/RecycleBin")
	}
	client.Close(fd)
    //新建copy
    fd, err1 = client.Open("/copy", common.O_RDWR)
	if err1 != nil {
		fd, _ = client.Create("/copy")
	}
	client.Close(fd)
	//新建test并写入"test"
	fd, err1 = client.Open("/test", common.O_RDWR)
	if err1 != nil {
		fd, _ = client.Create("/test")
		client.Write(fd, 0, []byte("test"))
	}
	client.Close(fd)
    
    //falseDelete
    req, err := http.NewRequest("GET", "/falseDelete?filename=test", nil)
    if err != nil {
        t.Fatal(err)
    }
    rr := httptest.NewRecorder()
    falseDelete(rr, req)
    if status := rr.Code; status != http.StatusOK {
        t.Errorf("handler returned wrong status code: got %v want %v",
            status, http.StatusOK)
    }
    expected := `delete sucess`
    if rr.Body.String() != expected {
        t.Errorf("handler returned unexpected body: got %v want %v",
            rr.Body.String(), expected)
    }

	req, err = http.NewRequest("GET", "/trueDelete?filename=test", nil)
    if err != nil {
        t.Fatal(err)
    }
    rr = httptest.NewRecorder()
    trueDelete(rr, req)
    if status := rr.Code; status != http.StatusOK {
        t.Errorf("handler returned wrong status code: got %v want %v",
            status, http.StatusOK)
    }
    expected = `delete sucess`
    if rr.Body.String() != expected {
        t.Errorf("handler returned unexpected body: got %v want %v",
            rr.Body.String(), expected)
    }

    //删除file
    client.Delete("/copy")
    client.Delete("/RecycleBin")
}

func Test_websocket(t *testing.T) {
    //新建file test1
	fd, err1 := client.Open("/test1", common.O_RDWR)
	if err1 != nil {
		fd, _ = client.Create("/test1")
	}
	client.Close(fd)

	req1, err1 := http.NewRequest("GET", "/ws?filename=test1", nil)
    if err1 != nil {
        t.Fatal(err1)
    }
    rr1 := httptest.NewRecorder()
    wsHandler(rr1, req1)

    req2, err2 := http.NewRequest("GET", "/ws?filename=test1", nil)
    if err2 != nil {
        t.Fatal(err2)
    }
    rr2 := httptest.NewRecorder()
    wsHandler(rr2, req2)

	//删除file test
	client.Delete("/test1")  
}