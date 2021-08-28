package main

import (
	"bufio"
	"fmt"
	common "gfs/Common"
	gfs "gfs/client"
	"io"
	"os"
	"strings"
)

var client *gfs.Client

func testCreate() bool {
	_, err := client.Create("/testFile")

	if err != nil {
		fmt.Println("Create : Test Failed!")
		return false
	}
	fmt.Println("Create: Test Pass!")
	return true
}

func testOpen() bool {
	_, err := client.Open("/testFile", common.O_RDWR)
	if err != nil {
		fmt.Println("Open : Test Failed!")
		return false
	}
	fmt.Println("Open: Test Pass!")
	return true
}

func testAppend() bool {
	fd, err := client.Open("/testFile", common.O_RDWR)
	if err != nil {
		fmt.Println("Append : Test Failed!(When Open)")
		return false
	}
	err = client.Append(fd, []byte("testAppendContent"))
	if err != nil {
		fmt.Println("Append : Test Failed!")
		return false
	}
	file, _ := os.OpenFile("../data/8098/testFile/chunk0.txt", os.O_RDONLY, 0666)
	reader := bufio.NewReader(file)
	var build strings.Builder
	for {
		str, err := reader.ReadString('\n')
		build.WriteString(str)
		if err == io.EOF {
			break
		}
	}

	if strings.Compare(build.String(), "testAppendContent") != 0 {
		fmt.Println("Append : Test Failed!")
		return false
	}
	fmt.Println("Append: Test Pass!")
	return true
}

func testRead() bool {
	fd, err := client.Open("/testFile", common.O_RDWR)
	if err != nil {
		fmt.Println("Read : Test Failed!(When Open)")
		return false
	}

	readBuf := make([]byte, 20)
	cnt, err := client.Read(fd, 0, readBuf)
	if err != nil {
		fmt.Println("Read: Test Failed!")
		return false
	}

	if strings.Compare(string(readBuf[:cnt]), "testAppendContent") != 0 {
		fmt.Println("Read : Test Failed!")
		return false
	}
	fmt.Println("Read: Test Pass!")
	return true

}

func testDelete() {
	err := client.Delete("/testFile")
	if err != nil {
		fmt.Println("Delete: Test Failed!")
	}
	fmt.Println("Delete: Test Pass!")
}

func testWrite() {
	fd, err := client.Open("/testFile", common.O_RDWR)
	if err != nil {
		fmt.Println("Write : Test Failed!(When Open)")
		return
	}
	err = client.Write(fd, 0, []byte("testWrite1"))
	if err != nil {
		fmt.Println("Write : Test Failed!")
	}

	file, _ := os.OpenFile("../data/8098/testFile/chunk0.txt", os.O_RDONLY, 0666)
	reader := bufio.NewReader(file)
	var build strings.Builder
	for {
		str, err := reader.ReadString('\n')
		build.WriteString(str)
		if err == io.EOF {
			break
		}
	}

	if strings.Compare(build.String(), "testWrite1Content") != 0 {
		fmt.Println("Write : Test Failed!")
		return
	}
	fmt.Println("Write: Test Pass!")
}

func main() {
	client = gfs.NewClient("127.0.0.1:8095")
	flag := testCreate()
	if flag {
		flag = testOpen()
	}
	if flag {
		flag = testAppend()
	}
	if flag {
		flag = testRead()
	}
	if flag {
		testWrite()
	}
	testDelete()
}
