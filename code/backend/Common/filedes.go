package common

import "fmt"

const (
	MaxFDCnt = 65536
)
type FileDescriptor struct {
	id uint
}
type MetaInfo struct {
	path string
	auth int
}
type FileTable struct {
	table  map[uint]MetaInfo
	cnt uint
}

func (ft *FileTable) GetAndSet (path string, auth int) (*FileDescriptor, error){
	if ft.cnt >= MaxFDCnt{
		return nil, fmt.Errorf("exceeds available file descriptor size")
	}
	ft.cnt ++
	for i := 0; i < MaxFDCnt; i++{
		if _, ok := ft.table[uint(i)]; !ok{
			ft.table[uint(i)] = MetaInfo{
				path: path,
				auth: auth,
			}
			return &FileDescriptor{id: uint(i)}, nil
		}
	}
	// should never reach here
	fmt.Printf("FileDescriptor Error: should never reach here!")
	return nil, nil
}

func (ft *FileTable) Delete(fd *FileDescriptor) error{
	if _, ok := ft.table[fd.id]; !ok{
		return fmt.Errorf("it has been closed")
	}
	ft.cnt--
	delete(ft.table, fd.id)
	return nil
}

func NewFileTable() *FileTable{
	return &FileTable{
		cnt: 0,
		table: make(map[uint]MetaInfo, MaxFDCnt),
	}
}

func (ft *FileTable) GetPath (fd *FileDescriptor) (string, error){
	if fd == nil{
		return "", fmt.Errorf("fd is null")
	}
	if metaInfo, ok := ft.table[fd.id]; !ok{
		return "", fmt.Errorf("fd error")
	}else {
		return metaInfo.path, nil
	}
}

func (ft *FileTable) HaveWriteAuth(fd *FileDescriptor)(bool, error){
	if fd == nil{
		return false, fmt.Errorf("fd is null")
	}
	if metaInfo, ok := ft.table[fd.id]; !ok{
		return false, fmt.Errorf("fd error")
	}else {
		auth := metaInfo.auth
		return auth == O_RDWR || auth == O_WRONLY, nil
	}
}

func (ft *FileTable) HaveReadAuth(fd *FileDescriptor)(bool, error){
	if fd == nil{
		return false, fmt.Errorf("fd is null")
	}
	if metaInfo, ok := ft.table[fd.id]; !ok{
		return false, fmt.Errorf("fd error")
	}else {
		auth := metaInfo.auth
		return auth == O_RDWR || auth == O_RDONLY, nil
	}
}