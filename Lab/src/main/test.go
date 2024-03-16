package main

import (
	"io/ioutil"
	"log"
	"os"
	//"fmt"
)

func main() {
	// 在默认的临时目录中创建临时文件
	customTempDir := "/home/runoob/repo/MIT6.824Labs/Lab/src/main/mr-tmp"
	tmpFile, err := ioutil.TempFile(customTempDir, "example")
	if err != nil {
		log.Fatal(err)
	}
	// 使用临时文件进行操作
	_, err = tmpFile.Write([]byte("Hello, World!\n"))
	if err != nil {
		log.Fatal(err)
	}
	tmpFile.Close()
	os.Rename(tmpFile.Name(), customTempDir+"/outputName")

}
