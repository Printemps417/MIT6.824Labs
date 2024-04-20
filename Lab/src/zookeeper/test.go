package main

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

func main() {
	// 连接到 ZooKeeper 服务
	conn, _, err := zk.Connect([]string{"172.17.0.2:2181"}, time.Second)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// 创建根节点
	rootPath := "/root"
	if err := createNode(conn, rootPath, ""); err != nil {
		panic(err)
	}

	// 创建子节点，并存储数据
	child1Path := rootPath + "/child1"
	child2Path := rootPath + "/child2"
	if err := createNode(conn, child1Path, "Data for Child1"); err != nil {
		panic(err)
	}
	if err := createNode(conn, child2Path, "Data for Child2"); err != nil {
		panic(err)
	}
	defer deleteNode(conn, child1Path)
	defer deleteNode(conn, child2Path)

	// 获取子节点数据并输出
	children, _, err := conn.Children(rootPath)
	if err != nil {
		panic(err)
	}
	for _, child := range children {
		path := rootPath + "/" + child
		data, _, err := conn.Get(path)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Node %s data: %s\n", path, string(data))
		printNodeVersions(conn, path)
	}
	// 设置 Watcher
	go watchNode(conn, child1Path)
	go changeNode(conn, child1Path, "Changed data for Child1")

	// 等待一段时间，触发 Watcher
	time.Sleep(5 * time.Second)
}

func createNode(conn *zk.Conn, path string, data string) error {
	// 创建节点
	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)
	_, err := conn.Create(path, []byte(data), flags, acl)
	if err == zk.ErrNodeExists {
		// 节点已存在，忽略错误
		return nil
	}
	return err
}

func changeNode(conn *zk.Conn, path string, data string) error {
	time.Sleep(3 * time.Second)
	// 获取节点的版本号
	_, stat, err := conn.Get(path)
	if err != nil {
		return err
	}

	// 修改节点数据
	_, err = conn.Set(path, []byte(data), stat.Version)
	if err != nil {
		return err
	}
	printNodeVersions(conn, path)
	return nil
}

func watchNode(conn *zk.Conn, path string) {
	for {
		_, _, ch, err := conn.GetW(path)
		if err != nil {
			panic(err)
		}
		// 等待更改事件发生
		event := <-ch
		newdata, _, err := conn.Get(event.Path)
		fmt.Printf("Node %s has been updated! New data: %s\n", event.Path, newdata)
	}
}
func printNodeVersions(conn *zk.Conn, path string) error {
	// 获取节点的状态信息
	_, stat, err := conn.Exists(path)
	if err != nil {
		return err
	}
	if stat == nil {
		return fmt.Errorf("node %s does not exist", path)
	}

	// 打印节点的版本号信息
	fmt.Printf("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n")
	fmt.Printf("Node: %s\n", path)
	fmt.Printf("Data Version: %d\n", stat.Version)
	fmt.Printf("Children Version: %d\n", stat.Cversion)
	fmt.Printf("ACL Version: %d\n", stat.Aversion)
	fmt.Printf("Ephemeral Owner: %d\n", stat.EphemeralOwner)
	fmt.Printf("Modified Zxid: %d\n", stat.Mzxid)
	fmt.Printf("Created Zxid: %d\n", stat.Czxid)
	fmt.Printf("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n")

	return nil
}
func deleteNode(conn *zk.Conn, path string) error {
	// 删除节点
	err := conn.Delete(path, -1) // -1 表示删除最新版本的节点
	if err != nil {
		return err
	}

	return nil
}
