package kvraft

import (
	"log"
	"os"
)

const Debug = true

var (
	//Debug bool
	logger *log.Logger
)

func init() {
	file, err := os.OpenFile("log.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Failed to open log file:", err)
	}
	logger = log.New(file, "", log.LstdFlags)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		logger.Printf(format, a...)
	}
	return
}
