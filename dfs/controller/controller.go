package main

import (
	"log"
	"net"
	"os"
)

func InitialLogger() {
	file, err := os.OpenFile("logs/controller_logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(file)
	log.Println("Controller start up complete")
}

func HandleRequests(listener net.Listener) {
	/*
	if conn, err := listener.Accept(); err == nil {

	}

	 */
}

func main() {

	//list of active nodes
	//bloom filter - ram
	//file index with info on file names, dirs, and locations - disk

	InitialLogger()
	listener, err := net.Listen("tcp", ":" + os.Args[1])
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	for {
		HandleRequests(listener)
	}
}
