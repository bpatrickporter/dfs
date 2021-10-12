package main

import (
	"dfs/messages"
	"fmt"
	"log"
	"net"
	"os"
)

func HandleArgs() string {
	port := os.Args[1]
	return port
}

func InitialLogger() {
	file, err := os.OpenFile("logs/controller_logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(file)
	log.Println("Controller start up complete")
}

func HandleConnection(conn net.Conn) {
	messageHandler := messages.NewMessageHandler(conn)
	for {
		request, _ := messageHandler.Receive()
		switch msg := request.Msg.(type) {
		case *messages.Wrapper_PutRequestMessage:
			//check if put is legal
			//return list of nodes
		case *messages.Wrapper_RegistrationMessage:
			node := msg.RegistrationMessage.GetNode()
			fmt.Println(node + " registered with controller")
			messageHandler.Close()

		case nil:
			log.Println("Received an empty message, termination connection.")
			messageHandler.Close()
			return
		default:
			continue
		}
	}
}

func main() {

	//list of active nodes
	//bloom filter - ram
	//file index with info on file names, dirs, and locations - disk
	port := HandleArgs()
	InitialLogger()
	listener, err := net.Listen("tcp", ":" + port)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	for {
		if conn, err := listener.Accept(); err == nil {
			log.Println("Accepted a connection.")
			go HandleConnection(conn)
		}
	}
}
