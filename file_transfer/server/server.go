package main

import (
	"chat/messages"
	"fmt"
	"log"
	"net"
)

var userMap map[string]*messages.MessageHandler

func handleClient(msgHandler *messages.MessageHandler) {
	defer msgHandler.Close()

	for {
		wrapper, _ := msgHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_RegistrationMessage:
			continue
		case *messages.Wrapper_ChatMessage:
			fmt.Println("<"+msg.ChatMessage.GetUsername()+"> ",
				msg.ChatMessage.MessageBody)
		case *messages.Wrapper_DirectMessage:
			continue
		case nil:
			log.Println("Received an empty message, terminating client")
			return
		default:
			log.Printf("Unexpected message type: %T", msg)
		}

	}
}

func main() {
	listener, err := net.Listen("tcp", ":9999")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	for {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := messages.NewMessageHandler(conn)
			go handleClient(msgHandler)
		}
	}
}
