package main

import (
	"bufio"
	"chat/messages"
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	user := os.Args[1]
	fmt.Println("Hello, " + user)

	host := os.Args[2]
	conn, err := net.Dial("tcp", host+":9999") // connect to localhost port 9999
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	msgHandler := messages.NewMessageHandler(conn)

	defer conn.Close()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("message> ")
		result := scanner.Scan() // Reads up to a \n newline character
		if result == false {
			break
		}

		message := scanner.Text()
		if len(message) != 0 {
			msg := messages.Chat{Username: user, MessageBody: message}
			wrapper := &messages.Wrapper{
				Msg: &messages.Wrapper_ChatMessage{ChatMessage: &msg},
			}
			msgHandler.Send(wrapper)
		}
	}
}
