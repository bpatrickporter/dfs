package main

import (
	"bufio"
	"dfs/messages"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

func HandleArgs() (string, string) {
	controller := os.Args[1]
	port := os.Args[2]
	//file := os.Args[3]
	//chunkSize, err := strconv.Atoi(os.Args[4])
	//return host, port, file, chunkSize, err
	return controller, port
}

func InitializeLogger() {
	file, err := os.OpenFile("logs/client_logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(file)
	log.Println("Client start up complete")
}

func GetMetadata(fileName string) (int, int32, string) {
	f, _ := os.Open(fileName)
	defer f.Close()
	fileInfo, _ := f.Stat()
	fileSize := fileInfo.Size()
	numChunks := int32(fileSize)/int32(64)
	checkSum := messages.GetCheckSum(*f)
	return int(fileSize), numChunks, checkSum
}

func GetInput(messageHandler *messages.MessageHandler) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		result := scanner.Scan() // Reads up to a \n newline character
		if result == false {
			break
		}

		message := scanner.Text()
		if len(message) != 0 {

			var wrapper *messages.Wrapper

			if strings.HasPrefix(message, "put"){
				var trimmed = strings.TrimPrefix(message, "/")
				var words = strings.Split(trimmed, " ")
				var file = words[1]
				chunkSize := 3
				_, chunks, checkSum := GetMetadata(file)
				metadata := &messages.Metadata{FileName: file, NumChunks: chunks, ChunkSize: int32(chunkSize), CheckSum: checkSum}
				msg := messages.PutRequest{Metadata: metadata}
				wrapper = &messages.Wrapper{
					Msg: &messages.Wrapper_PutRequestMessage{PutRequestMessage: &msg},
				}
				log.Println("Sending metadata.")
				messageHandler.Send(wrapper)

				/*
				f, _ := os.Open(file)
				log.Println("Opened file.")
				buffer := make([]byte, chunkSize)
				reader := bytes.NewReader(buffer)
				conn := messageHandler.GetConn()
				writer := bufio.NewWriter(conn)
				log.Println("Preparing to write file to connection.")
				for {
					log.Println("writing..")
					numBytes, err := f.Read(buffer)
					reader = bytes.NewReader(buffer)
					if err != nil {
						break
					}
					_, err = io.CopyN(writer, reader, int64(numBytes))
					if err != nil {
						fmt.Print(err.Error())
						break
					}
				}
				f.Close()

				 */
				log.Println("Done writing to connection.")
			} else if strings.HasPrefix(message, "get") {
				fmt.Println("get received")
			} else if strings.HasPrefix(message, "delete") {
				fmt.Println("delete received")
			} else if strings.HasPrefix(message, "ls") {
				fmt.Println("ls received")
			} else {
				fmt.Println("error ")
				}
			}

			//messageHandler.Send(wrapper)
		}
	}

func main() {
	controller, port := HandleArgs()
	InitializeLogger()
	conn, err := net.Dial("tcp", controller + ":" + port)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	defer conn.Close()
	fmt.Println("Connected to controller at " + controller + ":" + port)
	messageHandler := messages.NewMessageHandler(conn)
	go GetInput(messageHandler)
	for {
		wrapper, _ := messageHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_AcknowledgeMessage:
			status := msg.AcknowledgeMessage.GetCheckSumMatched()
			fmt.Println("File transfer status: " + strconv.FormatBool(status))
		case *messages.Wrapper_PutResponseMessage:
			available := msg.PutResponseMessage.GetAvailable()
			destinationNodes := msg.PutResponseMessage.GetNodes()
			log.Println("Received put response status: " + strconv.FormatBool(available))
			if available {
				log.Println("Preparing to send chunks")
				log.Println("Destination list length: " + strconv.Itoa(len(destinationNodes)) + " nodes")
				log.Println("Sending chunks to the following destinations: ")
				for node := range destinationNodes {
					log.Println(destinationNodes[node])
				}
			} else {
				log.Println("File with this name already exists, must delete first")
			}

		default:
			continue
		}
	}
}

