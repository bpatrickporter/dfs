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

func GetMetadata(fileName string) (int, string) {
	f, _ := os.Open(fileName)
	defer f.Close()
	fileInfo, _ := f.Stat()
	fileSize := fileInfo.Size()
	checkSum := messages.GetCheckSum(*f)
	return int(fileSize), checkSum
}

func UnpackMetadata(metadata *messages.Metadata) (string, int, int, int, string){
	return metadata.GetFileName(), int(metadata.GetFileSize()), int(metadata.GetNumChunks()), int(metadata.GetChunkSize()), metadata.GetCheckSum()
}

func PackagePutRequest(fileName string) *messages.Wrapper {
	fileSize, checkSum := GetMetadata(fileName)
	metadata := &messages.Metadata{FileName: fileName, FileSize: int32(fileSize), CheckSum: checkSum}
	msg := messages.PutRequest{Metadata: metadata}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_PutRequestMessage{PutRequestMessage: &msg},
	}
	return wrapper
}

func HandleInput(scanner *bufio.Scanner, controllerConn net.Conn) {
	message := scanner.Text()
	if len(message) != 0 {

		var wrapper *messages.Wrapper
		controllerMessageHandler := messages.NewMessageHandler(controllerConn)

		if strings.HasPrefix(message, "put"){
			var trimmed = strings.TrimPrefix(message, "/")
			var words = strings.Split(trimmed, " ")
			var fileName = words[1]
			wrapper = PackagePutRequest(fileName)
			log.Println("Sending metadata.")
			controllerMessageHandler.Send(wrapper)
			go HandleConnection(controllerMessageHandler)

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
			//send get request
			//go handleConnection()
		} else if strings.HasPrefix(message, "delete") {
			fmt.Println("delete received")
			//send dlete request
			//go handleconnection
		} else if strings.HasPrefix(message, "ls") {
			fmt.Println("ls received")
			//send ls request
			//go handleconnection
		} else {
			fmt.Println("error ")
		}
	}
}

func HandleConnection(messageHandler *messages.MessageHandler) {
	for {
		wrapper, _ := messageHandler.Receive()

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_AcknowledgeMessage:
			status := msg.AcknowledgeMessage.GetCheckSumMatched()
			fmt.Println("File transfer status: " + strconv.FormatBool(status))
		case *messages.Wrapper_PutResponseMessage:
			available := msg.PutResponseMessage.GetAvailable()
			destinationNodes := msg.PutResponseMessage.GetNodes()
			metadata := msg.PutResponseMessage.GetMetadata()
			log.Println("Received put response status: " + strconv.FormatBool(available))
			if available {
				log.Println("Preparing to send chunks")
				log.Println("Sending chunks to the following destinations: ")
				fileName, fileSize, numChunks, chunkSize, checkSum := UnpackMetadata(metadata)
				log.Printf("Metadata: \nName: %s \nSize: %d \nChunks: %d \nChunk Size: %d \n", fileName, fileSize, numChunks, chunkSize)
				log.Printf("Checksum: %s \n", checkSum)
				//send chunks out
				//close connections
				for node := range destinationNodes {
					log.Println(destinationNodes[node])
				}
			} else {
				fmt.Println("File with this name already exists, must delete first")
				log.Println("File already exists")
			}
			//return?
		case *messages.Wrapper_GetResponseMessage:
			log.Println("Get Response message received")
			//go get chunks
			//return
		case *messages.Wrapper_DeleteResponseMessage:
			log.Println("Delete Response message received")
			//return
		default:
			continue
		}
	}
}

func main() {
	controller, port := HandleArgs()
	InitializeLogger()
	controllerConn, err := net.Dial("tcp", controller + ":" + port)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	defer controllerConn.Close()
	fmt.Println("Connected to controller at " + controller + ":" + port)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		if result := scanner.Scan(); result != false {
			HandleInput(scanner, controllerConn)
		}
	}
}

