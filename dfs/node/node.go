package main

import (
	"bufio"
	"bytes"
	"dfs/messages"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
)

func HandleArgs() (string, string, string, string) {
	listeningPort := os.Args[1]
	rootDir := os.Args[2]
	controllerName := os.Args[3]
	controllerPort := os.Args[4]
	return listeningPort, rootDir, controllerName, controllerPort
}

func InitializeLogger() {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalln()
	}
	file, err := os.OpenFile("/home/bpporter/P1-patrick/dfs/logs/" + hostname  + "_logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(file)
	log.Println("Node start up complete")
}

func RegisterWithController(context context) error {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalln()
	}
	conn, err := net.Dial("tcp", context.controllerName + ":" + context.controllerPort)
	if err != nil {
		log.Fatalln(err.Error())
		return err
	}
	defer conn.Close()
	log.Println("Connected to controller: " + context.controllerName + ":" + context.controllerPort)
	registration := &messages.Registration{Node: hostname, Port: context.listeningPort}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_RegistrationMessage{RegistrationMessage: registration},
	}
	messageHandler := messages.NewMessageHandler(conn)
	messageHandler.Send(wrapper)
	log.Println("Node registered with controller")
	messageHandler.Close()
	return err
}

func HandleConnection(conn net.Conn, context context) {
	log.Println("Accepted a connection from " + conn.RemoteAddr().String())
	messageHandler := messages.NewMessageHandler(conn)
	for {
		request, _ := messageHandler.Receive()
		switch msg := request.Msg.(type) {
		case *messages.Wrapper_PutRequestMessage:
			log.Println("Put request received for")
			metadata := msg.PutRequestMessage.GetMetadata()
			fileName := metadata.GetFileName()
			log.Println("-> " + fileName)
			chunkSize := metadata.GetChunkSize()
			//checkSum := metadata.GetCheckSum()

			log.Println("Trying to create [" + context.rootDir + fileName + "]")
			file, err := os.Create(context.rootDir + fileName)
			defer file.Close()
			if err != nil {
				fmt.Println(err.Error())
				log.Println(err.Error())
				return
			}
			log.Println("File created" + fileName)
			buffer := make([]byte, chunkSize)
			writer := bufio.NewWriter(file)

			log.Println("Preparing to write chunk")
			numBytes, err := conn.Read(buffer)
			log.Println("Read from conn: " + strconv.Itoa(numBytes))
			if err != nil {
				fmt.Println(err.Error())
			}
			reader := bytes.NewReader(buffer)
			log.Println("read bytes into buffer")
			_, err = io.CopyN(writer, reader, int64(numBytes))
			log.Println("Copied buffer to writer")
			if err != nil {
				fmt.Println(err.Error())
				break
			}

			log.Println("File write complete")
			/*
			file2, err := os.Open(context.rootDir + fileName)
			if err != nil {
				fmt.Println(err.Error())
			}
			checkSum2 := messages.GetCheckSum(*file2)
			checkSumResults := strings.Compare(checkSum, checkSum2) == 0
			log.Println("Checksums match: " + strconv.FormatBool(checkSumResults))
			ack := &messages.Ack{CheckSumMatched: checkSumResults}
			response := &messages.Wrapper{
				Msg: &messages.Wrapper_AcknowledgeMessage{AcknowledgeMessage: ack},
			}
			err = messageHandler.Send(response)
			fmt.Println("Ack status: " + strconv.FormatBool(ack.CheckSumMatched))
			log.Println("Acknowledgment sent to client")
			if err != nil {
				return
			}

			 */
		case nil:
			log.Println("Received an empty message, terminating client")
			messageHandler.Close()
			return
		default:
			continue
		}
	}
}

func InitializeContext() context {
	listeningPort, rootDir, controllerName, controllerPort := HandleArgs()
	return context{rootDir: rootDir, listeningPort: listeningPort, controllerName: controllerName, controllerPort: controllerPort}
}

type context struct {
	rootDir string
	listeningPort string
	controllerName string
	controllerPort string
}

func main() {
	context := InitializeContext()
	InitializeLogger()
	err  := RegisterWithController(context)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	listener, err := net.Listen("tcp", ":" + context.listeningPort)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	for {
		if conn, err := listener.Accept(); err == nil {
			go HandleConnection(conn, context)
		}
	}
}
