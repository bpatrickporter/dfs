package main

import (
	"dfs/messages"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
)

func InitializeLogger() {
	file, err := os.OpenFile("logs/controller_logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(file)
	log.Println("Controller start up complete")
}

func UnpackMetadata(metadata *messages.Metadata) (string, int32, int32, string){
	log.Println("Put request received")
	log.Println("Unpacking file metadata")
	return metadata.GetFileName(), metadata.GetNumChunks(), metadata.GetChunkSize(), metadata.GetCheckSum()
}

func FileExists(fileName string, context *context) bool {
	_, exists := context.fileIndex[fileName]
	if exists {
		log.Println("Result: File exists")
	} else {
		log.Println("Result: File doesn't exist")
	}
	return exists
}

func GetDestinationNodes(context *context) []string {
	//get list of nodes randomly--for now return all active nodes
	destinationNodes := make([]string, 0)
	for node := range context.activeNodes {
		log.Println("Adding " + node + " to destination nodes")
		destinationNodes = append(destinationNodes, node)
	}
	return destinationNodes
}

func ValidatePutRequest(fileName string, context *context) (bool, []string) {
	log.Println("Checking file index for file")
	exists := FileExists(fileName, context)
	log.Println("Exists = " + strconv.FormatBool(exists))
	var destinationNodes []string
	if !exists {
		//add to bloom filter
		d := GetDestinationNodes(context)
		destinationNodes = append(destinationNodes, d...)
		log.Println("Adding destination nodes to file index")
		context.fileIndex[fileName] = destinationNodes
		log.Println("Added the following destination nodes for filename " + fileName)
		for i := range destinationNodes {
			log.Println("-> " + destinationNodes[i])
		}
	} else {
		destinationNodes = make([]string, 0)
	}
	return exists, destinationNodes
}

func PackagePutResponse(fileExists bool, destinationNodes *[]string) *messages.Wrapper {
	putResponse := &messages.PutResponse{Available: !fileExists, Nodes: *destinationNodes}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_PutResponseMessage{PutResponseMessage: putResponse},
	}
	log.Println("Sending put response to client")
	return wrapper
}

func RegisterNode(msg *messages.Wrapper_RegistrationMessage, context *context) {
	node := msg.RegistrationMessage.GetNode()
	port := msg.RegistrationMessage.GetPort()
	context.activeNodes[node + ":" + port] = struct{}{}
	log.Println(node + " registered with controller using port " + port)
	log.Print("Active nodes: ")
	for k, _ := range context.activeNodes {
		log.Printf("-> %s \n", k)
	}
}

func HandleConnection(conn net.Conn, context context) {
	log.Println("Accepted a connection from " + conn.RemoteAddr().String())
	messageHandler := messages.NewMessageHandler(conn)
	for {
		request, _ := messageHandler.Receive()
		switch msg := request.Msg.(type) {
		case *messages.Wrapper_RegistrationMessage:
			RegisterNode(msg, &context)
			messageHandler.Close()
			return
		case *messages.Wrapper_PutRequestMessage:
			fileName, _, _, _ := UnpackMetadata(msg.PutRequestMessage.GetMetadata())
			fileExists, destinationNodes := ValidatePutRequest(fileName, &context)
			wrapper := PackagePutResponse(fileExists, &destinationNodes)
			messageHandler.Send(wrapper)
		case *messages.Wrapper_GetRequestMessage:
			log.Println("Get request message received")
		case *messages.Wrapper_DeleteRequestMessage:
			log.Println("Delete request message received")
		case *messages.Wrapper_HeartbeatMessage:
			log.Println("Heartbeat received")
		case nil:
			fmt.Println("Received an empty message, termination connection.")
			messageHandler.Close()
			return
		default:
			continue
		}
	}
}

func InitializeContext() context {
	return context{activeNodes: make(map[string]struct{}), fileIndex: make(map[string][]string), bloomFilter: make(map[string]int)}
}

type context struct {
	activeNodes map[string]struct{}
	fileIndex map[string][]string
	bloomFilter map[string]int
}

func main() {
	context := InitializeContext()
	InitializeLogger()
	listener, err := net.Listen("tcp", ":" + os.Args[1])
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
