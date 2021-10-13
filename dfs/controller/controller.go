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

func UnpackMetadata(metadata *messages.Metadata) (string, int32, int32, string){
	return metadata.GetFileName(), metadata.GetNumChunks(), metadata.GetChunkSize(), metadata.GetCheckSum()
}

func FileExists(fileName string, context *context) bool {
	_, exists := context.fileIndex[fileName]
	return exists
}

func GetDestinationNodes(context *context) []string {
	//get list of nodes randomly--for now return all active nodes
	destinationNodes := make([]string, 0)
	for node := range context.activeNodes {
		log.Println("Adding " + node + " to destination nodes")
		destinationNodes = append(destinationNodes, node)
	}
	log.Println("Printing destination nodes after appending..")
	for node := range destinationNodes {
		log.Println("-> " + destinationNodes[node])
	}
	return destinationNodes
}

func HandleConnection(conn net.Conn, context context) {
	messageHandler := messages.NewMessageHandler(conn)
	for {
		request, _ := messageHandler.Receive()
		switch msg := request.Msg.(type) {
		case *messages.Wrapper_PutRequestMessage:
			log.Println("Put request received")
			log.Println("Unpacking file metadata")
			fileName, _, _, _ := UnpackMetadata(msg.PutRequestMessage.GetMetadata())
			log.Println("Checking file index for file")
			var available bool
			var destinationNodes []string
			if FileExists(fileName, &context) {
				log.Println("Result: File exists")
				available = false
				destinationNodes = make([]string, 0)
			} else {
				log.Println("Result: File doesn't exist")
				//add to bloomfilter
				d := GetDestinationNodes(&context)
				destinationNodes = append(destinationNodes, d...)
				log.Println("Adding destination nodes to file index")
				context.fileIndex[fileName] = destinationNodes
				log.Println("Added the follwing destination nodes for filename " + fileName)
				for i := range destinationNodes {
					log.Println("-> " + destinationNodes[i])
				}
				available = true
			}
			log.Println("Outside the if statement")
			for i := range destinationNodes {
				log.Println("-> " + destinationNodes[i])
			}
			putResponse := &messages.PutResponse{Available: available, Nodes: destinationNodes}
			wrapper := &messages.Wrapper{
				Msg: &messages.Wrapper_PutResponseMessage{PutResponseMessage: putResponse},
			}
			log.Println("Sending put response to client")
			messageHandler.Send(wrapper)
		case *messages.Wrapper_RegistrationMessage:
			node := msg.RegistrationMessage.GetNode()
			port := msg.RegistrationMessage.GetPort()
			context.activeNodes[node + ":" + port] = struct{}{}
			log.Println(node + " registered with controller using port " + port)
			messageHandler.Close()
			log.Print("Active nodes: ")
			for k, _ := range context.activeNodes {
				log.Printf("-> %s \n", k)
			}
			messageHandler.Close()
			return
		case nil:
			fmt.Println("Received an empty message, termination connection.")
			messageHandler.Close()
			return
		default:
			continue
		}
	}
}

type context struct {
	activeNodes map[string]struct{}
	fileIndex map[string][]string
	bloomFilter map[string]int
}

func newContext(activeNodes map[string]struct{}, fileIndex map[string][]string, bloomFilter map[string]int ) *context {

	c := context{activeNodes: activeNodes, fileIndex: fileIndex, bloomFilter: bloomFilter}
	return &c
}

func main() {
	context := context{activeNodes: make(map[string]struct{}), fileIndex: make(map[string][]string), bloomFilter: make(map[string]int)}
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
			go HandleConnection(conn, context)
		}
	}
}
