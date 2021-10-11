package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"dfs/messages"
	"fmt"
	"google.golang.org/protobuf/proto"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

func SendAck(ack *messages.Ack, conn net.Conn) error {
	serialized, err := proto.Marshal(ack)
	if err != nil {
		return err
	}

	prefix := make([]byte, 8)
	binary.LittleEndian.PutUint64(prefix, uint64(len(serialized)))
	conn.Write(prefix)
	conn.Write(serialized)

	fmt.Println("Ack Sent")
	return nil
}

func HandleRequest(conn net.Conn, rootDirectory string) {
		messageHandler := messages.NewMessageHandler(conn)
		defer messageHandler.Close()

		wrapper, _ := messageHandler.Receive()
		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_PutRequestMessage:
			log.Println("Put request received")
			metadata := msg.PutRequestMessage.GetMetadata()
			fileName := metadata.GetFileName()
			chunkSize := metadata.GetChunkSize()
			checkSum := metadata.GetCheckSum()

			file, err := os.Create(rootDirectory + fileName)
			log.Println("File created")
			if err != nil {
				fmt.Println(err.Error())
			}

			buffer := make([]byte, chunkSize)
			writer := bufio.NewWriter(file)

			log.Println("Preparing to write file")
			for {
				numBytes, err := conn.Read(buffer)
				if err != nil {
					fmt.Println(err.Error())
					break
				}
				reader := bytes.NewReader(buffer)
				_, err = io.CopyN(writer, reader, int64(numBytes))
				if err != nil {
					fmt.Println(err.Error())
					break
				}
				if numBytes < int(chunkSize) {
					break
				}
			}
			log.Println("File write complete")
			file2, err := os.Open(rootDirectory + fileName)
			if err != nil {
				fmt.Println(err.Error())
			}
			checkSum2 := messages.GetCheckSum(*file2)
			checkSumResults := strings.Compare(checkSum, checkSum2) == 0
			log.Println("Checksums match: " + strconv.FormatBool(checkSumResults))
			ack := &messages.Ack{CheckSumMatched: checkSumResults}
			err2 := SendAck(ack, conn)
			fmt.Println("Ack status: " + strconv.FormatBool(ack.CheckSumMatched))
			log.Println("Acknowledgment sent to client")
			if err2 != nil {
				return
			}
		case nil:
			log.Println("Received an empty message, terminating client")
		default:
			log.Println("Unexpected message type: %T", msg)
		}
}

func InitialLogger() {
	file, err := os.OpenFile("logs/node_logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(file)
	log.Println("Node start up complete")
}

func HandleArgs() (string, string) {
	port := os.Args[1]
	dir := os.Args[2]
	return port, dir
}

func main() {

	port, rootDir := HandleArgs()
	InitialLogger()
	listener, err := net.Listen("tcp", ":" + port)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	for {
		if conn, err := listener.Accept(); err == nil {
			go HandleRequest(conn, rootDir)
		}
	}
}
