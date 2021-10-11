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

func ReceiveMetadata(conn net.Conn) (*messages.Metadata, error) {
	prefix := make([]byte, 8)
	conn.Read(prefix)

	payloadSize := binary.LittleEndian.Uint64(prefix)
	payload := make([]byte, payloadSize)
	conn.Read(payload)

	metadata := &messages.Metadata{}
	err := proto.Unmarshal(payload, metadata)
	return metadata, err
}

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

func HandleRequests(listener net.Listener, rootDirectory string) {
	if conn, err := listener.Accept(); err == nil {

		metadata, _ := ReceiveMetadata(conn)
		fileName := metadata.FileName
		//numChunks := metadata.NumChunks
		chunkSize := metadata.ChunkSize
		checkSum := metadata.CheckSum

		file, err := os.Create(rootDirectory + fileName)
		if err != nil {
			fmt.Println(err.Error())
		}

		buffer := make([]byte, chunkSize)
		writer := bufio.NewWriter(file)

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

		file2, err := os.Open(rootDirectory + fileName)
		if err != nil {
			fmt.Println(err.Error())
		}
		checkSum2 := messages.GetCheckSum(*file2)

		ack := &messages.Ack{CheckSumMatched: strings.Compare(checkSum, checkSum2) == 0}
		err2 := SendAck(ack, conn)
		fmt.Println("Ack status: " + strconv.FormatBool(ack.CheckSumMatched))
		if err2 != nil {
			return
		}
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

func main() {

	rootDirectory := os.Args[2]
	InitialLogger()
	listener, err := net.Listen("tcp", ":" + os.Args[1])
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	for {
		HandleRequests(listener, rootDirectory)
	}
}
