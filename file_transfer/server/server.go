package main

import (
	"bufio"
	"encoding/binary"
	"file_transfer/messages"
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

func main() {

	listener, err := net.Listen("tcp", ":9999")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}

	for {
		if conn, err := listener.Accept(); err == nil {

			metadata, _ := ReceiveMetadata(conn)
			file_name := metadata.FileName
			num_chunks := metadata.NumChunks
			chunk_size := metadata.ChunkSize
			check_sum := metadata.CheckSum

			file, _ := os.Create("copy_" + file_name)
			reader := bufio.NewReader(conn)
			writer := bufio.NewWriter(file)

			for i := 1; i < int(num_chunks); i ++ {
				n, _ := io.CopyN(writer, reader, int64(chunk_size))
				fmt.Println("writing.." + strconv.Itoa(int(n)))
			}

			file2, err := os.Open("copy_" + file_name)
			if err != nil {
				fmt.Println(err.Error())
			}
			check_sum2 := messages.GetCheckSum(*file2)

			ack := &messages.Ack{CheckSumMatched: strings.Compare(check_sum, check_sum2) != 0}
			err2 := SendAck(ack, conn)
			fmt.Println("Ack status: " + strconv.FormatBool(ack.CheckSumMatched))
			if err2 != nil {
				return
			}
		}
	}
}
