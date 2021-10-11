package messages

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"os"
)

func GetCheckSum(f os.File) string {
	hasher := sha256.New()
	if _, err := io.Copy(hasher, &f); err != nil {
		log.Fatal(err)
	}
	return hex.EncodeToString(hasher.Sum(nil))
}
