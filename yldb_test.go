package yldb

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var (
	path = "./test_data/test_basic"
	r    = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func getRandomString(length int) []byte {
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bytes := []byte(str)
	var result []byte
	for i := 0; i < length; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return result
}

func TestBasic(t *testing.T) {
	db, err := Open(path)
	if db == nil || err != nil {
		t.Fatal(err)
	}
	_ = db.Set([]byte("123"), []byte("456"), nil)

	value, err := db.Get([]byte("123"), nil)
	fmt.Println(string(value))

	_ = db.Delete([]byte("123"), nil)
	value, err = db.Get([]byte("123"), nil)
	fmt.Println(err)

	_ = db.Set([]byte("123"), []byte("789"), nil)
	value, _ = db.Get([]byte("123"), nil)
	fmt.Println(string(value))
	db.Close()
}
