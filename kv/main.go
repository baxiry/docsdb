package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"modernc.org/kv"
)

func (s *Store) FirstKey(prefix string) string {
	it, ok, err := s.db.Seek([]byte(prefix))
	if err != nil {
		return fmt.Errorf("at seek: %s", err).Error()
	}

	if !ok {
	}

	k, _, err := it.Next()

	if err != nil {
		return err.Error()
	}

	if !strings.HasPrefix(string(k), prefix) {
		return "is not found"
	}

	return string(k)
}

func main() {
	s := NewStore("test.db")
	defer s.db.Close()

	s.setKeys()

	//	s.benchSet(10)

	k := s.FirstKey("user")
	fmt.Println("\nuser", k)

	k = s.FirstKey("emple")
	fmt.Println("\nemple", k)

	k = s.FirstKey("uses")
	fmt.Println("\nuses", k)

	k = s.FirstKey("m")
	fmt.Println("\nm", k)
}

func (s *Store) LastKey(prefix string) string {

	it, b, err := s.db.Seek([]byte(prefix))
	if err != nil {
		return err.Error()
	}
	if !b {
		fmt.Println("key is not exists")
	}

	k, _, err := it.Next()

	return string(k)
}

func (s *Store) benchGet(max uint64) {
	var i uint64
	var size int
	var binint = make([]byte, 8)
	start := time.Now()
	binary.BigEndian.PutUint64(binint, uint64(i))
	for i = 0; i < max; i++ {
		v, err := s.db.Get([]byte{}, append([]byte("user"), binint...))
		if err != nil {
			log.Fatal(err)
			break
		}
		size += len(v)
	}

	fmt.Println(time.Since(start))
	fmt.Println("read size:", size/1024, "kv")

}

func (s *Store) benchSet(max uint64) {
	var i uint64
	var binint = make([]byte, 8)
	start := time.Now()
	for i = 0; i < max; i++ {

		key := append([]byte("user"), binint...)
		fmt.Println("set ", string(key))

		binary.BigEndian.PutUint64(binint, uint64(i))
		err := s.db.Set(key, []byte("value"))

		if err != nil {
			println(err)
		}
	}
	fmt.Println(time.Since(start))
}

var data = map[string]string{

	"user0": "value0",
	"user1": "value1",
	"user2": "value2",
	"user3": "value3",

	"boxer0": "value4",
	"boxer1": "value5",
	"boxer2": "value6",
	"boxer3": "value7",

	"emple0": "value0",
	"emple1": "value1",
	"emple2": "value2",
	"emple3": "value3",

	"admin0": "value4",
	"admin1": "value5",
}

func (s *Store) setKeys() {
	for k, v := range data {

		fmt.Println("set:", k, v)
		err := s.db.Set([]byte(k), []byte(v))
		if err != nil {
			fmt.Println("err:", err)
		}
	}
}

type Store struct {
	db *kv.DB
}

func NewStore(dbFile string) *Store {

	// Clean up previous run's DB file
	os.RemoveAll(dbFile)

	// Define DB options, including ACID level
	options := &kv.Options{}

	// Open the database file
	db, err := kv.Create(dbFile, options)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}

	return &Store{db: db}

}
