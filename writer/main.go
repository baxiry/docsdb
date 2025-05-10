package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"go.etcd.io/bbolt"
)

func main() {
	db, err := bbolt.Open("my.db", 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	store := NewStore(db)
	_ = store
	go store.Writer()

	for i := 0; i <= 40; i++ {
		item := ItemData{"users", []byte("user:" + str(i)), []byte("value:" + str(i))}
		ed := EntriesData{} //op: i
		ed.Items = append(ed.Items, item)
		entry <- ed
		time.Sleep(time.Millisecond * 200)
	}
}

type Store struct {
	db         *bbolt.DB
	insertChan chan EntriesData
}
type EntriesData struct {
	op    int
	Items []ItemData
	Done  chan error
}

type ItemData struct {
	Bucket string
	Key    []byte
	Value  []byte
}

var entry = make(chan EntriesData, 1)
var Op int
var mutex sync.Mutex

func (s *Store) Writer() {
	var entries []EntriesData
	busy := false

	for {
		fmt.Println("entries size is ", len(entries))
		itm := <-entry

		mutex.Lock()
		itm.op = Op
		mutex.Unlock()

		entries = append(entries, itm)

		mutex.Lock()
		Op++
		mutex.Unlock()

		if len(entries) == 5 && !busy {
			readyEntries := entries
			entries = []EntriesData{}
			go func() {

				busy = true
				fmt.Println("busy", busy)

				for _, v := range readyEntries {

					itm := v.Items[0]
					fmt.Printf("write %d %s %s %s\n", v.op, itm.Bucket, itm.Key, itm.Value)
					time.Sleep(time.Millisecond * 100)
				}
				busy = false
				fmt.Println("busy", busy)

			}()
		}
	}
}
func NewStore(db *bbolt.DB) *Store {
	s := &Store{
		db: db,
	}
	go s.Writer()
	return s
}

var str = fmt.Sprint
