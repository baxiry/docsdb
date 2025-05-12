package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"go.etcd.io/bbolt"
)

const max = 10

var entries = make(chan EntriesData, max)

func (s *Store) Writer() {

	var entriesdata = []EntriesData{}
	var ready = true
	for {

		ready = false
		for _, v := range entriesdata {
			itm := v.Items[0]
			fmt.Printf("  write %d %s %s %s\n", v.op, itm.Bucket, itm.Key, itm.Value)
			time.Sleep(time.Millisecond * 30)
		}
		ready = true

		time.Sleep(time.Millisecond * 100)
		entriesdata = []EntriesData{}

		for {
			ent := <-entries
			if ent.brk {
				break
			}
			fmt.Println("reseve new data", ent.op)

			entriesdata = append(entriesdata, ent)
			if len(entriesdata) == max {
				fmt.Println(" full pool")
			}

			if !ready {
				continue
			}
			if len(entriesdata) == 5 {
				break
			}

		}
	}
}

func main() {
	db, err := bbolt.Open("my.db", 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	store := NewStore(db)
	go store.Writer()

	ed := EntriesData{} //op: i
	op := 0
	for i := 0; i <= 20; i++ {
		item := ItemData{"users", []byte("user:" + str(i)), []byte("value:" + str(i))}
		ed = EntriesData{} //op: i
		ed.Items = append(ed.Items, item)

		ed.op = op
		entries <- ed
		fmt.Println("send new data", ed.op)
		op++
		time.Sleep(time.Millisecond * 15)
	}

	ed.brk = true
	entries <- ed
	time.Sleep(time.Second * 2)

	store.Close()
}

type Store struct {
	db         *bbolt.DB
	insertChan chan EntriesData
}
type EntriesData struct {
	op    int
	Items []ItemData
	Done  chan error
	brk   bool
}

type ItemData struct {
	Bucket string
	Key    []byte
	Value  []byte
}

func NewStore(db *bbolt.DB) *Store {
	s := &Store{
		db: db,
	}
	//go s.Writer()
	return s
}

func (db *Store) Close() {}

var mut sync.RWMutex

var str = fmt.Sprint
