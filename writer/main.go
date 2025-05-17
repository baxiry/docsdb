package main

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"go.etcd.io/bbolt"
)

const max = 30

var (
	mut             = &sync.Mutex{}
	entries         = make(chan EntriesData, max)
	entriesdata     = []EntriesData{}
	TempEntriesdata = []EntriesData{}
)

func write(entriesdata []EntriesData) bool {

	TempEntriesdata = entriesdata

	entriesdata = []EntriesData{}

	fmt.Printf("\nstart writeing %d batchs\n", len(TempEntriesdata))

	for _, ed := range TempEntriesdata {
		itm := ed.Items[0]
		fmt.Printf("    write %d %s %s %s\n", ed.op, itm.Bucket, itm.Key, itm.Value)
		time.Sleep(time.Millisecond * 27)
	}

	for _, ed := range TempEntriesdata {
		ed.Done <- fmt.Errorf("%d", ed.op)
	}

	if len(entriesdata) == 0 {
		return true
	}

	return write(entriesdata)
}

func (s *Store) Writer() {
	var ent EntriesData
	var ready = true

	for {
		ent = <-entries
		fmt.Println(" reseve:", ent.op)

		mut.Lock()
		entriesdata = append(entriesdata, ent)
		mut.Unlock()

		mut.Lock()
		if !ready {
			mut.Unlock()
			continue
		}
		mut.Unlock()

		ready = false

		TempEntriesdata = entriesdata
		entriesdata = []EntriesData{}

		go func() {
			fmt.Printf("\nstart writeing %d batchs\n", len(TempEntriesdata))
			for _, ed := range TempEntriesdata {
				if ed.op == 0 {
					fmt.Println("last")
					continue
				}
				itm := ed.Items[0]
				fmt.Printf("    write %d %s %s %s\n", ed.op, itm.Bucket, itm.Key, itm.Value)
				time.Sleep(time.Millisecond * 27)
			}

			for _, ed := range TempEntriesdata {
				if ed.op == 0 {
					fmt.Println("elso last")
					continue
				}
				ed.Done <- fmt.Errorf("%d", ed.op)

			}

			mut.Lock()
			if len(entriesdata) > 0 {
				entries <- EntriesData{}
			}
			mut.Unlock()

			mut.Lock()
			ready = true
			mut.Unlock()
		}()
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

	for i := 1; i <= 50; i++ {
		go func(i int) {
			ed := EntriesData{}

			item := ItemData{"users", []byte("user:" + str(i)), []byte("value:" + str(i))}

			ed.Items = append(ed.Items, item)

			ed.op = i
			ed.Done = make(chan error, 1)
			entries <- ed
			fmt.Printf("send: %d\n", ed.op)

			if fmt.Sprint(<-ed.Done) != strconv.Itoa(ed.op) {
				fmt.Printf("----------- Failure ent: %d\n", ed.op)
			} else {
				fmt.Printf("----------- Succes ent: %d\n", ed.op)
			}

		}(i)

		time.Sleep(time.Millisecond * 5)
	}

	time.Sleep(time.Second * 5)

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
	return s
}

func (db *Store) Close() {}

var str = fmt.Sprint

// loop :

/*
	fmt.Printf("\nstart writeing %d batch\n", len(entriesdata))
	TempEntriesdata = entriesdata
	entriesdata = []EntriesData{}

	for _, ed := range TempEntriesdata {
		itm := ed.Items[0]
		fmt.Printf("    write %d %s %s %s\n", ed.op, itm.Bucket, itm.Key, itm.Value)
		time.Sleep(time.Millisecond * 27)
	}

	for _, ed := range TempEntriesdata {
		ed.Done <- nil
	}
	if len(entriesdata) == 0 {
		ready = true
	}
*/

// ticker:
/*

	var duration time.Duration = 50
	var ticker = time.NewTicker(time.Millisecond * duration)

	for {

		select {
		case ent = <-entries:

			println(" reseve:", ent.op)
			entriesdata = append(entriesdata, ent)

		case <-ticker.C:

			if len(entriesdata) == 0 {
				if duration >= 250 {
					continue
				}

				duration += 10
				ticker = time.NewTicker(time.Millisecond * duration)
				continue
			} else {
				ticker = time.NewTicker(time.Millisecond * 50)
			}

			fmt.Printf("\nstart writeing %d batch\n", len(entriesdata))

			TempEntriesdata = entriesdata
			entriesdata = []EntriesData{}

			for _, ed := range TempEntriesdata {
				itm := ed.Items[0]
				fmt.Printf("    write %d %s %s %s\n", ed.op, itm.Bucket, itm.Key, itm.Value)
				time.Sleep(time.Millisecond * 27)
			}

			for _, ed := range TempEntriesdata {
				ed.Done <- nil
			}
		}
	}

*/
