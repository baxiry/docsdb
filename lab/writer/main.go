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
	entriesdata     = make([]EntriesData, 0, max)
	TempEntriesdata = make([]EntriesData, 0, max)
)

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

func (s *Store) Writer() {

	var ent = EntriesData{}
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
					fmt.Println("last entre")
					continue
				}

				var opError int

				if len(ed.Items) > 1 {
					fmt.Println("start transaction")
					if ed.op > 30 {
						ed.Done <- fmt.Errorf("an error")
						continue
					}
				}

				if len(ed.Items) > 1 {
					for _, itm := range ed.Items {
						if ed.op == opError {
							fmt.Println("symulate transaction error", opError)
							time.Sleep(time.Millisecond)
							continue
						}
						fmt.Printf("   write %d %s %s %s\n", ed.op, itm.Bucket, itm.Key, itm.Value)
						time.Sleep(time.Millisecond)
					}
				} else {
					itm := ed.Items[0]
					fmt.Printf("   write %d %s %s %s\n", ed.op, itm.Bucket, itm.Key, itm.Value)
					time.Sleep(time.Millisecond)
				}
			}

			// symulate syncing time
			time.Sleep(time.Millisecond * 20)

			for _, ed := range TempEntriesdata {
				if ed.op == 0 {
					fmt.Println("last ak")
					continue
				}
				ed.Done <- fmt.Errorf("%d", ed.op)
				time.Sleep(time.Millisecond * 2)
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
			ed.op = i

			item := ItemData{"users", []byte("user:" + str(i)), []byte("value:" + str(i))}
			ed.Items = append(ed.Items, item)

			if i%5 == 0 {
				item.Bucket = "contacts"
				item.Key = []byte("user:" + str(i+100))
				item.Value = []byte("contacts value :" + str(i+100))

				ed.Items = append(ed.Items, item)
			}

			ed.Done = make(chan error, 1)
			entries <- ed
			fmt.Printf("send: %d\n", ed.op)

			res := <-ed.Done

			if fmt.Sprint(res) != strconv.Itoa(ed.op) {
				fmt.Printf("----------- Failure ent: %d: %v\n", ed.op, res)
			} else {
				fmt.Printf("----------- Succes ent: %d\n", ed.op)
			}

		}(i)

		time.Sleep(time.Millisecond * 1)
	}
	time.Sleep(time.Second * 2)

	store.Close()
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
