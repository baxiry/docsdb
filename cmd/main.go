package main

import (
	"fmt"
	"math/rand"

	"github.com/bashery/litedb/engine"
)

func main() {

	db := engine.NewDB("test.db")
	if db == nil {
		fmt.Println("what ??? ")
	}
	defer db.Close()

	query := f(`{"collection":"users", "action":"insert", "data":{"name":"%s", "age":%d}`, randName(), randAge())

	res := db.HandleQueries(query)
	fmt.Println(res)

	res = db.HandleQueries(`{"collection":"users", "action":"findMany", "match":{"name":{"$en": "m"}}}`)
	fmt.Println("end with 'm'\n", res)

	res = db.HandleQueries(`{"collection":"users", "action":"findMany", "match":{"name":{"$c": "i"}}}`)
	fmt.Println("contain with 'm'\n", res)

}

// List of 50 sample names.
var names = []string{
	"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy",
	"Kevin", "Linda", "Mike", "Nancy", "Oscar", "Penny", "Quentin", "Rachel", "Steve", "Tina",
	"Uma", "Vince", "Wendy", "Xander", "Yara", "Zayn", "Adam", "Bella", "Chris", "Diana",
	"Ethan", "Fiona", "George", "Hannah", "Isaac", "Jasmine", "Kyle", "Laura", "Mark", "Nora",
	"Oliver", "Paula", "Quin", "Ryan", "Sophia", "Thomas", "Olivia", "Peter", "Sara", "Ben",
}

// randName returns a random name from the names list.
func randName() string {
	return names[rand.Intn(len(names))]
}

// randAge returns a random age between 1 and 100.
func randAge() int {
	return rand.Intn(70) + 10
}

var f = fmt.Sprintf
