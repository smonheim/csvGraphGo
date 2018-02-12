package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

// This is our struct that holds each line of our csv
type Message struct {
	Name      string
	Enjoyment float64
	Price     float64
	TimeStamp float64
	Value     float64
}

func main() {

	// grab our csv file and create a new reader
	f, _ := os.Open("gamedata.csv")
	reader := csv.NewReader(bufio.NewReader(f))
	var thisName string
	var lines []Message
	var values []float64
	var nameList map[string]bool
	nameList = make(map[string]bool)

	// iterate through each line, break at EOF
	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		}
		if error != nil {
			log.Fatal(error)
		}

		// iterate through current line
		for index, element := range line {

			// if our current index is 0 it's the name
			if index == 0 {
				thisName = element

				// otherwise it's a float value
			} else {
				num, err := strconv.ParseFloat(element, 64)
				if err == nil {
					values = append(values, num)
				}
			}
		}

		// put all of the current line's converted values
		// (and the string) into the Message type struct
		lines = append(lines, Message{
			Name:      thisName,
			Enjoyment: values[0],
			Price:     values[1],
			TimeStamp: values[2],
			Value:     values[3],
		})

		// if the current string value is the first occurance,
		// add it to a map of the strings to maintain uniqueness
		if nameList[thisName] != true {
			nameList[thisName] = true
		}

		// zero out the values slice
		values = values[:0]
	}

	// delete the previous database
	os.Remove("./dummy.db")

	// open the sqlite db
	db, _ := sql.Open("sqlite3", "./dummy.db")
	db.Exec("PRAGMA journal_mode=WAL;")

	// for key in our name map create new tables
	for key := range nameList {
		thisLine := fmt.Sprintf(`CREATE TABLE %v (Name VARCHAR, Enjoyment FLOAT, Price FLOAT, `+
			`TimeStamp FLOAT, Value FLOAT);`, key)
		fmt.Println(thisLine)
		db.Exec(thisLine)
	}

	var deltaT []float64
	var prevT float64
	var currT float64
	var messageQueries []string

	prevT = lines[0].TimeStamp

	// iterate over lines, store the deltaT between messages in the deltaT slice
	for _, element := range lines {
		currT = element.TimeStamp
		deltaT = append(deltaT, (currT - prevT))
		prevT = element.TimeStamp

		// append each message to the slice of queries
		thisLine := fmt.Sprintf(`INSERT INTO %v (Name, Enjoyment, Price, TimeStamp, Value)`+
			`VALUES (?, ?, ?, ?, ?)`, element.Name)
		messageQueries = append(messageQueries, thisLine)
	}
	deltaT = append(deltaT, 0.0)

	// for query in queries slice, do the query and sleep for deltaT[nextmessage]
	for index, element := range messageQueries {
		query, err := db.Prepare(element)
		if err != nil {
			fmt.Println(err)
		}

		// open our transaction
		tx, err := db.Begin()
		if err != nil {
			fmt.Println(err)
		}

		// execute our sql insert query
		_, err = tx.Stmt(query).Exec(lines[index].Name, lines[index].Enjoyment, lines[index].Price,
			lines[index].TimeStamp, lines[index].Value)
		if err != nil {
			fmt.Println("doing rollback")
			tx.Rollback()
		} else {
			tx.Commit()
		}

		// sleep for deltaT seconds
		fmt.Println("Sleeping for: ", time.Duration((deltaT[index+1]*1000)*float64(time.Millisecond)))
		time.Sleep(time.Duration((deltaT[index+1] * 1000) * float64(time.Millisecond)))
	}

	db.Close()
}
