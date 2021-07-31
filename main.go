package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

var (
	ConnString string
	m          map[string]varvalues
)

type jsonread struct {
	Username string `json:"username"`
	Paswword string `json:"password"`
	Ip       string `json:"ip"`
	Port     string `json:"port"`
	Server   string `json:"server"`
}

func init() {
	jsonread := jsonread{}
	file, err := ioutil.ReadFile("internal/config/config.json")
	if err != nil {
		log.Fatal(err)
		return
	}
	err = json.Unmarshal([]byte(file), &jsonread)
	if err != nil {
		log.Fatal(err)
		return
	}
	ConnString = jsonread.Username + ":" + jsonread.Paswword + "@tcp(" + jsonread.Ip + ":" + jsonread.Port + ")/" + jsonread.Server

}

type varvalues struct {
	col1  int
	col2  string
	col3  string
	col4  string
	col5  float64
	col6  float64
	col7  float64
	col8  float64
	col9  float64
	col10 float64
}

func getsinglerow(key string) varvalues {
	return m[key]
}

func getmultiplerows(keys []string) []varvalues {
	listofvalues := []varvalues{}
	for _, key := range keys {
		listofvalues = append(listofvalues, m[key])
	}
	return listofvalues
}

func storeinmap() {
	db, err := sql.Open("mysql", ConnString)
	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}
	defer db.Close()

	rows, err := db.Query("select * from nse.nsedata")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		temp := varvalues{}
		err := rows.Scan(&key, &temp.col1, &temp.col2, &temp.col3, &temp.col4, &temp.col5, &temp.col6, &temp.col7, &temp.col8, &temp.col9, &temp.col10)
		if err != nil {
			log.Fatal(err)
		}
		m[key] = temp
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
}

//server
func connect() bool {
	ln, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()

	conn, err := ln.Accept()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	b, err := io.ReadAll(conn)
	if err != nil {
		log.Fatal(err)
	}
	str := (string)(b)
	fmt.Println(str)
	if str == "ready" {
		return true
	} else {
		return false
	}
}

func main() {
	var wg sync.WaitGroup
	m = make(map[string]varvalues)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if connect() {
				storeinmap()
			} else {
				storeinmap()
				return
			}
		}
	}()
	wg.Wait()
	fmt.Println(getsinglerow("20MICRONS_nse_EQ"))
	keys := []string{"182D161221_nse_TB", "182D200122_nse_TB"}
	fmt.Println(getmultiplerows(keys))
	//wait for thread to close
}
