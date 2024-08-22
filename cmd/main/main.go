package main

import (
	"fmt"

	"github.com/Qudecim/ipmc"
)

func main() {
	fmt.Println("Hi")

	config := ipmc.NewConfig("binlog/", 1e5, "snapshot/")
	app := ipmc.NewApp(config)
	app.Init()
	app.NewConnection()
	app.Set("hello_key", "hello_value")
	fmt.Println(app.Get("hello_key"))
	app.CloseConnection()
}
