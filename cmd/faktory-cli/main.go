package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/contribsys/faktory/client"
)

func main() {
	cl, err := client.Open()
	if err != nil {
		fail(err)
	}
	defer cl.Close()

	data, err := cl.Info()
	if err != nil {
		fail(err)
	}
	svr := data["server"].(map[string]interface{})
	fmt.Printf("Connected to %s %s\n", svr["description"], svr["faktory_version"])

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf("> ")
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Println("")
				return
			}
			fail(err)
		}

		if strings.HasPrefix(line, "END") {
			break
		}

		resp, err := cl.Generic(line)
		if err != nil {
			fmt.Printf("%s\n", err.Error())
		}
		fmt.Printf("%s\n", resp)
	}
}

func fail(err error) {
	fmt.Println(err.Error())
	os.Exit(-1)
}
