package main

import (
	"log"
	"os"

	"github.com/eoscanada/pitreos"
	flags "github.com/jessevdk/go-flags"
)

func main() {

	var opts pitreos.PitreosOptions
	_, err := flags.Parse(&opts)
	if err != nil {
		os.Exit(1)
	}

	pitre := pitreos.New(&opts)

	switch opts.Args.Command {
	case "backup":
		err := pitre.GenerateBackup()
		if err != nil {
			log.Fatalln(err)
		}

	case "restore":
		err := pitre.RestoreFromBackup()
		if err != nil {
			log.Fatalln(err)
		}

	default:
		log.Fatalln("Unknown command", opts.Args.Command)
	}

}
