package main

import (
	"log"

	"github.com/eoscanada/pitreos"
	flags "github.com/jessevdk/go-flags"
)

func main() {

	var opts pitreos.PitreosOptions
	_, err := flags.Parse(&opts)
	if err != nil {
		log.Fatalln(err)
	}

	switch opts.Args.Command {
	case "backup":
		err := pitreos.GenerateBackup(&opts)
		if err != nil {
			log.Fatalln(err)
		}

	case "restore":
		err := pitreos.RestoreFromBackup(&opts)
		if err != nil {
			log.Fatalln(err)
		}
	default:
		log.Fatalln("Unknown command", opts.Args.Command)
	}

}
