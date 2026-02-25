// cmd/atlas/main.go — CLI root. Dispatches to subcommand handlers.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: atlas <submit|status|cancel|watch|history> [options]")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "submit":
		runSubmit(os.Args[2:])
	case "status":
		runStatus(os.Args[2:])
	case "cancel":
		runCancel(os.Args[2:])
	case "watch":
		runWatch(os.Args[2:])
	case "history":
		runHistory(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %q\n", os.Args[1])
		fmt.Fprintln(os.Stderr, "Usage: atlas <submit|status|cancel|watch|history> [options]")
		os.Exit(1)
	}
}
