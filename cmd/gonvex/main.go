// Command gonvex retains only explicit database migrations. Application
// development and bundling are owned by the TypeScript CLI; keeping a small
// Go shim here makes accidental use fail clearly instead of rebuilding the
// removed compiled-Go plugin pipeline.
package main

import (
	"fmt"
	"os"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" {
		printHelp()
		return nil
	}
	switch args[0] {
	case "dev":
		return fmt.Errorf("gonvex dev is provided by the TypeScript CLI; run `npx gonvex dev` from the application package")
	case "migrate":
		return runMigrate(args[1:])
	case "internal":
		return runInternal(args[1:])
	default:
		printHelp()
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func printHelp() {
	fmt.Println("Gonvex CLI")
	fmt.Println("  npx gonvex dev [options]")
	fmt.Println("  gonvex migrate identity-v2 (--plan | --apply | --verify) [options]")
	fmt.Println("  gonvex internal (provision-tenant | resolve-identity | e2e-setup) [options]")
}
