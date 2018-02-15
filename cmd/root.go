package cmd

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/dgraph-io/dgraph/rdf"
	"github.com/spf13/cobra"
)

var (
	rdfFile  string
	lineNo   int
	errLines []int
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "drdf-validator",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(rdfFile) == 0 {
			return errors.New("must specify RDF")
		}

		f, err := os.Open(rdfFile)
		if err != nil {
			return err
		}
		defer f.Close()

		reader := bufio.NewReaderSize(f, 1<<20)
		for {
			data, _, err := reader.ReadLine()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}
			lineNo++
			line := strings.TrimSpace(string(data))
			_, err = rdf.Parse(line)
			if err != nil && err != rdf.ErrEmpty {
				fmt.Println(line)
				errLines = append(errLines, lineNo)
			}
		}
		fmt.Print("\n")

		fmt.Println(errLines)

		return nil
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&rdfFile, "rdf", "r", "", "rdf file")
}
