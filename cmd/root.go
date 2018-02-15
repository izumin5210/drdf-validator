package cmd

import (
	"bufio"
	"bytes"
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

		for {
			chunkBuf, err := readChunk(bufio.NewReaderSize(f, 1<<20))
			if err == io.EOF {
				if chunkBuf.Len() != 0 {
					parseChunk(chunkBuf)
				}
				break
			}
			if err != nil {
				log.Fatal(err)
			}
			parseChunk(chunkBuf)

			// fmt.Printf("%d\r", lineNo)
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

// https://github.com/dgraph-io/dgraph/blob/abfedfeb2b1017b582a9ed2d7735d49c0664c64e/dgraph/cmd/bulk/loader.go#L146-L176
func readChunk(r *bufio.Reader) (*bytes.Buffer, error) {
	batch := new(bytes.Buffer)
	batch.Grow(10 << 20)
	for lineCount := 0; lineCount < 1e5; lineCount++ {
		slc, err := r.ReadSlice('\n')
		if err == io.EOF {
			batch.Write(slc)
			return batch, err
		}
		if err == bufio.ErrBufferFull {
			// This should only happen infrequently.
			batch.Write(slc)
			var str string
			str, err = r.ReadString('\n')
			if err == io.EOF {
				batch.WriteString(str)
				return batch, err
			}
			if err != nil {
				return nil, err
			}
			batch.WriteString(str)
			continue
		}
		if err != nil {
			return nil, err
		}
		batch.Write(slc)
	}
	return batch, nil
}

func parseChunk(buf *bytes.Buffer) {
	done := false
	for !done {
		line, err := buf.ReadString('\n')
		lineNo++
		if err == io.EOF {
			done = true
		} else if err != nil {
			log.Fatal(err)
		}
		line = strings.TrimSpace(line)
		_, err = rdf.Parse(line)
		if err != nil && err != rdf.ErrEmpty {
			fmt.Println(line)
			errLines = append(errLines, lineNo)
		}
	}
}
