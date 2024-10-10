package ipmc

import (
	"os"
	"strings"
)

type keyValue struct {
	method   string
	key      string
	key_item string
	value    string
}

type BinlogWriter struct {
	stack chan *keyValue

	writes    int
	maxWrites int
	directory string

	current       string
	currentSource *os.File
}

func newBinlogWriter(directory string, maxWrites int) *BinlogWriter {
	return &BinlogWriter{
		directory: directory,
		current:   Timestamp(),
		stack:     make(chan *keyValue),
		maxWrites: maxWrites,
	}
}

func (b *BinlogWriter) run() {
	b.openBinlog()

	for {
		select {
		case item := <-b.stack:
			b.addToBinlog(item)

			b.writes++
			if b.writes > b.maxWrites {
				b.changeBinlog(Timestamp())
				b.writes = 0
			}

		}
	}
}

func (b *BinlogWriter) add(method string, key string, value string) {
	keyValueItem := &keyValue{method: method, key: key, value: value}
	b.stack <- keyValueItem
}

func (b *BinlogWriter) openBinlog() error {

	if b.currentSource != nil {
		b.currentSource.Close()
	}

	f, err := os.OpenFile(b.getCurrentBinlogPath(), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	b.currentSource = f

	return nil
}

func (b *BinlogWriter) addToBinlog(keyValueItem *keyValue) error {

	method := strings.ReplaceAll(keyValueItem.method, "\n", "\\n")
	key := strings.ReplaceAll(keyValueItem.key, "\n", "\\n")
	value := strings.ReplaceAll(keyValueItem.value, "\n", "\\n")

	text := method + "\n" + key + "\n" + value + "\n"

	if _, err := b.currentSource.WriteString(text); err != nil {
		return err
	}

	return nil
}

func (b *BinlogWriter) changeBinlog(timestampBinlog string) {
	b.current = timestampBinlog
	b.openBinlog()
}

func (b *BinlogWriter) getCurrentBinlogPath() string {
	return b.getBinlogPath(b.current)
}

func (b *BinlogWriter) getBinlogPath(binlog string) string {
	return b.directory + binlog
}
