package ipmc

import (
	"fmt"
	"os"
	"sync"
)

type App struct {
	data         map[string]*Item
	binlog       *BinlogWriter
	binlogReader *BinlogReader
	config       *Config

	rw sync.RWMutex
	Wg sync.WaitGroup
}

func NewApp(config *Config) *App {
	return &App{
		data:   make(map[string]*Item),
		binlog: newBinlogWriter(config.Binlog_directory, config.Binlog_max_writes),
		config: config,
	}
}

func (a *App) Init() {
	a.binlogReader = newBinlogReader(a, a.config.Binlog_directory)

	err := os.MkdirAll(a.config.Binlog_directory, 0755)
	if err != nil {
		fmt.Printf("Error creating directory: %v\n", err)
		return
	}

	err = os.MkdirAll(a.config.Snapshot_directory, 0755)
	if err != nil {
		fmt.Printf("Error creating directory: %v\n", err)
		return
	}

	a.binlogReader.read()

	go a.binlog.run()
}

func (a *App) NewConnection() {
	a.Wg.Add(1)
}

func (a *App) CloseConnection() {
	a.Wg.Done()
}

func (a *App) Set(key string, value string) {
	a.rw.Lock()
	item, exist := a.data[key]
	if exist {
		item.value = value
	} else {
		a.data[key] = newItem(key, value)
	}
	a.rw.Unlock()

	a.binlog.add("s", key, value)
}

func (a *App) Get(key string) (string, bool) {
	a.rw.RLock()
	value, ok := a.data[key]
	a.rw.RUnlock()
	if ok {
		return value.getValue(), ok
	}
	return "", ok
}

func (a *App) Delete(key string) {
	a.rw.Lock()
	_, exist := a.data[key]
	if exist {
		delete(a.data, key)
	}
	a.rw.Unlock()

	a.binlog.add("q", key, "")
}

func (a *App) Push(key string, value string) bool {
	a.rw.Lock()
	parent, exist := a.data[key]
	if !exist {
		parent = newItemList(key)
		a.data[key] = parent
	}
	valueItem, ok := a.data[value]
	if ok {
		parent.items[value] = valueItem
	}
	a.rw.Unlock()

	if ok {
		a.binlog.add("p", key, value)
	}
	return ok
}

func (a *App) Pull(key string) (map[string]string, bool) {
	items := make(map[string]string)

	a.rw.Lock()
	value, ok := a.data[key]
	if ok {
		for key, item := range value.items {
			items[key] = item.getValue()
		}
	}
	a.rw.Unlock()

	return items, ok
}

func (a *App) Remove(key string, value string) {
	a.rw.Lock()
	parent, exist := a.data[key]
	if exist {
		_, exist2 := parent.items[value]
		if exist2 {
			delete(parent.items, value)
			a.binlog.add("r", key, value)
		}
	}
	a.rw.Unlock()
}

func (a *App) Increment(key string) (int64, bool) {
	a.rw.Lock()
	item, exist := a.data[key]
	if exist {
		item.increment()
	} else {
		item = newIncrement(key)
		a.data[key] = item
	}
	a.rw.Unlock()

	a.binlog.add("i", key, "")
	return item.getIncrement(), true
}

func (a *App) Decrement(key string) (int64, bool) {
	a.rw.Lock()
	item, exist := a.data[key]
	if exist {
		item.decrement()
	} else {
		item = newIncrement(key)
		a.data[key] = item
	}
	a.rw.Unlock()

	a.binlog.add("d", key, "")
	return item.getIncrement(), true
}

func (a *App) forceSet(key string, value string) {
	item, exist := a.data[key]
	if exist {
		item.setValue(value)
	} else {
		a.data[key] = newItem(key, value)
	}
}

func (a *App) forcePush(key string, value string) {
	parent, exist := a.data[key]
	if !exist {
		parent = newItemList(key)
		a.data[key] = parent
	}
	valueItem, ok := a.data[value]
	if ok {
		parent.items[value] = valueItem
	}
}

func (a *App) forceRemove(key string, value string) {
	parent, exist := a.data[key]
	if exist {
		_, exist2 := parent.items[value]
		if exist2 {
			delete(parent.items, value)
		}
	}
}

func (a *App) forceDelete(key string) {
	_, exist := a.data[key]
	if exist {
		delete(a.data, key)
	}
}

func (a *App) forceIncrement(key string) {
	item, exist := a.data[key]
	if exist {
		item.increment()
	} else {
		a.data[key] = newIncrement(key)
	}
}

func (a *App) forceDecrement(key string) {
	item, exist := a.data[key]
	if exist {
		item.decrement()
	} else {
		a.data[key] = newIncrement(key)
	}
}
