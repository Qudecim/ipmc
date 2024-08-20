package ipmc

type Item struct {
	key             string
	value           string
	items           map[string]*Item
	increment_value int64
}

func (i *Item) getValue() string {
	return i.value
}

func (i *Item) setValue(value string) {
	i.value = value
}

func (i *Item) getIncrement() int64 {
	return i.increment_value
}

func (i *Item) increment() {
	i.increment_value++
}

func (i *Item) decrement() {
	i.increment_value--
}

func newItem(key string, value string) *Item {
	return &Item{
		key:   key,
		value: value,
		items: make(map[string]*Item),
	}
}

func newItemList(key string) *Item {
	return &Item{
		key:   key,
		items: make(map[string]*Item),
	}
}

func newIncrement(key string) *Item {
	return &Item{
		key:             key,
		increment_value: 0,
		items:           make(map[string]*Item),
	}
}
