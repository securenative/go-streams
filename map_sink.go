package go_streams

import "sync"

var defaultKeyExtractor KeyExtractor = func(entry Entry) string {
	return entry.Key
}

// ReplacingMapSink will write incoming entries to a map
// replacing the value for the same key
type ReplacingMapSink struct {
	m            map[string]interface{}
	mutex        *sync.RWMutex
	keyExtractor KeyExtractor
}

func NewReplacingMapSink() *ReplacingMapSink {
	return &ReplacingMapSink{
		m:            make(map[string]interface{}),
		mutex:        &sync.RWMutex{},
		keyExtractor: defaultKeyExtractor,
	}
}

func (this *ReplacingMapSink) SetKeyExtractor(extractor KeyExtractor) {
	this.keyExtractor = extractor
}

func (this *ReplacingMapSink) Single(entry Entry) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	key := this.keyExtractor(entry)
	this.m[key] = entry.Value
	return nil
}

func (this *ReplacingMapSink) Batch(entry ...Entry) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	for idx := range entry {
		key := this.keyExtractor(entry[idx])
		this.m[key] = entry[idx].Value
	}
	return nil
}

func (this *ReplacingMapSink) Map() map[string]interface{} {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return this.m
}

// IgnoringMapSink will write incoming entries to a map
// keeping only the first entry per key
type IgnoringMapSink struct {
	m            map[string]interface{}
	mutex        *sync.RWMutex
	keyExtractor KeyExtractor
}

func NewIgnoringMapSink() *IgnoringMapSink {
	return &IgnoringMapSink{
		m:            make(map[string]interface{}),
		mutex:        &sync.RWMutex{},
		keyExtractor: defaultKeyExtractor,
	}
}

func (this *IgnoringMapSink) SetKeyExtractor(extractor KeyExtractor) {
	this.keyExtractor = extractor
}

func (this *IgnoringMapSink) Single(entry Entry) error {
	key := this.keyExtractor(entry)
	if _, found := this.m[key]; !found {
		this.m[key] = entry.Value
	}
	return nil
}

func (this *IgnoringMapSink) Batch(entry ...Entry) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	for idx := range entry {
		key := this.keyExtractor(entry[idx])
		if _, found := this.m[key]; found {
			continue
		}
		this.m[key] = entry[idx].Value
	}
	return nil
}

func (this *IgnoringMapSink) Map() map[string]interface{} {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return this.m
}

// MultiMapSink will write incoming entries to a multimap
// adding values of the same keys to array
type MultiMapSink struct {
	m            map[string][]interface{}
	mutex        *sync.RWMutex
	keyExtractor KeyExtractor
}

func NewMultiMapSink() *MultiMapSink {
	return &MultiMapSink{
		m:            make(map[string][]interface{}),
		mutex:        &sync.RWMutex{},
		keyExtractor: defaultKeyExtractor,
	}
}

func (this *MultiMapSink) SetKeyExtractor(extractor KeyExtractor) {
	this.keyExtractor = extractor
}

func (this *MultiMapSink) Single(entry Entry) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	key := this.keyExtractor(entry)
	this.m[key] = append(this.m[key], entry.Value)
	return nil
}

func (this *MultiMapSink) Batch(entry ...Entry) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	for idx := range entry {
		key := this.keyExtractor(entry[idx])
		this.m[key] = append(this.m[key], entry[idx].Value)
	}
	return nil
}

func (this *MultiMapSink) Map() map[string][]interface{} {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return this.m
}
