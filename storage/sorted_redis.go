package storage

type redisSorted struct {
	name  string
	store *redisStore
}

func (rs *redisStore) initSorted() {
	rs.scheduled = &redisSorted{name: "scheduled", store: rs}
	rs.retries = &redisSorted{name: "retries", store: rs}
	rs.dead = &redisSorted{name: "dead", store: rs}
	rs.working = &redisSorted{name: "working", store: rs}
}

func (rs *redisSorted) Name() string {
	return rs.name
}

func (rs *redisSorted) Size() uint64 {
	return uint64(rs.store.client.ZCard(rs.name).Val())
}
func (rs *redisSorted) Clear() (uint64, error) {
	err := rs.store.client.Del(rs.name).Err()
	return 0, err
}
func (rs *redisSorted) Reset() int {
	rs.store.client.Del(rs.name)
	return 0
}

func (rs *redisSorted) AddElement(timestamp string, jid string, payload []byte) error {
	return nil
}

func (rs *redisSorted) Get(key []byte) ([]byte, error) {
	return nil, nil
}
func (rs *redisSorted) Page(int64, int64, func(index int, key []byte, data []byte) error) error {
	return nil
}
func (rs *redisSorted) Each(func(idx int, key []byte, data []byte) error) error {
	return nil
}

func (rs *redisSorted) Remove(key []byte) error {
	return nil
}
func (rs *redisSorted) RemoveElement(timestamp string, jid string) error {
	return nil
}
func (rs *redisSorted) RemoveBefore(timestamp string) ([][]byte, error) {
	return nil, nil
}

// Move the given key from this SortedSet to the given
// SortedSet atomically.  The given func may mutate the payload and
// return a new tstamp.
func (rs *redisSorted) MoveTo(SortedSet, string, string, func([]byte) (string, []byte, error)) error {
	return nil
}
