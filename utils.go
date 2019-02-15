package cmap


func hash(key string) uint64 {
	seed := uint64(13131)
	var hash uint64
	for i := 0; i < len(key); i++ {
		hash = hash*seed + uint64(key[i])
	}
	return (hash & 0x7FFFFFFFFFFFFFFF)
}