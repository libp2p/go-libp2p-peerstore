package pstoremem

import (
	"math/rand"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/libp2p/go-libp2p-core/peer"
)

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func getAllocs(t *testing.T, num int, key string, value func() string) int64 {
	getAlloc := func() uint64 {
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		return stats.TotalAlloc
	}

	m := NewPeerMetadata()
	start := getAlloc()
	for i := 0; i < num; i++ {
		m.Put(peer.ID(randomString(10)), key, value())
	}
	return int64(getAlloc()) - int64(start)
}

func TestInterning(t *testing.T) {
	const num = 10000

	t.Run("using the AgentVersion key", func(t *testing.T) {
		const value = "agent version 1234"
		allocsConstValue := getAllocs(t, num, "AgentVersion", func() string { return value })
		allocsRandomValue := getAllocs(t, num, "AgentVersion", func() string { return randomString(len(value)) })
		fraction := float64(allocsConstValue) / float64(allocsRandomValue)
		t.Logf("using a constant AgentVersion reduced memory usage to %f", fraction)
		require.Greater(t, fraction, 0.5)
		require.Less(t, fraction, 0.6)
	})

	t.Run("using a different key", func(t *testing.T) {
		const value = "agent version 1234"
		allocsConstValue := getAllocs(t, num, "other key", func() string { return value })
		allocsRandomValue := getAllocs(t, num, "other key", func() string { return randomString(len(value)) })
		fraction := float64(allocsConstValue) / float64(allocsRandomValue)
		require.Greater(t, fraction, 0.8)
		require.Less(t, fraction, 1.2)
	})
}
