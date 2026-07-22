package badger

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Repro for dgraph-io/badger#2249 — a transaction Commit racing DB.Close()
// can send on db.writeCh after close(db.writeCh). sendToWriteCh's
// blockWrites check and the channel send are not atomic with respect to
// DB.close(), which stores blockWrites=1 and then closes the channel.
func TestCloseCommitRace(t *testing.T) {
	for i := 0; i < 250; i++ {
		db, err := Open(DefaultOptions(t.TempDir()).WithLoggingLevel(ERROR))
		require.NoError(t, err)

		start := make(chan struct{})
		var wg sync.WaitGroup
		for w := 0; w < 32; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				<-start
				for j := 0; ; j++ {
					// Batch of sets: sendToWriteCh's per-entry size loop sits
					// between the blockWrites check and the channel send, so a
					// bigger batch widens the racy window.
					err := db.Update(func(txn *Txn) error {
						for k := 0; k < 16; k++ {
							if err := txn.Set([]byte(fmt.Sprintf("k%d-%d-%d", w, j, k)), []byte("v")); err != nil {
								return err
							}
						}
						return nil
					})
					if err != nil {
						// ErrBlockedWrites / ErrDBClosed once Close begins.
						return
					}
				}
			}(w)
		}
		close(start)
		time.Sleep(time.Duration(50+i%200) * time.Microsecond)
		require.NoError(t, db.Close())
		wg.Wait()
	}
}
