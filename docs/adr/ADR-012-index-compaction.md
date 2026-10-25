# ADR-012: Index Compaction for Memory Reclamation

## Status
Accepted

## Date
2025-12-14

## Context

Go maps don't release memory when entries are deleted. The `delete()` operation marks slots as empty but doesn't shrink the underlying hash table. In high-churn scenarios (frequent add/remove cycles), index maps grow unbounded.

```go
// This leaks memory over time
for i := 0; i < 1000000; i++ {
    id, _ := c.AddFunc("@hourly", job)
    c.Remove(id)
}
// Maps still hold memory for 1M entries
```

**Problem:** Memory bloat in long-running schedulers with dynamic job management.

## Decision

Track deletion count and periodically rebuild maps when threshold exceeded:

```go
type Cron struct {
    entryIndex     map[EntryID]*Entry
    nameIndex      map[string]*Entry
    indexDeletions int  // Removals since last compaction
}

const indexCompactionThreshold = 1000

func (c *Cron) maybeCompactIndexes() {
    if c.indexDeletions < indexCompactionThreshold {
        return
    }
    // Only compact once more entries were deleted than remain
    currentSize := len(c.entryIndex)
    if currentSize > 0 && c.indexDeletions <= currentSize {
        return
    }

    // Copy the live entries into right-sized new maps; Go maps never shrink
    newEntryIndex := make(map[EntryID]*Entry, currentSize)
    maps.Copy(newEntryIndex, c.entryIndex)
    c.entryIndex = newEntryIndex
    newNameIndex := make(map[string]*Entry, len(c.nameIndex))
    maps.Copy(newNameIndex, c.nameIndex)
    c.nameIndex = newNameIndex
    c.indexDeletions = 0
}
```

**Trigger conditions:**
1. At least 1000 deletions since last compaction
2. More deletions than remaining entries (more than half of the entries present before the deletions are gone)

**Correction (2026-10-25):** The first version of this ADR showed a 50%-of-current-entries condition and a rebuild from the heap. Neither existed in the code: the condition above dates from commit 78151c2 (2025-12-01, #107), before this ADR was written. Pinned by `TestIndexCompaction` in `cron_test.go`.

## Consequences

### Positive

- **Memory reclamation**: Maps shrink after high-churn periods
- **Bounded overhead**: Only compacts when beneficial
- **No API changes**: Transparent to users
- **Index maps stay authoritative**: Copying the live maps keeps them consistent without touching the heap

### Negative

- **O(n) compaction cost**: Rebuilding maps takes linear time
- **Deletion tracking overhead**: Counter increment on every remove
- **Threshold tuning**: Fixed values may not suit all workloads

### Neutral

- **Amortized cost**: Compaction is rare in typical usage
- **Only affects high-churn**: Stable workloads never trigger

## Alternatives Considered

### 1. Never Compact

- **Rejected**: Memory grows unbounded in dynamic workloads
- Production systems can't tolerate memory leaks

### 2. Compact on Every Delete

- **Rejected**: O(n) cost per deletion is prohibitive
- Would make Remove() unacceptably slow

### 3. Time-Based Compaction

```go
if time.Since(lastCompaction) > time.Hour {
    compact()
}
```

- **Rejected**: May compact when unnecessary
- Doesn't correlate with actual memory pressure

### 4. Memory Pressure Detection

```go
if runtime.MemStats.HeapAlloc > threshold {
    compact()
}
```

- **Rejected**: Complex to tune threshold
- Global metric doesn't identify specific map bloat

### 5. Use sync.Map

- **Rejected**: sync.Map has same issue (no shrinking)
- Would lose type safety

## References

- Go maps internals: https://go.dev/blog/maps
- ADR-011: Dual-Index Maps Strategy
