package store

import (
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

var (
	// ErrBucketNotFound is returned when a bucket is
	// requested but no bucket is found with the given
	// BucketId.
	ErrBucketNotFound = errors.New("store: bucket not found")

	// ErrBucketAlreadyExists is returned when CreateBucket
	// is called with an already existing BucketId.
	ErrBucketAlreadyExists = errors.New("store: bucket already exists")

	// ErrBucketIsFull is returned when appending to a
	// bucket that is completely full.
	ErrBucketIsFull = errors.New("store: bucket is full")

	// ErrInvalidAppend is returned when an append
	// operation is attempted with a non-zero idx that is
	// not equal to lastIdx+1.
	ErrInvalidAppend = errors.New("store: the idx passed to Append is invalid")
)

// Store manages and keeps track of buckets.
//
// Each of these buckets contain a list with bucket values.
// The store interface is only responsible for managing the
// buckets, the values and properties of the buckets are
// managed through the bucket interface. The store is
// thread-safe.
//
// The current implementation uses the cockroach pebble key
// value store to save data to disk. Buckets are cached in
// memory.
type Store interface {
	// GetBucket retrieves a bucket.
	GetBucket(id BucketID) (Bucket, error)

	// CreateBucket creates a new bucket.
	CreateBucket(id BucketID, key BucketKey) (Bucket, error)

	// DeleteBucket deletes a bucket.
	DeleteBucket(bkt Bucket) error

	// GC cleans up the cache and removes expired buckets.
	GC() error

	// Close closes the store.
	Close() error
}

// pebbleStore implements the Store interface.
type pebbleStore struct {
	opts     *StoreOptions // Options for the underlying Pebble store.
	db       *pebble.DB    // Underlying Pebble store.
	gcTicker *time.Ticker  // GC ticker.
	cache    sync.Map      // Cache with buckets.
}

// StoreOptions contains the configuration options for the
// store.
type StoreOptions struct {
	PebbleOpts *pebble.Options // Options for the underlying Pebble store.
	CacheTTL   uint32          // Time to live for cached buckets in hours. (default: 24)
	GCInterval uint32          // Interval for triggering the GC function in hours. (default: 6)
}

// OpenStore opens a new store instance using the given
// options.
func OpenStore(path string, opts *StoreOptions) (str Store, err error) {
	if opts == nil {
		opts = &StoreOptions{
			PebbleOpts: &pebble.Options{},
			CacheTTL:   24,
			GCInterval: 6,
		}
	}

	db, err := pebble.Open(path, opts.PebbleOpts)
	if err != nil {
		return nil, err
	}

	// Start the GC ticker, the ticker will call GC
	// periodically and is stopped when the store is closed.
	var gcTicker *time.Ticker
	if opts.GCInterval > 0 {
		gcTicker = time.NewTicker(time.Duration(opts.GCInterval) * time.Hour)
		go func() {
			for range gcTicker.C {
				if err := str.GC(); err != nil {
					panic(err)
				}
			}
		}()
	}

	return &pebbleStore{
		opts:     opts,
		db:       db,
		gcTicker: gcTicker,
	}, nil
}

// GetBucket retrieves a bucket.
//
// The bucket is retrieved from the cache when available,
// otherwise it is retrieved from the underlying pebble
// store. If the bucket is not found in the store,
// ErrBucketNotFound is returned.
func (str *pebbleStore) GetBucket(id BucketID) (Bucket, error) {
	if bkt, ok := str.cache.Load(id); ok {
		return bkt.(*pebbleBucket), nil
	}

	data, closer, err := str.db.Get(getPebbleBucketKey(id))
	if err != nil {
		return nil, ErrBucketNotFound
	}

	bkt := &pebbleBucket{
		id:    id,
		data:  data,
		store: str,
	}
	bkt.lastIdx = fetchLastIdx(bkt)

	// Use LoadOrStore to avoid race conditions.
	cache, _ := str.cache.LoadOrStore(id, bkt)
	return cache.(*pebbleBucket), closer.Close()
}

// CreateBucket creates a new bucket.
//
// When a bucket for the given BucketId already exists,
// ErrBucketAlreadyExists is returned.
func (str *pebbleStore) CreateBucket(id BucketID, key BucketKey) (Bucket, error) {
	if bkt, err := str.GetBucket(id); !errors.Is(err, ErrBucketNotFound) {
		return bkt, ErrBucketAlreadyExists
	}

	data := make([]byte, 4+BucketKeyLength)
	binary.BigEndian.PutUint32(data[:4], getCurrentTimestamp())
	copy(data[4:], key[:])
	bkt := &pebbleBucket{
		store: str,
		id:    id,
		data:  data,
	}

	// Check whether bucket does not exist to avoid
	// race conditions.
	if cache, loaded := str.cache.LoadOrStore(id, bkt); loaded {
		return cache.(*pebbleBucket), ErrBucketAlreadyExists
	}

	return bkt, str.db.Set(getPebbleBucketKey(bkt.id), bkt.data, nil)
}

// DeleteBucket deletes a bucket.
//
// Deleting a bucket removes the bucket from the cache and
// underlying pebble store, this includes all the related
// bucket values.
func (str *pebbleStore) DeleteBucket(bkt Bucket) error {
	if err := bkt.DeleteValues(BucketRange{Start: 0, End: math.MaxUint16}); err != nil {
		return err
	}

	str.cache.Delete(bkt.GetBucketID())
	return str.db.Delete(getPebbleBucketKey(bkt.(*pebbleBucket).id), nil)
}

// GC cleans up the cache and removes expired buckets.
//
// This function is called periodically by the GC ticker and
// is normally not called manually.
func (str *pebbleStore) GC() error {
	// Delete all items from cache that are expired.
	now := getCurrentTimestamp()
	str.cache.Range(func(key, val any) bool {
		if now-getTimestamp(val.(*pebbleBucket)) >= str.opts.CacheTTL {
			str.cache.Delete(key)
		}
		return true
	})

	// Delete all expired buckets.
	iter := str.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{bucketTable},
		UpperBound: []byte{bucketTable + 1},
	})
	bkt := &pebbleBucket{store: str}
	for iter.First(); iter.Valid(); iter.Next() {
		bkt.id = BucketID(iter.Key()[1:])
		bkt.data = iter.Value()

		// Buckets with a lifetime of 0 are permanent and
		// are never garbage collected.
		if GetBucketLifetime(bkt.id) == 0 {
			continue
		}

		gcOn := getTimestamp(bkt) + (uint32(GetBucketLifetime(bkt.id)) * 24)
		if now >= gcOn {
			if err := str.DeleteBucket(bkt); err != nil {
				_ = iter.Close()
				return err
			}
		}
	}

	return iter.Close()
}

// Close closes the store.
//
// Close the underlying pebble database, clean the
// cache and stop the GC ticker.
func (str *pebbleStore) Close() error {
	if str.gcTicker != nil {
		str.gcTicker.Stop()
	}

	str.cache.Range(func(key, val any) bool {
		str.cache.Delete(key)
		return true
	})

	return str.db.Close()
}

// First byte of the underlying pebble db key, this byte is
// prepended before the actual key. This allows store.GC to
// iterate over all the buckets in the store without having
// to iterate over the bucket values.
const (
	bucketTable = iota
	valueTable
)

// getPebbleBucketKey returns the pebble bucket table key
// for the given BucketId.
func getPebbleBucketKey(id BucketID) []byte {
	return append([]byte{bucketTable}, id[:]...)
}

// getPebbleValueKey returns the pebble value table key for
// the given BucketId and idx.
func getPebbleValueKey(id BucketID, idx uint16) []byte {
	key := make([]byte, 19)
	key[0] = valueTable
	copy(key[1:], id[:])
	binary.BigEndian.PutUint16(key[1+BucketIDLength:], idx)
	return key
}
