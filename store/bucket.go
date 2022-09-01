package store

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
)

// Bucket keeps track of a list of values.
//
// A bucket instance can be retrieved from the store.
// Buckets keep track of there own values, these are:
//   - bucket id
//   - bucket key
//   - bucket access timestamp
//   - lastIdx (last index of value table, cached but not stored in the pebble store)
//   - the bucket values (stored in the value table)
//
// The bucket interface is thread-safe.
type Bucket interface {
	// GetBucketID returns the bucket id.
	GetBucketID() BucketID

	// GetBucketKey returns the bucket key.
	GetBucketKey() BucketKey

	// GetValues retrieves values from the bucket.
	GetValues(rng BucketRange) ([]BucketValue, error)

	// PutValues puts values into the bucket.
	PutValues(values []BucketValue) error

	// AppendValues adds values to the bucket.
	AppendValues(values []BucketValue) error

	// DeleteValues deletes values from the bucket.
	DeleteValues(rng BucketRange) error
}

const (
	BucketIDLength  = 16
	BucketKeyLength = 32
)

// BucketID is the unique identifier of a bucket. The first
// 14 bytes are random, the 15th byte is the lifetime, and
// the 16th byte contains the permissions.
type BucketID *[BucketIDLength]byte

// BucketKey is used to grant protected access. Its stored
// in the last 32 bytes of the bucket data.
type BucketKey *[BucketKeyLength]byte

// BucketPermissions contains the permissions of a bucket.
//
// Permissions can be requested for public and protected
// access. Public permissions are accessible by anyone,
// protected permissions are only accessible by users that
// know the bucket key. Users that can write can always
// append to the bucket. Write permission is required for
// deleting values from the store.
type BucketPermissions struct {
	Read   bool
	Write  bool
	Append bool
}

// GetBucketLifetime returns the lifetime of a bucket, 0 if
// bucket has an infinite lifetime.
func GetBucketLifetime(id BucketID) byte {
	return id[14]
}

// GetPermissions returns the permissions of a bucket.
//
// Authorized identifies whether the bucket is accessed by
// an user that knows the bucket key.
func GetBucketPermissions(id BucketID, authorized bool) BucketPermissions {
	if authorized {
		return BucketPermissions{
			Read:   id[15]&1 != 0 || id[15]&8 != 0,
			Write:  id[15]&2 != 0 || id[15]&16 != 0,
			Append: id[15]&2 != 0 || id[15]&32 != 0,
		}
	} else {
		return BucketPermissions{
			Read:   id[15]&1 != 0,
			Write:  id[15]&2 != 0,
			Append: id[15]&4 != 0,
		}
	}
}

// BucketValue represents a single value stored in a bucket.
//
// The bucket value contains an unique bucket index and a
// value. The value is stored in the value table with the
// bucket id + value idx as key.
type BucketValue struct {
	Idx   uint16 // If value is 0, append to the end of the bucket.
	Value []byte
}

// BucketRange represents a range of values from a bucket
// marked by a start / end idx.
type BucketRange struct {
	Start uint16
	End   uint16
}

// pebbleBucket implements the Bucket interface.
type pebbleBucket struct {
	id   BucketID
	data []byte // First 4 bytes contain the timestamp, other 32 are the key.

	store   *pebbleStore // Parent store.
	lastIdx atomic.Int32 // Highest index in the value table.
}

// GetBucketID returns the bucket id.
func (bkt *pebbleBucket) GetBucketID() BucketID {
	return bkt.id
}

// GetBucketKey returns the bucket key.
func (bkt *pebbleBucket) GetBucketKey() BucketKey {
	return BucketKey(bkt.data[4:])
}

// GetValues retrieves values from the bucket.
func (bkt *pebbleBucket) GetValues(rng BucketRange) ([]BucketValue, error) {
	iter := bkt.store.db.NewIter(&pebble.IterOptions{
		LowerBound: getPebbleValueKey(bkt.id, rng.Start),
		UpperBound: getPebbleValueKey(bkt.id, rng.End),
	})

	values := make([]BucketValue, 0, int(math.Min(float64(rng.End-rng.Start), 2048)))
	for iter.First(); iter.Valid(); iter.Next() {
		values = append(values, BucketValue{
			Idx:   binary.BigEndian.Uint16(iter.Key()[1+BucketIDLength:]),
			Value: iter.Value(), // TODO(danger): don't we need to copy this?
		})
	}

	if err := refreshTimestamp(bkt, bkt.store.db); err != nil {
		return values, err
	}

	return values, iter.Close()
}

// PutValues puts values into the bucket.
//
// Values with an idx of 0 are appended to the end of the
// bucket, when the bucket is full ErrBucketFull is
// returned. When a value is empty, the existing
// bucket value at that idx is freed.
func (bkt *pebbleBucket) PutValues(values []BucketValue) error {
	batch := bkt.store.db.NewBatch()
	key := getPebbleValueKey(bkt.id, 0)
	for _, val := range values {
		// Append operation, use lastIdx + 1 as idx.
		if val.Idx == 0 {
			if idx := bkt.lastIdx.Add(1); idx <= math.MaxUint16 {
				val.Idx = uint16(idx)
			} else {
				bkt.lastIdx.Add(-1) // Hack to prevent the lastIdx from overflowing.
				return ErrBucketIsFull
			}
		}

		// Write operation to higher idx than lastIdx.
		if bkt.lastIdx.Load() < int32(val.Idx) {
			bkt.lastIdx.Store(int32(val.Idx))
		}

		// Write value to database, free value if empty.
		binary.BigEndian.PutUint16(key[1+BucketIDLength:], val.Idx)
		if len(val.Value) > 0 {
			if err := batch.Set(key, val.Value, nil); err != nil {
				return err
			}
		} else {
			if err := batch.Delete(key, nil); err != nil {
				return err
			}
		}
	}

	if err := refreshTimestamp(bkt, batch); err != nil {
		return err
	}

	return bkt.store.db.Apply(batch, nil)
}

// AppendValues adds values to the bucket.
//
// The idx of the passed values must be 0 or a valid idx. An
// idx is valid when it is the last idx + 1.
func (bkt *pebbleBucket) AppendValues(values []BucketValue) error {
	batch := bkt.store.db.NewBatch()
	key := getPebbleValueKey(bkt.id, 0)
	for _, val := range values {
		if val.Idx != 0 {
			// When append is called, but the idx is not 0,
			// verify whether the idx is equal to lastIdx+1.
			// This is useful when a user only has append
			// permissions and needs to make sure that its
			// value is inserted at a specific idx.
			if !bkt.lastIdx.CompareAndSwap(int32(val.Idx)-1, int32(val.Idx)) {
				return ErrInvalidAppend
			}
		} else {
			if idx := bkt.lastIdx.Add(1); idx <= math.MaxUint16 {
				val.Idx = uint16(idx)
			} else {
				bkt.lastIdx.Add(-1) // Hack to prevent the lastIdx from overflowing.
				return ErrBucketIsFull
			}
		}

		binary.BigEndian.PutUint16(key[1+BucketIDLength:], val.Idx)
		if err := batch.Set(key, val.Value, nil); err != nil {
			return err
		}
	}

	if err := refreshTimestamp(bkt, batch); err != nil {
		return err
	}

	return bkt.store.db.Apply(batch, nil)
}

// DeleteValues deletes values from the bucket
func (bkt *pebbleBucket) DeleteValues(rng BucketRange) error {
	batch := bkt.store.db.NewBatch()
	if err := batch.DeleteRange(
		getPebbleValueKey(bkt.id, rng.Start),
		getPebbleValueKey(bkt.id, rng.End),
		nil,
	); err != nil {
		return err
	}

	if err := refreshTimestamp(bkt, batch); err != nil {
		return err
	}

	if err := bkt.store.db.Apply(batch, nil); err != nil {
		return err
	}

	// Refresh lastIdx when delete removes the last value.
	// Use compare-and-swap to prevent race condition.
	lastIdx := bkt.lastIdx.Load()
	if rng.Start < uint16(lastIdx) && rng.End > uint16(lastIdx) {
		newIdx := fetchLastIdx(bkt)
		bkt.lastIdx.CompareAndSwap(lastIdx, int32(newIdx))
	}
	return nil
}

// fetchLastIdx returns the last idx in the value table for
// a bucket.
func fetchLastIdx(bkt *pebbleBucket) uint16 {
	iter := bkt.store.db.NewIter(&pebble.IterOptions{
		LowerBound: getPebbleValueKey(bkt.id, 0),
		UpperBound: getPebbleValueKey(bkt.id, math.MaxUint16),
	})
	defer iter.Close()

	if iter.Last() {
		return binary.BigEndian.Uint16(iter.Key()[1+BucketIDLength:])
	} else {
		return 0
	}
}

// refreshTimestamp updates the timestamp in the bucket.
func refreshTimestamp(bkt *pebbleBucket, writer pebble.Writer) error {
	now := getCurrentTimestamp()
	arr := make([]byte, 4)
	binary.BigEndian.PutUint32(arr, now)

	if !bytes.Equal(bkt.data[:4], arr) {
		copy(bkt.data[:4], arr)
		return writer.Set(getPebbleBucketKey(bkt.id), bkt.data, pebble.NoSync)
	}
	return nil
}

// getTimestamp returns the last access time of the bucket.
func getTimestamp(bkt *pebbleBucket) uint32 {
	return binary.BigEndian.Uint32(bkt.data)
}

// getCurrentTimestamp returns the current timestamp.
func getCurrentTimestamp() uint32 {
	return uint32(time.Since(time.Unix(0, 0)) / (time.Hour))
}
