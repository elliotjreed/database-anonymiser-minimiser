package fktracker

import (
	"sync"
	"testing"
)

func TestNewTracker(t *testing.T) {
	tracker := NewTracker()
	if tracker == nil {
		t.Fatal("NewTracker() returned nil")
	}
	if tracker.exportedKeys == nil {
		t.Error("exportedKeys map should be initialized")
	}
}

func TestRecordAndHasValue(t *testing.T) {
	tracker := NewTracker()

	// Record some values
	tracker.RecordValue("users", "id", 1)
	tracker.RecordValue("users", "id", 2)
	tracker.RecordValue("users", "id", 3)

	// Check HasValue for recorded values
	if !tracker.HasValue("users", "id", 1) {
		t.Error("HasValue(users, id, 1) = false, want true")
	}
	if !tracker.HasValue("users", "id", 2) {
		t.Error("HasValue(users, id, 2) = false, want true")
	}
	if !tracker.HasValue("users", "id", 3) {
		t.Error("HasValue(users, id, 3) = false, want true")
	}

	// Check HasValue for non-recorded values
	if tracker.HasValue("users", "id", 4) {
		t.Error("HasValue(users, id, 4) = true, want false")
	}

	// Check HasValue for different table
	if tracker.HasValue("orders", "id", 1) {
		t.Error("HasValue(orders, id, 1) = true, want false")
	}
}

func TestRecordNilValue(t *testing.T) {
	tracker := NewTracker()

	// Recording nil should not add anything
	tracker.RecordValue("users", "id", nil)

	count := tracker.GetExportedCount("users", "id")
	if count != 0 {
		t.Errorf("GetExportedCount after recording nil = %d, want 0", count)
	}
}

func TestHasValueWithNil(t *testing.T) {
	tracker := NewTracker()

	// NULL values should always be allowed
	if !tracker.HasValue("users", "id", nil) {
		t.Error("HasValue(users, id, nil) = false, want true (NULL always allowed)")
	}
}

func TestGetExportedValues(t *testing.T) {
	tracker := NewTracker()

	// Empty case
	values := tracker.GetExportedValues("users", "id")
	if values != nil {
		t.Error("GetExportedValues on empty tracker should return nil")
	}

	// After recording values
	tracker.RecordValue("users", "id", 1)
	tracker.RecordValue("users", "id", 2)
	tracker.RecordValue("users", "id", 3)

	values = tracker.GetExportedValues("users", "id")
	if len(values) != 3 {
		t.Errorf("len(GetExportedValues) = %d, want 3", len(values))
	}

	// Check that all values are present
	valueSet := make(map[any]struct{})
	for _, v := range values {
		valueSet[v] = struct{}{}
	}
	for _, expected := range []int64{1, 2, 3} {
		if _, ok := valueSet[expected]; !ok {
			t.Errorf("GetExportedValues missing value %d", expected)
		}
	}
}

func TestGetExportedCount(t *testing.T) {
	tracker := NewTracker()

	// Empty case
	count := tracker.GetExportedCount("users", "id")
	if count != 0 {
		t.Errorf("GetExportedCount on empty = %d, want 0", count)
	}

	// After recording values
	tracker.RecordValue("users", "id", 1)
	tracker.RecordValue("users", "id", 2)

	count = tracker.GetExportedCount("users", "id")
	if count != 2 {
		t.Errorf("GetExportedCount = %d, want 2", count)
	}

	// Recording duplicate should not increase count
	tracker.RecordValue("users", "id", 1)
	count = tracker.GetExportedCount("users", "id")
	if count != 2 {
		t.Errorf("GetExportedCount after duplicate = %d, want 2", count)
	}
}

func TestNormalizeValue(t *testing.T) {
	tracker := NewTracker()

	// Different integer types should normalize to the same value
	tracker.RecordValue("users", "id", int(1))

	// Check with different integer types
	if !tracker.HasValue("users", "id", int64(1)) {
		t.Error("int64(1) should match recorded int(1)")
	}
	if !tracker.HasValue("users", "id", int32(1)) {
		t.Error("int32(1) should match recorded int(1)")
	}
	if !tracker.HasValue("users", "id", uint(1)) {
		t.Error("uint(1) should match recorded int(1)")
	}
}

func TestStringValues(t *testing.T) {
	tracker := NewTracker()

	// Record string values
	tracker.RecordValue("users", "uuid", "abc-123")
	tracker.RecordValue("users", "uuid", "def-456")

	if !tracker.HasValue("users", "uuid", "abc-123") {
		t.Error("HasValue(users, uuid, abc-123) = false, want true")
	}
	if !tracker.HasValue("users", "uuid", "def-456") {
		t.Error("HasValue(users, uuid, def-456) = false, want true")
	}
	if tracker.HasValue("users", "uuid", "xyz-789") {
		t.Error("HasValue(users, uuid, xyz-789) = true, want false")
	}
}

func TestByteSliceNormalization(t *testing.T) {
	tracker := NewTracker()

	// Record as string
	tracker.RecordValue("users", "data", "test")

	// Check with []byte
	if !tracker.HasValue("users", "data", []byte("test")) {
		t.Error("[]byte should match string after normalization")
	}
}

func TestMultipleTables(t *testing.T) {
	tracker := NewTracker()

	// Record values for multiple tables
	tracker.RecordValue("users", "id", 1)
	tracker.RecordValue("users", "id", 2)
	tracker.RecordValue("orders", "id", 100)
	tracker.RecordValue("orders", "id", 101)
	tracker.RecordValue("orders", "user_id", 1)

	// Check users.id
	if tracker.GetExportedCount("users", "id") != 2 {
		t.Error("users.id count should be 2")
	}

	// Check orders.id
	if tracker.GetExportedCount("orders", "id") != 2 {
		t.Error("orders.id count should be 2")
	}

	// Check orders.user_id
	if tracker.GetExportedCount("orders", "user_id") != 1 {
		t.Error("orders.user_id count should be 1")
	}

	// Values should be isolated to their table.column
	if tracker.HasValue("users", "id", 100) {
		t.Error("users.id should not have value 100")
	}
	if tracker.HasValue("orders", "id", 1) {
		t.Error("orders.id should not have value 1")
	}
}

func TestConcurrentAccess(t *testing.T) {
	tracker := NewTracker()

	var wg sync.WaitGroup
	numGoroutines := 10
	valuesPerGoroutine := 100

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < valuesPerGoroutine; j++ {
				value := goroutineID*valuesPerGoroutine + j
				tracker.RecordValue("users", "id", value)
			}
		}(i)
	}

	wg.Wait()

	// Verify count
	expectedCount := numGoroutines * valuesPerGoroutine
	actualCount := tracker.GetExportedCount("users", "id")
	if actualCount != expectedCount {
		t.Errorf("GetExportedCount = %d, want %d", actualCount, expectedCount)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < valuesPerGoroutine; j++ {
				value := goroutineID*valuesPerGoroutine + j
				if !tracker.HasValue("users", "id", value) {
					t.Errorf("Missing value %d", value)
				}
			}
		}(i)
	}

	wg.Wait()
}
