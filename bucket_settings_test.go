package tests

import (
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/backup"
	"github.com/couchbase/backup/archive"
	"github.com/couchbase/backup/couchbase"
	"github.com/couchbase/backup/value"
)

// Tests all error cases when we don't have a bucket and try to restore. This
// means that we check that all restore configuratoins work no matter what is
// skipped during the restore.
func TestRestoreNoBucketNoBackupConfig(t *testing.T) {
	defer cleanup()
	defer deleteAllBuckets(testHost, t)
	deleteAllBuckets(testHost, t)
	createCouchbaseBucket(testHost, "default", "", t)

	backupName := "bucket-config-test"

	loadData(testHost, "default", "", 5000, "full", t)
	loadViews(testHost, "default", "first", 12, 2, t)

	config := value.CreateBackupConfig("", "", make([]string, 0),
		make([]string, 0), make([]string, 0), make([]string, 0),
		false, true, false, false, false, false)

	a, err := archive.MountArchive(testDir)
	checkError(err, t)

	checkError(a.CreateBackup(backupName, config), t)

	// Backup the data
	name, err := backup.Backup(a, backupName, testHost, restUsername, restPassword,
		4, false, false)
	checkError(err, t)

	info, err := a.IncrBackupInfo(backupName, name)
	checkError(err, t)

	count := info["default"].NumDocs
	if count != 5000 {
		t.Fatal("Expected to backup 5000 items, got " + strconv.Itoa(count))
	}

	deleteBucket(testHost, "default", t, true)

	// Do a restore where the views are the first thing to be restored, make sure
	// we fail to restore the views because no bucket exists
	err = backup.Restore(a, backupName, testHost, restUsername, restPassword, "",
		"", false, config)
	_, ok := err.(couchbase.BucketNotFoundError)
	if err == nil || !ok {
		t.Fatal("Expected BucketNotFoundError")
	}

	// Do a restore where the gsi indexes are the first thing to be restored, make
	// sure we fail to restore the gsi indexes because no bucket exists
	config = value.CreateBackupConfig("", "", make([]string, 0),
		make([]string, 0), make([]string, 0), make([]string, 0),
		false, true, true, false, false, false)
	err = backup.Restore(a, backupName, testHost, restUsername, restPassword, "",
		"", false, config)
	_, ok = err.(couchbase.BucketNotFoundError)
	if err == nil || !ok {
		t.Fatal("Expected BucketNotFoundError")
	}

	// Do a restore where the full text indexes are the first thing to be restored,
	// make sure we fail to restore the full text indexes because no bucket exists
	config = value.CreateBackupConfig("", "", make([]string, 0),
		make([]string, 0), make([]string, 0), make([]string, 0),
		false, true, true, true, false, false)
	err = backup.Restore(a, backupName, testHost, restUsername, restPassword, "",
		"", false, config)
	_, ok = err.(couchbase.BucketNotFoundError)
	if err == nil || !ok {
		t.Fatal("Expected BucketNotFoundError")
	}

	// Do a restore where data is the first thing to be restored, make sure we fail
	// to restore the data because no bucket exists
	config = value.CreateBackupConfig("", "", make([]string, 0),
		make([]string, 0), make([]string, 0), make([]string, 0),
		false, true, true, true, true, false)
	err = backup.Restore(a, backupName, testHost, restUsername, restPassword, "",
		"", false, config)
	_, ok = err.(couchbase.BucketNotFoundError)
	if err == nil || !ok {
		t.Fatal("Expected BucketNotFoundError")
	}
}

// Test that when we have no bucket, but do have a bucket config that we create a new
// bucket and can properly restore all data.
func TestRestoreNoBucketWithBackupConfig(t *testing.T) {
	defer cleanup()
	defer deleteAllBuckets(testHost, t)
	deleteAllBuckets(testHost, t)
	createCouchbaseBucket(testHost, "default", "", t)

	backupName := "bucket-config-test"

	loadData(testHost, "default", "", 5000, "full", t)
	loadViews(testHost, "default", "first", 12, 2, t)

	config := value.CreateBackupConfig("", "", make([]string, 0),
		make([]string, 0), make([]string, 0), make([]string, 0),
		false, false, false, false, false, false)

	a, err := archive.MountArchive(testDir)
	checkError(err, t)

	checkError(a.CreateBackup(backupName, config), t)

	// Backup the data
	name, err := backup.Backup(a, backupName, testHost, restUsername, restPassword,
		4, false, false)
	checkError(err, t)

	info, err := a.IncrBackupInfo(backupName, name)
	checkError(err, t)

	count := info["default"].NumDocs
	if count != 5000 {
		t.Fatal("Expected to backup 5000 items, got " + strconv.Itoa(count))
	}

	deleteBucket(testHost, "default", t, true)

	err = backup.Restore(a, backupName, testHost, restUsername, restPassword, "",
		"", false, config)
	checkError(err, t)

	time.Sleep(5 * time.Second)
	items, err := getNumItems(testHost, restUsername, restPassword, "default")
	checkError(err, t)

	if items != 5000 {
		t.Fatalf("Expected 5000 items, got %d", items)
	}
}