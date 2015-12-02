package tests

import (
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/backup"
	"github.com/couchbase/backup/archive"
	"github.com/couchbase/backup/value"
)

func TestBackupRestore(t *testing.T) {
	defer cleanup()
	defer deleteAllBuckets(testHost, t)
	deleteAllBuckets(testHost, t)
	createCouchbaseBucket(testHost, "default", "", t)

	backupName := "restore-test"

	loadData(testHost, "default", "", 5000, "full", t)

	config := value.CreateBackupConfig("", "", make([]string, 0),
		make([]string, 0), make([]string, 0), make([]string, 0),
		false, false, false, false, false, false)

	a, err := archive.MountArchive(testDir)
	checkError(err, t)

	checkError(a.CreateBackup(backupName, config), t)

	// Test that restoring data when noe exists gives an error
	err = backup.Restore(a, backupName, testHost, restUsername, restPassword, "name",
		"name", false, config)
	if err == nil {
		t.Fatal(err.Error())
	} else if _, ok := err.(backup.NothingToRestoreError); !ok {
		t.Fatal(err.Error())
	}

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
	createCouchbaseBucket(testHost, "default", "", t)

	// Check that using an invalid start point causes an error
	err = backup.Restore(a, backupName, testHost, restUsername, restPassword, "name",
		name, false, config)
	if err == nil {
		t.Fatal(err.Error())
	} else if _, ok := err.(backup.RestorePointError); !ok {
		t.Fatal(err.Error())
	}

	// Check that using an invalid end point causes an error
	err = backup.Restore(a, backupName, testHost, restUsername, restPassword, name,
		"end", false, config)
	if err == nil {
		t.Fatal(err.Error())
	} else if _, ok := err.(backup.RestorePointError); !ok {
		t.Fatal(err.Error())
	}

	// Restore the data using explicit start/end specification
	err = backup.Restore(a, backupName, testHost, restUsername, restPassword, name,
		name, false, config)
	checkError(err, t)

	time.Sleep(5 * time.Second)
	items, err := getNumItems(testHost, restUsername, restPassword, "default")
	if err != nil {
		t.Fatalf("Error getting item count: %s", err.Error())
	}
	if items != 5000 {
		t.Fatalf("Expected 5000 items, got %d", items)
	}

	deleteBucket(testHost, "default", t, true)
	createCouchbaseBucket(testHost, "default", "", t)

	// Restore the data without explicitly setting the start/end point
	err = backup.Restore(a, backupName, testHost, restUsername, restPassword, "",
		"", false, config)
	checkError(err, t)

	time.Sleep(5 * time.Second)
	items, err = getNumItems(testHost, restUsername, restPassword, "default")
	checkError(err, t)

	if items != 5000 {
		t.Fatalf("Expected 5000 items, got %d", items)
	}
}
