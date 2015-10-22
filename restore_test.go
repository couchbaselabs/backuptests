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
	defer deleteAllBuckets(testHostNoAuth, t)
	deleteAllBuckets(testHostNoAuth, t)
	createCouchbaseBucket(testHostNoAuth, "default", "", t)

	backupName := "restore-test"

	loadData(testHostNoAuth, "default", "", 5000, "full", t)

	config := value.CreateBackupConfig("", "", make([]string, 0),
		make([]string, 0), make([]string, 0), make([]string, 0),
		false, false, false, false, false, false)

	a, err := archive.MountArchive(testDir)
	if err != nil {
		t.Fatal(err.Error())
	}

	if err := a.CreateBackup(backupName, config); err != nil {
		t.Fatal(err.Error())
	}

	// Backup the data
	name, err := backup.Backup(a, backupName, testHost, "Administrator", "password",
		4, false, false)
	if err != nil {
		t.Fatal(err.Error())
	}

	info, err := a.IncrBackupInfo(backupName, name)
	if err != nil {
		t.Fatal(err.Error())
	}

	count := info["default"].NumDocs
	if count != 5000 {
		t.Fatal("Expected to backup 5000 items, got " + strconv.Itoa(count))
	}

	deleteBucket(testHostNoAuth, "default", t, true)
	createCouchbaseBucket(testHostNoAuth, "default", "", t)

	// Restore the data
	err = backup.Restore(a, backupName, testHost, "Administrator", "password", name, name, config)
	if err != nil {
		t.Fatal(err.Error())
	}

	time.Sleep(5 * time.Second)
	items, err := getNumItems(testHost, "Administrator", "password", "default")
	if err != nil {
		t.Fatalf("Error getting item count: %s", err.Error())
	}
	if items != 5000 {
		t.Fatalf("Expected 5000 items, got %d", items)
	}
}
