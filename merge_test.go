package tests

import (
	"testing"

	"github.com/couchbase/backup/archive"
	"github.com/couchbase/backup/value"
)

func TestMerge(t *testing.T) {
	defer cleanup()
	defer deleteAllBuckets(testHost, t)
	cleanup()
	deleteAllBuckets(testHost, t)
	createCouchbaseBucket(testHost, "default", "", t)

	setName := "incr-backup-test"

	config := value.CreateBackupConfig("", "", make([]string, 0),
		make([]string, 0), make([]string, 0), make([]string, 0),
		false, false, false, false, false, false)

	a, err := archive.MountArchive(testDir, true)
	checkError(err, t)

	checkError(a.CreateBackup(setName, config), t)

	// Do full backup
	loadData(testHost, "default", "", 5000, "full", t)

	name1, err := executeBackup(a, setName, "archive", testHost, restUsername, restPassword,
		4, false, false)

	info, err := a.IncrBackupInfo(setName, name1)
	checkError(err, t)

	count := info["default"].NumDocs
	if count != 5000 {
		t.Fatal("Expected to backup 5000 items, got %d", count)
	}

	// Do first incremental backup
	loadData(testHost, "default", "", 4000, "incr-1-", t)

	name2, err := executeBackup(a, setName, "archive", testHost, restUsername, restPassword,
		4, false, false)

	info, err = a.IncrBackupInfo(setName, name2)
	checkError(err, t)

	count = info["default"].NumDocs
	if count != 4000 {
		t.Fatal("Expected to backup 4000 items, got %d", count)
	}

	// Do second incremental backup
	loadData(testHost, "default", "", 3000, "incr-2-", t)

	name3, err := executeBackup(a, setName, "archive", testHost, restUsername, restPassword,
		4, false, false)

	info, err = a.IncrBackupInfo(setName, name3)
	checkError(err, t)

	count = info["default"].NumDocs
	if count != 3000 {
		t.Fatal("Expected to backup 3000 items, got %d", count)
	}

	// Do third incremental backup
	loadData(testHost, "default", "", 2000, "incr-3-", t)

	name4, err := executeBackup(a, setName, "archive", testHost, restUsername, restPassword,
		4, false, false)

	info, err = a.IncrBackupInfo(setName, name4)
	checkError(err, t)

	count = info["default"].NumDocs
	if count != 2000 {
		t.Fatal("Expected to backup 2000 items, got %d", count)
	}

	// Merge the backups and make sure all the items show up in the merged backup
	_, err = a.MergeIncrBackups(setName, name1, name4)
	checkError(err, t)

	info, err = a.IncrBackupInfo(setName, name4)
	checkError(err, t)

	count = info["default"].NumDocs
	if count != 14000 {
		t.Fatalf("Expected to backup 14000 items, got %d", count)
	}

	binfo, err := a.BackupInfo(setName)
	if binfo.NumIncrBackups != 1 {
		t.Fatalf("Expected 1 incr backups after merge, got %d", count)
	}
}
