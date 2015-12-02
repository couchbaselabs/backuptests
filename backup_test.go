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

func TestBackupBadPassword(t *testing.T) {
	defer cleanup()
	defer deleteAllBuckets(testHost, t)
	deleteAllBuckets(testHost, t)
	createCouchbaseBucket(testHost, "default", "", t)

	loadData(testHost, "default", "", 5000, "full", t)

	backupName := "badpassword-test"
	config := value.CreateBackupConfig("", "", make([]string, 0),
		make([]string, 0), make([]string, 0), make([]string, 0),
		false, false, false, false, false, false)

	a, err := archive.MountArchive(testDir)
	checkError(err, t)

	checkError(a.CreateBackup(backupName, config), t)

	// Test bad password
	_, err = backup.Backup(a, backupName, testHost, restUsername, "badpassword",
		4, false, false)
	if err == nil {
		t.Fatal("Backup succeeded, but expected failure")
	}

	if herr, ok := err.(couchbase.HttpError); !ok {
		t.Fatal("Expected error to be of type HttpError")
		if herr.Code() != 401 {
			t.Fatal("Expected HttpError to be an authentication error")
		}
	}

	// Test bad username
	_, err = backup.Backup(a, backupName, testHost, "Adminiator", restPassword,
		4, false, false)
	if err == nil {
		t.Fatal("Backup succeeded, but expected failure")
	}

	if herr, ok := err.(couchbase.HttpError); !ok {
		t.Fatal("Expected error to be of type HttpError")
		if herr.Code() != 401 {
			t.Fatal("Expected HttpError to be an authentication error")
		}
	}
}

func TestFullBackup(t *testing.T) {
	defer cleanup()
	defer deleteAllBuckets(testHost, t)
	deleteAllBuckets(testHost, t)
	createCouchbaseBucket(testHost, "default", "", t)
	createCouchbaseBucket(testHost, "saslbucket", "saslpwd", t)

	loadData(testHost, "default", "", 5000, "full", t)
	loadData(testHost, "saslbucket", "saslpwd", 2500, "full", t)

	config := value.CreateBackupConfig("", "", make([]string, 0),
		make([]string, 0), make([]string, 0), make([]string, 0),
		false, false, false, false, false, false)

	a, err := archive.MountArchive(testDir)
	checkError(err, t)

	checkError(a.CreateBackup("full-backup-test", config), t)

	name, err := backup.Backup(a, "full-backup-test", testHost, restUsername, restPassword,
		4, false, false)
	checkError(err, t)

	info, err := a.IncrBackupInfo("full-backup-test", name)
	checkError(err, t)

	count := info["default"].NumDocs
	if count != 5000 {
		t.Fatal("Expected to backup 5000 items, got " + strconv.Itoa(count))
	}

	count = info["saslbucket"].NumDocs
	if count != 2500 {
		t.Fatal("Expected to backup 2500 items, got " + strconv.Itoa(count))
	}
}

func TestIncrementalBackup(t *testing.T) {
	defer cleanup()
	defer deleteAllBuckets(testHost, t)
	deleteAllBuckets(testHost, t)
	createCouchbaseBucket(testHost, "default", "", t)

	setName := "incr-backup-test"

	config := value.CreateBackupConfig("", "", make([]string, 0),
		make([]string, 0), make([]string, 0), make([]string, 0),
		false, false, false, false, false, false)

	a, err := archive.MountArchive(testDir)
	checkError(err, t)

	checkError(a.CreateBackup(setName, config), t)

	// Do full backup
	loadData(testHost, "default", "", 5000, "full", t)

	name1, err := backup.Backup(a, setName, testHost, restUsername, restPassword,
		4, false, false)

	info, err := a.IncrBackupInfo(setName, name1)
	checkError(err, t)

	count := info["default"].NumDocs
	if count != 5000 {
		t.Fatal("Expected to backup 5000 items, got " + strconv.Itoa(count))
	}

	// Do first incremental backup
	loadData(testHost, "default", "", 4000, "incr-1-", t)

	name2, err := backup.Backup(a, setName, testHost, restUsername, restPassword,
		4, false, false)

	info, err = a.IncrBackupInfo(setName, name2)
	checkError(err, t)

	count = info["default"].NumDocs
	if count != 4000 {
		t.Fatal("Expected to backup 4000 items, got " + strconv.Itoa(count))
	}

	// Do second incremental backup
	loadData(testHost, "default", "", 3000, "incr-2-", t)

	name3, err := backup.Backup(a, setName, testHost, restUsername, restPassword,
		4, false, false)

	info, err = a.IncrBackupInfo(setName, name3)
	checkError(err, t)

	count = info["default"].NumDocs
	if count != 3000 {
		t.Fatal("Expected to backup 3000 items, got " + strconv.Itoa(count))
	}

	// Do third incremental backup
	loadData(testHost, "default", "", 2000, "incr-3-", t)

	name4, err := backup.Backup(a, setName, testHost, restUsername, restPassword,
		4, false, false)

	info, err = a.IncrBackupInfo(setName, name4)
	checkError(err, t)

	count = info["default"].NumDocs
	if count != 2000 {
		t.Fatal("Expected to backup 2000 items, got " + strconv.Itoa(count))
	}

	// Restore the data without explicitly setting the start/end point in
	// order to restore all backed up data.
	deleteBucket(testHost, "default", t, true)
	createCouchbaseBucket(testHost, "default", "", t)

	err = backup.Restore(a, setName, testHost, restUsername, restPassword, "",
		"", false, config)
	checkError(err, t)

	time.Sleep(5 * time.Second)
	items, err := getNumItems(testHost, restUsername, restPassword, "default")
	checkError(err, t)

	if items != 14000 {
		t.Fatalf("Expected 14000 items, got %d", items)
	}

	// Restore only the 2nd and 3rd backup
	deleteBucket(testHost, "default", t, true)
	createCouchbaseBucket(testHost, "default", "", t)

	err = backup.Restore(a, setName, testHost, restUsername, restPassword, name2,
		name3, false, config)
	checkError(err, t)

	time.Sleep(5 * time.Second)
	items, err = getNumItems(testHost, restUsername, restPassword, "default")
	checkError(err, t)

	if items != 7000 {
		t.Fatalf("Expected 7000 items, got %d", items)
	}

	// Restore everything after and including the 3rd backup, don't specify the end
	deleteBucket(testHost, "default", t, true)
	createCouchbaseBucket(testHost, "default", "", t)

	err = backup.Restore(a, setName, testHost, restUsername, restPassword, name3,
		"", false, config)
	checkError(err, t)

	time.Sleep(5 * time.Second)
	items, err = getNumItems(testHost, restUsername, restPassword, "default")
	checkError(err, t)

	if items != 5000 {
		t.Fatalf("Expected 5000 items, got %d", items)
	}

	// Restore everything before and including the 2nd backup, don't specify start
	deleteBucket(testHost, "default", t, true)
	createCouchbaseBucket(testHost, "default", "", t)

	err = backup.Restore(a, setName, testHost, restUsername, restPassword, "",
		name2, false, config)
	checkError(err, t)

	time.Sleep(5 * time.Second)
	items, err = getNumItems(testHost, restUsername, restPassword, "default")
	checkError(err, t)

	if items != 9000 {
		t.Fatalf("Expected 9000 items, got %d", items)
	}
}

func TestBackupNoBucketsExist(t *testing.T) {
	defer cleanup()
	defer deleteAllBuckets(testHost, t)
	deleteAllBuckets(testHost, t)

	config := value.CreateBackupConfig("", "", make([]string, 0),
		make([]string, 0), make([]string, 0), make([]string, 0),
		false, false, false, false, false, false)

	a, err := archive.MountArchive(testDir)
	checkError(err, t)

	checkError(a.CreateBackup("full-backup-test", config), t)

	name, err := backup.Backup(a, "full-backup-test", testHost, restUsername, restPassword,
		4, false, false)
	checkError(err, t)

	info, err := a.IncrBackupInfo("full-backup-test", name)
	checkError(err, t)

	if len(info) != 0 {
		t.Fatal("Expected that no buckets were backed up")
	}
}

func TestBackupDeleteBucketBackupAgain(t *testing.T) {
	defer cleanup()
	defer deleteAllBuckets(testHost, t)
	deleteAllBuckets(testHost, t)
	createCouchbaseBucket(testHost, "default", "", t)

	backupName := "backupdelbackup-test"

	loadData(testHost, "default", "", 5000, "one", t)

	config := value.CreateBackupConfig("", "", make([]string, 0),
		make([]string, 0), make([]string, 0), make([]string, 0),
		false, false, false, false, false, false)

	a, err := archive.MountArchive(testDir)
	checkError(err, t)

	checkError(a.CreateBackup(backupName, config), t)

	name, err := backup.Backup(a, backupName, testHost, restUsername, restPassword,
		4, false, false)
	checkError(err, t)

	info, err := a.IncrBackupInfo(backupName, name)
	checkError(err, t)

	count := info["default"].NumDocs
	if count != 5000 {
		t.Fatal("Expected to backup 5000 items, got " + strconv.Itoa(count))
	}

	deleteAllBuckets(testHost, t)
	createCouchbaseBucket(testHost, "default", "", t)
	loadData(testHost, "default", "", 10000, "two", t)

	name, err = backup.Backup(a, backupName, testHost, restUsername, restPassword,
		4, false, false)
	checkError(err, t)

	info, err = a.IncrBackupInfo(backupName, name)
	checkError(err, t)

	count = info["default"].NumDocs
	if count != 10000 {
		t.Fatal("Expected to backup 10000 items, got " + strconv.Itoa(count))
	}
}

func TestBackupWithMemcachedBucket(t *testing.T) {
	defer cleanup()
	defer deleteAllBuckets(testHost, t)
	deleteAllBuckets(testHost, t)
	createCouchbaseBucket(testHost, "default", "", t)
	createMemcachedBucket(testHost, "mcd", "", t)

	backupName := "skip-mcd-bucket-test"

	loadData(testHost, "default", "", 5000, "one", t)

	config := value.CreateBackupConfig("", "", make([]string, 0),
		make([]string, 0), make([]string, 0), make([]string, 0),
		false, false, false, false, false, false)

	a, err := archive.MountArchive(testDir)
	checkError(err, t)

	checkError(a.CreateBackup(backupName, config), t)

	name, err := backup.Backup(a, backupName, testHost, restUsername, restPassword,
		4, false, false)
	checkError(err, t)

	info, err := a.IncrBackupInfo(backupName, name)
	checkError(err, t)

	if len(info) != 1 {
		t.Fatal("Expected only 1 bucket to be backed up")
	}

	if _, ok := info["default"]; !ok {
		t.Fatal("Expected default bucket to be backed up")
	}
}

func TestBackupWithIncludeBuckets(t *testing.T) {
	defer cleanup()
	defer deleteAllBuckets(testHost, t)
	deleteAllBuckets(testHost, t)
	createCouchbaseBucket(testHost, "default", "", t)
	createCouchbaseBucket(testHost, "saslbucket", "saslpwd", t)

	loadData(testHost, "default", "", 5000, "full", t)
	loadData(testHost, "saslbucket", "saslpwd", 2500, "full", t)

	include_buckets := []string{"default"}
	config := value.CreateBackupConfig("", "", make([]string, 0),
		include_buckets, make([]string, 0), make([]string, 0),
		false, false, false, false, false, false)

	a, err := archive.MountArchive(testDir)
	checkError(err, t)

	checkError(a.CreateBackup("full-backup-test", config), t)

	name, err := backup.Backup(a, "full-backup-test", testHost, restUsername, restPassword,
		4, false, false)
	checkError(err, t)

	info, err := a.IncrBackupInfo("full-backup-test", name)
	checkError(err, t)

	count := info["default"].NumDocs
	if count != 5000 {
		t.Fatal("Expected to backup 5000 items, got " + strconv.Itoa(count))
	}
}

func TestBackupWithExcludeBuckets(t *testing.T) {
	defer cleanup()
	defer deleteAllBuckets(testHost, t)
	deleteAllBuckets(testHost, t)
	createCouchbaseBucket(testHost, "default", "", t)
	createCouchbaseBucket(testHost, "saslbucket", "saslpwd", t)

	loadData(testHost, "default", "", 5000, "full", t)
	loadData(testHost, "saslbucket", "saslpwd", 2500, "full", t)

	exclude_buckets := []string{"default"}
	config := value.CreateBackupConfig("", "", exclude_buckets,
		make([]string, 0), make([]string, 0), make([]string, 0),
		false, false, false, false, false, false)

	a, err := archive.MountArchive(testDir)
	checkError(err, t)

	checkError(a.CreateBackup("full-backup-test", config), t)

	name, err := backup.Backup(a, "full-backup-test", testHost, restUsername, restPassword,
		4, false, false)
	checkError(err, t)

	info, err := a.IncrBackupInfo("full-backup-test", name)
	checkError(err, t)

	count := info["saslbucket"].NumDocs
	if count != 2500 {
		t.Fatal("Expected to backup 2500 items, got " + strconv.Itoa(count))
	}
}
