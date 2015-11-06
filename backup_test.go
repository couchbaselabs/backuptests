package tests

import (
	"strconv"
	"testing"

	"github.com/couchbase/backup"
	"github.com/couchbase/backup/archive"
	"github.com/couchbase/backup/couchbase"
	"github.com/couchbase/backup/value"
)

func TestBackupBadPassword(t *testing.T) {
	defer cleanup()
	defer deleteAllBuckets(testHostNoAuth, t)
	deleteAllBuckets(testHostNoAuth, t)
	createCouchbaseBucket(testHostNoAuth, "default", "", t)

	loadData(testHostNoAuth, "default", "", 5000, "full", t)

	backupName := "badpassword-test"
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
	defer deleteAllBuckets(testHostNoAuth, t)
	deleteAllBuckets(testHostNoAuth, t)
	createCouchbaseBucket(testHostNoAuth, "default", "", t)
	createCouchbaseBucket(testHostNoAuth, "saslbucket", "saslpwd", t)

	loadData(testHostNoAuth, "default", "", 5000, "full", t)
	loadData(testHostNoAuth, "saslbucket", "saslpwd", 2500, "full", t)

	config := value.CreateBackupConfig("", "", make([]string, 0),
		make([]string, 0), make([]string, 0), make([]string, 0),
		false, false, false, false, false, false)

	a, err := archive.MountArchive(testDir)
	if err != nil {
		t.Fatal(err.Error())
	}

	if err := a.CreateBackup("full-backup-test", config); err != nil {
		t.Fatal(err.Error())
	}

	name, err := backup.Backup(a, "full-backup-test", testHost, restUsername, restPassword,
		4, false, false)
	if err != nil {
		t.Fatal(err.Error())
	}

	info, err := a.IncrBackupInfo("full-backup-test", name)
	if err != nil {
		t.Fatal(err.Error())
	}

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
	defer deleteAllBuckets(testHostNoAuth, t)
	deleteAllBuckets(testHostNoAuth, t)
	createCouchbaseBucket(testHostNoAuth, "default", "", t)

	setName := "incr-backup-test"

	config := value.CreateBackupConfig("", "", make([]string, 0),
		make([]string, 0), make([]string, 0), make([]string, 0),
		false, false, false, false, false, false)

	a, err := archive.MountArchive(testDir)
	if err != nil {
		t.Fatal(err.Error())
	}

	if err := a.CreateBackup(setName, config); err != nil {
		t.Fatal(err.Error())
	}

	// Do full backup
	loadData(testHostNoAuth, "default", "", 5000, "full", t)

	name, err := backup.Backup(a, setName, testHost, restUsername, restPassword,
		4, false, false)

	info, err := a.IncrBackupInfo(setName, name)
	if err != nil {
		t.Fatal(err.Error())
	}

	count := info["default"].NumDocs
	if count != 5000 {
		t.Fatal("Expected to backup 5000 items, got " + strconv.Itoa(count))
	}

	// Do first incremental backup
	loadData(testHostNoAuth, "default", "", 4000, "incr-1-", t)

	name, err = backup.Backup(a, setName, testHost, restUsername, restPassword,
		4, false, false)

	info, err = a.IncrBackupInfo(setName, name)
	if err != nil {
		t.Fatal(err.Error())
	}

	count = info["default"].NumDocs
	if count != 4000 {
		t.Fatal("Expected to backup 4000 items, got " + strconv.Itoa(count))
	}

	// Do second incremental backup
	loadData(testHostNoAuth, "default", "", 3000, "incr-2-", t)

	name, err = backup.Backup(a, setName, testHost, restUsername, restPassword,
		4, false, false)

	info, err = a.IncrBackupInfo(setName, name)
	if err != nil {
		t.Fatal(err.Error())
	}

	count = info["default"].NumDocs
	if count != 3000 {
		t.Fatal("Expected to backup 3000 items, got " + strconv.Itoa(count))
	}

	// Do third incremental backup
	loadData(testHostNoAuth, "default", "", 2000, "incr-3-", t)

	name, err = backup.Backup(a, setName, testHost, restUsername, restPassword,
		4, false, false)

	info, err = a.IncrBackupInfo(setName, name)
	if err != nil {
		t.Fatal(err.Error())
	}

	count = info["default"].NumDocs
	if count != 2000 {
		t.Fatal("Expected to backup 2000 items, got " + strconv.Itoa(count))
	}
}

func TestBackupNoBucketsExist(t *testing.T) {
	defer cleanup()
	defer deleteAllBuckets(testHostNoAuth, t)
	deleteAllBuckets(testHostNoAuth, t)

	config := value.CreateBackupConfig("", "", make([]string, 0),
		make([]string, 0), make([]string, 0), make([]string, 0),
		false, false, false, false, false, false)

	a, err := archive.MountArchive(testDir)
	if err != nil {
		t.Fatal(err.Error())
	}

	if err := a.CreateBackup("full-backup-test", config); err != nil {
		t.Fatal(err.Error())
	}

	name, err := backup.Backup(a, "full-backup-test", testHost, restUsername, restPassword,
		4, false, false)
	if err != nil {
		t.Fatal(err.Error())
	}

	info, err := a.IncrBackupInfo("full-backup-test", name)
	if err != nil {
		t.Fatal(err.Error())
	}

	if len(info) != 0 {
		t.Fatal("Expected that no buckets were backed up")
	}
}

func TestBackupDeleteBucketBackupAgain(t *testing.T) {
	defer cleanup()
	defer deleteAllBuckets(testHostNoAuth, t)
	deleteAllBuckets(testHostNoAuth, t)
	createCouchbaseBucket(testHostNoAuth, "default", "", t)

	backupName := "backupdelbackup-test"

	loadData(testHostNoAuth, "default", "", 5000, "one", t)

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

	name, err := backup.Backup(a, backupName, testHost, restUsername, restPassword,
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

	deleteAllBuckets(testHostNoAuth, t)
	createCouchbaseBucket(testHostNoAuth, "default", "", t)
	loadData(testHostNoAuth, "default", "", 10000, "two", t)

	name, err = backup.Backup(a, backupName, testHost, restUsername, restPassword,
		4, false, false)
	if err != nil {
		t.Fatal(err.Error())
	}

	info, err = a.IncrBackupInfo(backupName, name)
	if err != nil {
		t.Fatal(err.Error())
	}

	count = info["default"].NumDocs
	if count != 10000 {
		t.Fatal("Expected to backup 10000 items, got " + strconv.Itoa(count))
	}
}

func TestBackupWithMemcachedBucket(t *testing.T) {
	defer cleanup()
	defer deleteAllBuckets(testHostNoAuth, t)
	deleteAllBuckets(testHostNoAuth, t)
	createCouchbaseBucket(testHostNoAuth, "default", "", t)
	createMemcachedBucket(testHostNoAuth, "mcd", "", t)

	backupName := "skip-mcd-bucket-test"

	loadData(testHostNoAuth, "default", "", 5000, "one", t)

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

	name, err := backup.Backup(a, backupName, testHost, restUsername, restPassword,
		4, false, false)
	if err != nil {
		t.Fatal(err.Error())
	}

	info, err := a.IncrBackupInfo(backupName, name)
	if err != nil {
		t.Fatal(err.Error())
	}

	if len(info) != 1 {
		t.Fatal("Expected only 1 bucket to be backed up")
	}

	if _, ok := info["default"]; !ok {
		t.Fatal("Expected default bucket to be backed up")
	}
}
