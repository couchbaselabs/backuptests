package tests

import (
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/gocb"
)

const testDir string = "/tmp/backup-test"
const testHost = "http://127.0.0.1:9000"
const restUsername = "Administrator"
const restPassword = "password"

func cleanup() {
	os.RemoveAll(testDir)
}

func loadData(host string, bucket string, password string, items int,
	prefix string, t *testing.T) {
	connection, err := gocb.Connect(host)
	if err != nil {
		t.Fatal("Test data loader cannot connect to the cluster: " + err.Error())
	}
	b, err := connection.OpenBucket(bucket, password)
	if err != nil {
		t.Fatal("Test data loader cannot connect to the bucket: " + err.Error())
	}

	for i := 0; i < items; i++ {
		key := prefix + strconv.Itoa(i)
		_, err := b.Insert(key, map[string]interface{}{"x": i}, 0)
		if err != nil {
			t.Fatal("Error setting `" + key + "`, " + err.Error())
		}
	}

	b.Close()
}

func createMemcachedBucket(host, bucket, password string, t *testing.T) {
	settings := &gocb.BucketSettings{
		FlushEnabled:  false,
		IndexReplicas: false,
		Name:          bucket,
		Password:      password,
		Quota:         256,
		Replicas:      0,
		Type:          gocb.Memcached,
	}

	createBucket(host, settings, t)
}

func createCouchbaseBucket(host, bucket, password string, t *testing.T) {
	settings := &gocb.BucketSettings{
		FlushEnabled:  false,
		IndexReplicas: false,
		Name:          bucket,
		Password:      password,
		Quota:         256,
		Replicas:      0,
		Type:          gocb.Couchbase,
	}

	createBucket(host, settings, t)
}

func createBucket(host string, settings *gocb.BucketSettings, t *testing.T) {

	connection, err := gocb.Connect(host)
	if err != nil {
		t.Fatal("Unable to connect to cluster: " + err.Error())
	}

	manager := connection.Manager("Administrator", "password")
	err = manager.InsertBucket(settings)
	if err != nil {
		t.Fatal("Bucket creation failed: " + err.Error())
	}

	for i := 0; i < 30; i++ {
		if isBucketReady(host, settings.Name, t) {
			return
		}
		time.Sleep(1 * time.Second)
	}

	t.Fatal("Bucket creation timed out")
}

func isBucketReady(host, bucket string, t *testing.T) bool {
	url := "/pools/default/buckets/" + bucket

	req, err := http.NewRequest("GET", host+url, nil)
	if err != nil {
		t.Fatalf("Failed to create http request: %s", err.Error())
	}
	req.SetBasicAuth("Administrator", "password")

	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Error sending http request: %s", err.Error())
	} else if resp.StatusCode != http.StatusOK {
		return false
	}

	defer resp.Body.Close()

	type overlay struct {
		Name  string `json:"name"`
		Nodes []struct {
			Status string `json:"status"`
		} `json:"nodes"`
	}

	var data overlay
	decoder := json.NewDecoder(resp.Body)
	if err = decoder.Decode(&data); err != nil {
		t.Fatalf("Error decoding response: %s", err.Error())
	}

	if resp.StatusCode != http.StatusOK {
		return false
	}

	if len(data.Nodes) == 0 {
		return false
	}

	for _, node := range data.Nodes {
		if node.Status != "healthy" {
			return false
		}
	}

	return true
}

func deleteAllBuckets(host string, t *testing.T) {
	connection, err := gocb.Connect(host)
	if err != nil {
		t.Fatal("Unable to connect to cluster: " + err.Error())
	}

	manager := connection.Manager("Administrator", "password")
	buckets, err := manager.GetBuckets()
	if err != nil {
		t.Fatal("Unable to get all buckets: " + err.Error())
	}

	for _, bucket := range buckets {
		if err := manager.RemoveBucket(bucket.Name); err != nil {
			t.Fatalf("Error deleting bucket %s", bucket.Name)
		}
	}
}

func deleteBucket(host string, bucket string, t *testing.T, noErr bool) {
	connection, err := gocb.Connect(host)
	if err != nil {
		t.Fatal("Unable to connect to cluster: " + err.Error())
	}

	manager := connection.Manager("Administrator", "password")
	if err := manager.RemoveBucket(bucket); err != nil && !noErr {
		t.Fatalf("Error deleting bucket %s", bucket)
	}
}

func getNumItems(host, username, password, bucket string) (uint64, error) {
	req, err := http.NewRequest("GET", host+"/pools/default/buckets/"+bucket, nil)
	if err != nil {
		return 0, err
	}
	req.SetBasicAuth(username, password)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}

	type Overlay struct {
		Stats struct {
			ItemCount uint64 `json:"itemCount"`
		} `json:"basicStats"`
	}

	var data Overlay
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&data)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()

	return data.Stats.ItemCount, nil
}
