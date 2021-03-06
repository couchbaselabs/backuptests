package tests

import (
	"testing"

	"github.com/couchbase/backup/couchbase"
	"github.com/couchbase/backup/value"
)

func TestGetPutViews(t *testing.T) {
	bucket := "default"
	defer deleteAllBuckets(testHost, t)
	deleteAllBuckets(testHost, t)
	createCouchbaseBucket(testHost, bucket, "", t)

	rest := couchbase.CreateRestClient(testHost, rbacUsername, rbacPassword, nil)
	ddocs := make([]value.DDoc, 0)

	single := make(map[string]map[string]map[string]string)
	single["views"] = make(map[string]map[string]string)
	single["views"]["single"] = make(map[string]string)
	single["views"]["single"]["map"] = "function (doc, meta) {\n emit(meta.id, null);\n}"

	ddocs = append(ddocs, value.DDoc{"_design/single", "xxxxx", single})

	multi := make(map[string]map[string]map[string]string)
	multi["views"] = make(map[string]map[string]string)
	multi["views"]["first"] = make(map[string]string)
	multi["views"]["first"]["map"] = "function (doc, meta) {\n emit(meta.id, null);\n}"
	multi["views"]["second"] = make(map[string]string)
	multi["views"]["second"]["map"] = "function (doc, meta) {\n emit(meta.id, null);\n}"

	ddocs = append(ddocs, value.DDoc{"_design/multi", "xxxxx", multi})

	withreduce := make(map[string]map[string]map[string]string)
	withreduce["views"] = make(map[string]map[string]string)
	withreduce["views"]["reduced"] = make(map[string]string)
	withreduce["views"]["reduced"]["map"] = "function (doc, meta) {\n emit(meta.id, null);\n}"
	withreduce["views"]["reduced"]["reduce"] = "_count"

	ddocs = append(ddocs, value.DDoc{"_design/red", "xxxxx", withreduce})

	spatialsingle := make(map[string]map[string]string)
	spatialsingle["spatial"] = make(map[string]string)
	spatialsingle["spatial"]["spat"] = "function (doc) {\n  if (doc.geometry) {\n" +
		"    emit([doc.geometry], null);\n  }\n}"

	ddocs = append(ddocs, value.DDoc{"_design/spatsingle", "xxxxx", spatialsingle})

	spatialmulti := make(map[string]map[string]string)
	spatialmulti["spatial"] = make(map[string]string)
	spatialmulti["spatial"]["locate"] = "function (doc) {\n  if (doc.geometry) {\n" +
		"    emit([doc.geometry], null);\n  }\n}"
	spatialmulti["spatial"]["plot"] = "function (doc) {\n  if (doc.geometry) {\n" +
		"    emit([doc.geometry], null);\n  }\n}"

	ddocs = append(ddocs, value.DDoc{"_design/spatmulti", "xxxxx", spatialmulti})

	checkError(rest.PutViews(bucket, ddocs), t)

	views, err := rest.GetViews(bucket)
	checkError(err, t)

	if len(views) != 5 {
		t.Fatal("Expected to get 5 views")
	}
}
