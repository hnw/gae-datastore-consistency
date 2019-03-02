// How to deploy:
//   $ appcfg.py update . -A [application_id]

// +build appengine

package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"time"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	aelog "google.golang.org/appengine/log"

	"github.com/hnw/summary"
)

type queryType int

const (
	lookUpByKey     = iota
	indexedQuery    = iota
	projectionQuery = iota
	ancestorQuery   = iota
	numTest         = 100
)

func init() {
	http.HandleFunc("/cat", catHandler)
	http.HandleFunc("/info", infoHandler)
	http.HandleFunc("/count", countHandler)
	http.HandleFunc("/count2", myCountHandler)
	http.HandleFunc("/stat", statKindHandler)
	http.HandleFunc("/insertAndLookUpByKey", insertAndLookUpByKeyHandler)
	http.HandleFunc("/insertAndIndexedQuery", insertAndIndexedQueryHandler)
	http.HandleFunc("/insertAndProjectionQuery", insertAndProjectionQueryHandler)
	http.HandleFunc("/insertAndAncestorQuery", insertAndAncestorQueryHandler)
}

type testkind struct {
	Name      string    `datastore:"name"`
	Value     string    `datastore:"value"`
	CreatedAt time.Time `datastore:"created_at"`
	UpdatedAt time.Time `datastore:"updated_at"`
}

type statKind struct {
	Count     int64     `datastore:"count"`
	Bytes     int64     `datastore:"bytes"`
	Timestamp time.Time `datastore:"timestamp"`

	KindName            string `datastore:"kind_name"`
	EntityBytes         int64  `datastore:"entity_bytes"`
	BuiltinIndexBytes   int64  `datastore:"builtin_index_bytes"`
	BuiltinIndexCount   int64  `datastore:"builtin_index_count"`
	CompositeIndexbytes int64  `datastore:"composite_index_bytes"`
	CompositeIndexCount int64  `datastore:"composite_index_count"`
}

func insertAndLookUpByKeyHandler(w http.ResponseWriter, r *http.Request) {
	insertAndTestQuery(w, r, lookUpByKey)
}

func insertAndIndexedQueryHandler(w http.ResponseWriter, r *http.Request) {
	insertAndTestQuery(w, r, indexedQuery)
}

func insertAndProjectionQueryHandler(w http.ResponseWriter, r *http.Request) {
	insertAndTestQuery(w, r, projectionQuery)
}

func insertAndAncestorQueryHandler(w http.ResponseWriter, r *http.Request) {
	insertAndTestQuery(w, r, ancestorQuery)
}

func insertAndTestQuery(w http.ResponseWriter, r *http.Request, qType queryType) {
	ctx := appengine.NewContext(r)
	smRetry := summary.NewSummary()
	smDuration := summary.NewSummary()
	prefixString := time.Now().Format("2006-01-02 15:04:05.999999999 ")

	var ancestorKey *datastore.Key

	if qType == ancestorQuery {
		k := datastore.NewKey(ctx, "testkind", fmt.Sprintf("%sname-root", prefixString), 0, nil)
		var err error
		ancestorKey, err = datastore.Put(ctx, k, &testkind{})
		if err != nil {
			aelog.Errorf(ctx, "datastore.Put failed: %v", err)
			fmt.Fprintf(w, "datastore.Put failed: %v", err)
			return
		}
	}

	for i := 0; i < numTest; i++ {
		alreadyLogged := false
		d := &testkind{
			Name:      fmt.Sprintf("%sname%d", prefixString, i),
			Value:     fmt.Sprintf("%svalue%d", prefixString, i),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		k := datastore.NewKey(ctx, "testkind", fmt.Sprintf("%sname%d", prefixString, i), 0, ancestorKey)
		lastUpdated := time.Now()
		k, err := datastore.Put(ctx, k, d)
		if err != nil {
			aelog.Errorf(ctx, "datastore.Put failed: %v", err)
			continue
		}

		var q *datastore.Query
		if qType == indexedQuery {
			q = datastore.NewQuery("testkind").
				Filter("name =", fmt.Sprintf("%sname%d", prefixString, i))
		} else if qType == projectionQuery {
			q = datastore.NewQuery("testkind").Project("value", "updated_at").
				Filter("name =", fmt.Sprintf("%sname%d", prefixString, i))
		} else if qType == ancestorQuery {
			q = datastore.NewQuery("testkind").Ancestor(ancestorKey).
				Filter("name =", fmt.Sprintf("%sname%d", prefixString, i))
		}

		nTry := 0
		for {
			var ds []testkind
			nTry++
			if qType == lookUpByKey {
				ds = make([]testkind, 1, 1)
				if err := datastore.Get(ctx, k, &ds[0]); err != nil {
					if err == datastore.ErrNoSuchEntity {
						// リトライ（強整合性が保証されているなら絶対に通らない分岐のはず）
						aelog.Errorf(ctx, "datastore.Get failure : err = %v", err)
					} else {
						aelog.Errorf(ctx, "err = %v", err)
						return
					}
				}
			} else if _, err := q.GetAll(ctx, &ds); err != nil {
				// qが正しければエラーはでないはず
				aelog.Errorf(ctx, "err == %v", err)
				return
			}
			if len(ds) != 1 {
				if !alreadyLogged {
					aelog.Infof(ctx, "len(ds) == %d", len(ds))
					alreadyLogged = true
				}
				// do nothing
			} else if ds[0].Value != fmt.Sprintf("%svalue%d", prefixString, i) {
				if !alreadyLogged {
					aelog.Infof(ctx, "ds[0].Value == %s", ds[0].Value)
					alreadyLogged = true
				}
				// do nothing
			} else {
				smRetry.Add(float64(nTry))
				smDuration.Add(time.Now().Sub(lastUpdated).Seconds() * 1000)
				break
			}

			time.Sleep(5 * time.Millisecond)

			if nTry > 400 {
				smRetry.Add(float64(nTry))
				smDuration.Add(time.Now().Sub(lastUpdated).Seconds() * 1000)
				aelog.Errorf(ctx, "nTry > 400. break.")
				break
			}
		}
	}

	fmt.Fprintln(w, "<html><pre>")

	fmt.Fprintln(w, "### Retry ###")
	smRetry.PrintSummary(w)

	fmt.Fprintln(w, "### Duration[ms] ###")
	smDuration.PrintSummary(w)

	fmt.Fprintln(w, "</pre></html>")
}

func countHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	q := datastore.NewQuery("testkind")
	var cnt int
	var err error
	if cnt, err = q.Count(ctx); err != nil {
		aelog.Errorf(ctx, "q.Count failed: %v", err)
		fmt.Fprintf(w, "q.Count failed: %v", err)
		return
	}
	fmt.Fprintln(w, "<html><pre>")

	fmt.Fprintln(w, "### Count ###")
	fmt.Fprintf(w, "%d\n", cnt)

	fmt.Fprintln(w, "</pre></html>")
}

func myCountHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	q := datastore.NewQuery("testkind").KeysOnly().Order("__key__").Limit(1000)
	cnt := 0
	for {
		var ks []*datastore.Key
		var err error
		aelog.Infof(ctx, "q.GetAll()")
		if ks, err = q.GetAll(ctx, nil); err != nil {
			aelog.Errorf(ctx, "q.GetAll failed: %v", err)
			fmt.Fprintf(w, "q.GetAll failed: %v", err)
			return
		}
		if len(ks) == 0 {
			break
		}
		aelog.Infof(ctx, "len(ks)=%d", len(ks))
		cnt += len(ks)
		lastKey := ks[len(ks)-1]
		q = datastore.NewQuery("testkind").KeysOnly().Filter("__key__ >", lastKey).Order("__key__").Limit(1000)
	}
	fmt.Fprintln(w, "<html><pre>")

	fmt.Fprintln(w, "### Count ###")
	fmt.Fprintf(w, "%d\n", cnt)

	fmt.Fprintln(w, "</pre></html>")
}

func statKindHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	q := datastore.NewQuery("__Stat_Kind__").Filter("kind_name =", "testkind")
	var stats []statKind
	if _, err := q.GetAll(ctx, &stats); err != nil {
		aelog.Errorf(ctx, "q.GetAll failed: %v", err)
		fmt.Fprintf(w, "q.GetAll failed: %v", err)
		return
	}
	if len(stats) == 0 {
		aelog.Errorf(ctx, "ERROR: len(stats) == 0")
		fmt.Fprintf(w, "ERROR: len(stats) == 0")
		return
	}
	fmt.Fprintln(w, "<html><pre>")

	fmt.Fprintln(w, "### Count ###")
	fmt.Fprintf(w, "%v\n", stats[0].Count)

	fmt.Fprintln(w, "</pre></html>")
}

func infoHandler(w http.ResponseWriter, r *http.Request) {

	fmt.Fprintln(w, "<html><pre>")

	fmt.Fprintln(w, "### rumtime ###")
	fmt.Fprintf(w, "runtime.GOARCH: %v\n", runtime.GOARCH)
	fmt.Fprintf(w, "runtime.GOOS: %v\n", runtime.GOOS)
	fmt.Fprintf(w, "runtime.NumCPU(): %v\n", runtime.NumCPU())

	fmt.Fprintln(w, "### os ###")
	fmt.Fprintf(w, "os.Environ(): %v\n", os.Environ())
	if dir, err := os.Getwd(); err == nil {
		fmt.Fprintf(w, "os.Getwd(): %v\n", dir)
	}

	fmt.Fprintln(w, "### os ###")
	ls(w, r.FormValue("path"))

	fmt.Fprintln(w, "</pre></html>")
}

func catHandler(w http.ResponseWriter, r *http.Request) {
	cat(w, r.FormValue("path"))
}

func ls(w http.ResponseWriter, path string) {
	if path == "" {
		path = "."
	}
	d, err := os.Open(path)
	if err != nil {
		fmt.Fprintln(w, err)
		return
	}
	defer d.Close()
	fi, err := d.Readdir(-1)
	if err != nil {
		fmt.Fprintln(w, err)
		return
	}
	for _, fi := range fi {
		fmt.Fprintf(w, "%s %8d %s %s\n", fi.Mode().String(), fi.Size(), fi.ModTime().Format("2006-01-02 15:04:05"), fi.Name())
	}
}

func cat(w http.ResponseWriter, path string) {
	if path == "" {
		ls(w, ".")
		return
	}
	finfo, err := os.Stat(path)
	if err != nil {
		fmt.Fprintln(w, err)
		return
	}
	if finfo.Mode().IsDir() {
		ls(w, path)
		return
	} else if !finfo.Mode().IsRegular() {
		return
	}

	fh, err := os.Open(path)
	if fh == nil {
		fmt.Fprintf(os.Stderr, "cat: can't open %s: error %s\n", path, err)
		return
	}

	_, err = io.Copy(w, fh)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cat: error %s\n", err)
		return
	}
}
