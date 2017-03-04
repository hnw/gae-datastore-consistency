// How to deploy:
//   $ appcfg.py update . -A [application_id]

// +build appengine

package main

import (
	"fmt"
	"net/http"
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

	fmt.Fprintln(w, "<pre>")

	fmt.Fprintln(w, "### Retry ###")
	smRetry.PrintSummary(w)

	fmt.Fprintln(w, "### Duration[ms] ###")
	smDuration.PrintSummary(w)

	fmt.Fprintln(w, "</pre>")
}
