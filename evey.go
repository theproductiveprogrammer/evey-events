package main

import (
	"log"
	"net/http"
	"path"
	"regexp"
	"strings"
)

/*    understand/
 * main entry point into our program
 *
 *    way/
 * Create a context and handle /put and /get requests
 * from our clients.
 */
func main() {
	ctx := &context{
		port: "127.0.0.1:7749",
		db:   path.Join("..", "data"),
		logs: []msglog{},
	}

	with_ctx := func(fn reqHandler) httpHandler {
		return func(w http.ResponseWriter, r *http.Request) {
			fn(ctx, r, w)
		}
	}

	http.HandleFunc("/put/", with_ctx(put))
	http.HandleFunc("/get/", with_ctx(get))

	log.Println("Starting server on", ctx.port, "writing to", ctx.db)
	log.Fatal(http.ListenAndServe(ctx.port, nil))
}

/*    way/
 * Get the queue name from the URL and the message from the
 * body and save the message to the queue
 */
func put(ctx *context, r *http.Request, w http.ResponseWriter) {
	name := r.URL.Path[len("/put/"):]
	if isInvalidName(name) {
		w.WriteHeader(400)
		return
	}
}

func get(ctx *context, r *http.Request, w http.ResponseWriter) {
}

/*    understand/
 * A limitation we have currently is we simply save our
 * queue as `queue.log` filename to disk. For this reason
 * queues must have simple, valid, filenames
 */
func isInvalidName(n string) bool {
	n = strings.TrimSpace(n)
	if len(n) == 0 {
		return true
	}
	r, _ := regexp.MatchString(`[^-.A-Za-z0-9]`, n)
	return r
}

type msglog struct {
	name string
	msgs []byte
}

type context struct {
	port string
	db   string
	logs []msglog
}

type reqHandler func(*context, *http.Request, http.ResponseWriter)
type httpHandler func(http.ResponseWriter, *http.Request)
