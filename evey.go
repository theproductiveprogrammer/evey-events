package main

import (
	"log"
	"net/http"
	"path"
	"regexp"
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
  name := getQueueName(r)
  if len(name) == 0 {
		w.WriteHeader(400)
		return
	}
}


/*    way/
 * Trim the prefix from the path then return the 
 * name if valid.
 */
func getQueueName(r *http.Request) string {
  name := r.URL.Path
  for i := 1;i < len(name);i++  {
    if name[i] == '/' {
      name = name[i+1:]
      break
    }
  }
	if isInvalidName(name) {
    return ""
  } else {
    return name
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
