package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"regexp"
	"strconv"
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
		logs: []*msglog{},
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
		err_("put: Invalid/Missing queue name", 400, w)
		return
	}
	hlen := r.Header["Content-Length"]
	if len(hlen) == 0 {
		err_("put: No content-length found", 400, w)
		return
	}
	len_, err := strconv.ParseUint(hlen[0], 10, 32)
	if err != nil {
		err_("put: Invalid content-length", 400, w)
		return
	}
	if len_ > 1024 {
		err_("put: Message content too big", 400, w)
		return
	}
	num, err := save(name, uint32(len_), r.Body, ctx)
	if err != nil {
		err_(err.Error(), 500, w)
		return
	}
	fmt.Fprintln(w, num)
}

/*    way/
 * Find the appropriate queue (create if doesn't exist)
 * and save the data to it
 */
func save(name string, len_ uint32, inp io.ReadCloser, ctx *context) (int, error) {
	msglog := findLog(name, ctx)
	if msglog == nil {
		msglog = createLog(name, ctx)
		if msglog == nil {
			return 0, errors.New("save: failed to create log")
		}
	}
	offset, err := saveMsg(len_, inp, msglog)
	if err != nil {
		return 0, err
	}
	msglog.msgs = append(msglog.msgs, offset)
	return len(msglog.msgs), nil
}

func createLog(name string, ctx *context) *msglog {
	logfile := path.Join(ctx.db, name+".log")
	f, err := os.OpenFile(logfile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Println("createLog:", err)
		return nil
	}
	_, err = f.Write(DBHEADER)
	if err != nil {
		log.Println("createLog:", err)
		return nil
	}
	msglog := &msglog{
		name: name,
		f:    f,
		msgs: []int64{},
	}
	ctx.logs = append(ctx.logs, msglog)
	return msglog
}

func saveMsg(len_ uint32, inp io.ReadCloser, log_ *msglog) (int64, error) {
	inf, err := log_.f.Stat()
	if err != nil {
		return 0, err
	}
	_, err = log_.f.Write(RECHEADER)
	if err != nil {
		return 0, err
	}
	err = binary.Write(log_.f, binary.LittleEndian, len_)
	if err != nil {
		return 0, err
	}
	_, err = log_.f.Write([]byte{'\n'})
	if err != nil {
		return 0, err
	}
	buf := make([]byte, 1024)
	n, err := inp.Read(buf)
	for n > 0 || err == nil {
		if n > 0 {
			_, err := log_.f.Write(buf[:n])
			if err != nil {
				return 0, err
			}
		}
		if err != nil {
			break
		}
		n, err = inp.Read(buf)
	}
	if err == io.EOF {
		return inf.Size(), nil
	}
	return 0, err
}

func findLog(name string, ctx *context) *msglog {
	for _, log := range ctx.logs {
		if strings.ToLower(log.name) == strings.ToLower(name) {
			return log
		}
	}
	return nil
}

/*    way/
 * Trim the prefix from the path then return the
 * name if valid.
 */
func getQueueName(r *http.Request) string {
	name := r.URL.Path
	for i := 1; i < len(name); i++ {
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

func err_(error string, code int, w http.ResponseWriter) {
	log.Println(error)
	http.Error(w, error, code)
}

type msglog struct {
	name string
	f    *os.File
	msgs []int64
}

type context struct {
	port string
	db   string
	logs []*msglog
}

type reqHandler func(*context, *http.Request, http.ResponseWriter)
type httpHandler func(http.ResponseWriter, *http.Request)

var DBHEADER = []byte("EE|v1|")
var RECHEADER = []byte("\n|EE|")
