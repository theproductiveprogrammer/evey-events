package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
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
 * Initialize the state, load existing logs, set up the client
 * request handlers and start our server.
 */
func main() {
  state := initState()
	loadLogs(state)
  setupRequestHandlers(state)

	log.Println("Starting server on", state.addr, "writing to", state.db)
	log.Fatal(http.ListenAndServe(state.addr, nil))
}

func initState() *state {
	return &state {
		addr: "127.0.0.1:7749",
		db:   path.Join("..", "evey-data"),
		logs: []*msglog{},
	}
}

func setupRequestHandlers(state *state) {
	with_state := func(fn reqHandler) httpHandler {
		return func(w http.ResponseWriter, r *http.Request) {
			fn(state, r, w)
		}
	}

	http.HandleFunc("/put/", with_state(put))
	http.HandleFunc("/get/", with_state(get))

}

/*    way/
 * Get the queue name from the URL and the message from the
 * body and save the message to the queue
 */
func put(state *state, r *http.Request, w http.ResponseWriter) {
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
	num, err := save(name, uint32(len_), r.Body, state)
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
func save(name string, len_ uint32, inp io.ReadCloser, state *state) (int, error) {
	mlg := findLog(name, state)
	if mlg == nil {
		mlg = createLog(name, state)
		if mlg == nil {
			return 0, errors.New("save: failed to create log")
		}
	}
	offset, err := saveMsg(len_, inp, mlg)
	if err != nil {
		return 0, err
	}
  recoffset := offset + RECHEADERSZ
  mlg.msgs = append(mlg.msgs, recptr {
    off: recoffset,
    sz: len_,
  })
	return len(mlg.msgs), nil
}

func loadLogs(state *state) {
	files, err := ioutil.ReadDir(state.db)
	if err != nil {
    log.Panic("Failed to read:", state.db)
	}
	for _, f := range files {
		loadLog(f, state)
	}
}

/*    way/
 * If this looks like a log file, we read in the
 * header, then walk the records checking that
 * each starts with a valid header and keeping
 * track of the offsets and sizes
 */
func loadLog(inf os.FileInfo, state *state) {
	name := inf.Name()
	if !strings.HasSuffix(name, ".log") {
		return
	}
	logfile := path.Join(state.db, name)
	f, err := os.OpenFile(logfile, os.O_RDWR, 0644)
	if err != nil {
		log.Panic("loadLog:Failed to open:", f.Name, err)
	}
	hdr := make([]byte, len(DBHEADER))
	_, err = io.ReadFull(f, hdr)
	if err != nil {
		log.Panic("loadLog:Failed to read:", f.Name, err)
	}
	if bytes.Compare(DBHEADER, hdr) != 0 {
		log.Panic("loadLog:Invalid DB header:", f.Name)
	}
	var msgs []recptr
	sz := inf.Size()
	offset := int64(len(DBHEADER))
	for offset < sz {
		reclen, err := getRecLen(offset, f)
		if err != nil {
			log.Panic("loadLog:", err.Error(), " at offset:", offset, " for file:", name)
		}
    recoffset := offset + RECHEADERSZ
    msgs = append(msgs, recptr {
      off: recoffset,
      sz: reclen,
    })
		offset = recoffset + int64(reclen)
	}
	name = name[:len(name)-len(".log")]
	state.logs = append(state.logs, &msglog{
		name: name,
		f:    f,
		msgs: msgs,
	})
}

func getRecLen(offset int64, f *os.File) (uint32, error) {
	pfxsz := len(RECHEADER)
	hdr := make([]byte, RECHEADERSZ)

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return 0, errors.New("Seek Failed")
	}
	if _, err := io.ReadFull(f, hdr); err != nil {
		return 0, errors.New("Read Failed")
	}
	if bytes.Compare(RECHEADER, hdr[:len(RECHEADER)]) != 0 {
		return 0, errors.New("Invalid Rec Header")
	}
	if hdr[len(hdr)-1] != '\n' {
		return 0, errors.New("Invalid header '\n'")
	}
	b_ := bytes.NewReader(hdr[pfxsz : pfxsz+4])
	var v uint32
	if err := binary.Read(b_, binary.LittleEndian, &v); err != nil {
		return 0, errors.New("Failed reading Rec size")
	}
	return v, nil
}

func createLog(name string, state *state) *msglog {
	logfile := path.Join(state.db, name+".log")
	f, err := os.OpenFile(logfile, os.O_CREATE|os.O_RDWR, 0644)
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
		msgs: nil,
	}
	state.logs = append(state.logs, msglog)
	return msglog
}

func saveMsg(len_ uint32, inp io.ReadCloser, mlg *msglog) (int64, error) {
	if _, err := mlg.f.Seek(0, io.SeekEnd); err != nil {
		return 0, err
	}
	inf, err := mlg.f.Stat()
	if err != nil {
		return 0, err
	}
	if _, err := mlg.f.Write(RECHEADER); err != nil {
		return 0, err
	}
	if err := binary.Write(mlg.f, binary.LittleEndian, len_); err != nil {
		return 0, err
	}
	if _, err := mlg.f.Write([]byte{'\n'}); err != nil {
		return 0, err
	}
	buf := make([]byte, 1024)
	n, err := inp.Read(buf)
	for n > 0 || err == nil {
		if n > 0 {
			if _, err := mlg.f.Write(buf[:n]); err != nil {
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

func findLog(name string, state *state) *msglog {
	for _, log := range state.logs {
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

func get(state *state, r *http.Request, w http.ResponseWriter) {
	name := getQueueName(r)
	if len(name) == 0 {
		err_("get: Invalid/Missing queue name", 400, w)
		return
	}
	mlg := findLog(name, state)
	if mlg == nil {
		err_("get: No log found:"+name, 404, w)
		return
	}
	qv := r.URL.Query()["n"]
	if qv == nil || len(qv) == 0 {
		err_("get: Missing msg number", 400, w)
		return
	}
	n, err := strconv.ParseUint(qv[0], 10, 32)
	if err != nil || n < 1 {
		err_("get: Invalid msg number", 400, w)
		return
	}
  n -= 1
	if int(n) >= len(mlg.msgs) {
    w.WriteHeader(204)
    return
  }
  sendLog(mlg, uint32(n), w)
}

func sendLog(mlg *msglog, n uint32, w http.ResponseWriter) {
	recptr := mlg.msgs[n]
	rec := make([]byte, recptr.sz)
	if n, _ := mlg.f.ReadAt(rec, recptr.off); n < len(rec) {
		err_("Failed reading record", 500, w)
		return
	}
	w.Write(rec)
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
	msgs []recptr
}

type state struct {
	addr string
	db   string
	logs []*msglog
}

type recptr struct {
  off int64
  sz uint32
}

type reqHandler func(*state, *http.Request, http.ResponseWriter)
type httpHandler func(http.ResponseWriter, *http.Request)

var DBHEADER = []byte("EE|v1|")
var RECHEADER = []byte("\n|EE|")
var RECHEADERSZ = int64(len(RECHEADER) + 4 + 1) /* 4: uint32 len + 1: '\n' */
