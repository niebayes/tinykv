package test_raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv"
	"log"
	"os"
	"strconv"
	"time"
)

// true to turn on debugging/logging.
const debug = tinykv.DEBUG
const LOGTOFILE = false

// what topic the log message is related to.
// logs are organized by topics which further consists of events.
type logTopic string

const (
	TEST logTopic = "TEST"
)

type Logger struct {
	logToFile      bool
	logFile        *os.File
	verbosityLevel int // logging verbosity is controlled over environment verbosity variable.
	startTime      time.Time
}

func makeLogger(logToFile bool, logFileName string) *Logger {
	logger := &Logger{}
	logger.init(LOGTOFILE, logFileName)
	return logger
}

func (logger *Logger) init(logToFile bool, logFileName string) {
	logger.logToFile = logToFile
	logger.verbosityLevel = getVerbosityLevel()
	logger.startTime = time.Now()

	// set log config.
	if logger.logToFile {
		logger.setLogFile(logFileName)
	}
	log.SetFlags(log.Flags() & ^(log.Ldate | log.Ltime)) // not show date and time.
}

func (logger *Logger) setLogFile(filename string) {
	// FIXME(bayes): What to do with this file if backed up?
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("failed to create file %v", filename)
	}
	log.SetOutput(f)
	logger.logFile = f
}

func (logger *Logger) printf(topic logTopic, format string, a ...interface{}) {
	// print iff debug is set.
	if debug {
		// time := time.Since(logger.startTime).Milliseconds()
		time := time.Since(logger.startTime).Microseconds()
		// e.g. 008256 VOTE ...
		prefix := fmt.Sprintf("%010d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func (logger *Logger) close() {
	if logger.logToFile {
		err := logger.logFile.Close()
		if err != nil {
			log.Fatal("failed to close log file")
		}
	}
}

// not delete this for backward compatibility.
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debug {
		log.Printf(format, a...)
	}
	return
}

// retrieve the verbosity level from an environment variable
// VERBOSE=0/1/2/3 <=>
func getVerbosityLevel() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

//
// partition events.
//

func (l *Logger) NewConf(s1, s2 []uint64) {
	l.printf(TEST, "s1:%v  s2:%v", s1, s2)
}

func (l *Logger) AddFilter() {
	l.printf(TEST, "ADD FILTER")
}

func (l *Logger) ClearFilter() {
	l.printf(TEST, "REM FILTER")
}

//
// persistence events
//

func (l *Logger) StopServer(id int) {
	l.printf(TEST, "N%v STOP", id)
}

func (l *Logger) RestartServer(id int) {
	l.printf(TEST, "N%v RESTART", id)
}
