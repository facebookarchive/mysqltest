// Package mysqltest provides standalone instances of mysql sutable for use in
// tests.
package mysqltest

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	// We're optionally provide a DB instance backed by this driver.
	_ "github.com/go-sql-driver/mysql"

	"github.com/ParsePlatform/go.freeport"
)

var configTemplate, configTemplateErr = template.New("config").Parse(`
[mysqld]
datadir                         = {{.DataDir}}
innodb-buffer-pool-size         = 5M
innodb-buffer-pool-stats        = 0
innodb-log-file-size            = 1M
innodb-read-io-threads          = 2
innodb_additional_mem_pool_size = 1M
innodb_mirrored_log_groups      = 1
key_buffer_size                 = 16K
max-binlog-size                 = 256K
max-delayed-threads             = 5
max_allowed_packet              = 256K
net_buffer_length               = 2K
port                            = {{.Port}}
socket                          = {{.Socket}}
sort_buffer_size                = 32K
sql_mode                        = ''
table_cache                     = 2
thread_cache_size               = 2
thread_stack                    = 128K
`)

func init() {
	if configTemplateErr != nil {
		panic(configTemplateErr)
	}
}

// Fatalf is satisfied by testing.T or testing.B.
type Fatalf interface {
	Fatalf(format string, args ...interface{})
}

// Server is a unique instance of a mysqld.
type Server struct {
	Port    int
	DataDir string
	Socket  string
	T       Fatalf
	cmd     *exec.Cmd
}

// Start the server, this will return once the server has been started.
func (s *Server) Start() {
	port, err := freeport.Get()
	if err != nil {
		s.T.Fatalf(err.Error())
	}
	s.Port = port

	dir, err := ioutil.TempDir("", "mysql-DataDir-")
	if err != nil {
		s.T.Fatalf(err.Error())
	}
	s.DataDir = dir
	s.Socket = filepath.Join(dir, "socket")

	cf, err := os.Create(filepath.Join(dir, "my.cnf"))
	if err != nil {
		s.T.Fatalf(err.Error())
	}

	if err := configTemplate.Execute(cf, s); err != nil {
		s.T.Fatalf(err.Error())
	}

	defaultsFile := fmt.Sprintf("--defaults-file=%s", cf.Name())
	s.cmd = exec.Command("mysql_install_db", defaultsFile)
	s.cmd.Stdout = os.Stdout
	s.cmd.Stderr = os.Stderr
	if err := s.cmd.Run(); err != nil {
		s.T.Fatalf(err.Error())
	}

	s.cmd = exec.Command("mysqld", defaultsFile)
	s.cmd.Stdout = os.Stdout
	s.cmd.Stderr = os.Stderr
	if err := s.cmd.Start(); err != nil {
		s.T.Fatalf(err.Error())
	}
}

// Stop the server, this will also remove all data.
func (s *Server) Stop() {
	s.cmd.Process.Kill()
	os.RemoveAll(s.DataDir)
}

// DSN for the mysql server, suitable for use with sql.Open. The suffix is in
// the form "dbname?param=value".
func (s *Server) DSN(suffix string) string {
	return fmt.Sprintf("root@tcp(localhost:%d)/%s", s.Port, suffix)
}

// DB for the server. The suffix is in the form "dbname?param=value".
func (s *Server) DB(suffix string) *sql.DB {
	for {
		db, err := sql.Open("mysql", s.DSN(suffix))
		if err != nil {
			s.T.Fatalf(err.Error())
		}

		// Note the comment /* ping */ is important.
		const pingQuery = "/* ping */ SELECT 1"
		_, err = db.Exec(pingQuery)
		if err == nil {
			return db
		}
		if !strings.HasSuffix(err.Error(), "connection refused") {
			s.T.Fatalf(err.Error())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// NewStartedServer creates a new server starts it.
func NewStartedServer(t Fatalf) *Server {
	s := &Server{T: t}
	s.Start()
	return s
}

// NewServerDB creates a new server, starts it, creates the named DB, and
// returns both.
func NewServerDB(t Fatalf, db string) (*Server, *sql.DB) {
	s := NewStartedServer(t)
	if _, err := s.DB("").Exec("create database " + db); err != nil {
		t.Fatalf(err.Error())
	}
	return s, s.DB(db)
}
