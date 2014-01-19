package main

import (
	"database/sql"
	"fmt"
	"os"
	"path"

	"github.com/jmhodges/levigo"
	_ "github.com/go-sql-driver/mysql"
)

type TestDB interface {
	CreateKey(key string, value []byte)
	Seek(key string, read bool)
	Close()
	Prune()
}

// FILES DB
type FilesDB struct {
	basePath string
	buf []byte
}

func NewFilesDB(basePath string) *FilesDB {
	err := os.MkdirAll(basePath, 0700)
	if err != nil {
		panic(err)
	}
	return &FilesDB {
		basePath: basePath,
		buf: make([]byte, 4096),
	}
}

func (f *FilesDB) keyToFile(key string) string {
	return fmt.Sprintf("%s/%c%c/%c%c/%s", *filesBasePath, key[0], key[1], key[2], key[3], key[4:])
}

// exists returns whether the given file or directory exists or not
func exists(path string) bool {
    _, err := os.Stat(path)
    if err == nil { return true }
    if os.IsNotExist(err) { return false }
    panic(err)
}

func (f *FilesDB) CreateKey(key string, value []byte) {
	filename := f.keyToFile(key)
	if !exists(path.Dir(filename)) {
		err := os.MkdirAll(path.Dir(filename), 0700)
		if err != nil {
			panic(err)
		}
	}
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	n, err := file.Write(value)
	if n != len(value) || err != nil {
		panic(err)
	}
}

func (f *FilesDB) Seek(key string, read bool) {
	file, err := os.Open(f.keyToFile(key))
	if err != nil {
		panic(err)
	}

	defer file.Close()

	for {
		n, err := file.Read(f.buf)
		if n == 0 {
			break
		}
		if err != nil {
			panic(err)
		}
	}
}

func (f *FilesDB) Close() {}
func (f *FilesDB) Prune() {}

// LEVELDB
type LevelDB struct {
	it *levigo.Iterator
	db *levigo.DB
	wo *levigo.WriteOptions
	wb *levigo.WriteBatch
	bsize int
}

func NewLevelDB(basePath string) *LevelDB {
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(50 * 1024 * 1024))
	opts.SetCreateIfMissing(true)
	opts.SetCompression(levigo.SnappyCompression)
	db, err := levigo.Open(basePath, opts)
	if err != nil {
		panic(err)
	}
	if db == nil {
		panic(db)
	}

	ro := levigo.NewReadOptions()
	fmt.Println("Making snapshot...")
	snapshot := db.NewSnapshot()
	fmt.Println("Done.")
	ro.SetSnapshot(snapshot)

	return &LevelDB{
		it: db.NewIterator(ro),
		db: db,
		wo: levigo.NewWriteOptions(),
		wb: levigo.NewWriteBatch(),
	}
}

func (l *LevelDB) Flush() {
	if err := l.db.Write(l.wo, l.wb); err != nil {
		panic(err)
	}
	l.wb.Close()
	l.wb = levigo.NewWriteBatch()
	l.bsize = 0
}

func (l *LevelDB) CreateKey(key string, value []byte) {
	l.wb.Put([]byte(key), value)
	l.bsize++
	if l.bsize > 4096 {
		l.Flush()
	}
}

func (l *LevelDB) Seek(key string, read bool) {
	l.it.Seek([]byte(key))
	if !l.it.Valid() {
		panic("iterator invalid")
	}
	if read {
		l.it.Value()
	}
}

func (l *LevelDB) Prune() {
	// Magic values per https://code.google.com/p/leveldb/source/browse/include/leveldb/db.h#131.
	keyspace := levigo.Range{
		Start: nil,
		Limit: nil,
	}

	l.db.CompactRange(keyspace)
}

func (l *LevelDB) Close() {
	l.Flush()
	l.it.Close()
	l.db.Close()
}

// MYSQL
type MysqlDB struct {
	conn *sql.DB
}

func NewMysqlDB(dsn string) *MysqlDB {
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}

	_, err = conn.Exec("TRUNCATE dbbench")
	if err != nil {
		panic(err)
	}

	return &MysqlDB{
		conn: conn,
	}
}

func (m *MysqlDB) CreateKey(key string, value []byte) {
	r, err := m.conn.Exec("INSERT INTO dbbench (rkey, val) VALUES (?, ?)", key, value)
	lii, liiErr := r.LastInsertId()
	rowsAffected, raErr := r.RowsAffected()
	fmt.Println("creating key...", rowsAffected, raErr, lii, liiErr)
	if err != nil {
		panic(err)
	}
}

func (m *MysqlDB) Seek(key string, read bool) {
	var err error
	if read {
		_, err = m.conn.Exec("SELECT rkey, val FROM dbbench WHERE rkey = ?", key)
	} else {
		_, err = m.conn.Exec("SELECT rkey FROM dbbench WHERE rkey = ?", key)
	}
	if err != nil {
		panic(err)
	}
}

func (m *MysqlDB) Close() {
	m.conn.Close()
}

func (m *MysqlDB) Prune() {
}
