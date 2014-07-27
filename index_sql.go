package blkidx

import (
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"strings"
)

type sqlIndex struct {
	db *sql.DB

	insertStmt          *sql.Stmt
	updateStmt          *sql.Stmt
	lookupStmt          *sql.Stmt
	findEqualHashesStmt *sql.Stmt
	allNamesStmt        *sql.Stmt
	removeStmt          *sql.Stmt
	countStmt           *sql.Stmt
}

var _ Index = (*sqlIndex)(nil)

func NewSqlIndex(db *sql.DB) (Index, error) {
	var err error

	if err = initOrUpgradeDb(db); err != nil {
		return nil, err
	}

	idx := &sqlIndex{db: db}

	idx.insertStmt, err = db.Prepare(sqlIndex_insert)
	if err != nil {
		return nil, err
	}
	idx.updateStmt, err = db.Prepare(sqlIndex_update)
	if err != nil {
		return nil, err
	}
	idx.lookupStmt, err = db.Prepare(sqlIndex_lookup)
	if err != nil {
		return nil, err
	}
	idx.findEqualHashesStmt, err = db.Prepare(sqlIndex_findEqualHashes)
	if err != nil {
		return nil, err
	}
	idx.allNamesStmt, err = db.Prepare(sqlIndex_allNames)
	if err != nil {
		return nil, err
	}
	idx.removeStmt, err = db.Prepare(sqlIndex_remove)
	if err != nil {
		return nil, err
	}
	idx.countStmt, err = db.Prepare(sqlIndex_count)
	if err != nil {
		return nil, err
	}

	return idx, nil
}

func (s *sqlIndex) Store(blob *Blob) error {
	if err := blob.Validate(); err != nil {
		return err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	var res sql.Result
	var sqlErr error
	var action string
	if blob.Version == 0 {
		action = "insert"
		res, sqlErr = tx.Stmt(s.insertStmt).Exec(blob.Name, blob.Version, blob.IndexTime,
			blob.Size, blob.ModTime, blob.HashAlgorithm,
			sqlSB(blob.Hash), blob.HashBlockSize, sqlSSB(blob.HashedBlocks))

	} else {
		action = "update"
		res, sqlErr = tx.Stmt(s.updateStmt).Exec(blob.IndexTime,
			blob.Size, blob.ModTime, blob.HashAlgorithm,
			sqlSB(blob.Hash), blob.HashBlockSize, sqlSSB(blob.HashedBlocks),
			blob.Name, blob.Version-1)
	}
	if sqlErr != nil {
		tx.Rollback()
		return fmt.Errorf("%s got error %v", action, sqlErr)
	}
	if x, _ := res.RowsAffected(); x != 1 {
		tx.Rollback()
		return &OptimisticLockingError{
			Name:          blob.Name,
			FailedVersion: blob.Version,
		}
	}
	return tx.Commit()
}

func (s *sqlIndex) LookupByName(name string) (*Blob, error) {
	b := new(Blob)
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	row := tx.Stmt(s.lookupStmt).QueryRow(name)
	var hash sqlSB
	var hashBlocks sqlSSB
	err = row.Scan(&b.Name, &b.Version, &b.IndexTime,
		&b.Size, &b.ModTime, &b.HashAlgorithm,
		&hash, &b.HashBlockSize, &hashBlocks)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	b.IndexTime = b.IndexTime.UTC()
	b.ModTime = b.ModTime.UTC()
	b.Hash = []byte(hash)
	b.HashedBlocks = [][]byte(hashBlocks)
	return b, nil
}

func (s *sqlIndex) FindEqualHashes() (rv []Names, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.Stmt(s.findEqualHashesStmt).Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var currentHash string
	var equal Names
	for rows.Next() {
		var h, n string
		err = rows.Scan(&h, &n)
		if err != nil {
			return nil, err
		}
		if currentHash == "" || currentHash == h {
			equal = append(equal, n)
		} else {
			rv = append(rv, equal)
			equal = append(Names(nil), n) // new equal slice
		}
		currentHash = h
	}
	if len(equal) > 0 {
		rv = append(rv, equal)
	}
	return
}

func (s *sqlIndex) AllNames() (rv Names, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.Stmt(s.allNamesStmt).Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var n string
		err = rows.Scan(&n)
		if err != nil {
			return nil, err
		}
		rv = append(rv, n)
	}
	return
}

func (s *sqlIndex) Remove(names Names) (err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	stmt := tx.Stmt(s.removeStmt)
	for _, name := range names {
		_, err = stmt.Exec(name)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	tx.Commit()
	return nil
}

func (s *sqlIndex) Count() (int, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var count int
	row := tx.Stmt(s.countStmt).QueryRow()
	err = row.Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

const (
	sqlIndex_version = 1

	sqlIndex_init = `
	CREATE TABLE IF NOT EXISTS t_blobs (
		name               TEXT     NOT NULL PRIMARY KEY,
		version            INTEGER  NOT NULL,
		index_time         DATETIME NOT NULL,
		size               INTEGER  NOT NULL,
		mod_time           DATETIME NOT NULL,
		hash_algorithm     INTEGER  NOT NULL,
		hash               TEXT     NOT NULL,
		hash_block_size    INTEGER  NOT NULL,
		hashed_blocks      TEXT     NOT NULL
	)`

	sqlIndex_fields = `
	name, version, index_time,
	size, mod_time, hash_algorithm,
	hash, hash_block_size, hashed_blocks`

	sqlIndex_insert = `INSERT INTO t_blobs (` + sqlIndex_fields + `) values (?,?,?,?,?,?,?,?,?)`

	sqlIndex_update = `UPDATE t_blobs SET
		index_time      = ?,
		size            = ?,
		mod_time        = ?,
		hash_algorithm  = ?,
		hash            = ?,
		hash_block_size = ?,
		hashed_blocks   = ?
		WHERE
		name = ? AND version = ?`

	sqlIndex_lookup = `SELECT ` + sqlIndex_fields + ` FROM t_blobs WHERE name=?`

	sqlIndex_findEqualHashes = `
	SELECT hash, name
	FROM t_blobs
	WHERE hash IN (
		SELECT hash
		FROM t_blobs
		GROUP BY hash HAVING COUNT(*) > 1
	)
	ORDER BY hash`

	sqlIndex_allNames = `SELECT name FROM t_blobs`

	sqlIndex_remove = `DELETE FROM t_blobs WHERE name = ?`

	sqlIndex_count = `SELECT COUNT(*) FROM t_blobs`
)

func initOrUpgradeDb(db *sql.DB) error {
	_, err := db.Exec(sqlIndex_init)
	if err != nil {
		return err
	}
	return nil
}

type sqlSB []byte

type sqlSSB [][]byte

var _ sql.Scanner = (*sqlSB)(nil)
var _ driver.Value = (*sqlSB)(nil)
var _ sql.Scanner = (*sqlSSB)(nil)
var _ driver.Value = (*sqlSSB)(nil)

func (b sqlSB) Value() (driver.Value, error) {
	return base64.StdEncoding.EncodeToString([]byte(b)), nil
}
func (b *sqlSB) Scan(value interface{}) error {
	var err error
	var v []byte = value.([]byte)
	*b, err = decodeSlice(v)
	return err
}
func (b sqlSSB) Value() (driver.Value, error) {
	var s []string
	for _, sb := range b {
		s = append(s, base64.StdEncoding.EncodeToString([]byte(sb)))
	}
	return strings.Join(s, ","), nil
}
func (b *sqlSSB) Scan(value interface{}) error {
	xs := strings.Split(string(value.([]byte)), ",")
	for _, x := range xs {
		y, err := decodeSlice([]byte(x))
		if err != nil {
			return err
		}
		*b = append(*b, y)
	}
	return nil
}

func decodeSlice(b64 []byte) ([]byte, error) {
	dst := make([]byte, base64.StdEncoding.DecodedLen(len(b64)))
	n, err := base64.StdEncoding.Decode(dst, b64)
	return dst[:n], err
}
