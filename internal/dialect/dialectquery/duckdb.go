package dialectquery

import "fmt"

type DuckDB struct{}

var _ Querier = (*DuckDB)(nil)

func (s *DuckDB) CreateTable(tableName string) string {
	q := `CREATE SEQUENCE seq_id START 1;
		CREATE TABLE %s (
		id INTEGER PRIMARY KEY DEFAULT nextval('seq_id'),
		version_id INTEGER NOT NULL,
		is_applied INTEGER NOT NULL,
		tstamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
	)`
	return fmt.Sprintf(q, tableName)
}

func (s *DuckDB) InsertVersion(tableName string) string {
	q := `INSERT INTO %s (version_id, is_applied) VALUES (?, ?)`
	return fmt.Sprintf(q, tableName)
}

func (s *DuckDB) DeleteVersion(tableName string) string {
	q := `DELETE FROM %s WHERE version_id=?`
	return fmt.Sprintf(q, tableName)
}

func (s *DuckDB) GetMigrationByVersion(tableName string) string {
	q := `SELECT tstamp, is_applied FROM %s WHERE version_id=? ORDER BY tstamp DESC LIMIT 1`
	return fmt.Sprintf(q, tableName)
}

func (s *DuckDB) ListMigrations(tableName string) string {
	q := `SELECT version_id, is_applied from %s ORDER BY id DESC`
	return fmt.Sprintf(q, tableName)
}

func (s *DuckDB) GetLatestVersion(tableName string) string {
	q := `SELECT MAX(version_id) FROM %s`
	return fmt.Sprintf(q, tableName)
}
