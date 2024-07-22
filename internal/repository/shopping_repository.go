package repository

import (
	"database/sql"
	"time"
)

func GetLotDetailsByShortNumber(db *sql.DB, shortNumber string, currentDateTime time.Time) (int64, error) {
	var lotID int64
	query := `
        SELECT l.id
        FROM lots l
        JOIN accounts a ON l.account_id = a.id
        WHERE a.short_number = ? AND l.starts_at <= ? AND l.ends_at >= ?
    `
	err := db.QueryRow(query, shortNumber, currentDateTime, currentDateTime).Scan(&lotID)
	if err != nil {
		return 0, err
	}
	return lotID, nil
}

func InsertLotMessageAndUpdate(db *sql.DB, lotID int64, msg string, dt time.Time, clientID int64) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		"INSERT INTO lot_sms_messages (lot_id, msg, dt, client_id) VALUES (?, ?, ?, ?)",
		lotID, msg, dt, clientID,
	)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}
