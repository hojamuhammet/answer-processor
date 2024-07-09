package repository

import (
	"database/sql"
	"strings"
	"time"
)

func GetLotByShortNumber(db *sql.DB, shortNumber string, currentDateTime time.Time) (int64, error) {
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

func InsertLotSMSMessage(db *sql.DB, lotID int64, msg string, dt time.Time, clientID int64) error {
	_, err := db.Exec(
		"INSERT INTO lot_sms_messages (lot_id, msg, dt, client_id) VALUES (?, ?, ?, ?)",
		lotID, msg, dt, clientID,
	)
	return err
}

func IsValidUniqueCode(db *sql.DB, lotID int64, uniqueCode string) bool {
	var storedUniqueCode string
	err := db.QueryRow("SELECT unique_code FROM lots WHERE id = ?", lotID).Scan(&storedUniqueCode)
	if err != nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(storedUniqueCode), strings.TrimSpace(uniqueCode))
}
