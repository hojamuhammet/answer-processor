package repository

import (
	"database/sql"
	"errors"
	"time"
)

func GetVotingDetails(db *sql.DB, shortNumber string, currentDateTime time.Time) (int64, string, error) {
	var votingID int64
	var status string
	query := `
        SELECT v.id, v.status
        FROM votings v
        JOIN accounts a ON v.account_id = a.id
        WHERE a.short_number = ? AND v.starts_at <= ? AND v.ends_at >= ?
    `
	err := db.QueryRow(query, shortNumber, currentDateTime, currentDateTime).Scan(&votingID, &status)
	if err != nil {
		return 0, "", err
	}
	return votingID, status, nil
}

func GetVotingItemDetails(db *sql.DB, votingID int64, voteCode string) (int64, string, error) {
	var votingItemID int64
	var title string
	query := `SELECT id, title FROM voting_items WHERE voting_id = ? AND LOWER(TRIM(vote_code)) = LOWER(TRIM(?))`
	err := db.QueryRow(query, votingID, voteCode).Scan(&votingItemID, &title)
	if err != nil {
		return 0, "", errors.New("voting item not found for vote code")
	}
	return votingItemID, title, nil
}

func HasClientVoted(db *sql.DB, votingID, clientID int64, status string, currentDateTime time.Time) (bool, error) {
	var count int
	var err error

	switch status {
	case "daily":
		startOfDay := time.Date(currentDateTime.Year(), currentDateTime.Month(), currentDateTime.Day(), 0, 0, 0, 0, currentDateTime.Location())
		endOfDay := startOfDay.Add(24 * time.Hour)
		err = db.QueryRow(
			"SELECT COUNT(*) FROM voting_sms_messages WHERE voting_id = ? AND client_id = ? AND dt >= ? AND dt < ?",
			votingID, clientID, startOfDay, endOfDay,
		).Scan(&count)
	case "one":
		err = db.QueryRow(
			"SELECT COUNT(*) FROM voting_sms_messages WHERE voting_id = ? AND client_id = ?",
			votingID, clientID,
		).Scan(&count)
	case "unlimited":
		return false, nil
	}

	if err != nil {
		loggers.ErrorLogger.Error("Failed to check if client has voted", "error", err)
		return false, err
	}
	return count > 0, nil
}

func InsertVotingMessageAndUpdateCount(db *sql.DB, votingID, votingItemID int64, msg string, dt time.Time, clientID int64) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		"INSERT INTO voting_sms_messages (voting_id, voting_item_id, msg, dt, client_id) VALUES (?, ?, ?, ?, ?)",
		votingID, votingItemID, msg, dt, clientID,
	)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(
		"UPDATE voting_items SET votes_count = votes_count + 1 WHERE id = ?",
		votingItemID,
	)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}
