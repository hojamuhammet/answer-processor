package repository

import (
	"database/sql"
	"errors"
	"time"
)

func GetVotingItemTitle(db *sql.DB, votingItemID int64) (string, error) {
	var title string
	query := `
		SELECT title
		FROM voting_items
		WHERE id = ?
	`
	err := db.QueryRow(query, votingItemID).Scan(&title)
	if err != nil {
		return "", errors.New("voting item title not found")
	}
	return title, nil
}

func GetVotingByShortNumber(db *sql.DB, shortNumber string, currentDateTime time.Time) (int64, error) {
	var votingID int64
	query := `
		SELECT v.id
		FROM votings v
		JOIN accounts a ON v.account_id = a.id
		WHERE a.short_number = ? AND v.starts_at <= ? AND v.ends_at >= ?
	`
	err := db.QueryRow(query, shortNumber, currentDateTime, currentDateTime).Scan(&votingID)
	if err != nil {
		return 0, err
	}
	return votingID, nil
}

func GetVotingItemIDByVoteCode(db *sql.DB, votingID int64, voteCode string) (int64, error) {
	var votingItemID int64
	query := `
		SELECT id
		FROM voting_items
		WHERE voting_id = ? AND LOWER(TRIM(vote_code)) = LOWER(TRIM(?))
	`
	err := db.QueryRow(query, votingID, voteCode).Scan(&votingItemID)
	if err != nil {
		return 0, errors.New("voting item not found for vote code")
	}
	return votingItemID, nil
}

func HasClientVoted(db *sql.DB, votingID, clientID int64) bool {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM voting_sms_messages WHERE voting_id = ? AND client_id = ?", votingID, clientID).Scan(&count)
	if err != nil {
		loggers.ErrorLogger.Error("Failed to check if client has voted", "error", err)
		return false
	}
	return count > 0
}

func InsertVotingMessage(db *sql.DB, votingID, votingItemID int64, msg string, dt time.Time, clientID int64) error {
	_, err := db.Exec(
		"INSERT INTO voting_sms_messages (voting_id, voting_item_id, msg, dt, client_id) VALUES (?, ?, ?, ?, ?)",
		votingID, votingItemID, msg, dt, clientID,
	)
	return err
}

func UpdateVoteCount(db *sql.DB, votingItemID int64) error {
	_, err := db.Exec(
		"UPDATE voting_items SET votes_count = votes_count + 1 WHERE id = ?",
		votingItemID,
	)
	return err
}
