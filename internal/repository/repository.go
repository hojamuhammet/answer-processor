package repository

import "database/sql"

func GetAccountType(db *sql.DB, shortNumber string) (string, error) {
	var accountType string
	err := db.QueryRow("SELECT type FROM accounts WHERE short_number = ?", shortNumber).Scan(&accountType)
	if err != nil {
		return "", err
	}
	return accountType, nil
}
