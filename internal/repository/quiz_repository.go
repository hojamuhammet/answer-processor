package repository

import (
	"answers-processor/pkg/logger"
	"database/sql"
	"errors"
	"strings"
	"time"
)

var loggers *logger.Loggers

func Init(logInstance *logger.Loggers) {
	loggers = logInstance
}

func InsertClientIfNotExists(db *sql.DB, phoneNumber string) (int64, error) {
	var id int64
	err := db.QueryRow("SELECT id FROM clients WHERE phone = ?", phoneNumber).Scan(&id)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	if id == 0 {
		result, err := db.Exec("INSERT INTO clients (phone, created_at, updated_at) VALUES (?, NOW(), NOW())", phoneNumber)
		if err != nil {
			return 0, err
		}
		id, err = result.LastInsertId()
		if err != nil {
			return 0, err
		}
		loggers.InfoLogger.Info("Inserted new client", "phone", phoneNumber)
	} else {
		loggers.InfoLogger.Info("Client already exists", "phone", phoneNumber)
	}
	return id, nil
}

func GetAccountAndQuestions(db *sql.DB, shortNumber string, currentDateTime time.Time) (string, []string, []int64, error) {
	query := `
		SELECT z.title, q.question, q.id
		FROM questions q
		LEFT JOIN quizzes z ON q.quiz_id = z.id
		LEFT JOIN accounts a ON z.account_id = a.id
		WHERE a.short_number = ? AND q.starts_at <= ? AND q.ends_at >= ?
	`

	rows, err := db.Query(query, shortNumber, currentDateTime, currentDateTime)
	if err != nil {
		return "", nil, nil, err
	}
	defer rows.Close()

	var title string
	var questions []string
	var questionIDs []int64
	for rows.Next() {
		var question string
		var questionID int64
		if err := rows.Scan(&title, &question, &questionID); err != nil {
			return "", nil, nil, err
		}
		questions = append(questions, question)
		questionIDs = append(questionIDs, questionID)
	}
	if err := rows.Err(); err != nil {
		return "", nil, nil, err
	}

	if len(questions) == 0 {
		loggers.ErrorLogger.Error("no questions found for short number", "short_number", shortNumber)
		return "", nil, nil, errors.New("no questions found")
	}

	return title, questions, questionIDs, nil
}

func GetQuestionAnswers(db *sql.DB, questionID int64) ([]string, error) {
	var answers string
	err := db.QueryRow("SELECT answer FROM questions WHERE id = ?", questionID).Scan(&answers)
	if err != nil {
		return nil, err
	}
	answerList := strings.Split(answers, ",")
	for i := range answerList {
		answerList[i] = sanitizeAnswer(answerList[i])
	}
	return answerList, nil
}

func sanitizeAnswer(answer string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(answer), " ", ""))
}

func GetQuestionScore(db *sql.DB, questionID int64) (int, error) {
	var score int
	err := db.QueryRow("SELECT score FROM questions WHERE id = ?", questionID).Scan(&score)
	if err != nil {
		return 0, err
	}
	return score, nil
}

func HasClientScored(db *sql.DB, questionID, clientID int64) bool {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM answers WHERE question_id = ? AND client_id = ? AND score > 0", questionID, clientID).Scan(&count)
	if err != nil {
		loggers.ErrorLogger.Error("Failed to check if client has scored", "error", err)
		return false
	}
	return count > 0
}

func GetNextSerialNumber(db *sql.DB, questionID int64) (int, error) {
	var serialNumber int
	err := db.QueryRow("SELECT IFNULL(MAX(serial_number), 0) + 1 FROM answers WHERE question_id = ?", questionID).Scan(&serialNumber)
	if err != nil {
		return 0, err
	}
	return serialNumber, nil
}

func GetNextSerialNumberForCorrect(db *sql.DB, questionID int64) (int, error) {
	var serialNumberForCorrect int
	err := db.QueryRow("SELECT IFNULL(MAX(serial_number_for_correct), 0) + 1 FROM answers WHERE question_id = ? AND score > 0", questionID).Scan(&serialNumberForCorrect)
	if err != nil {
		return 0, err
	}
	return serialNumberForCorrect, nil
}

func InsertAnswer(db *sql.DB, questionID int64, msg string, dt time.Time, clientID int64, score int, serialNumber int, serialNumberForCorrect int) error {
	_, err := db.Exec(
		"INSERT INTO answers (question_id, msg, dt, client_id, score, quiz_id, serial_number, serial_number_for_correct) VALUES (?, ?, ?, ?, ?, (SELECT quiz_id FROM questions WHERE id = ?), ?, ?)",
		questionID, msg, dt, clientID, score, questionID, serialNumber, serialNumberForCorrect,
	)
	return err
}
