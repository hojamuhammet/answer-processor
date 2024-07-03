package service

import (
	"answers-processor/internal/repository"
	"answers-processor/pkg/logger"
	"database/sql"
	"strings"
	"time"
)

func ProcessMessage(db *sql.DB, body []byte, logInstance *logger.Loggers) {
	message := string(body)
	parts := parseMessageParts(message)
	if len(parts) < 4 { // Ensure there are at least 4 parts: src, dst, txt, date
		logInstance.ErrorLogger.Error("Failed to parse message: input does not match format", "message", message)
		return
	}

	source := parts["src"]
	destination := parts["dst"]
	text := parts["txt"]
	date := parts["date"]

	parsedDate, err := time.Parse(time.RFC3339, date)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to parse date", "date", date, "error", err)
		return
	}

	clientID, err := repository.InsertClientIfNotExists(db, source)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to insert or find client", "error", err)
		return
	}

	title, questions, questionIDs, err := repository.GetAccountAndQuestions(db, destination, parsedDate)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to find quiz and questions by short number and date", "error", err)
		return
	}

	logInstance.InfoLogger.Info("Quiz found", "title", title)
	for i, question := range questions {
		logInstance.InfoLogger.Info("Question found", "question", question)
		correctAnswer, err := repository.GetQuestionAnswer(db, questionIDs[i])
		if err != nil {
			logInstance.ErrorLogger.Error("Failed to get question answer", "error", err)
			continue
		}

		isCorrect := compareAnswers(correctAnswer, text)
		score := 0
		if isCorrect {
			score, err = repository.GetQuestionScore(db, questionIDs[i])
			if err != nil {
				logInstance.ErrorLogger.Error("Failed to get question score", "error", err)
				continue
			}
		}

		err = repository.InsertAnswer(db, questionIDs[i], text, parsedDate, clientID, score, isCorrect)
		if err != nil {
			logInstance.ErrorLogger.Error("Failed to insert answer", "error", err)
			continue
		}
		logInstance.InfoLogger.Info("Answer inserted", "question_id", questionIDs[i], "is_correct", isCorrect, "score", score)
	}
}

func compareAnswers(correctAnswer, userAnswer string) bool {
	correctAnswer = strings.ToLower(strings.TrimSpace(correctAnswer))
	userAnswer = strings.ToLower(strings.TrimSpace(userAnswer))
	return correctAnswer == userAnswer
}

func parseMessageParts(message string) map[string]string {
	result := make(map[string]string)
	parts := strings.Split(message, ", ")
	for _, part := range parts {
		if strings.Contains(part, "=") {
			keyValue := strings.SplitN(part, "=", 2)
			if len(keyValue) == 2 {
				key := keyValue[0]
				value := keyValue[1]
				result[key] = value
			}
		}
	}
	return result
}
