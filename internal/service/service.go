package service

import (
	websocket "answers-processor/internal/delivery"
	"answers-processor/internal/domain"
	"answers-processor/internal/infrastructure/message_broker"
	"answers-processor/internal/repository"
	"answers-processor/pkg/logger"
	"database/sql"
	"encoding/json"
	"strings"
	"time"
)

const customDateFormat = "2006-01-02T15:04:05"

func ProcessMessage(db *sql.DB, messageBroker *message_broker.MessageBrokerClient, wsServer *websocket.WebSocketServer, message domain.SMSMessage, logInstance *logger.Loggers) {
	if db == nil {
		logInstance.ErrorLogger.Error("Database instance is nil in ProcessMessage")
		return
	}
	logInstance.InfoLogger.Info("Processing message", "message", message)

	parsedDate, err := time.Parse(customDateFormat, message.Date)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to parse date", "date", message.Date, "error", err)
		return
	}

	clientID, err := repository.InsertClientIfNotExists(db, message.Source)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to insert or find client", "error", err)
		return
	}

	accountType, err := repository.GetAccountType(db, message.Destination)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to get account type", "error", err)
		return
	}

	switch accountType {
	case "quiz":
		processQuiz(db, clientID, message.Destination, message.Text, parsedDate, logInstance, wsServer)
	case "voting":
		processVoting(db, messageBroker, clientID, message, parsedDate, logInstance)
	case "shopping":
		processShopping(db, messageBroker, clientID, message, parsedDate, logInstance)
	default:
		logInstance.ErrorLogger.Error("Unknown account type", "account_type", accountType)
	}
}

func processQuiz(db *sql.DB, clientID int64, destination, text string, parsedDate time.Time, logInstance *logger.Loggers, wsServer *websocket.WebSocketServer) {
	logInstance.InfoLogger.Info("Processing quiz message", "destination", destination, "text", text)

	title, questions, questionIDs, err := repository.GetAccountAndQuestions(db, destination, parsedDate)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to find quiz and questions by short number and date", "error", err)
		return
	}

	logInstance.InfoLogger.Info("Quiz found", "title", title)
	for i, question := range questions {
		logInstance.InfoLogger.Info("Question found", "question", question)
		correctAnswers, err := repository.GetQuestionAnswers(db, questionIDs[i])
		if err != nil {
			logInstance.ErrorLogger.Error("Failed to get question answers", "error", err)
			continue
		}

		isCorrect := compareAnswers(correctAnswers, text)
		score := 0
		var serialNumber, serialNumberForCorrect int
		if isCorrect {
			score, err = repository.GetQuestionScore(db, questionIDs[i])
			if err != nil {
				logInstance.ErrorLogger.Error("Failed to get question score", "error", err)
				continue
			}

			serialNumber, err = repository.GetNextSerialNumber(db, questionIDs[i])
			if err != nil {
				logInstance.ErrorLogger.Error("Failed to get next serial number", "error", err)
				continue
			}

			serialNumberForCorrect, err = repository.GetNextSerialNumberForCorrect(db, questionIDs[i])
			if err != nil {
				logInstance.ErrorLogger.Error("Failed to get next serial number for correct answers", "error", err)
				continue
			}

			correctAnswerMessage := domain.CorrectAnswerMessage{
				Answer:                 text,
				Score:                  score,
				Date:                   parsedDate.Format(customDateFormat),
				SerialNumber:           serialNumber,
				SerialNumberForCorrect: serialNumberForCorrect,
			}
			message, _ := json.Marshal(correctAnswerMessage)
			wsServer.Broadcast(destination, message)
		}

		if repository.HasClientScored(db, questionIDs[i], clientID) {
			logInstance.InfoLogger.Info("Client has already answered with a score", "client_id", clientID, "question_id", questionIDs[i])
			continue
		}

		err = repository.InsertAnswer(db, questionIDs[i], text, parsedDate, clientID, score, serialNumber, serialNumberForCorrect)
		if err != nil {
			logInstance.ErrorLogger.Error("Failed to insert answer", "error", err)
			continue
		}
		logInstance.InfoLogger.Info("Answer inserted", "question_id", questionIDs[i], "is_correct", isCorrect, "score", score, "serial_number", serialNumber, "serial_number_for_correct", serialNumberForCorrect)
	}
}

func processVoting(db *sql.DB, messageBroker *message_broker.MessageBrokerClient, clientID int64, message domain.SMSMessage, parsedDate time.Time, logInstance *logger.Loggers) {
	votingID, err := repository.GetVotingByShortNumber(db, message.Destination, parsedDate)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to find voting by short number and date", "error", err)
		return
	}

	logInstance.InfoLogger.Info("Processing vote", "vote_code", message.Text)

	votingItemID, err := repository.GetVotingItemIDByVoteCode(db, votingID, message.Text)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to find voting item by vote code", "vote_code", message.Text, "error", err)
		return
	}

	voteLimit, err := repository.GetVotingLimit(db, votingID)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to get voting limit", "error", err)
		return
	}

	if repository.HasClientVoted(db, votingID, clientID, voteLimit, parsedDate) {
		logInstance.InfoLogger.Info("Client has already voted according to the limit", "client_id", clientID, "voting_id", votingID, "limit", voteLimit)
		return
	}

	err = repository.InsertVotingMessage(db, votingID, votingItemID, message.Text, parsedDate, clientID)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to insert voting message", "error", err)
		return
	}

	err = repository.UpdateVoteCount(db, votingItemID)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to update vote count", "error", err)
		return
	}

	votingItemTitle, err := repository.GetVotingItemTitle(db, votingItemID)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to get voting item title", "error", err)
		return
	}

	smsText := votingItemTitle + " ucin beren sesiniz kabul edildi"
	err = messageBroker.SendMessage(message.Destination, message.Source, smsText)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to send message notification", "error", err)
	} else {
		logInstance.InfoLogger.Info("Message notification sent successfully", "to", message.Source)
	}

	logInstance.InfoLogger.Info("Vote recorded", "voting_id", votingID, "voting_item_id", votingItemID, "client_id", clientID)
}

func processShopping(db *sql.DB, messageBroker *message_broker.MessageBrokerClient, clientID int64, message domain.SMSMessage, parsedDate time.Time, logInstance *logger.Loggers) {
	lotID, err := repository.GetLotByShortNumber(db, message.Destination, parsedDate)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to find lot by short number and date", "error", err)
		return
	}

	err = repository.InsertLotSMSMessage(db, lotID, message.Text, parsedDate, clientID)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to insert lot SMS message", "error", err)
		return
	}

	logInstance.InfoLogger.Info("Message recorded successfully", "lot_id", lotID, "client_id", clientID)

	// Send message notification
	err = messageBroker.SendMessage(message.Destination, message.Source, "Shopping vote is accepted via smpp.")
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to send message notification", "error", err)
	} else {
		logInstance.InfoLogger.Info("Message notification sent successfully", "to", message.Source)
	}
}

func compareAnswers(correctAnswers []string, userAnswer string) bool {
	userAnswer = sanitizeAnswer(userAnswer)
	for _, correctAnswer := range correctAnswers {
		if sanitizeAnswer(correctAnswer) == userAnswer {
			return true
		}
	}
	return false
}

func sanitizeAnswer(answer string) string {
	return strings.ToLower(strings.TrimSpace(answer))
}
