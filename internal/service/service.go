package service

import (
	websocket "answers-processor/internal/delivery"
	"answers-processor/internal/domain"
	"answers-processor/internal/infrastructure/message_broker"
	"answers-processor/internal/repository"
	"answers-processor/pkg/logger"
	"answers-processor/pkg/utils"
	"database/sql"
	"encoding/json"
	"strings"
	"time"
)

const customDateFormat = "2006-01-02T15:04:05"

func ProcessMessage(db *sql.DB, messageBroker *message_broker.MessageBrokerClient, quizWSServer, votingWSServer, shoppingWSServer *websocket.WebSocketServer, message domain.SMSMessage, logInstance *logger.Loggers) {
	if db == nil {
		logInstance.ErrorLogger.Error("Database instance is nil in ProcessMessage")
		return
	}

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
		logInstance.ErrorLogger.Error("Failed to get account type", "number", message.Destination)
		return
	}

	switch accountType {
	case "quiz":
		processQuiz(db, clientID, message, parsedDate, logInstance, quizWSServer)
	case "voting":
		processVoting(db, messageBroker, votingWSServer, clientID, message, parsedDate, logInstance)
	case "shopping":
		processShopping(db, messageBroker, shoppingWSServer, clientID, message, parsedDate, logInstance)
	default:
		logInstance.ErrorLogger.Error("Unknown account type", "account_type", accountType)
	}
}

func processQuiz(db *sql.DB, clientID int64, message domain.SMSMessage, parsedDate time.Time, logInstance *logger.Loggers, wsServer *websocket.WebSocketServer) {
	destination := message.Destination
	text := message.Text

	_, questions, questionIDs, quizID, err := repository.GetAccountAndQuestions(db, destination, parsedDate)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to find quiz and questions", "error", err)
		return
	}

	logInstance.InfoLogger.Info("Quiz found", "quiz_id", quizID, "questions_count", len(questions))

	// Batch check if the client has already scored for these questions
	scoredMap, err := repository.HasClientScoredBatch(db, questionIDs, clientID)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to batch check if client has scored", "error", err)
		return
	}

	for i := range questions {
		questionID := questionIDs[i]
		correctAnswers, err := repository.GetQuestionAnswers(db, questionID)
		if err != nil {
			logInstance.ErrorLogger.Error("Failed to get question answers", "error", err)
			continue
		}

		isCorrect := compareAnswers(correctAnswers, text)
		if isCorrect {
			// Check if the client has already answered correctly
			if scoredMap[questionID] {
				continue
			}

			score, err := repository.GetQuestionScore(db, questionID)
			if err != nil {
				logInstance.ErrorLogger.Error("Failed to get question score", "error", err)
				continue
			}

			serialNumber, err := repository.GetNextSerialNumber(db, questionID)
			if err != nil {
				logInstance.ErrorLogger.Error("Failed to get next serial number", "error", err)
				continue
			}

			serialNumberForCorrect, err := repository.GetNextSerialNumberForCorrect(db, questionID)
			if err != nil {
				logInstance.ErrorLogger.Error("Failed to get next serial number for correct answers", "error", err)
				continue
			}

			starredSrc := utils.StarMiddleDigits(message.Source)

			correctAnswerMessage := domain.CorrectAnswerMessage{
				Answer:                 text,
				Score:                  score,
				Date:                   parsedDate.Format(customDateFormat),
				SerialNumber:           serialNumber,
				SerialNumberForCorrect: serialNumberForCorrect,
				StarredSrc:             starredSrc,
				QuizID:                 quizID,
				QuestionID:             questionID,
			}
			msg, _ := json.MarshalIndent(correctAnswerMessage, "", "    ")
			wsServer.Broadcast(destination, msg)

			// Mark as scored to avoid re-checking
			scoredMap[questionID] = true

			logInstance.InfoLogger.Info("Correct answer processed and broadcasted", "answer", correctAnswerMessage.Answer, "score", correctAnswerMessage.Score, "date", correctAnswerMessage.Date, "serial_number", correctAnswerMessage.SerialNumber, "serial_number_for_correct", correctAnswerMessage.SerialNumberForCorrect, "starred_src", correctAnswerMessage.StarredSrc, "quiz_id", correctAnswerMessage.QuizID, "question_id", correctAnswerMessage.QuestionID)
		}

		if scoredMap[questionID] {
			continue
		}

		err = repository.InsertAnswer(db, questionID, text, parsedDate, clientID, 0, 0, 0)
		if err != nil {
			logInstance.ErrorLogger.Error("Failed to insert answer", "error", err)
			continue
		}
		logInstance.InfoLogger.Info("Answer inserted", "question_id", questionID, "is_correct", isCorrect)
	}
}

func processVoting(db *sql.DB, messageBroker *message_broker.MessageBrokerClient, wsServer *websocket.WebSocketServer, clientID int64, message domain.SMSMessage, parsedDate time.Time, logInstance *logger.Loggers) {
	votingID, status, err := repository.GetVotingDetails(db, message.Destination, parsedDate)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to find voting by short number and date", "error", err)
		return
	}

	votingItemID, votingItemTitle, err := repository.GetVotingItemDetails(db, votingID, message.Text)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to find voting item by vote code", "error", err)
		return
	}

	hasVoted, err := repository.HasClientVoted(db, votingID, clientID, status, parsedDate)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to check if client has voted", "error", err)
		return
	}
	if hasVoted {
		return
	}

	err = repository.InsertVotingMessageAndUpdateCount(db, votingID, votingItemID, message.Text, parsedDate, clientID)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to insert voting message and update count", "error", err)
		return
	}

	smsText := votingItemTitle + " ucin beren sesiniz kabul edildi"
	err = messageBroker.SendMessage(message.Destination, message.Source, smsText)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to send message notification", "error", err)
	} else {
		logInstance.InfoLogger.Info("Message notification sent successfully", "to", message.Source)
	}

	votingMessage := domain.VotingMessage{
		VotingID:     votingID,
		VotingItemID: votingItemID,
		ClientID:     clientID,
		Message:      message.Text,
		Date:         parsedDate.Format(customDateFormat),
	}
	msg, _ := json.MarshalIndent(votingMessage, "", "    ")
	wsServer.Broadcast(message.Destination, msg)

	logInstance.InfoLogger.Info("Voting message processed and broadcasted", "voting_id", votingID, "voting_item_id", votingItemID, "client_id", clientID, "message", message.Text, "date", parsedDate.Format(customDateFormat))
}

func processShopping(db *sql.DB, messageBroker *message_broker.MessageBrokerClient, wsServer *websocket.WebSocketServer, clientID int64, message domain.SMSMessage, parsedDate time.Time, logInstance *logger.Loggers) {
	lotID, err := repository.GetLotDetailsByShortNumber(db, message.Destination, parsedDate)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to find lot by short number and date", "error", err)
		return
	}

	err = repository.InsertLotMessageAndUpdate(db, lotID, message.Text, parsedDate, clientID)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to insert lot SMS message and update", "error", err)
		return
	}

	logInstance.InfoLogger.Info("Message recorded successfully", "lot_id", lotID, "client_id", clientID)

	// Send message notification
	err = messageBroker.SendMessage(message.Destination, message.Source, "Shopping vote is accepted.")
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to send message notification", "error", err)
	} else {
		logInstance.InfoLogger.Info("Message notification sent successfully", "to", message.Source)
	}

	// Broadcast to WebSocket
	shoppingMessage := domain.ShoppingMessage{
		LotID:    lotID,
		ClientID: clientID,
		Message:  message.Text,
		Date:     parsedDate.Format(customDateFormat),
		Src:      message.Source,
	}
	msg, _ := json.MarshalIndent(shoppingMessage, "", "    ")
	wsServer.Broadcast(message.Destination, msg)

	logInstance.InfoLogger.Info("Shopping message processed and broadcasted", "lot_id", lotID, "client_id", clientID, "message", message.Text, "date", parsedDate.Format(customDateFormat))
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
	parts := strings.Split(answer, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return strings.ToLower(strings.Join(parts, ","))
}
