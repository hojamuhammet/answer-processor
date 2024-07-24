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
	"sync"
	"time"
)

type Service struct {
	DB            *sql.DB
	MessageBroker *message_broker.MessageBrokerClient
	WSServer      *websocket.WebSocketServer
	LogInstance   *logger.Loggers
	mu            sync.Mutex
}

const customDateFormat = "2006-01-02T15:04:05"

func NewService(db *sql.DB, messageBroker *message_broker.MessageBrokerClient, wsServer *websocket.WebSocketServer, logInstance *logger.Loggers) *Service {
	return &Service{
		DB:            db,
		MessageBroker: messageBroker,
		WSServer:      wsServer,
		LogInstance:   logInstance,
	}
}

func (s *Service) ProcessMessage(message domain.SMSMessage) {
	if s.DB == nil {
		s.LogInstance.ErrorLogger.Error("Database instance is nil in ProcessMessage")
		return
	}

	parsedDate, err := time.Parse(customDateFormat, message.Date)
	if err != nil {
		s.LogInstance.ErrorLogger.Error("Failed to parse date", "date", message.Date, "error", err)
		return
	}

	s.mu.Lock()
	clientID, err := repository.InsertClientIfNotExists(s.DB, message.Source)
	s.mu.Unlock()
	if err != nil {
		s.LogInstance.ErrorLogger.Error("Failed to insert or find client", "error", err)
		return
	}

	s.mu.Lock()
	accountType, err := repository.GetAccountType(s.DB, message.Destination)
	s.mu.Unlock()
	if err != nil {
		s.LogInstance.ErrorLogger.Error("Failed to get account type", "number", message.Destination)
		return
	}

	switch accountType {
	case "quiz":
		s.processQuiz(clientID, message, parsedDate)
	case "voting":
		s.processVoting(clientID, message, parsedDate)
	case "shopping":
		s.processShopping(clientID, message, parsedDate)
	default:
		s.LogInstance.ErrorLogger.Error("Unknown account type", "account_type", accountType)
	}
}

func (s *Service) processQuiz(clientID int64, message domain.SMSMessage, parsedDate time.Time) {
	destination := message.Destination
	text := message.Text

	s.mu.Lock()
	_, questions, questionIDs, quizID, err := repository.GetAccountAndQuestions(s.DB, destination, parsedDate)
	s.mu.Unlock()
	if err != nil {
		s.LogInstance.ErrorLogger.Error("Failed to find quiz and questions", "error", err)
		return
	}

	s.LogInstance.InfoLogger.Info("Quiz found", "quiz_id", quizID, "questions_count", len(questions))

	s.mu.Lock()
	scoredMap, err := repository.HasClientScoredBatch(s.DB, questionIDs, clientID)
	s.mu.Unlock()
	if err != nil {
		s.LogInstance.ErrorLogger.Error("Failed to batch check if client has scored", "error", err)
		return
	}

	for i := range questions {
		questionID := questionIDs[i]
		s.mu.Lock()
		correctAnswers, err := repository.GetQuestionAnswers(s.DB, questionID)
		s.mu.Unlock()
		if err != nil {
			s.LogInstance.ErrorLogger.Error("Failed to get question answers", "error", err)
			continue
		}

		isCorrect := compareAnswers(correctAnswers, text)

		s.mu.Lock()
		serialNumber, err := repository.GetNextSerialNumber(s.DB, questionID)
		s.mu.Unlock()
		if err != nil {
			s.LogInstance.ErrorLogger.Error("Failed to get next serial number", "error", err)
			continue
		}

		if scoredMap[questionID] {
			s.mu.Lock()
			err = repository.InsertAnswer(s.DB, questionID, text, parsedDate, clientID, 0, serialNumber, 0)
			s.mu.Unlock()
			if err != nil {
				s.LogInstance.ErrorLogger.Error("Failed to insert answer", "error", err)
				continue
			}
			s.LogInstance.InfoLogger.Info("Answer inserted for previously correctly answered question", "question_id", questionID, "serial_number", serialNumber)
			continue
		}

		serialNumberForCorrect := 0
		score := 0
		if isCorrect {
			s.mu.Lock()
			serialNumberForCorrect, err = repository.GetNextSerialNumberForCorrect(s.DB, questionID)
			s.mu.Unlock()
			if err != nil {
				s.LogInstance.ErrorLogger.Error("Failed to get next serial number for correct answers", "error", err)
				continue
			}

			s.mu.Lock()
			score, err = repository.GetQuestionScore(s.DB, questionID)
			s.mu.Unlock()
			if err != nil {
				s.LogInstance.ErrorLogger.Error("Failed to get question score", "error", err)
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
			s.WSServer.Broadcast(destination, msg)

			scoredMap[questionID] = true

			s.LogInstance.InfoLogger.Info("Correct answer processed and broadcasted", "answer", correctAnswerMessage.Answer, "score", correctAnswerMessage.Score, "date", correctAnswerMessage.Date, "serial_number", correctAnswerMessage.SerialNumber, "serial_number_for_correct", correctAnswerMessage.SerialNumberForCorrect, "starred_src", correctAnswerMessage.StarredSrc, "quiz_id", correctAnswerMessage.QuizID, "question_id", correctAnswerMessage.QuestionID)
		}

		s.mu.Lock()
		err = repository.InsertAnswer(s.DB, questionID, text, parsedDate, clientID, score, serialNumber, serialNumberForCorrect)
		s.mu.Unlock()
		if err != nil {
			s.LogInstance.ErrorLogger.Error("Failed to insert answer", "error", err)
			continue
		}
		s.LogInstance.InfoLogger.Info("Answer inserted", "question_id", questionID, "is_correct", isCorrect, "serial_number", serialNumber, "serial_number_for_correct", serialNumberForCorrect)
	}
}

func (s *Service) processVoting(clientID int64, message domain.SMSMessage, parsedDate time.Time) {
	s.mu.Lock()
	votingID, status, err := repository.GetVotingDetails(s.DB, message.Destination, parsedDate)
	s.mu.Unlock()
	if err != nil {
		s.LogInstance.ErrorLogger.Error("Failed to find voting by short number and date", "error", err)
		return
	}

	s.mu.Lock()
	votingItemID, votingItemTitle, err := repository.GetVotingItemDetails(s.DB, votingID, message.Text)
	s.mu.Unlock()
	if err != nil {
		s.LogInstance.ErrorLogger.Error("Failed to find voting item by vote code", "error", err)
		return
	}

	s.mu.Lock()
	hasVoted, err := repository.HasClientVoted(s.DB, votingID, clientID, status, parsedDate)
	s.mu.Unlock()
	if err != nil {
		s.LogInstance.ErrorLogger.Error("Failed to check if client has voted", "error", err)
		return
	}
	if hasVoted {
		return
	}

	s.mu.Lock()
	err = repository.InsertVotingMessageAndUpdateCount(s.DB, votingID, votingItemID, message.Text, parsedDate, clientID)
	s.mu.Unlock()
	if err != nil {
		s.LogInstance.ErrorLogger.Error("Failed to insert voting message and update count", "error", err)
		return
	}

	smsText := votingItemTitle + " ucin beren sesiniz kabul edildi"
	err = s.MessageBroker.SendMessage(message.Destination, message.Source, smsText)
	if err != nil {
		s.LogInstance.ErrorLogger.Error("Failed to send message notification", "error", err)
	} else {
		s.LogInstance.InfoLogger.Info("Message notification sent successfully", "to", message.Source)
	}

	votingMessage := domain.VotingMessage{
		VotingID:     votingID,
		VotingItemID: votingItemID,
		ClientID:     clientID,
		Message:      message.Text,
		Date:         parsedDate.Format(customDateFormat),
	}
	msg, _ := json.MarshalIndent(votingMessage, "", "    ")
	s.WSServer.Broadcast(message.Destination, msg)

	s.LogInstance.InfoLogger.Info("Voting message processed and broadcasted", "voting_id", votingID, "voting_item_id", votingItemID, "client_id", clientID, "message", message.Text, "date", parsedDate.Format(customDateFormat))
}

func (s *Service) processShopping(clientID int64, message domain.SMSMessage, parsedDate time.Time) {
	s.mu.Lock()
	lotID, err := repository.GetLotDetailsByShortNumber(s.DB, message.Destination, parsedDate)
	s.mu.Unlock()
	if err != nil {
		s.LogInstance.ErrorLogger.Error("Failed to find lot by short number and date", "error", err)
		return
	}

	s.mu.Lock()
	err = repository.InsertLotMessageAndUpdate(s.DB, lotID, message.Text, parsedDate, clientID)
	s.mu.Unlock()
	if err != nil {
		s.LogInstance.ErrorLogger.Error("Failed to insert lot SMS message and update", "error", err)
		return
	}

	s.LogInstance.InfoLogger.Info("Message recorded successfully", "lot_id", lotID, "client_id", clientID)

	// Send message notification
	err = s.MessageBroker.SendMessage(message.Destination, message.Source, "Shopping vote is accepted.")
	if err != nil {
		s.LogInstance.ErrorLogger.Error("Failed to send message notification", "error", err)
	} else {
		s.LogInstance.InfoLogger.Info("Message notification sent successfully", "to", message.Source)
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
	s.WSServer.Broadcast(message.Destination, msg)

	s.LogInstance.InfoLogger.Info("Shopping message processed and broadcasted", "lot_id", lotID, "client_id", clientID, "message", message.Text, "date", parsedDate.Format(customDateFormat))
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
