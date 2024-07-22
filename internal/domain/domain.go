package domain

type SMSMessage struct {
	Source      string `json:"src"`
	Destination string `json:"dst"`
	Text        string `json:"txt"`
	Date        string `json:"date"`
	Parts       int    `json:"parts"`
}

type CorrectAnswerMessage struct {
	Answer                 string `json:"answer"`
	Score                  int    `json:"score"`
	Date                   string `json:"date"`
	SerialNumber           int    `json:"serial_number"`
	SerialNumberForCorrect int    `json:"serial_number_for_correct"`
	StarredSrc             string `json:"starred_src"`
	QuizID                 int64  `json:"quiz_id"`
	QuestionID             int64  `json:"question_id"`
}

type VotingMessage struct {
	VotingID     int64  `json:"voting_id"`
	VotingItemID int64  `json:"voting_item_id"`
	ClientID     int64  `json:"client_id"`
	Message      string `json:"message"`
	Date         string `json:"date"`
}

type ShoppingMessage struct {
	LotID    int64  `json:"lot_id"`
	ClientID int64  `json:"client_id"`
	Message  string `json:"message"`
	Date     string `json:"date"`
	Src      string `json:"src"`
}
