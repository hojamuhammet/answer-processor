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
}
