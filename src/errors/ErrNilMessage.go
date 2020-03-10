package errors

type ErrNilMessage struct {}

func (err *ErrNilMessage) Error() string {
	return "message value is nil"
}