package rtm

type Notifctn int
type NotifctnCh chan Notifctn
type NotifctnChs []chan Notifctn

var (
	NotifctnStop Notifctn = 1 //stop
)
