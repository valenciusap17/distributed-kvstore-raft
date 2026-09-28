package logger

import (
	"os"

	"github.com/sirupsen/logrus"
)

var Log *logrus.Logger

func init() {
	Log = logrus.New()
	Log.SetFormatter(&logrus.JSONFormatter{})
	Log.SetLevel(logrus.DebugLevel)
	Log.SetOutput(os.Stdout)
}
