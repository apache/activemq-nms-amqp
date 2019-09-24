using Apache.NMS;
using NLog;

namespace NMS.AMQP.Test.TestAmqp
{
    class NLogAdapter : ITrace
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        public bool IsDebugEnabled => Logger.IsDebugEnabled;

        public bool IsErrorEnabled => Logger.IsErrorEnabled;

        public bool IsFatalEnabled => Logger.IsFatalEnabled;

        public bool IsInfoEnabled => Logger.IsInfoEnabled;

        public bool IsWarnEnabled => Logger.IsWarnEnabled;

        public void Debug(string message) => Logger.Debug(message);

        public void Error(string message) => Logger.Error(message);

        public void Fatal(string message) => Logger.Fatal(message);

        public void Info(string message) => Logger.Info(message);

        public void Warn(string message) => Logger.Warn(message);
    }
}