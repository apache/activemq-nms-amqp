using System;
using Apache.NMS;

namespace NMS.AMQP.Test.TestAmqp
{
    class Logger : ITrace
    {
        public enum LogLevel
        {
            OFF = -1,
            FATAL,
            ERROR,
            WARN,
            INFO,
            DEBUG
        }

        private LogLevel lv;

        public void LogException(Exception ex)
        {
            this.Warn("Exception: " + ex.Message);
        }

        public Logger() : this(LogLevel.WARN)
        {
        }

        public Logger(LogLevel lvl)
        {
            lv = lvl;
        }

        public bool IsDebugEnabled
        {
            get { return lv >= LogLevel.DEBUG; }
        }

        public bool IsErrorEnabled
        {
            get { return lv >= LogLevel.ERROR; }
        }

        public bool IsFatalEnabled
        {
            get { return lv >= LogLevel.FATAL; }
        }

        public bool IsInfoEnabled
        {
            get { return lv >= LogLevel.INFO; }
        }

        public bool IsWarnEnabled
        {
            get { return lv >= LogLevel.WARN; }
        }

        public void Debug(string message)
        {
            if (IsDebugEnabled)
                Console.WriteLine("Debug: {0}", message);
        }

        public void Error(string message)
        {
            if (IsErrorEnabled)
                Console.WriteLine("Error: {0}", message);
        }

        public void Fatal(string message)
        {
            if (IsFatalEnabled)
                Console.WriteLine("Fatal: {0}", message);
        }

        public void Info(string message)
        {
            if (IsInfoEnabled)
                Console.WriteLine("Info: {0}", message);
        }

        public void Warn(string message)
        {
            if (IsWarnEnabled)
                Console.WriteLine("Warn: {0}", message);
        }
    }
}