using System;
using Apache.NMS.AMQP.Util;

namespace Apache.NMS.AMQP.Meta
{
    public sealed class TransactionInfo : ResourceInfo
    {
        public TransactionInfo(Id transactionId, Id sessionId) : base(transactionId)
        {
            if (transactionId == null)
                throw new ArgumentNullException(nameof(transactionId), "Transaction Id cannot be null");

            if (sessionId == null)
                throw new ArgumentNullException(nameof(sessionId), "Session Id cannot be null");

            SessionId = sessionId;
        }

        public Id SessionId { get; }
        public bool IsInDoubt { get; private set; }

        public void SetInDoubt()
        {
            IsInDoubt = true;
        }

        public byte[] ProviderTxId { get; set; }
    }
}