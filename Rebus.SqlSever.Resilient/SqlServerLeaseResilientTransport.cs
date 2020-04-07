using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Polly;
using Polly.Retry;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Threading;
using Rebus.Time;

namespace Rebus.SqlServer.Transport.Resilient
{
    public class SqlServerLeaseResilientTransport : SqlServerLeaseTransport
    {
        private static readonly IEnumerable<TimeSpan> RetryTimes = new[]
        {
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(2),
            TimeSpan.FromSeconds(3)
        };

        private static readonly AsyncRetryPolicy RetryPolicy = Policy
                                                     .Handle<SqlException>(SqlServerTransientExceptionDetector.ShouldRetryOn)
                                                     .Or<TimeoutException>()
                                                     .OrInner<Win32Exception>(SqlServerTransientExceptionDetector.ShouldRetryOn)
                                                     .WaitAndRetryAsync(RetryTimes);
        public SqlServerLeaseResilientTransport(IDbConnectionProvider connectionProvider, string inputQueueName, IRebusLoggerFactory rebusLoggerFactory, IAsyncTaskFactory asyncTaskFactory, IRebusTime rebusTime, TimeSpan leaseInterval, TimeSpan? leaseTolerance, Func<string> leasedByFactory, SqlServerLeaseTransportOptions options) : base(connectionProvider, inputQueueName, rebusLoggerFactory, asyncTaskFactory, rebusTime, leaseInterval, leaseTolerance, leasedByFactory, options)
        {
        }

        protected override Task DeleteMessage(long messageId, CancellationToken cancellationToken)
        {
            return RetryPolicy.ExecuteAsync(
                async (token) => await base.DeleteMessage(messageId, token), cancellationToken);
        }

        protected override Task UpdateLease(IDbConnectionProvider connectionProvider, string tableName, long messageId, TimeSpan? leaseInterval, CancellationToken cancellationToken)
        {
            return RetryPolicy.ExecuteAsync(
                async (token) => await base.UpdateLease(connectionProvider, tableName, messageId, leaseInterval, cancellationToken), cancellationToken);
        }
    }
}
