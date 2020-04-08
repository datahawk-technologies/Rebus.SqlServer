using System;
using System.Collections.Generic;
using System.Text;
using Rebus.Logging;
using Rebus.SqlServer.Transport;
using Rebus.SqlServer.Transport.Resilient;
using Rebus.Threading;
using Rebus.Time;
using Rebus.Transport;

namespace Rebus.Config
{

    /// <summary>
    /// Configuration extensions for the SQL transport
    /// </summary>
    public static class SqlServerTransportResilientConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use SQL Server as its transport. Unlike the <c>UseSqlServer</c> calls the leased version of the SQL 
        /// Server transport does not hold a transaction open for the entire duration of the message handling. Instead it marks a
        /// message as being "leased" for a period of time. If the lease has expired then a worker is permitted to acquire the that
        /// message again and try reprocessing
        /// </summary>
        /// <param name="configurer">Static to extend</param>
        /// <param name="transportOptions">Options controlling the transport setup</param>
        /// <param name="inputQueueName">Queue name to process messages from</param>
        public static SqlServerLeaseTransportOptions UseSqlServerInResilientLeaseMode(this StandardConfigurer<ITransport> configurer, SqlServerLeaseTransportOptions transportOptions, string inputQueueName)
        {
            return SqlServerTransportConfigurationExtensions.Configure(
                    configurer,
                    (context, provider, inputQueue) =>
                    {
                        if (transportOptions.LeasedByFactory == null)
                        {
                            transportOptions.SetLeasedByFactory(() => Environment.MachineName);
                        }

                        return new SqlServerLeaseResilientTransport(
                            provider,
                            transportOptions.InputQueueName,
                            context.Get<IRebusLoggerFactory>(),
                            context.Get<IAsyncTaskFactory>(),
                            context.Get<IRebusTime>(),
                            transportOptions.LeaseInterval ?? SqlServerLeaseTransport.DefaultLeaseTime,
                            transportOptions.LeaseTolerance ?? SqlServerLeaseTransport.DefaultLeaseTolerance,
                            transportOptions.LeasedByFactory,
                            transportOptions

                        );
                    },
                    transportOptions
                )
                .ReadFrom(inputQueueName);
        }

    }
}
