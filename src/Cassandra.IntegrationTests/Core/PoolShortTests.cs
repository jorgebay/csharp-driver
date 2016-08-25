using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.Policies.Util;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Requests;
using Cassandra.Tasks;
using Cassandra.Tests;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category("short"), Category("debug")]
    public class PoolShortTests : TestGlobals
    {
        private ITestCluster _testCluster;

        [TestFixtureSetUp]
        public void OnTestFixtureSetUp()
        {
            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;
            _testCluster = TestClusterManager.CreateNew(2);
            var builder = Cluster.Builder().AddContactPoint(_testCluster.InitialContactPoint);
            using (var cluster = builder.Build())
            {
                var session = (Session)cluster.Connect();
                session.Execute(string.Format(TestUtils.CreateKeyspaceSimpleFormat, "ks1", 2));
                session.Execute("CREATE TABLE ks1.table1 (id1 int, id2 int, PRIMARY KEY (id1, id2))");
            }
        }

        [TestFixtureTearDown]
        public void OnTestFixtureTearDown()
        {
            TestClusterManager.TryRemove();
        }

        private void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            Console.WriteLine("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Unhandled exception: {0}; Is terminating: {1}", e.ExceptionObject, e.IsTerminating);
        }

        [TearDown]
        public void OnTearDown()
        {
            _testCluster.Start(2);
        }

        [Test, Timeout(1000 * 60 * 8)]
        public void StopForce_With_Inflight_Requests([Range(1, 35)]int r)
        {
//            if (CassandraVersion < Version.Parse("2.1"))
//            {
//                Assert.Ignore();
//            }
            const int connectionLength = 4;

            var builder = Cluster.Builder()
                .AddContactPoint(_testCluster.InitialContactPoint)
                .WithPoolingOptions(new PoolingOptions()
                    .SetCoreConnectionsPerHost(HostDistance.Local, connectionLength)
                    .SetMaxConnectionsPerHost(HostDistance.Local, connectionLength)
                    .SetHeartBeatInterval(0))
                .WithRetryPolicy(AlwaysIgnoreRetryPolicy.Instance)
                .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(0))
                .WithLoadBalancingPolicy(new RoundRobinPolicy());
            using (var cluster = builder.Build())
            {
                var session = (Session)cluster.Connect();
                var ps = session.Prepare("INSERT INTO ks1.table1 (id1, id2) VALUES (?, ?)");
                Console.WriteLine("--Warmup");
                Task.Factory.StartNew(() =>
                {
                    var t = ExecuteMultiple(_testCluster, session, ps, false, 1, 100);
                    t.Wait();
                    Assert.AreEqual(2, t.Result.Length);
                }).Wait();
                // Wait for all connections to be opened
                Thread.Sleep(1000);
                var hosts = cluster.AllHosts().ToArray();
                TestHelper.WaitUntil(() =>
                    hosts.Sum(h => session
                        .GetOrCreateConnectionPool(h, HostDistance.Local)
                        .OpenConnections.Count()
                    ) == hosts.Length * connectionLength);
                Assert.AreEqual(
                    hosts.Length * connectionLength, 
                    hosts.Sum(h => session.GetOrCreateConnectionPool(h, HostDistance.Local).OpenConnections.Count()));
                Console.WriteLine("--Starting");

                Interlocked.Exchange(ref Connection.CounterSendEnqueue, 0);
                Interlocked.Exchange(ref Connection.CounterSendEnqueueFail, 0);
                Interlocked.Exchange(ref Connection.CounterSendEnqueueFailCalled, 0);
                Interlocked.Exchange(ref Connection.CounterSendEnqueueCancel, 0);
                Interlocked.Exchange(ref RequestExecution.CountStart, 0);
                Interlocked.Exchange(ref RequestExecution.CountRetry, 0);
                Interlocked.Exchange(ref RequestExecution.CountRetryResponse, 0);
                Interlocked.Exchange(ref RequestExecution.CountResponse, 0);
                Interlocked.Exchange(ref Connection.CounterMetadata, 0);
                Interlocked.Exchange(ref Connection.CounterOperationCreated, 0);
                Interlocked.Exchange(ref Connection.CounterOperationChangedState, 0);
                Interlocked.Exchange(ref Connection.CounterOperationMarkCompleted, 0);
                Interlocked.Exchange(ref Connection.CounterCallback, 0);
                Interlocked.Exchange(ref Connection.CounterSocketExceptionCallback, 0);
                Interlocked.Exchange(ref Connection.CounterSocketExceptionCallbackReceived, 0);
                Interlocked.Exchange(ref Connection.CounterPendingAdd, 0);
                ExecuteMultiple(_testCluster, session, ps, true, 8000, 200000).Wait();
                PrintCounters();
            }
        }

        private static void PrintCounters()
        {
            Console.WriteLine("Enqueued: " + Interlocked.Read(ref Connection.CounterSendEnqueue));
            Console.WriteLine("  Enqueued cancelled: " + Interlocked.Read(ref Connection.CounterSendEnqueueCancel));
            Console.WriteLine("  Not Enqueued (direct fail): " + Interlocked.Read(ref Connection.CounterSendEnqueueFail));
            Console.WriteLine("  Not Enqueued (direct fail) called: " + Interlocked.Read(ref Connection.CounterSendEnqueueFailCalled));
            Console.WriteLine("Start: " + Interlocked.Read(ref RequestExecution.CountStart));
            Console.WriteLine("Retried: " + Interlocked.Read(ref RequestExecution.CountRetry));
            Console.WriteLine("Retry responses: " + Interlocked.Read(ref RequestExecution.CountRetryResponse));
            Console.WriteLine("Responses: " + Interlocked.Read(ref RequestExecution.CountResponse));
            Console.WriteLine("Callbacks: " + Interlocked.Read(ref Connection.CounterCallback));
            Console.WriteLine("Socket exception callbacks: " + Interlocked.Read(ref Connection.CounterSocketExceptionCallback));
            Console.WriteLine("Socket exception callbacks (received): " + Interlocked.Read(ref Connection.CounterSocketExceptionCallbackReceived));
            Console.WriteLine("Operations created: " + Interlocked.Read(ref Connection.CounterOperationCreated));
            Console.WriteLine("Operations changed state: " + Interlocked.Read(ref Connection.CounterOperationChangedState));
            Console.WriteLine("Operations added to pending: " + Interlocked.Read(ref Connection.CounterPendingAdd));
            Console.WriteLine("Operations mark as completed: " + Interlocked.Read(ref Connection.CounterOperationMarkCompleted));
            Console.WriteLine("Metadata queries: " + Interlocked.Read(ref Connection.CounterMetadata));
        }

        public class RetryOnceRetryPolicy : IRetryPolicy
        {
            public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved,
                                               int nbRetry)
            {
                if (nbRetry > 1)
                {
                    return RetryDecision.Ignore();
                }
                return RetryDecision.Retry(null);
            }

            public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
            {
                if (nbRetry > 1)
                {
                    return RetryDecision.Ignore();
                }
                return RetryDecision.Retry(null);
            }

            public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
            {
                if (nbRetry > 1)
                {
                    return RetryDecision.Ignore();
                }
                return RetryDecision.Retry(null);
            }
        }

        private Task<string[]> ExecuteMultiple(ITestCluster testCluster, Session session, PreparedStatement ps, bool stopNode, int maxConcurrency, int repeatLength)
        {
            var hosts = new ConcurrentDictionary<string, bool>();
            var tcs = new TaskCompletionSource<string[]>();
            var receivedCounter = 0L;
            var sendCounter = 0L;
            var currentlySentCounter = 0L;
            var stopMark = repeatLength / 8L;
            var timer = new Timer(_ =>
            {
                Console.WriteLine("!-!-! {0} Received, {1} Sent", Thread.VolatileRead(ref receivedCounter), Thread.VolatileRead(ref currentlySentCounter));
                PrintCounters();
            }, null, 60000L, 60000L);
            Action sendNew = null;
            sendNew = () =>
            {
                var sent = Interlocked.Increment(ref sendCounter);
                if (sent > repeatLength)
                {
                    return;
                }
                Interlocked.Increment(ref currentlySentCounter);
                var statement = ps.Bind(DateTime.Now.Millisecond, (int)sent);
                var executeTask = session.ExecuteAsync(statement);
                executeTask.ContinueWith(t =>
                {
                    if (t.Exception != null)
                    {
                        tcs.TrySetException(t.Exception.InnerException);
                        return;
                    }
                    hosts.AddOrUpdate(t.Result.Info.QueriedHost.ToString(), true, (k, v) => v);
                    var received = Interlocked.Increment(ref receivedCounter);
                    if (stopNode && received == stopMark)
                    {

                        Console.WriteLine("--Starting stop forcefully node2");
                        Task.Factory.StartNew(() =>
                        {
                            Console.WriteLine("--Stopping forcefully node2 (already sent {0})", Interlocked.Read(ref currentlySentCounter));
                            testCluster.StopForce(2);
                            Console.WriteLine("--Stopped node2 (already sent {0})", Interlocked.Read(ref currentlySentCounter));
                        }, TaskCreationOptions.LongRunning);
                    }
                    if (received == repeatLength)
                    {
                        // Mark this as finished
                        Console.WriteLine("--Marking as completed");
                        timer.Dispose();
                        tcs.TrySetResult(hosts.Keys.ToArray());
                        return;
                    }
                    sendNew();
                }, TaskContinuationOptions.ExecuteSynchronously);
            };

            for (var i = 0; i < maxConcurrency; i++)
            {
                sendNew();
            }
            return tcs.Task;
        }
        
        /// <summary>
        /// Async semaphore implementation, as its not available in the .NET Framework 
        /// </summary>
        private class AsyncSemaphore
        {
            private readonly Queue<TaskCompletionSource<bool>> _waiters = new Queue<TaskCompletionSource<bool>>();
            private int _currentCount;

            public AsyncSemaphore(int initialCount)
            {
                if (initialCount < 0)
                {
                    throw new ArgumentOutOfRangeException("initialCount");
                }
                _currentCount = initialCount;
            }

            public Task WaitAsync()
            {
                lock (_waiters)
                {
                    if (_currentCount > 0)
                    {
                        --_currentCount;
                        return TaskHelper.Completed;
                    }
                    var w = new TaskCompletionSource<bool>();
                    _waiters.Enqueue(w);
                    return w.Task;
                }
            }

            public void Release()
            {
                TaskCompletionSource<bool> toRelease = null;
                lock (_waiters)
                {
                    if (_waiters.Count > 0)
                    {
                        toRelease = _waiters.Dequeue();
                    }
                    else
                    {
                        ++_currentCount;
                    }
                }
                if (toRelease != null)
                {
                    toRelease.SetResult(true);
                }
            }
        }
    }
}