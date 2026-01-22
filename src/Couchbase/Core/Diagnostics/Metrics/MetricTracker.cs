using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using Couchbase.Analytics;
using Couchbase.Core.Diagnostics.Tracing;
using Couchbase.Core.Exceptions.KeyValue;
using Couchbase.Core.IO.Connections;
using Couchbase.Core.IO.Operations;
using Couchbase.Core.Retry.Query;
using Couchbase.Core.Retry.Search;
using Couchbase.Utils;
using Couchbase.Views;

#nullable enable

namespace Couchbase.Core.Diagnostics.Metrics
{
    /// <summary>
    /// Methods for easily tracking metrics via the .NET metrics system.
    /// </summary>
    internal static class MetricTracker
    {
        public const string MeterName = "CouchbaseNetClient";

        public static class Names
        {
            // ReSharper disable MemberHidesStaticFromOuterClass

            public const string Connections = "db.couchbase.connections";
            public const string Operations = "db.couchbase.operations";
            public const string OperationCounts = "db.couchbase.operations.count";
            public const string OperationStatus = "db.couchbase.operations.status";
            public const string Orphans = "db.couchbase.orphans";
            public const string Retries = "db.couchbase.retries";
            public const string SendQueueFullErrors = "db.couchbase.sendqueue.fullerrors";
            public const string SendQueueLength = "db.couchbase.sendqueue.length";
            public const string Timeouts = "db.couchbase.timeouts";

            // ReSharper restore MemberHidesStaticFromOuterClass
        }

        private static readonly Meter KeyValueMeter = new(MeterName, "1.0.0");

        private static readonly MappedHistogram<long> Operations =
            MappedMetric.CreateHistogram(KeyValueMeter, Names.Operations, "us",
                "Duration of operations, excluding retries");

        private static readonly MappedCounter<long> OperationCounts =
            MappedMetric.CreateCounter(KeyValueMeter, Names.OperationCounts, "{operations}",
                "Number of operations executed");

        private static readonly MappedCounter<long> ResponseStatus =
            MappedMetric.CreateCounter(KeyValueMeter, Names.OperationStatus, "{operations}", "KVResponse");

        private static readonly MappedCounter<long> Orphans =
            MappedMetric.CreateCounter(KeyValueMeter, Names.Orphans, "{operations}",
                "Number of operations sent which did not receive a response");

        private static readonly MappedCounter<long> Retries =
            MappedMetric.CreateCounter(KeyValueMeter, Names.Retries, "{operations}", "Number of operations retried");

        private static readonly MappedCounter<long> SendQueueFullErrors =
            MappedMetric.CreateCounter(KeyValueMeter, Names.SendQueueFullErrors, "{operations}",
                "Number operations rejected due to a full send queue");

        private static readonly MappedCounter<long> Timeouts =
            MappedMetric.CreateCounter(KeyValueMeter, Names.Timeouts, "{operations}", "Number of operations timed out");

        static MetricTracker()
        {
            // Due to lazy initialization, we should initialize Observable metrics here rather than static fields

            MappedMetric.CreateObservableGauge(KeyValueMeter, Names.Connections, "{connections}",
                "Number of active connections", convention => new Measurement<int>(
                    MultiplexingConnection.GetConnectionCount(),
                    MappedMetric.BuildTags(new TagList
                    {
                        { OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.Kv.Name }
                    }, convention)));

            MappedMetric.CreateObservableGauge(KeyValueMeter, Names.SendQueueLength, "{operations}",
                "Number of operations queued to be sent", convention => new Measurement<int>(
                    ConnectionPoolBase.GetSendQueueLength(),
                    MappedMetric.BuildTags(new TagList
                    {
                        { OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.Kv.Name }
                    }, convention)));
        }

        public static class KeyValue
        {
            /// <summary>
            /// Tracks the first attempt of an operation.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void TrackOperation(OperationBase operation, TimeSpan duration, Type? errorType)
            {
                var tagList = new TagList
                {
                    { OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.Kv.Name },
                    { OuterRequestSpans.Attributes.Operation, operation.OpCode.ToMetricTag() },
                    { OuterRequestSpans.Attributes.BucketName, operation.BucketName },
                    { OuterRequestSpans.Attributes.ScopeName, operation.SName },
                    { OuterRequestSpans.Attributes.CollectionName, operation.CName },
                    { OuterRequestSpans.Attributes.Outcome, GetOutcome(errorType) },
                };

                tagList.AddClusterLabelsIfProvided(operation.Span);

                Operations.Record(duration.ToMicroseconds(), tagList, operation.Span);
                OperationCounts.Add(1, tagList, operation.Span);
            }

            /// <summary>
            /// Tracks the response status for each response from the server.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void TrackResponseStatus(OpCode opCode, ResponseStatus status)
            {
                var tags = new TagList
                {
                    { OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.Kv.Name },
                    { OuterRequestSpans.Attributes.Operation, opCode.ToMetricTag() },
                    { OuterRequestSpans.Attributes.ResponseStatus, status }
                };

                ResponseStatus.Add(1, tags, null);
            }

            /// <summary>
            /// Tracks an operation retry.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void TrackRetry(OpCode opCode) =>
                Retries.Add(1, new TagList
                {
                    { OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.Kv.Name },
                    { OuterRequestSpans.Attributes.Operation, opCode.ToMetricTag() }
                }, null);

            /// <summary>
            /// Track an orphaned operation.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void TrackOrphaned() =>
                Orphans.Add(1, new TagList
                {
                    { OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.Kv.Name }
                }, null);

            /// <summary>
            /// Tracks an operation rejected due to a full connection pool send queue.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void TrackSendQueueFull() =>
                SendQueueFullErrors.Add(1, new TagList
                {
                    { OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.Kv.Name }
                }, null);

            /// <summary>
            /// Tracks an operation which has failed due to a timeout.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void TrackTimeout(OpCode opCode) =>
                Timeouts.Add(1, new TagList
                {
                    { OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.Kv.Name },
                    { OuterRequestSpans.Attributes.Operation, opCode.ToMetricTag() }
                }, null);
        }

        public static class N1Ql
        {
            /// <summary>
            /// Tracks the first attempt of an operation.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void TrackOperation(QueryRequest queryRequest, TimeSpan duration, Type? errorType)
            {
                var tags = new TagList
                {
                    new(OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.N1QLQuery),
                    new(OuterRequestSpans.Attributes.Operation, OuterRequestSpans.ServiceSpan.N1QLQuery),
                    new(OuterRequestSpans.Attributes.BucketName, queryRequest.Options?.BucketName),
                    new(OuterRequestSpans.Attributes.ScopeName, queryRequest.Options?.ScopeName),
                    new(OuterRequestSpans.Attributes.Outcome, GetOutcome(errorType))
                };

                tags.AddClusterLabelsIfProvided(queryRequest.Options?.RequestSpanValue);
                Operations.Record(duration.ToMicroseconds(), tags, queryRequest.Options?.RequestSpanValue);
            }
        }

        public static class Analytics
        {
            /// <summary>
            /// Tracks the first attempt of an operation.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void TrackOperation(AnalyticsRequest analyticsRequest, TimeSpan duration, Type? errorType)
            {
                var tags = new TagList
                {
                    new(OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.AnalyticsQuery),
                    new(OuterRequestSpans.Attributes.BucketName, analyticsRequest.Options?.BucketName),
                    new(OuterRequestSpans.Attributes.ScopeName, analyticsRequest.Options?.ScopeName),
                    new(OuterRequestSpans.Attributes.Outcome, GetOutcome(errorType))
                };

                tags.AddClusterLabelsIfProvided(analyticsRequest.Options?.RequestSpanValue);

                Operations.Record(duration.ToMicroseconds(), tags, analyticsRequest.Options?.RequestSpanValue);
            }
        }

        public static class Search
        {
            /// <summary>
            /// Tracks the first attempt of an operation.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void TrackOperation(FtsSearchRequest searchRequest, TimeSpan duration, Type? errorType)
            {
                var tags = new TagList
                {
                    new(OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.SearchQuery),
                    new(OuterRequestSpans.Attributes.ScopeName, searchRequest.Options?.ScopeName),
                    new(OuterRequestSpans.Attributes.Outcome, GetOutcome(errorType))
                };

                tags.AddClusterLabelsIfProvided(searchRequest.Options?.RequestSpanValue);

                Operations.Record(duration.ToMicroseconds(), tags, searchRequest.Options?.RequestSpanValue);
            }
        }

        public static class Views
        {
            /// <summary>
            /// Tracks the first attempt of an operation.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void TrackOperation(ViewQuery viewQuery, TimeSpan duration, Type? errorType)
            {
                var tags = new TagList
                {
                    new(OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.ViewQuery),
                    new(OuterRequestSpans.Attributes.BucketName, viewQuery.BucketName),
                    new(OuterRequestSpans.Attributes.Outcome, GetOutcome(errorType))
                };

                tags.AddClusterLabelsIfProvided(((IViewQuery)viewQuery).RequestSpanValue);
                Operations.Record(duration.ToMicroseconds(), tags, ((IViewQuery)viewQuery).RequestSpanValue);
            }
        }

        // internal for unit testing
        internal static string GetOutcome(Type? errorType)
        {
            if (errorType is null)
            {
                return "Success";
            }

            if (errorType == typeof(DocumentNotFoundException))
            {
                // Fast path for this common error type
                return "DocumentNotFound";
            }

            var couchbaseException = typeof(CouchbaseException);
            if (errorType == couchbaseException || !couchbaseException.IsAssignableFrom(errorType))
            {
                // In the case where this is not an inherited exception, say "Error" not "Couchbase".

                // Also returns "Error" for non-Couchbase exceptions so metric cardinality is not increased
                // for every possible .NET exception type. This can be revised in the future if necessary.

                return "Error";
            }

            const string ExceptionSuffix = "Exception";

            var outcome = errorType.Name;
#if NET6_0_OR_GREATER
            if (outcome.AsSpan().EndsWith(ExceptionSuffix)) // Faster comparison for .NET 6 and later
#else
            if (outcome.EndsWith(ExceptionSuffix, StringComparison.Ordinal))
#endif
            {
                // Strip "Exception" from the end of the type name, matches the behavior of the Java SDK
                outcome = outcome.Substring(0, outcome.Length - ExceptionSuffix.Length);
            }
            return outcome;
        }

    }
}
