using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using Couchbase;
using Couchbase.Analytics;
using Couchbase.Core.Diagnostics;
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
    internal sealed class MetricTracker : IDisposable
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

        private readonly ObservabilitySemanticConvention _convention;
        private readonly Meter _keyValueMeter;
        private readonly MappedHistogram<long> _operations;
        private readonly MappedCounter<long> _operationCounts;
        private readonly MappedCounter<long> _responseStatus;
        private readonly MappedCounter<long> _orphans;
        private readonly MappedCounter<long> _retries;
        private readonly MappedCounter<long> _sendQueueFullErrors;
        private readonly MappedCounter<long> _timeouts;
        private Func<ClusterLabels?>? _clusterLabelsProvider;

        public MetricTracker(ClusterOptions options)
        {
            if (options is null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            _convention = ResolveConvention(options);
            _keyValueMeter = new Meter(MeterName, "1.0.0");

            _operations = MappedMetric.CreateHistogram(_keyValueMeter, _convention, Names.Operations, "us",
                "Duration of operations, excluding retries");

            _operationCounts = MappedMetric.CreateCounter(_keyValueMeter, _convention, Names.OperationCounts, "{operations}",
                "Number of operations executed");

            _responseStatus = MappedMetric.CreateCounter(_keyValueMeter, _convention, Names.OperationStatus, "{operations}",
                "KVResponse");

            _orphans = MappedMetric.CreateCounter(_keyValueMeter, _convention, Names.Orphans, "{operations}",
                "Number of operations sent which did not receive a response");

            _retries = MappedMetric.CreateCounter(_keyValueMeter, _convention, Names.Retries, "{operations}",
                "Number of operations retried");

            _sendQueueFullErrors = MappedMetric.CreateCounter(_keyValueMeter, _convention, Names.SendQueueFullErrors, "{operations}",
                "Number operations rejected due to a full send queue");

            _timeouts = MappedMetric.CreateCounter(_keyValueMeter, _convention, Names.Timeouts, "{operations}",
                "Number of operations timed out");

            var connectionTags = new TagList
            {
                { OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.Kv.Name }
            };

            MappedMetric.CreateObservableGauge(_keyValueMeter, _convention, Names.Connections, "{connections}",
                "Number of active connections", MultiplexingConnection.GetConnectionCount, connectionTags);

            var sendQueueTags = new TagList
            {
                { OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.Kv.Name }
            };

            MappedMetric.CreateObservableGauge(_keyValueMeter, _convention, Names.SendQueueLength, "{operations}",
                "Number of operations queued to be sent", ConnectionPoolBase.GetSendQueueLength, sendQueueTags);

            KeyValue = new KeyValueMetricTracker(this);
            N1Ql = new QueryMetricTracker(this);
            Analytics = new AnalyticsMetricTracker(this);
            Search = new SearchMetricTracker(this);
            Views = new ViewMetricTracker(this);
        }

        internal void SetClusterLabelsProvider(Func<ClusterLabels?> provider)
        {
            _clusterLabelsProvider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        public KeyValueMetricTracker KeyValue { get; }

        public QueryMetricTracker N1Ql { get; }

        public AnalyticsMetricTracker Analytics { get; }

        public SearchMetricTracker Search { get; }

        public ViewMetricTracker Views { get; }

        private static ObservabilitySemanticConvention ResolveConvention(ClusterOptions options)
        {
            var envConvention = ObservabilitySemanticConventionParser.FromEnvironment();
            if (options.ObservabilitySemanticConvention == ObservabilitySemanticConvention.Legacy
                && envConvention != ObservabilitySemanticConvention.Legacy)
            {
                return envConvention;
            }

            return options.ObservabilitySemanticConvention;
        }

        private void AddClusterLabels(ref TagList tags)
        {
            var clusterLabels = _clusterLabelsProvider?.Invoke();
            if (clusterLabels?.ClusterName is not null)
            {
                tags.Add(OuterRequestSpans.Attributes.ClusterName, clusterLabels.ClusterName);
            }

            if (clusterLabels?.ClusterUuid is not null)
            {
                tags.Add(OuterRequestSpans.Attributes.ClusterUuid, clusterLabels.ClusterUuid);
            }
        }

        public sealed class KeyValueMetricTracker
        {
            private readonly MetricTracker _owner;

            internal KeyValueMetricTracker(MetricTracker owner)
            {
                _owner = owner;
            }

            /// <summary>
            /// Tracks the first attempt of an operation.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void TrackOperation(OperationBase operation, TimeSpan duration, Type? errorType)
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

                _owner.AddClusterLabels(ref tagList);

                _owner._operations.Record(duration.ToMicroseconds(), tagList);
                _owner._operationCounts.Add(1, tagList);
            }

            /// <summary>
            /// Tracks the response status for each response from the server.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void TrackResponseStatus(OpCode opCode, ResponseStatus status)
            {
                var tags = new TagList
                {
                    { OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.Kv.Name },
                    { OuterRequestSpans.Attributes.Operation, opCode.ToMetricTag() },
                    { OuterRequestSpans.Attributes.ResponseStatus, status }
                };

                _owner._responseStatus.Add(1, tags);
            }

            /// <summary>
            /// Tracks an operation retry.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void TrackRetry(OpCode opCode) =>
                _owner._retries.Add(1, new TagList
                {
                    { OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.Kv.Name },
                    { OuterRequestSpans.Attributes.Operation, opCode.ToMetricTag() }
                });

            /// <summary>
            /// Track an orphaned operation.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void TrackOrphaned() =>
                _owner._orphans.Add(1, new TagList
                {
                    { OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.Kv.Name }
                });

            /// <summary>
            /// Tracks an operation rejected due to a full connection pool send queue.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void TrackSendQueueFull() =>
                _owner._sendQueueFullErrors.Add(1, new TagList
                {
                    { OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.Kv.Name }
                });

            /// <summary>
            /// Tracks an operation which has failed due to a timeout.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void TrackTimeout(OpCode opCode) =>
                _owner._timeouts.Add(1, new TagList
                {
                    { OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.Kv.Name },
                    { OuterRequestSpans.Attributes.Operation, opCode.ToMetricTag() }
                });
        }

        public sealed class QueryMetricTracker
        {
            private readonly MetricTracker _owner;

            internal QueryMetricTracker(MetricTracker owner)
            {
                _owner = owner;
            }

            /// <summary>
            /// Tracks the first attempt of an operation.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void TrackOperation(QueryRequest queryRequest, TimeSpan duration, Type? errorType)
            {
                var tags = new TagList
                {
                    new(OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.N1QLQuery),
                    new(OuterRequestSpans.Attributes.Operation, OuterRequestSpans.ServiceSpan.N1QLQuery),
                    new(OuterRequestSpans.Attributes.BucketName, queryRequest.Options?.BucketName),
                    new(OuterRequestSpans.Attributes.ScopeName, queryRequest.Options?.ScopeName),
                    new(OuterRequestSpans.Attributes.Outcome, GetOutcome(errorType))
                };

                _owner.AddClusterLabels(ref tags);
                _owner._operations.Record(duration.ToMicroseconds(), tags);
            }
        }

        public sealed class AnalyticsMetricTracker
        {
            private readonly MetricTracker _owner;

            internal AnalyticsMetricTracker(MetricTracker owner)
            {
                _owner = owner;
            }

            /// <summary>
            /// Tracks the first attempt of an operation.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void TrackOperation(AnalyticsRequest analyticsRequest, TimeSpan duration, Type? errorType)
            {
                var tags = new TagList
                {
                    new(OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.AnalyticsQuery),
                    new(OuterRequestSpans.Attributes.BucketName, analyticsRequest.Options?.BucketName),
                    new(OuterRequestSpans.Attributes.ScopeName, analyticsRequest.Options?.ScopeName),
                    new(OuterRequestSpans.Attributes.Outcome, GetOutcome(errorType))
                };

                _owner.AddClusterLabels(ref tags);

                _owner._operations.Record(duration.ToMicroseconds(), tags);
            }
        }

        public sealed class SearchMetricTracker
        {
            private readonly MetricTracker _owner;

            internal SearchMetricTracker(MetricTracker owner)
            {
                _owner = owner;
            }

            /// <summary>
            /// Tracks the first attempt of an operation.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void TrackOperation(FtsSearchRequest searchRequest, TimeSpan duration, Type? errorType)
            {
                var tags = new TagList
                {
                    new(OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.SearchQuery),
                    new(OuterRequestSpans.Attributes.ScopeName, searchRequest.Options?.ScopeName),
                    new(OuterRequestSpans.Attributes.Outcome, GetOutcome(errorType))
                };

                _owner.AddClusterLabels(ref tags);

                _owner._operations.Record(duration.ToMicroseconds(), tags);
            }
        }

        public sealed class ViewMetricTracker
        {
            private readonly MetricTracker _owner;

            internal ViewMetricTracker(MetricTracker owner)
            {
                _owner = owner;
            }

            /// <summary>
            /// Tracks the first attempt of an operation.
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void TrackOperation(ViewQuery viewQuery, TimeSpan duration, Type? errorType)
            {
                var tags = new TagList
                {
                    new(OuterRequestSpans.Attributes.Service, OuterRequestSpans.ServiceSpan.ViewQuery),
                    new(OuterRequestSpans.Attributes.BucketName, viewQuery.BucketName),
                    new(OuterRequestSpans.Attributes.Outcome, GetOutcome(errorType))
                };

                _owner.AddClusterLabels(ref tags);
                _owner._operations.Record(duration.ToMicroseconds(), tags);
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

        public void Dispose()
        {
            _keyValueMeter.Dispose();
        }
    }
}
