using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Couchbase.Core.Diagnostics;
using Couchbase.Core.Diagnostics.Metrics;
using Couchbase.Core.Diagnostics.Tracing;
using Xunit;

namespace Couchbase.UnitTests.Core.Diagnostics.Metrics
{
    public class MappedMetricTests
    {
        [Fact]
        public void BuildTags_UsesModernNames_WhenRequested()
        {
            var tags = new TagList
            {
                { "db.system", "couchbase" },
                { "unmapped.key", "v" }
            };

            var mapped = MappedMetric.BuildTags(tags, ObservabilitySemanticConvention.Modern);

            var values = new Dictionary<string, object?>();
            foreach (var tag in mapped)
            {
                values[tag.Key] = tag.Value;
            }

            Assert.False(values.ContainsKey("db.system"));
            Assert.Equal("couchbase", values["db.system.name"]);
            Assert.Equal("v", values["unmapped.key"]);
        }

        [Fact]
        public void MappedCounter_UsesModernInstrument_WhenSpanIsModern()
        {
            var published = new List<string>();
            using var meter = new Meter("MappedMetricTests");
            using var listener = new MeterListener();

            listener.InstrumentPublished = (instrument, _) =>
            {
                published.Add(instrument.Name);
                listener.EnableMeasurementEvents(instrument);
            };
            listener.Start();

            var counter = MappedMetric.CreateCounter(meter, "db.couchbase.operations", "{operations}", "ops");
            var span = new RequestSpanWrapper(NoopRequestSpan.Instance, convention: ObservabilitySemanticConvention.Modern);

            counter.Add(1, new TagList(), span);

            Assert.Contains("db.client.operation.duration", published);
            Assert.DoesNotContain("db.couchbase.operations", published);
        }

        [Fact]
        public void MappedCounter_UsesBothInstruments_WhenSpanIsBoth()
        {
            var published = new List<string>();
            using var meter = new Meter("MappedMetricTests.Both");
            using var listener = new MeterListener();

            listener.InstrumentPublished = (instrument, _) =>
            {
                published.Add(instrument.Name);
                listener.EnableMeasurementEvents(instrument);
            };
            listener.Start();

            var counter = MappedMetric.CreateCounter(meter, "db.couchbase.operations", "{operations}", "ops");
            var span = new RequestSpanWrapper(NoopRequestSpan.Instance, convention: ObservabilitySemanticConvention.Both);

            counter.Add(1, new TagList(), span);

            Assert.Contains("db.client.operation.duration", published);
            Assert.Contains("db.couchbase.operations", published);
        }
    }
}
