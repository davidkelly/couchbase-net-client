using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Couchbase.Core.Diagnostics;
using Couchbase.Core.Diagnostics.Metrics;
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
        public void MappedCounter_UsesModernInstrument_WhenConventionIsModern()
        {
            var published = new List<string>();
            var meterName = "MappedMetricTests";
            using var meter = new Meter(meterName);
            using var listener = new MeterListener();

            listener.InstrumentPublished = (instrument, publishedListener) =>
            {
                if (instrument.Meter.Name != meterName)
                {
                    return;
                }

                published.Add(instrument.Name);
                publishedListener.EnableMeasurementEvents(instrument);
            };
            listener.Start();

            var counter = MappedMetric.CreateCounter(meter, ObservabilitySemanticConvention.Modern,
                "db.couchbase.operations", "{operations}", "ops");

            counter.Add(1, new TagList());

            Assert.Contains("db.client.operation.duration", published);
            Assert.DoesNotContain("db.couchbase.operations", published);
        }

        [Fact]
        public void MappedCounter_UsesLegacyInstrument_WhenConventionIsLegacy()
        {
            var published = new List<string>();
            var meterName = "MappedMetricTests.Legacy";
            using var meter = new Meter(meterName);
            using var listener = new MeterListener();

            listener.InstrumentPublished = (instrument, publishedListener) =>
            {
                if (instrument.Meter.Name != meterName)
                {
                    return;
                }

                published.Add(instrument.Name);
                publishedListener.EnableMeasurementEvents(instrument);
            };
            listener.Start();

            var counter = MappedMetric.CreateCounter(meter, ObservabilitySemanticConvention.Legacy,
                "db.couchbase.operations", "{operations}", "ops");

            counter.Add(1, new TagList());

            Assert.DoesNotContain("db.client.operation.duration", published);
            Assert.Contains("db.couchbase.operations", published);

        }

        [Fact]
        public void MappedCounter_UsesBothInstruments_WhenConventionIsBoth()
        {
            var published = new List<string>();
            var meterName = "MappedMetricTests.Both";
            using var meter = new Meter(meterName);
            using var listener = new MeterListener();

            listener.InstrumentPublished = (instrument, publishedListener) =>
            {
                if (instrument.Meter.Name != meterName)
                {
                    return;
                }

                published.Add(instrument.Name);
                publishedListener.EnableMeasurementEvents(instrument);
            };
            listener.Start();

            var counter = MappedMetric.CreateCounter(meter, ObservabilitySemanticConvention.Both,
                "db.couchbase.operations", "{operations}", "ops");

            counter.Add(1, new TagList());

            Assert.Contains("db.client.operation.duration", published);
            Assert.Contains("db.couchbase.operations", published);
        }
    }
}
