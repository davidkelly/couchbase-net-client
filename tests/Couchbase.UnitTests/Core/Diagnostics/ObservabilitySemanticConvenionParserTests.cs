#nullable enable
using Couchbase.Core.Diagnostics;
using Xunit;

namespace Couchbase.UnitTests.Core.Diagnostics
{
    public class ObservabilitySemanticConventionParserTests
    {
        [Theory]
        [InlineData(null, ObservabilitySemanticConvention.Legacy)]
        [InlineData("", ObservabilitySemanticConvention.Legacy)]
        [InlineData("   ", ObservabilitySemanticConvention.Legacy)]
        [InlineData(",", ObservabilitySemanticConvention.Legacy)]
        [InlineData(",,", ObservabilitySemanticConvention.Legacy)]
        [InlineData("foo", ObservabilitySemanticConvention.Legacy)]
        [InlineData("foo,bar", ObservabilitySemanticConvention.Legacy)]
        public void Parse_ReturnsLegacy_WhenUnsetEmptyOrNoRecognizedTokens(
            string? raw,
            ObservabilitySemanticConvention expected)
        {
            var actual = ObservabilitySemanticConventionParser.Parse(raw);
            Assert.Equal(expected, actual);
        }

        [Theory]
        [InlineData("database", ObservabilitySemanticConvention.Modern)]
        [InlineData("DATABASE", ObservabilitySemanticConvention.Modern)]
        [InlineData(" database ", ObservabilitySemanticConvention.Modern)]
        [InlineData("foo,database,bar", ObservabilitySemanticConvention.Modern)]
        [InlineData("foo, database ,bar", ObservabilitySemanticConvention.Modern)]
        public void Parse_ReturnsStandard_WhenDatabaseTokenPresent(
            string raw,
            ObservabilitySemanticConvention expected)
        {
            var actual = ObservabilitySemanticConventionParser.Parse(raw);

            Assert.Equal(expected, actual);
        }

        [Theory]
        [InlineData("database/dup", ObservabilitySemanticConvention.Both)]
        [InlineData("DATABASE/DUP", ObservabilitySemanticConvention.Both)]
        [InlineData(" database/dup ", ObservabilitySemanticConvention.Both)]
        [InlineData("foo,database/dup,bar", ObservabilitySemanticConvention.Both)]
        [InlineData("database,database/dup", ObservabilitySemanticConvention.Both)]
        [InlineData("database/dup,database", ObservabilitySemanticConvention.Both)]
        public void Parse_ReturnsBoth_WhenDatabaseDupTokenPresent(
            string raw,
            ObservabilitySemanticConvention expected)
        {
            var actual = ObservabilitySemanticConventionParser.Parse(raw);

            Assert.Equal(expected, actual);
        }

        [Theory]
        // Empty entries should be ignored, trimming should work.
        [InlineData(" , database , ", ObservabilitySemanticConvention.Modern)]
        [InlineData(" , database/dup , ", ObservabilitySemanticConvention.Both)]
        [InlineData(" , , database , , ", ObservabilitySemanticConvention.Modern)]
        [InlineData(" , , database/dup , , ", ObservabilitySemanticConvention.Both)]
        public void Parse_IgnoresEmptyEntries_AndTrimsTokens(
            string raw,
            ObservabilitySemanticConvention expected)
        {
            var actual = ObservabilitySemanticConventionParser.Parse(raw);

            Assert.Equal(expected, actual);
        }
    }
}
