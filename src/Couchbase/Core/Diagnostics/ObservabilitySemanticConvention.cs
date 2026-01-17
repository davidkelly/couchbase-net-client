#nullable enable
using System;

namespace Couchbase.Core.Diagnostics
{
    /// <summary>
    /// Controls whether Couchbase spans emit legacy attribute names (default)
    /// or modern OpenTelemetry semantic-convention names.
    /// This is a process-global setting: if any cluster enables modern mode,
    /// the entire process emits modern attribute names.
    /// </summary>
    public enum ObservabilitySemanticConvention
    {
        /// <summary>
        /// Legacy attribute names are the default, which couchbase has historically used.
        /// </summary>
        Legacy = 0,
        /// <summary>
        /// Modern attribute names are OpenTelemetry semantic-convention names.
        /// </summary>
        Modern = 1,
        /// <summary>
        /// Both legacy and modern attribute names are emitted.  Helpful while transitioning
        /// from one to the other.
        /// </summary>
        Both = 2
    }


    internal static class ObservabilitySemanticConventionParser
    {
        /// <summary>
        /// Get the current observability semantic convention mode from the environment variable.
        /// </summary>
        /// <returns>ObservabilitySemanticConvention from the environment, or Legacy by default.
        /// </returns>
        public static ObservabilitySemanticConvention FromEnvironment()
        {
            var raw = Environment.GetEnvironmentVariable("OTEL_SEMCONV_STABILITY_OPT_IN");
            return Parse(raw);
        }

        internal static ObservabilitySemanticConvention Parse(string? raw)
        {
            var mode = ObservabilitySemanticConvention.Legacy;
            if (raw is null) return mode;

            // if more than one value, database/dup takes precedence.
            foreach (var part in raw.Split([','], StringSplitOptions.RemoveEmptyEntries))
            {
                var token = part.Trim();

                if (token.Equals("database/dup", StringComparison.OrdinalIgnoreCase))
                    return ObservabilitySemanticConvention.Both;

                if (token.Equals("database", StringComparison.OrdinalIgnoreCase))
                    mode = ObservabilitySemanticConvention.Modern;
            }

            return mode;
        }
    }
}
