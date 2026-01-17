using System;
using System.Collections.Generic;

namespace Couchbase.Core.Diagnostics;

internal static class SemanticConventionEmitter
{
    // Literal mapping: legacy attribute key -> modern attribute key
    private static readonly IReadOnlyDictionary<string, string> LegacyToModern =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["db.system"] = "db.system.name",
            ["db.couchbase.cluster_name"] = "couchbase.cluster.name",
            ["db.couchbase.cluster_uuid"] = "couchbase.cluster.uuid",
            ["db.name"] = "db.namespace",
            ["db.couchbase.scope"] = "couchbase.scope.name",
            ["db.couchbase.collection"] = "couchbase.collection.name",
            ["db.couchbase.retries"] = "couchbase.retries",
            ["db.couchbase.durability"] = "couchbase.durability",
            ["db.statement"] = "db.query.text",
            ["db.operation"] = "db.operation.name",
            ["outcome"] = "error.type",
            ["net.transport"] = "network.transport",
            ["net.host.name"] = "",  // empty string will not emit anything.
            ["net.host.port"] = "",
            ["net.peer.name"] = "server.address",
            ["net.peer.port"] = "server.port",
            ["db.couchbase.local_id"] = "couchbase.local_id",
            ["db.couchbase.operation_id"] = "couchbase.operation_id",
        };

    internal static void EmitAttribute<T>(
        ObservabilitySemanticConvention mode,
        string key,
        T value,
        Action<string, T> setAttribute)
    {
        // If the key isn't mapped, treat it as "neutral": emit as-is in all modes.
        if (!LegacyToModern.TryGetValue(key, out var modernKey))
        {
            setAttribute(key, value);
            return;
        }

        switch (mode)
        {
            case ObservabilitySemanticConvention.Legacy:
                setAttribute(key, value);
                return;

            case ObservabilitySemanticConvention.Modern:
                if (modernKey.Length == 0) return;
                setAttribute(modernKey, value);
                return;

            case ObservabilitySemanticConvention.Both:
                setAttribute(key, value);
                if (modernKey.Length == 0) return;
                setAttribute(modernKey, value);
                return;

            default:
                // In case we grow our enum, default to legacy to preserve compatibility.
                setAttribute(key, value);
                return;
        }
    }
}
