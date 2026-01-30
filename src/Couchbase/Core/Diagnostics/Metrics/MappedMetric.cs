using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;

#nullable enable

namespace Couchbase.Core.Diagnostics.Metrics;

internal static class MappedMetric
{
    private static readonly IReadOnlyDictionary<string, string> LegacyToModernMetricNames =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["db.couchbase.operations"] = "db.client.operation.duration"
        };

    internal static MappedHistogram<long> CreateHistogram(
        Meter meter,
        ObservabilitySemanticConvention convention,
        string legacyName,
        string unit,
        string description)
    {
        var modernName = GetModernMetricName(legacyName);
        return new MappedHistogram<long>(meter, legacyName, modernName, unit, description, convention);
    }

    internal static MappedCounter<long> CreateCounter(
        Meter meter,
        ObservabilitySemanticConvention convention,
        string legacyName,
        string unit,
        string description)
    {
        var modernName= GetModernMetricName(legacyName);
        return new MappedCounter<long>(meter, legacyName, modernName, unit, description, convention);
    }

    internal static void CreateObservableGauge<T>(
        Meter meter,
        ObservabilitySemanticConvention convention,
        string legacyName,
        string unit,
        string description,
        Func<T> observeValue,
        TagList legacyTags)
        where T : struct
    {
        var modernName = GetModernMetricName(legacyName);
        var hasMapping = modernName != legacyName;

        switch (convention)
        {
            case ObservabilitySemanticConvention.Legacy:
                meter.CreateObservableGauge(legacyName,
                    observeValue: () => new Measurement<T>(observeValue(), legacyTags),
                    unit: unit,
                    description: description);
                return;
            case ObservabilitySemanticConvention.Modern:
                meter.CreateObservableGauge(hasMapping ? modernName : legacyName,
                    observeValue: () => new Measurement<T>(observeValue(),
                        BuildTags(legacyTags, ObservabilitySemanticConvention.Modern)),
                    unit: unit,
                    description: description);
                return;
            case ObservabilitySemanticConvention.Both:
                meter.CreateObservableGauge(legacyName,
                    observeValue: () => new Measurement<T>(observeValue(), legacyTags),
                    unit: unit,
                    description: description);
                if (hasMapping)
                {
                    meter.CreateObservableGauge(modernName,
                        observeValue: () => new Measurement<T>(observeValue(),
                            BuildTags(legacyTags, ObservabilitySemanticConvention.Modern)),
                        unit: unit,
                        description: description);
                }
                return;
            default:
                meter.CreateObservableGauge(legacyName,
                    observeValue: () => new Measurement<T>(observeValue(), legacyTags),
                    unit: unit,
                    description: description);
                return;
        }
    }

    internal static TagList BuildTags(TagList legacyTags, ObservabilitySemanticConvention convention)
    {
        // be efficient and return legacy if that's all we are expecting...
        if (convention == ObservabilitySemanticConvention.Legacy)
        {
            return legacyTags;
        }

        var mapped = new TagList();
        foreach (var tag in legacyTags)
        {
            SemanticConventionEmitter.EmitAttribute(convention, tag.Key, tag.Value,
                (k, v) => mapped.Add(k, v));
        }

        return mapped;
    }

    private static string GetModernMetricName(string legacyName)
    {
        return LegacyToModernMetricNames.TryGetValue(legacyName, out var modernName) ? modernName : legacyName;
    }
}

internal sealed class MappedHistogram<T> where T : struct
{
    private readonly ObservabilitySemanticConvention _convention;
    private readonly Histogram<T>? _legacy;
    private readonly Histogram<T>? _modern;
    private readonly bool _hasMapping;
    internal MappedHistogram(Meter meter, string legacyName, string modernName, string unit, string description,
        ObservabilitySemanticConvention convention)
    {
        _convention = convention;
        _hasMapping = modernName != legacyName;

        if (!_hasMapping)
        {
            _legacy = meter.CreateHistogram<T>(legacyName, unit, description);
            return;
        }

        switch (_convention)
        {
            case ObservabilitySemanticConvention.Modern:
                _modern = meter.CreateHistogram<T>(modernName, unit, description);
                return;
            case ObservabilitySemanticConvention.Both:
                _legacy = meter.CreateHistogram<T>(legacyName, unit, description);
                _modern = meter.CreateHistogram<T>(modernName, unit, description);
                return;
            case ObservabilitySemanticConvention.Legacy:
            default:
                _legacy = meter.CreateHistogram<T>(legacyName, unit, description);
                return;
        }
    }

    internal void Record(T value, TagList legacyTags)
    {
        switch (_convention)
        {
            case ObservabilitySemanticConvention.Legacy:
                _legacy!.Record(value, legacyTags);
                return;
            case ObservabilitySemanticConvention.Modern:
                (_modern ?? _legacy)!.Record(value,
                    MappedMetric.BuildTags(legacyTags, ObservabilitySemanticConvention.Modern));
                return;
            case ObservabilitySemanticConvention.Both:
                _legacy!.Record(value, legacyTags);
                var modernTags = MappedMetric.BuildTags(legacyTags, ObservabilitySemanticConvention.Modern);
                if (_hasMapping)
                {
                    _modern!.Record(value, modernTags);
                    return;
                }

                _legacy.Record(value, modernTags);
                return;
            default:
                _legacy!.Record(value, legacyTags);
                return;
        }
    }
}

internal sealed class MappedCounter<T> where T : struct
{
    private readonly ObservabilitySemanticConvention _convention;
    private readonly Counter<T>? _legacy;
    private readonly Counter<T>? _modern;
    private readonly bool _hasMapping;
    internal MappedCounter(Meter meter, string legacyName, string modernName, string unit, string description,
        ObservabilitySemanticConvention convention)
    {
        _convention = convention;
        _hasMapping = modernName != legacyName;

        if (!_hasMapping)
        {
            _legacy = meter.CreateCounter<T>(legacyName, unit, description);
            return;
        }

        switch (_convention)
        {
            case ObservabilitySemanticConvention.Modern:
                _modern = meter.CreateCounter<T>(modernName, unit, description);
                return;
            case ObservabilitySemanticConvention.Both:
                _legacy = meter.CreateCounter<T>(legacyName, unit, description);
                _modern = meter.CreateCounter<T>(modernName, unit, description);
                return;
            case ObservabilitySemanticConvention.Legacy:
            default:
                _legacy = meter.CreateCounter<T>(legacyName, unit, description);
                return;
        }
    }

    internal void Add(T value, TagList legacyTags)
    {
        switch (_convention)
        {
            case ObservabilitySemanticConvention.Legacy:
                _legacy!.Add(value, legacyTags);
                return;
            case ObservabilitySemanticConvention.Modern:
                (_modern ?? _legacy)!.Add(value,
                    MappedMetric.BuildTags(legacyTags, ObservabilitySemanticConvention.Modern));
                return;
            case ObservabilitySemanticConvention.Both:
                _legacy!.Add(value, legacyTags);
                var modernTags = MappedMetric.BuildTags(legacyTags, ObservabilitySemanticConvention.Modern);
                if (_hasMapping)
                {
                    _modern!.Add(value, modernTags);
                    return;
                }

                _legacy.Add(value, modernTags);
                return;
            default:
                _legacy!.Add(value, legacyTags);
                return;
        }
    }
}
