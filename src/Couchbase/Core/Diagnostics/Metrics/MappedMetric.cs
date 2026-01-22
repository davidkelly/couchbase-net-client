using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using Couchbase.Core.Diagnostics;
using Couchbase.Core.Diagnostics.Tracing;

#nullable enable

namespace Couchbase.Core.Diagnostics.Metrics;

internal static class MappedMetric
{
    private static readonly ObservabilitySemanticConvention DefaultConvention =
        ObservabilitySemanticConventionParser.FromEnvironment();

    private static readonly IReadOnlyDictionary<string, string> LegacyToModernMetricNames =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["db.couchbase.operations"] = "db.couchbase.operation"
        };

    internal static MappedHistogram<long> CreateHistogram(
        Meter meter,
        string legacyName,
        string unit,
        string description)
    {
        var modernName = GetModernMetricName(legacyName);
        return new MappedHistogram<long>(meter, legacyName, modernName, unit, description);
    }

    internal static MappedCounter<long> CreateCounter(
        Meter meter,
        string legacyName,
        string unit,
        string description)
    {
        var modernName = GetModernMetricName(legacyName);
        return new MappedCounter<long>(meter, legacyName, modernName, unit, description);
    }

    internal static void CreateObservableGauge<T>(
        Meter meter,
        string legacyName,
        string unit,
        string description,
        Func<ObservabilitySemanticConvention, Measurement<T>> observeValue)
        where T : struct
    {
        var modernName = GetModernMetricName(legacyName);
        var hasMapping = modernName != legacyName;
        var convention = DefaultConvention;

        switch (convention)
        {
            case ObservabilitySemanticConvention.Legacy:
                meter.CreateObservableGauge(legacyName,
                    observeValue: () => observeValue(ObservabilitySemanticConvention.Legacy),
                    unit: unit,
                    description: description);
                return;
            case ObservabilitySemanticConvention.Modern:
                meter.CreateObservableGauge(hasMapping ? modernName : legacyName,
                    observeValue: () => observeValue(ObservabilitySemanticConvention.Modern),
                    unit: unit,
                    description: description);
                return;
            case ObservabilitySemanticConvention.Both:
                meter.CreateObservableGauge(legacyName,
                    observeValue: () => observeValue(ObservabilitySemanticConvention.Legacy),
                    unit: unit,
                    description: description);
                if (hasMapping)
                {
                    meter.CreateObservableGauge(modernName,
                        observeValue: () => observeValue(ObservabilitySemanticConvention.Modern),
                        unit: unit,
                        description: description);
                }
                return;
            default:
                meter.CreateObservableGauge(legacyName,
                    observeValue: () => observeValue(ObservabilitySemanticConvention.Legacy),
                    unit: unit,
                    description: description);
                return;
        }
    }

    internal static TagList BuildTags(TagList legacyTags, ObservabilitySemanticConvention convention)
    {
        if (convention == ObservabilitySemanticConvention.Legacy)
        {
            return legacyTags;
        }

        var mapped = new TagList();
        foreach (var tag in legacyTags)
        {
            SemanticConventionEmitter.EmitAttribute(ObservabilitySemanticConvention.Modern, tag.Key, tag.Value,
                (k, v) => mapped.Add(k, v));
        }

        return mapped;
    }

    internal static ObservabilitySemanticConvention ResolveConvention(IRequestSpan? span)
    {
        return span is RequestSpanWrapper wrapper ? wrapper.ObservabilitySemanticConvention : DefaultConvention;
    }

    internal static string GetModernMetricName(string legacyName)
    {
        return LegacyToModernMetricNames.TryGetValue(legacyName, out var modernName)
            ? modernName
            : legacyName;
    }
}

internal abstract class MappedInstrumentBase
{
    private int _resolvedConvention = -1;
    private int _resolvedOnce;

    protected ObservabilitySemanticConvention EnsureInitialized(IRequestSpan? span)
    {
        if (Volatile.Read(ref _resolvedOnce) == 1)
        {
            return (ObservabilitySemanticConvention)Volatile.Read(ref _resolvedConvention);
        }

        var resolved = MappedMetric.ResolveConvention(span);
        if (Interlocked.Exchange(ref _resolvedOnce, 1) == 0)
        {
            InitializeInstruments(resolved);
            Volatile.Write(ref _resolvedConvention, (int)resolved);
        }

        return (ObservabilitySemanticConvention)Volatile.Read(ref _resolvedConvention);
    }

    protected abstract void InitializeInstruments(ObservabilitySemanticConvention convention);
}

internal sealed class MappedHistogram<T> : MappedInstrumentBase where T : struct
{
    private readonly Meter _meter;
    private readonly string _legacyName;
    private readonly string _modernName;
    private readonly string _unit;
    private readonly string _description;
    private Histogram<T>? _legacy;
    private Histogram<T>? _modern;
    private readonly bool _hasMapping;
    internal MappedHistogram(Meter meter, string legacyName, string modernName, string unit, string description)
    {
        _meter = meter;
        _legacyName = legacyName;
        _modernName = modernName;
        _unit = unit;
        _description = description;
        _hasMapping = modernName != legacyName;
    }

    internal void Record(T value, TagList legacyTags, IRequestSpan? span)
    {
        var convention = EnsureInitialized(span);
        switch (convention)
        {
            case ObservabilitySemanticConvention.Legacy:
                _legacy!.Record(value, legacyTags);
                return;
            case ObservabilitySemanticConvention.Modern:
                (_modern ?? _legacy)!.Record(value,
                    MappedMetric.BuildTags(legacyTags, ObservabilitySemanticConvention.Modern));
                return;
            case ObservabilitySemanticConvention.Both:
                if (_modern is null)
                {
                    _legacy!.Record(value, legacyTags);
                    _legacy.Record(value, MappedMetric.BuildTags(legacyTags, ObservabilitySemanticConvention.Modern));
                    return;
                }

                _legacy!.Record(value, legacyTags);
                _modern.Record(value, MappedMetric.BuildTags(legacyTags, ObservabilitySemanticConvention.Modern));
                return;
            default:
                _legacy!.Record(value, legacyTags);
                return;
        }
    }

    protected override void InitializeInstruments(ObservabilitySemanticConvention convention)
    {
        if (!_hasMapping)
        {
            _legacy = _meter.CreateHistogram<T>(_legacyName, _unit, _description);
            return;
        }

        switch (convention)
        {
            case ObservabilitySemanticConvention.Modern:
                _modern = _meter.CreateHistogram<T>(_modernName, _unit, _description);
                return;
            case ObservabilitySemanticConvention.Both:
                _legacy = _meter.CreateHistogram<T>(_legacyName, _unit, _description);
                _modern = _meter.CreateHistogram<T>(_modernName, _unit, _description);
                return;
            case ObservabilitySemanticConvention.Legacy:
            default:
                _legacy = _meter.CreateHistogram<T>(_legacyName, _unit, _description);
                return;
        }
    }
}

internal sealed class MappedCounter<T> : MappedInstrumentBase where T : struct
{
    private readonly Meter _meter;
    private readonly string _legacyName;
    private readonly string _modernName;
    private readonly string _unit;
    private readonly string _description;
    private Counter<T>? _legacy;
    private Counter<T>? _modern;
    private readonly bool _hasMapping;
    internal MappedCounter(Meter meter, string legacyName, string modernName, string unit, string description)
    {
        _meter = meter;
        _legacyName = legacyName;
        _modernName = modernName;
        _unit = unit;
        _description = description;
        _hasMapping = modernName != legacyName;
    }

    internal void Add(T value, TagList legacyTags, IRequestSpan? span)
    {
        var convention = EnsureInitialized(span);
        switch (convention)
        {
            case ObservabilitySemanticConvention.Legacy:
                _legacy!.Add(value, legacyTags);
                return;
            case ObservabilitySemanticConvention.Modern:
                (_modern ?? _legacy)!.Add(value,
                    MappedMetric.BuildTags(legacyTags, ObservabilitySemanticConvention.Modern));
                return;
            case ObservabilitySemanticConvention.Both:
                if (_modern is null)
                {
                    _legacy!.Add(value, legacyTags);
                    _legacy.Add(value, MappedMetric.BuildTags(legacyTags, ObservabilitySemanticConvention.Modern));
                    return;
                }

                _legacy!.Add(value, legacyTags);
                _modern.Add(value, MappedMetric.BuildTags(legacyTags, ObservabilitySemanticConvention.Modern));
                return;
            default:
                _legacy!.Add(value, legacyTags);
                return;
        }
    }

    protected override void InitializeInstruments(ObservabilitySemanticConvention convention)
    {
        if (!_hasMapping)
        {
            _legacy = _meter.CreateCounter<T>(_legacyName, _unit, _description);
            return;
        }

        switch (convention)
        {
            case ObservabilitySemanticConvention.Modern:
                _modern = _meter.CreateCounter<T>(_modernName, _unit, _description);
                return;
            case ObservabilitySemanticConvention.Both:
                _legacy = _meter.CreateCounter<T>(_legacyName, _unit, _description);
                _modern = _meter.CreateCounter<T>(_modernName, _unit, _description);
                return;
            case ObservabilitySemanticConvention.Legacy:
            default:
                _legacy = _meter.CreateCounter<T>(_legacyName, _unit, _description);
                return;
        }
    }
}
