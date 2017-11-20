﻿using System.Collections.Generic;
using Couchbase.Utils;
using Newtonsoft.Json;

namespace Couchbase.Core.Monitoring
{
    internal class PingReport : IPingReport
    {
        internal PingReport(string reportId, uint configRev, Dictionary<string, IEnumerable<IEndpointDiagnostics>> services)
        {
            Id = reportId;
            ConfigRev = configRev;
            Services = services;
        }

        [JsonProperty("id")]
        public string Id { get; }

        [JsonProperty("version")]
        public short Version { get; } = 1;

        [JsonProperty("config_rev")]
        public uint ConfigRev { get; }

        [JsonProperty("sdk")]
        public string Sdk { get; } = ClientIdentifier.GetClientDescription();

        [JsonProperty("services")]
        public Dictionary<string, IEnumerable<IEndpointDiagnostics>> Services { get; }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this, Formatting.None);
        }
    }
}
