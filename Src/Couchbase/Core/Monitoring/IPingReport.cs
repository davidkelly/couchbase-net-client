﻿using System.Collections.Generic;

namespace Couchbase.Core.Monitoring
{
    public interface IPingReport
    {
        /// <summary>
        /// Gets the report identifier.
        /// </summary>
        string Id { get; }

        /// <summary>
        /// Gets the Ping Report version.
        /// </summary>
        short Version { get; }

        /// <summary>
        /// Gets the bucket configuration revision.
        /// </summary>
        uint ConfigRev { get; }

        /// <summary>
        /// Gets the SDK identifier.
        /// </summary>
        string Sdk { get; }

        /// <summary>
        /// Gets the service endpoints.
        /// </summary>
        Dictionary<string, IEnumerable<IEndpointDiagnostics>> Services { get; }
    }
}
