using System;
using System.Threading;
using Couchbase.Utils;
using CancellationTokenCls = System.Threading.CancellationToken;

#nullable enable

namespace Couchbase.Management.Users
{
    public class GetUserOptions
    {
        public string DomainNameValue { get; set; } = "local";
        internal CancellationToken TokenValue { get; set; } = CancellationTokenCls.None;
        internal TimeSpan TimeoutValue { get; set; } = ClusterOptions.Default.ManagementTimeout;

        public GetUserOptions DomainName(string domainName)
        {
            DomainNameValue = domainName;
            return this;
        }

        /// <summary>
        /// Allows to pass in a custom CancellationToken from a CancellationTokenSource.
        /// Note that CancellationToken() takes precedence over Timeout(). If both CancellationToken and Timeout are set, the former will be used in the operation.
        /// </summary>
        /// <param name="cancellationToken">The Token to cancel the operation.</param>
        /// <returns>This class for method chaining.</returns>
        public GetUserOptions CancellationToken(CancellationToken cancellationToken)
        {
            TokenValue = cancellationToken;
            return this;
        }

        /// <summary>
        /// Allows to set a Timeout for the operation.
        /// Note that CancellationToken() takes precedence over Timeout(). If both CancellationToken and Timeout are set, the former will be used in the operation.
        /// </summary>
        /// <param name="timeout">The duration of the Timeout. Set to 75s by default.</param>
        /// <returns>This class for method chaining.</returns>
        public GetUserOptions Timeout(TimeSpan timeout)
        {
            TimeoutValue = timeout;
            return this;
        }

        public static GetUserOptions Default => new GetUserOptions();
        public static ReadOnly DefaultReadOnly => Default.AsReadOnly();

        public void Deconstruct(out CancellationToken tokenValue, out TimeSpan timeoutValue)
        {
            tokenValue = TokenValue;
            timeoutValue = TimeoutValue;
        }

        public ReadOnly AsReadOnly()
        {
            this.Deconstruct(out CancellationToken tokenValue, out TimeSpan timeoutValue);
            return new ReadOnly(tokenValue, timeoutValue);
        }
        public record ReadOnly(CancellationToken TokenValue, TimeSpan TimeoutValue);
    }
}


/* ************************************************************
 *
 *    @author Couchbase <info@couchbase.com>
 *    @copyright 2021 Couchbase, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 * ************************************************************/
