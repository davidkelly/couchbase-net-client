using System.Net;

namespace Couchbase.Core.Exceptions.Analytics
{
    public class AnalyticsErrorContext : IErrorContext
    {
        public string DispatchedFrom { get; internal set; }

        public string DispatchedTo { get; internal set; }

        public string Statement { get; internal set; }

        public string ContextId { get; internal set; }

        public HttpStatusCode HttpStatus { get; internal set; }

        public string Message { get; internal set; }
    }
}
