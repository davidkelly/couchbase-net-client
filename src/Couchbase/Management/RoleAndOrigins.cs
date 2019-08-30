using System.Collections.Generic;

namespace Couchbase.Management
{
    public class RoleAndOrigins
    {
        public Role Role { get; internal set; }
        public IEnumerable<Origin> Origins { get; internal set; }
    }
}