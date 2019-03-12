﻿using System;

namespace Couchbase
{
    public class ScopeMissingException : Exception
    {
        public ScopeMissingException()
        {
        }

        public ScopeMissingException(string message) : base(message)
        {
        }

        public ScopeMissingException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
