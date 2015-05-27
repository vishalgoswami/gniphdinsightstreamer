using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FHTRUE.Bigdata.Streamer
{
    class GnipWarningException : Exception
    {
        public string Message { get; set; }
        public GnipWarningException(string message)
        {
            this.Message = message;
        }
    }
}
