using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FHTRUE.Bigdata.Streamer
{
    class GnipDisconnectException : Exception
    {
        public string Message { get; set; }
        public GnipDisconnectException (string message)
        {
            this.Message = message;
        }
    }
}
