using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FHTRUE.Bigdata.Streamer
{
    class JobConfiguration
    {
        public int JobId { get; set; }
        public Source Source { get; set; }
        public Channel Channel { get; set; }
        public List<Sink> Sinks { get; set; }
    }
    class Source
    {
        public string StreamName { get; set; }
        public string StreamType { get; set; }
        public string EndPoint { get; set; }
        public string UserName { get; set; }
        public string Password { get; set; }
    }

    public enum SinkType { AzureServiceBusEventHub = 1, BlobStorage=2 }

    class Sink
    {
        public SinkType Type { get; set; }
        public bool Backup { get; set; }
        private string _location;
        public string Location
        {
            get
            {
                DateTime now = DateTime.UtcNow;
                var dateformat = now.ToString("yyyy:MM:dd");
                var tokens = dateformat.Split(':');

                return _location.Replace("{YEAR}", tokens[0]).Replace("{MONTH}", tokens[1]).Replace("{DAY}", tokens[2]);
            }
            set
            {
                _location = value;
            }
        }
    }
    class AzureServiceBusEventHub: Sink
    {
        public string RootConnection { get; set; }
        public string SendConnection { get; set; }
        public string Namespace { get; set; }
        public string Name { get; set; }
    }
    class BlobStorage : Sink
    {
        public string Connection { get; set; }
        public string Name { get; set; }
    }
    class Channel
    {
        public int MessageSize { get; set; }
        public Batch Batch { get; set; }
        public int Retry { get; set; }
        public int SleepTime { get; set; }
    }
    class Batch
    {
        public bool Sized { get; set; }
        public UInt32 Size { get; set; }
    }
    class Metadata
    {
        public string ProcessingStatus { get; set; }
        public int RowCount { get; set; }
        public string ThreadName { get; set; }
    }
}
