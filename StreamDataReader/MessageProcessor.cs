using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;

namespace FHTRUE.Bigdata.Streamer
{
    class MessageProcessor
    {
        public string message;
        private MessageProcessorCallback callback;
        private List<Sink> Sinks;
        public Metadata metadata;

        public MessageProcessor(string message, List<Sink> sinks, MessageProcessorCallback callbackDelegate, Metadata metadata)
        {
            this.message = message;
            Sinks = sinks;
            callback = callbackDelegate;
            this.metadata = metadata;
        }
        public void BackupProcess()
        {
            Console.WriteLine("[Backup Thread-{0}]: processing message...{1}", Thread.CurrentThread.Name, message.Length);
            //Thread.Sleep(1000);

            foreach (var sink in Sinks)
            {
                switch (sink.Type)
                {
                    case SinkType.AzureServiceBusEventHub:
                        
                        break;
                    case SinkType.BlobStorage:

                        var name = "tweets_thread_" + Thread.CurrentThread.Name + ".txt";
                        var dir = @"backup\"+sink.Location;
                        if (!Directory.Exists(dir))
                        {
                            Directory.CreateDirectory(dir);
                        }
                        using (var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(message)))
                        {

                            using (FileStream file = new FileStream(dir+@"\"+name, FileMode.Create, FileAccess.Write))
                            {
                                memoryStream.WriteTo(file);
                            }
                        }
                        break;

                }
            }
            if (callback != null)
                callback(string.Format("[Backup Thread-{0}] has processed data", Thread.CurrentThread.Name));
        }
        public void Process()
        {
            Console.WriteLine("[Thread-{0}]: processing message...{1}", Thread.CurrentThread.Name, message.Length );
            foreach (var sink in Sinks)
            {
                switch (sink.Type)
                {
                    case SinkType.AzureServiceBusEventHub:
                        var azureEventHubSink = (AzureServiceBusEventHub)sink;
                        var eventHubClient = EventHubClient.CreateFromConnectionString(azureEventHubSink.SendConnection, azureEventHubSink.Name);

                        try
                        {
                            eventHubClient.Send(new EventData(Encoding.UTF8.GetBytes(message)));
                        }
                        catch (Exception exception)
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine("{0} > Exception: {1}", DateTime.Now.ToString(), exception.Message);
                            Console.ResetColor();
                        }
                        break;
                    case SinkType.BlobStorage:
                        var azureblobSink = (BlobStorage)sink;
                        CloudStorageAccount storageAccount = CloudStorageAccount.Parse(azureblobSink.Connection);
                        // Create the blob client.
                        CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                        // Retrieve a reference to a container. 
                        CloudBlobContainer container = blobClient.GetContainerReference(azureblobSink.Name);
                        // Create the container if it doesn't already exist.
                        container.CreateIfNotExists(BlobContainerPublicAccessType.Container);
                        // Retrieve reference to a blob named ""data/tweets_thread_"+Thread.CurrentThread.Name+".txt"".
                        var suffix = DateTime.UtcNow.ToString("yyyyMMdd.HHmmss.ffffff");
                        CloudBlockBlob blockBlob = container.GetBlockBlobReference( azureblobSink.Location +"/"+Thread.CurrentThread.Name+"-"+ suffix +".txt");
                        blockBlob.Metadata.Add("ProcessingStatus", metadata.ProcessingStatus);
                        blockBlob.Metadata.Add("RowCount", string.Empty+metadata.RowCount);
                        blockBlob.Metadata.Add("ThreadName", metadata.ThreadName);
                        // Create or overwrite the "data/tweets.txt" blob with contents from a local file.
                        using (var memoryStream =  new MemoryStream(Encoding.UTF8.GetBytes(message)))
                        {
                            blockBlob.UploadFromStream(memoryStream);
                        } 
                        break;

                }
            }
            if (callback != null)
                callback(string.Format("[Thread-{0}] has processed data", Thread.CurrentThread.Name));
        }

      
    }
}
