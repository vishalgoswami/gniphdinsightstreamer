using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventHubConsumer
{
    class Program
    {
        public static void Consume()
        {
            var eventHubClient = EventHubClient.CreateFromConnectionString("Endpoint=sb://testeventhubfhtrue-ns.servicebus.windows.net/;SharedAccessKeyName=TestPolicy;SharedAccessKey=Fz9sFMXZoPZn10GDpeORa+OhcFGig0BgakZJWihJaeU=", "testeventhubfhtrue");
            EventHubConsumerGroup group = eventHubClient.GetDefaultConsumerGroup();
            var receiver = group.CreateReceiver(eventHubClient.GetRuntimeInformation().PartitionIds[0]);
            bool receive = true;
            string myOffset;
            while (receive)
            {
                var message = receiver.Receive();
                if (message != null) {
                    myOffset = message.Offset;
                    string body = Encoding.UTF8.GetString(message.GetBytes());
                    Console.WriteLine(String.Format("Received message offset: {0} \nbody: {1}", myOffset, body));
                }
            }
     
        }

        public static void Upload()
        {
            
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=https;AccountName=teststorageaccountfh1;AccountKey=ramrGxXNbp7tdNnL8mF+Se2cwMdh+c4pSsKCdYcoYsvDrinrZw3aW7YVDzxoneZM6pBMJUGsM+8lxIeNkyN+AA==");
            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            // Retrieve a reference to a container. 
            CloudBlobContainer container = blobClient.GetContainerReference("testhdinsightclusterfhtrue");
            // Create the container if it doesn't already exist.
            container.CreateIfNotExists(BlobContainerPublicAccessType.Container);
            // Retrieve reference to a blob named ""data/tweets_thread_"+Thread.CurrentThread.Name+".txt"".
            CloudBlockBlob blockBlob = container.GetBlockBlobReference("HdiSamples/TwitterTrendsSampleData/tweets.txt");
          
            // Create or overwrite the "data/tweets.txt" blob with contents from a local file.
            MemoryStream memoryStream = new MemoryStream(); 
            using (var fileStream = File.OpenRead(@"C:\Users\goswamiv\Downloads\tweets1.txt"))
            {
                fileStream.CopyTo(memoryStream);
                blockBlob.UploadFromStream(memoryStream);
            }
            
        }
        static void Main(string[] args)
        {
            //Program.Consume();
            Program.Upload();
            Console.WriteLine("Press enter to exit ...");
            Console.ReadLine();
        }
    }
}
