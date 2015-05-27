using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.IO.Compression;
using System.Threading;

namespace FHTRUE.Bigdata.Streamer
{
    public delegate void MessageProcessorCallback(string threadResponse);

    class Streamer
    {
        public enum MESSAGE_TYPE { Activities =1, Keep_Alive, No_Data, SystemMessage_Info, SystemMessage_Warn, SystemMessage_Error};

        public static void ResultCallback(string threadResponse)
        {
            Console.WriteLine(threadResponse);
        }

        public static void  GetStream(JobConfiguration jobConfiguration)
        {
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(jobConfiguration.Source.EndPoint);
            request.Method = "GET";
            string credentials = jobConfiguration.Source.UserName + ":" + jobConfiguration.Source.Password;
            request.Headers["Authorization"] = "Basic " + Convert.ToBase64String(Encoding.ASCII.GetBytes(credentials));
            request.Headers["Accept-Encoding"] = "deflate, gzip";

            request.KeepAlive = false; ;
            request.Timeout = 40000; // just above 30 secs for keep-alive

            var batchCounter = jobConfiguration.Channel.Batch.Size;

            var retryConnection = jobConfiguration.Channel.Retry;

            while (retryConnection > 0)
            {
                StringBuilder bufferMessagesToSendBeoreRetryConnection = new StringBuilder();
                MessageProcessor messageProcessorToRunBeforeRetryConnectionToClearBuffer = new MessageProcessor(
                                                string.Empty,
                                                jobConfiguration.Sinks,
                                                new MessageProcessorCallback(ResultCallback),
                                                new Metadata
                                                {
                                                    ProcessingStatus = "Submitted",
                                                    RowCount = jobConfiguration.Channel.MessageSize,
                                                    ThreadName = string.Empty
                                                }
                                            );

                if (retryConnection != jobConfiguration.Channel.Retry) {
                    Console.WriteLine("Going to Retry connection after {0} seconds.", jobConfiguration.Channel.SleepTime / 1000);
                    TimeSpan ts = new TimeSpan(0, 0, 0, jobConfiguration.Channel.SleepTime / 1000, 0);
                    Thread.Sleep(ts);
                }

                try
                {
                    WebResponse response = request.GetResponse();
                    {
                        using (var stream = response.GetResponseStream())
                        {
                            using (var gstream = new GZipStream(stream, CompressionMode.Decompress, false))
                            {
                                var reader = new StreamReader(gstream);
                                {
                                    StringBuilder message = new StringBuilder();
                                    int messageSizeCounter = jobConfiguration.Channel.MessageSize;
                                    var retry = jobConfiguration.Channel.Retry;
                                    var sleepTime = jobConfiguration.Channel.SleepTime;
                                    DateTime st = DateTime.Now; long msgCounter = 0;
                                    bool flushBuffer = false;

                                    while (!jobConfiguration.Channel.Batch.Sized || batchCounter > 0)
                                    {
                                        flushBuffer = false;
                                        TimeSpan span = DateTime.Now.Subtract(st);
                                        var minsElapsed = span.TotalMinutes;
                                        var secsElapsed = span.TotalSeconds;

                                        Console.WriteLine("{1} {0} - Throughput = {2} msg/min {3} msg/sec", st.ToLongTimeString(), DateTime.Now.ToLongTimeString(),
                                                msgCounter / minsElapsed, msgCounter / secsElapsed);

                                        bool ignoreMessage = false;

                                        string currentMessage = string.Empty;
                                        DateTime dtReadRequest = DateTime.Now;
                                        // Threded run of read operation to avoid blocking read. If the read operation takes more than 5 mins then clear the buffered messages
                                        Action asyncAction = () =>
                                        {
                                            currentMessage = reader.ReadLine();
                                        };
                                        bool done = false;
                                        asyncAction.BeginInvoke((res) => done = true, null);
                                        while (!done)
                                        {
                                            Thread.Sleep(10000); // 10 sec wait
                                            TimeSpan waitedFor = DateTime.Now.Subtract(dtReadRequest);
                                            Console.WriteLine("Waiting for message for {0} Hrs...", waitedFor.TotalHours);
                                            //If the read operation is still not returned anything, and we have waited for 5 mins and buffer has some items then clear it.
                                            if (waitedFor.TotalMinutes > 5.00 && message.Length > 0)
                                            {
                                                message.Length--;
                                                var wellFormedMessage = message.ToString();
                                                messageSizeCounter = jobConfiguration.Channel.MessageSize;
                                                message.Clear();

                                                // start the Processor thread
                                                var threadName = jobConfiguration.Source.StreamName + "_" + DateTime.Now.Ticks;
                                                MessageProcessor messageProcessor = new MessageProcessor(
                                                    wellFormedMessage,
                                                    jobConfiguration.Sinks,
                                                    new MessageProcessorCallback(ResultCallback),
                                                    new Metadata
                                                    {
                                                        ProcessingStatus = "Submitted",
                                                        RowCount = jobConfiguration.Channel.MessageSize,
                                                        ThreadName = threadName
                                                    }
                                                );
                                                Thread thread = new Thread(new ThreadStart(messageProcessor.Process));
                                                thread.Name = threadName;
                                                thread.Start();

                                                Thread backupThread = new Thread(new ThreadStart(messageProcessor.BackupProcess));
                                                backupThread.Name = thread.Name + "_backup";
                                                backupThread.Start();
                                            }
                                        }

                                        // Read operation is successfull. Check the type of message now.
                                        var messageType = GetMessageType(currentMessage);
                                        switch (messageType)
                                        {
                                            case MESSAGE_TYPE.SystemMessage_Error:
                                                throw new GnipDisconnectException(currentMessage);
                                            //break;
                                            case MESSAGE_TYPE.SystemMessage_Info:
                                                ignoreMessage = true;
                                                break;
                                            case MESSAGE_TYPE.SystemMessage_Warn:
                                                throw new GnipWarningException(currentMessage);
                                            //break;
                                            case MESSAGE_TYPE.Keep_Alive:
                                                ignoreMessage = true;
                                                retry--;
                                                break;
                                            case MESSAGE_TYPE.Activities:
                                                ignoreMessage = false;
                                                retry = jobConfiguration.Channel.Retry;
                                                break;
                                            case MESSAGE_TYPE.No_Data:
                                                ignoreMessage = true;
                                                retry--;
                                                break;
                                            default:
                                                ignoreMessage = true;
                                                retry--;
                                                break;
                                        }
                                        //if there are no activities for at least 5 retries, then go to sleep for 30 secs and dont hurt outer while loop.
                                        if (retry < 0)
                                        {
                                            // before going to sleep check if there are any buffered messages left which needs to cleared.
                                            if (message.Length > 0)
                                            {
                                                flushBuffer = true; // making it true will clear the buffered messages. using it as just a flag.
                                                messageSizeCounter = -1; // maing it negative will clear the buffered messages.
                                            }
                                            if (flushBuffer == false)
                                            {
                                                Console.WriteLine("Going to sleep mode for {0} seconds.", sleepTime / 1000);
                                                TimeSpan ts = new TimeSpan(0, 0, 0, sleepTime / 1000, 0);
                                                Thread.Sleep(ts);
                                                retry = jobConfiguration.Channel.Retry;
                                            }
                                        }
                                        if (ignoreMessage && !flushBuffer)
                                            continue;

                                        if (!flushBuffer)
                                        {
                                            msgCounter++;
                                            message.Append(currentMessage);
                                            message.Append(Environment.NewLine);
                                            messageSizeCounter--;

                                            bufferMessagesToSendBeoreRetryConnection = message;
                                        }

                                        if (messageSizeCounter < 0)
                                        {
                                            if (batchCounter > 0)
                                                batchCounter--;
                                            message.Length--;
                                            var wellFormedMessage = message.ToString();
                                            messageSizeCounter = jobConfiguration.Channel.MessageSize;
                                            message.Clear();

                                            // start the Processor thread
                                            var threadName = jobConfiguration.Source.StreamName + "_" + DateTime.Now.Ticks;

                                           
                                            MessageProcessor messageProcessor = new MessageProcessor(
                                                wellFormedMessage,
                                                jobConfiguration.Sinks,
                                                new MessageProcessorCallback(ResultCallback),
                                                new Metadata
                                                {
                                                    ProcessingStatus = "Submitted",
                                                    RowCount = jobConfiguration.Channel.MessageSize,
                                                    ThreadName = threadName
                                                }
                                            );
                                            

                                            Thread thread = new Thread(new ThreadStart(messageProcessor.Process));
                                            thread.Name = threadName;
                                            thread.Start();

                                            Thread backupThread = new Thread(new ThreadStart(messageProcessor.BackupProcess));
                                            backupThread.Name = thread.Name + "_backup";
                                            backupThread.Start();
                                            //thread.Join();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                catch (IOException ex)
                {
                    Console.WriteLine("[Exception]: [{0}]: IO Exception {1}/{2}", DateTime.UtcNow.ToLongDateString(), ex.Message, ex.StackTrace);
                    retryConnection = 0;
                    // Abort. Fix the IO Exception
                }
                catch (WebException we)
                {
                    Console.WriteLine("[Exception]: Web Exception, Timeout", we.Message);
                    retryConnection--;
                    if (!string.IsNullOrEmpty(bufferMessagesToSendBeoreRetryConnection.ToString()))
                    {
                        var threadName =jobConfiguration.Source.StreamName + "_" + DateTime.Now.Ticks;
                        messageProcessorToRunBeforeRetryConnectionToClearBuffer.message = bufferMessagesToSendBeoreRetryConnection.ToString();
                        messageProcessorToRunBeforeRetryConnectionToClearBuffer.metadata.ThreadName = threadName;
                        Thread thread = new Thread(new ThreadStart(messageProcessorToRunBeforeRetryConnectionToClearBuffer.Process));
                        thread.Name = threadName;
                        thread.Start();
                        Thread backupThread = new Thread(new ThreadStart(messageProcessorToRunBeforeRetryConnectionToClearBuffer.BackupProcess));
                        backupThread.Name = thread.Name + "_backup";
                        backupThread.Start();
                    }

                    // Reconnect and retry for n times.
                }
                catch (GnipDisconnectException ge)
                {
                    Console.WriteLine("[Exception]: System Error Message {0}", ge.Message);
                    retryConnection--;
                    if (!string.IsNullOrEmpty(bufferMessagesToSendBeoreRetryConnection.ToString()))
                    {
                        var threadName = jobConfiguration.Source.StreamName + "_" + DateTime.Now.Ticks;
                        messageProcessorToRunBeforeRetryConnectionToClearBuffer.message = bufferMessagesToSendBeoreRetryConnection.ToString();
                        messageProcessorToRunBeforeRetryConnectionToClearBuffer.metadata.ThreadName = threadName;
                        Thread thread = new Thread(new ThreadStart(messageProcessorToRunBeforeRetryConnectionToClearBuffer.Process));
                        thread.Name = threadName;
                        thread.Start();
                        Thread backupThread = new Thread(new ThreadStart(messageProcessorToRunBeforeRetryConnectionToClearBuffer.BackupProcess));
                        backupThread.Name = thread.Name + "_backup";
                        backupThread.Start();
                    }
                
                    // Reconnect and retry for n times.
                }
                catch (GnipWarningException gwe)
                {
                    Console.WriteLine("[Exception]: System Warning Message {0}", gwe.Message);
                    retryConnection = 0;
                    // Abort. Fix the warning
                }
                catch (Exception ex)
                {
                    Console.WriteLine("[Exception]: [{0}]: Generic Exception {1}/{2}", DateTime.UtcNow.ToLongDateString(), ex.Message, ex.StackTrace);
                    retryConnection = 0;
                    // Abort. Fix the Exception
                }
            }
        }

        private static MESSAGE_TYPE GetMessageType(string currentMessage)
        {
            if (currentMessage.StartsWith("{\"error\":",StringComparison.OrdinalIgnoreCase))
                return MESSAGE_TYPE.SystemMessage_Error;
            else if (currentMessage.StartsWith("{\"info\":", StringComparison.OrdinalIgnoreCase))
                return MESSAGE_TYPE.SystemMessage_Error;
            else if (currentMessage.StartsWith("{\"warn\":", StringComparison.OrdinalIgnoreCase))
                return MESSAGE_TYPE.SystemMessage_Error;
            else if (currentMessage.StartsWith("\r\n"))
                return MESSAGE_TYPE.Keep_Alive;
            else if (string.IsNullOrEmpty(currentMessage) || currentMessage.Length == 0)
                return MESSAGE_TYPE.No_Data;
            return MESSAGE_TYPE.Activities;
        }

        public static JobConfiguration GetJob()
        {
            var sinks = new List<Sink>();
            sinks.Add(new BlobStorage { 
                Connection = "",
                Name = "testhdinsightcluster",
                Type=SinkType.BlobStorage,
                Backup = false,
                Location = "fhtrue/data/gnip/{YEAR}/{MONTH}/{DAY}/working_0",
            });
            /*sinks.Add(new AzureServiceBusEventHub
            {
                SendConnection = "",
                RootConnection = "",
                Name = "testeventhub",
                Namespace = "testeventhub-ns",
                Type = SinkType.AzureServiceBusEventHub,
                Backup = true
            });*/
            JobConfiguration jobConfiguration = new JobConfiguration
            {
                JobId = 1,
                Source = new Source
                {
                    EndPoint = "https://stream.gnip.com:443/accounts/fleishman/publishers/twitter/streams/track/prod.json?client=1",
                    StreamType = "TwitterGNIPPowerTrack",
                    StreamName = "PowerTrack(prod)",
                    UserName = "",
                    Password = ""
                },
                Sinks = sinks,
                Channel = new Channel { Batch = new Batch { Sized = false, Size = 10 }, MessageSize = 15 , Retry = 5, SleepTime = 30000}
            };
            return jobConfiguration;
        }
      

        /* Entry Point */
        static void Main(string[] args)
        {

            Streamer.GetStream(Streamer.GetJob());
            Console.WriteLine("Press any key to exit..." );
            Console.ReadLine();
        }
    }
}
