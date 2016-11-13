using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Azure.Devices;
using Newtonsoft.Json;
using System.Configuration;

namespace CloudMessageProcessor
{
    class IOTEventProcessor : IEventProcessor
    {
        ServiceClient serviceClient;
        string storageConnectionString = null;
        string iotHubconnectionString = null;
        string blobContainerName = null;

        public IOTEventProcessor()
        {
            try
            {
                storageConnectionString = ConfigurationManager.AppSettings["StorageConnectionString"].ToString();
                iotHubconnectionString = ConfigurationManager.AppSettings["IoTHubConnectionString"].ToString();

                //Create ServiceClient from the iotHubconnectionString
                serviceClient = ServiceClient.CreateFromConnectionString(iotHubconnectionString);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            return Task.FromResult<object>(null);
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            return Task.FromResult<object>(null);
        }

        async Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            foreach (EventData eventData in messages)
            {
                byte[] data = eventData.GetBytes();

                if (eventData.Properties.ContainsKey("messageType") && (string)eventData.Properties["messageType"] == "deviceready")
                {
                    var msgStr = System.Text.Encoding.Default.GetString(data);
                    dynamic msgData = JsonConvert.DeserializeObject(msgStr);
                    var deviceName = Convert.ToString(msgData.DeviceName);
                    var sessionId = Convert.ToString(msgData.SessionId);
                    var blobs = Convert.ToString(msgData.BlobNames);
                    var blobNames = JsonConvert.DeserializeObject(blobs, typeof(List<string>));

                    Console.WriteLine(string.Format("Message received.  Session Id: '{0}'", sessionId));
                    await SendCloudToDeviceMessageAsync(sessionId, blobNames, deviceName);
                    await context.CheckpointAsync(eventData);
                }
                else if (eventData.Properties.ContainsKey("messageType") && (string)eventData.Properties["messageType"] == "machineData")
                {
                    var msgStr = System.Text.Encoding.Default.GetString(data);
                    dynamic msgData = JsonConvert.DeserializeObject(msgStr);
                    var deviceName = Convert.ToString(msgData.DeviceName);
                    var sessionId = Convert.ToString(msgData.SessionId);
                    var blobUrl = Convert.ToString(msgData.BlobUrl);
                    var Data = Convert.ToString(msgData.MachineData);
                    StorageClient.UploadFileToBlobAsync(blobUrl, Data);
                }
            }
        }

        private async Task SendCloudToDeviceMessageAsync(string sessionId, List<string> blobNames, string deviceName)
        {
            string uri = GenerateBlobUriAsync(sessionId, blobNames);
            var msgData = new
            {
                messageType = "fileupload",
                fileUri = uri,
                blobContainerName = blobContainerName
            };
            var messageString = JsonConvert.SerializeObject(msgData);
            var commandMessage = new Message(Encoding.ASCII.GetBytes(messageString));
            commandMessage.Properties["messageType"] = "fileupload";
            commandMessage.Properties["fileUri"] = uri;
            commandMessage.Ack = DeliveryAcknowledgement.None;

            Console.WriteLine(string.Format("Sending message to device with the file upload blob URIs for session id {0}...", sessionId));

            await serviceClient.SendAsync(deviceName, commandMessage);
        }

        private string GenerateBlobUriAsync(string sessionId, List<string> blobNames)
        {
            //DateTime now = DateTime.Now;
            //blobContainerName = now.Date.ToString("ddMMMyyyy").ToLower() + now.Hour.ToString() + now.Minute.ToString() + now.Second.ToString();
            int blobContainerCount = Properties.Settings.Default.BlobContainerCount;
            blobContainerCount++;
            if (blobContainerCount < 10)
                blobContainerName = "00" + blobContainerCount.ToString();
            else if (blobContainerCount < 100)
                blobContainerName = "0" + blobContainerCount.ToString();
            else
                blobContainerName = blobContainerCount.ToString();
            Properties.Settings.Default.BlobContainerCount = blobContainerCount;
            Properties.Settings.Default.Save();

            // Create/ get the blob container reference.
            var storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();
            var blobContainer = blobClient.GetContainerReference(blobContainerName);
            if(blobContainer.Exists())
            {
                Parallel.ForEach(blobContainer.ListBlobs(), x => ((CloudBlob)x).Delete());
            }
            else
            {
                blobContainer.Create();
            }
            BlobContainerPermissions perm = new BlobContainerPermissions();
            perm.PublicAccess = BlobContainerPublicAccessType.Container;
            blobContainer.SetPermissions(perm);

            StringBuilder paths = new StringBuilder();
            for (int i = 0; i < blobNames.Count; i++)
            {
                CloudBlockBlob blob = blobContainer.GetBlockBlobReference(string.Format("{0}.csv", blobNames[i].ToUpper()));
                blob.DeleteIfExists();
                SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy();
                sasConstraints.SharedAccessStartTime = DateTime.UtcNow.AddMinutes(-5);
                sasConstraints.SharedAccessExpiryTime = DateTime.UtcNow.AddHours(512);
                sasConstraints.Permissions = SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write;
                string sasBlobToken = blob.GetSharedAccessSignature(sasConstraints);

                paths.Append(blobNames[i]);
                paths.Append("|");
                paths.Append(blob.Uri + sasBlobToken);
                if (i != blobNames.Count - 1)
                    paths.Append("|");
            }
            return paths.ToString();
        }

    }
}
