using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using BlobContainerSynchronizer;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace BlobContainerSynchronizerConsoleTest
{
    public class BlobContainerSynchronizerTest
    {
        private TimeSpan synchronizationFrequency = TimeSpan.FromSeconds(10);

        /// <summary>
        /// How often to check for updates to the blobs
        /// </summary>
        public TimeSpan SynchronizationFrequency
        {
            get { return synchronizationFrequency; }
            set { synchronizationFrequency = value; }
        }

        /// <summary>
        /// Tests both the container synchronizer and the single blob synchronizer
        /// </summary>
        public async Task RunTest()
        {
#if DEBUG
            var listeners = new TraceListener[] { new TextWriterTraceListener(Console.Out) };
            Debug.Listeners.AddRange(listeners);
#endif
            string storageConnectionString = CloudConfigurationManager.GetSetting("StorageConnectionString");
            const string containerName = "synctest";

            var containerSyncPeriodicTask = StartContainerSyncTask(storageConnectionString, containerName);

            var blobSyncPeriodicTask = StartBlobSyncTask(storageConnectionString, containerName);

            Task uploadBlobsTask = UploadBlobs(storageConnectionString, containerName);

            // Use WhenAny so that an exception in any of the tasks is thrown immediately
            await Task.WhenAny(containerSyncPeriodicTask, blobSyncPeriodicTask, uploadBlobsTask);
        }

        /// <summary>
        /// Downloads of a single blob in the specified container
        /// </summary>
        private async Task StartBlobSyncTask(string storageConnectionString, string containerName)
        {
            string singleBlobDestinationDir = Path.Combine(Environment.CurrentDirectory, "SingleBlobDownloadedFiles");
            Directory.CreateDirectory(singleBlobDestinationDir);
            string blobName = 0.ToString();
            string filePath = Path.Combine(singleBlobDestinationDir, blobName);

            var blobSynchronizer = new BlobSynchronizer(storageConnectionString, containerName, blobName,
                filePath)
            {
                BlobSyncResultAction =
                    result => Console.WriteLine("Blob Downloaded (single blob sync) -- {0}", result)
            };

            // This will not return unless there is an exception
            await blobSynchronizer.SyncPeriodicAsync(SynchronizationFrequency);
        }

        /// <summary>
        /// Downloads the specified container
        /// </summary>
        private async Task StartContainerSyncTask(string storageConnectionString, string containerName)
        {
            string containerDestinationDir = Path.Combine(Environment.CurrentDirectory, "ContainerDownloadedFiles");
            Directory.CreateDirectory(containerDestinationDir);

            var containerSynchronizer = new ContainerSynchronizer(storageConnectionString, containerName,
                containerDestinationDir)
            {
                BlobSyncResultAction =
                    result => Console.WriteLine("Blob Downloaded (container sync) -- {0}", result)
            };

            // This will not return unless there is an exception
            await containerSynchronizer.SyncPeriodicAsync(SynchronizationFrequency);
        }

        /// <summary>
        /// Uploads block blobs into the specified container.
        /// A blob is uploaded every 0-19 seconds
        /// and given a random name 0-9 and value 0-999
        /// </summary>
        private static async Task UploadBlobs(string storageConnectionString, string containerName)
        {
            var random = new Random((int)DateTimeOffset.UtcNow.Ticks);
            var cloudStorageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            cloudBlobClient.DefaultRequestOptions = new BlobRequestOptions
            {
                RetryPolicy = new ExponentialRetry()
            };
            var blobContainer = cloudBlobClient.GetContainerReference(containerName);

            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(random.Next(0, 20)));
                int blobName = random.Next(0, 10);
                Debug.WriteLine("Uploading blob {0}", blobName);
                CloudBlockBlob blob = blobContainer.GetBlockBlobReference(blobName.ToString());
                await blob.UploadTextAsync(random.Next(0, 1000).ToString());
            }
        }
    }
}
