using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace BlobContainerSynchronizer
{
    /// <summary>
    /// Synchronizer for keeping a whole container downloaded to a specified local directory
    /// </summary>
    public class ContainerSynchronizer
    {
        private const string EtagsFileName = "BlobEtags.json";
        private readonly string etagsFilePath;
        private readonly string destinationDirectory;
        private readonly CloudBlobContainer blobContainer;
        private int downloadParallelism = 5;

        /// <summary>
        /// The number of blobs to download in parallel
        /// </summary>
        public int DownloadParallelism
        {
            get { return downloadParallelism; }
            set { downloadParallelism = value; }
        }
        
        /// <summary>
        /// Action to call when a blob is downloaded. Used to update
        /// local state when a blob data is received and to log
        /// blob updates.
        /// </summary>
        public Action<BlobSyncResult> BlobSyncResultAction { get; set; }

        /// <summary>
        /// Constructs a container synchronizer to keep the blobs in the cloud container downloaded to the destination directory
        /// </summary>
        /// <param name="storageConnectionString"></param>
        /// <param name="containerName"></param>
        /// <param name="destinationDirectory"></param>
        public ContainerSynchronizer(string storageConnectionString, string containerName, string destinationDirectory)
        {
            if (storageConnectionString == null) throw new ArgumentNullException("storageConnectionString");
            if (containerName == null) throw new ArgumentNullException("containerName");
            if (destinationDirectory == null) throw new ArgumentNullException("destinationDirectory");

            this.destinationDirectory = destinationDirectory;
            var cloudStorageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            cloudBlobClient.DefaultRequestOptions = new BlobRequestOptions
            {
                RetryPolicy = new ExponentialRetry()
            };
            blobContainer = cloudBlobClient.GetContainerReference(containerName);
            etagsFilePath = Path.Combine(destinationDirectory, EtagsFileName);
        }

        /// <summary>
        /// Syncs the container to disk every <paramref name="delayBetweenSynchronizations"/>.
        /// Continuously performs the synchronization unless an exception is thrown.
        /// </summary>
        /// <param name="delayBetweenSynchronizations">The time between synchronization checks for blob updates.</param>
        public async Task SyncPeriodicAsync(TimeSpan delayBetweenSynchronizations)
        {
            while (true)
            {
                await SyncAsync();
                Debug.WriteLine("Waiting {0} until next synchronization", delayBetweenSynchronizations);
                await Task.Delay(delayBetweenSynchronizations);
            }
        }

        /// <summary>
        /// Performs a single synchronization of the blobs in the container to the given directory
        /// </summary>
        /// <returns>Task to await on for asynchronous completion</returns>
        public async Task SyncAsync()
        {
            Debug.WriteLine("Synchronizing '{0}' to '{1}'", blobContainer.Name, destinationDirectory);
            BlobContinuationToken continuationToken = null;
            Etags etags = await Etags.ReadEtagsAsync(etagsFilePath);
            do
            {
                if (continuationToken != null)
                {
                    Debug.WriteLine("Downloading next chunk of blob list items");                    
                }

                BlobResultSegment listBlobsResultSegment =
                    await
                        blobContainer.ListBlobsSegmentedAsync(string.Empty, true, BlobListingDetails.Metadata, DownloadParallelism, continuationToken, null,
                            new OperationContext());

                continuationToken = listBlobsResultSegment.ContinuationToken;

                var cloudBlobs = listBlobsResultSegment.Results.OfType<ICloudBlob>();
                
                await DownloadBlobsAsync(cloudBlobs, etags);
                
                await etags.SaveEtagsAsync(etagsFilePath);
            } while (continuationToken != null);
        }

        /// <summary>
        /// Downloads the blobs in the list to disk if they have etags that are newer than 
        /// the ones in the <paramref name="etags"/>
        /// </summary>
        private async Task DownloadBlobsAsync(IEnumerable<ICloudBlob> cloudBlobs, Etags etags)
        {
            var downloadTasks = new List<Task>();
            var stopwatch = Stopwatch.StartNew();
            foreach (var blob in cloudBlobs)
            {
                string etag = blob.Properties.ETag;
                if (etags.IsNewEtagAndUpdate(blob.Name, etag))
                {
                    Debug.WriteLine("Downloading blob '{0}'", blob.Uri);
                    var downloadtask = DownloadBlob(blob);
                    downloadTasks.Add(downloadtask);
                }
                else
                {
                    Debug.WriteLine("Blob '{0}' is already downloaded", blob.Uri);
                }
            }

            await Task.WhenAll(downloadTasks);

            Debug.WriteLine("It took {0} to download {1} files", stopwatch.Elapsed, downloadTasks.Count);
        }

        private async Task DownloadBlob(ICloudBlob blob)
        {
            var filePath = Path.Combine(destinationDirectory, blob.Name);
            var stopwatch = Stopwatch.StartNew();
            await blob.DownloadToFileAsync(filePath, FileMode.OpenOrCreate);
            stopwatch.Stop();
            InvokeBlobSyncResultAction(blob, filePath, stopwatch.Elapsed);
        }

        private void InvokeBlobSyncResultAction(ICloudBlob blob, string filePath, TimeSpan elapsed)
        {
            if (BlobSyncResultAction == null) return;

            BlobSyncResultAction(
                new BlobSyncResult
                {
                    BlobUri = blob.Uri,
                    BlobLastModifiedTime = blob.Properties.LastModified,
                    BlobSizeInBytes = blob.Properties.Length,
                    FilePath = filePath,
                    TimeToDownload = elapsed
                });
        }
    }
}
