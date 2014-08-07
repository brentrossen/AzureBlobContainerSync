using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace BlobContainerSynchronizer
{
    /// <summary>
    /// Manages the synchronization of a single blob. 
    /// </summary>
    public class BlobSynchronizer
    {
        private const string EtagFilePostFix = "BlobEtags.json";
        private readonly string destinationFilePath;
        private readonly string etagFilePath;
        private readonly ICloudBlob cloudBlob;      

        /// <summary>
        /// Action to call when the blob is downloaded. Used to update
        /// local state when a blob data is received and to log
        /// blob updates.
        /// </summary>
        public Action<BlobSyncResult> BlobSyncResultAction { get; set; }

        /// <summary>
        /// Constructs a blob synchronizer instance
        /// </summary>
        /// <param name="storageConnectionString">The connection string for the storage account where the blob is located</param>
        /// <param name="containerName">The container holding the target blob</param>
        /// <param name="blobName">The name of the blob to synchronize</param>
        /// <param name="destinationFilePath">The local file path to store the blob (the etag file will also be placed in the same folder)</param>
        public BlobSynchronizer(string storageConnectionString, string containerName, string blobName, string destinationFilePath)
        {
            if (storageConnectionString == null) throw new ArgumentNullException("storageConnectionString");
            if (containerName == null) throw new ArgumentNullException("containerName");
            if (blobName == null) throw new ArgumentNullException("blobName");
            if (destinationFilePath == null) throw new ArgumentNullException("destinationFilePath");

            this.destinationFilePath = destinationFilePath;
            var cloudStorageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            cloudBlobClient.DefaultRequestOptions = new BlobRequestOptions
            {
                // Use exponential retry to backoff if there is throttling due to too many simultaneous downloads on the same blob
                RetryPolicy = new ExponentialRetry() 
            };
            var blobContainer = cloudBlobClient.GetContainerReference(containerName);
            cloudBlob = blobContainer.GetBlobReferenceFromServer(blobName);

            // Get the directory where the file is going to be stored and store the etags there as well
            string directoryName = Path.GetDirectoryName(destinationFilePath);

            // Name the etags file based on the blob name
            string etagsFileName = blobName + EtagFilePostFix;
            etagFilePath = !string.IsNullOrEmpty(directoryName)
                ? Path.Combine(directoryName, etagsFileName)
                : etagsFileName;
        }

        /// <summary>
        /// Syncs the blob to disk every <paramref name="delayBetweenSynchronizations"/>.
        /// Does not return.
        /// </summary>
        /// <param name="delayBetweenSynchronizations">The frequency to check for blob updates.</param>
        public async Task SyncPeriodicAsync(TimeSpan delayBetweenSynchronizations)
        {
            while (true)
            {
                await SynchronizeBlobsAsync();
                Debug.WriteLine("Waiting {0} until next synchronization on {1}", delayBetweenSynchronizations, cloudBlob.Name);
                await Task.Delay(delayBetweenSynchronizations);
            }
        }

        /// <summary>
        /// Synchronizes the blobs in the container to the given directory
        /// </summary>
        /// <returns>Task to await on for asynchronous completion</returns>
        public async Task SynchronizeBlobsAsync()
        {
            Debug.WriteLine("Synchronizing '{0}' to '{1}'", cloudBlob.Name, destinationFilePath);
            Etags etags = await Etags.ReadEtagsAsync(etagFilePath);
            await cloudBlob.FetchAttributesAsync();
            await DownloadBlobAsync(etags);
            await etags.SaveEtagsAsync(etagFilePath);
        }

        /// <summary>
        /// Downloads the blob to disk if it has an etag newer than 
        /// the one in the <paramref name="etags"/>
        /// </summary>
        private async Task DownloadBlobAsync(Etags etags)
        {
            string etag = cloudBlob.Properties.ETag;
            if (etags.IsNewEtagAndUpdate(cloudBlob.Name, etag))
            {
                Debug.WriteLine("Downloading blob '{0}'", cloudBlob.Uri);
                await DownloadBlobAsync(cloudBlob);
            }
            else
            {
                Debug.WriteLine("Blob '{0}' is already downloaded", cloudBlob.Uri);
            }
        }

        /// <summary>
        /// Downloads the cloud blob to the destination file path
        /// </summary>
        private async Task DownloadBlobAsync(ICloudBlob blob)
        {
            var stopwatch = Stopwatch.StartNew();
            await blob.DownloadToFileAsync(destinationFilePath, FileMode.OpenOrCreate);
            stopwatch.Stop();
            Debug.WriteLine("Downloaded '{0}' in {1}", cloudBlob.Name, stopwatch.Elapsed);
            InvokeBlobSyncResultAction(blob, destinationFilePath, stopwatch.Elapsed);
        }

        private void InvokeBlobSyncResultAction(ICloudBlob blob, string filePath, TimeSpan elapsed)
        {
            if (BlobSyncResultAction == null) return;
            Debug.WriteLine("Calling BlobSyncResultAction for blob '{0}'", blob.Uri);
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
