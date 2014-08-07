using System;

namespace BlobContainerSynchronizer
{
    /// <summary>
    /// The result of a blob synchronization action
    /// </summary>
    public struct BlobSyncResult
    {
        /// <summary>
        /// The uri of the cloud blob
        /// </summary>
        public Uri BlobUri;

        /// <summary>
        /// Total time required to downlod the blob
        /// </summary>
        public TimeSpan TimeToDownload;
        
        /// <summary>
        /// The size of the downloaded blob in bytes
        /// </summary>
        public long BlobSizeInBytes;

        /// <summary>
        /// The last time the blob was modified in Storage
        /// </summary>
        public DateTimeOffset? BlobLastModifiedTime;

        /// <summary>
        /// The path to the file on disk
        /// </summary>
        public string FilePath;

        /// <summary>
        /// Creates a string representing the sync result
        /// </summary>
        public override string ToString()
        {
            return
                string.Format(
                    "BlobSyncResult BlobUri: {0}, TimeToDownload: {1}, BlobSizeInBytes: {2}, BlobLastModifiedTime: {3}, FilePath: {4}",
                    BlobUri, TimeToDownload, BlobSizeInBytes, BlobLastModifiedTime, FilePath);
        }
    }
}