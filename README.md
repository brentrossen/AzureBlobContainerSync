AzureBlobContainerSync
======================

BlobContainerSynchronizer and BlobSynchronizer manage keeping local files up to date with what's stored in your Azure Storage container/blob. The system uses the .NET 4.5+ async-await pattern to keep the thread requirements of the synchronization lightweight.

# Getting Started
1) Reference the BlobContainerSynchronizer.dll either by referencing the NuGet package (https://www.nuget.org/packages/AzureBlobContainerSync/) or cloning the repo and building the code.

2) Create a BlobSynchronizer or ContainerSynchronizer

```
var blobSynchronizer = new BlobSynchronizer(storageConnectionString, containerName, 
	blobName, filePath)
{
	BlobSyncResultAction = 
		result => Console.WriteLine(
			"Blob Downloaded (single blob sync) -- {0}", result)
};
```

```
var containerSynchronizer = new ContainerSynchronizer(storageConnectionString, 
	containerName, containerDestinationDir)
{
	BlobSyncResultAction =
		result => Console.WriteLine(
			"Blob Downloaded (container sync) -- {0}", result)
};
```

3) Start synchronizing

```
// Checks for new data every 10 seconds
// This will not return unless there is an exception
await blobSynchronizer.SyncPeriodicAsync(
	delayBetweenSynchronizations: TimeSpan.FromSeconds(10));
```

```
// Checks for new data every 10 seconds
// This will not return unless there is an exception
await containerSynchronizer.SyncPeriodicAsync(
	delayBetweenSynchronizations: TimeSpan.FromSeconds(10));
```

That's it. Now your local data will stay up to date whenever the cloud data is updated.

## Sample Test Code
See the BlobContainerSynchronizerConsoleTest for a test sample. The test sample demonstrates using the library for blob synchronization and randomly updates blobs in a container to demonstrate updates happening asynchronously.