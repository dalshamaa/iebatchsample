using System;
using System.Collections.Generic;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;

namespace IEBatchWrapper
{
    internal class StorageHelpers
    {
        internal static CloudBlobClient blobClient;

        /// <summary>
        /// Creates a blob client
        /// </summary>
        /// <param name="storageAccountName">The name of the storage Account</param>
        /// <param name="storageAccountKey">The key of the storage Account</param>
        /// <returns>CloudBlobClient</returns>
        internal static CloudBlobClient CreateCloudBlobClient(string storageAccountName, string storageAccountKey)
        {
            // Construct the storage account connection string
            string storageConnectionString =
                $"DefaultEndpointsProtocol=https;AccountName={storageAccountName};AccountKey={storageAccountKey}";

            // Retrieve the storage account
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the blob client
            blobClient = storageAccount.CreateCloudBlobClient();
            return blobClient;
        }

        /// <summary>
        /// Checks if the outputContainerName exists and creates the container if it does not exist.
        /// </summary>
        /// <param name="outputContainerName"></param>
        /// <returns>CloudBlobContainer</returns>
        internal static CloudBlobContainer CreateContainerForExportIfNotExists(string outputContainerName)
        {
            CloudBlobContainer container = blobClient.GetContainerReference(outputContainerName);
            container.CreateIfNotExistsAsync().Wait();
            return container;
        }

        /// <summary>
        /// Given an input container name and the bacpac name, the method validates that both exist in the storage account.
        /// </summary>
        /// <param name="inputContainerName"></param>
        /// <returns></returns>
        internal static bool ValidateImportBacpacExists(string inputContainerName, string bacpacName)
        {
            return blobClient.GetContainerReference(inputContainerName).GetBlockBlobReference(bacpacName).Exists();
        }

        internal static string GetExportContainerSasUrl(CloudBlobContainer container)
        {
            // Set the expiry time and permissions for the blob shared access signature. 
            // In this case, no start time is specified, so the shared access signature 
            // becomes valid immediately
            string containerSasToken = container.GetSharedAccessSignature(new SharedAccessBlobPolicy()
            {
                SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddDays(1),
                Permissions = SharedAccessBlobPermissions.Write
            });

            return container.Uri.AbsoluteUri + containerSasToken;
        }

        internal static string GetImportContainerSasUrl(string containerName, string blob)
        {
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            var blobRef = container.GetBlockBlobReference(blob);
            string containerSasToken = container.GetSharedAccessSignature(new SharedAccessBlobPolicy()
            {
                SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddDays(1),
                Permissions = SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.List
            });

            return container.Uri + "/" + blob + containerSasToken;
        }

        internal static ResourceFile GetImportBacpac(string containerName, string blobName)
        {
            string containerSasUrl = StorageHelpers.GetImportContainerSasUrl(containerName, blobName);
            List<ResourceFile> inputFiles = new List<ResourceFile> { };
            return ResourceFile.FromStorageContainerUrl(containerSasUrl, "blobs");
        }
    }
}
