using System;
using System.Collections.Generic;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;

namespace IEBatchWrapper.Helpers
{
    internal class AzureBatchHelpers
    {
        internal static void CreateBatchPool(BatchClient batchClient, VirtualMachineConfiguration vmConfiguration)
        {
            try
            {
                Console.WriteLine("Checking if pool already exists [{0}]...", Constants.BatchVariableNames.PoolId);
                if (batchClient.PoolOperations.GetPool(Constants.BatchVariableNames.PoolId) != null)
                {
                    Console.WriteLine("Pool already exists. Skipping.");
                    return;
                }

                CloudPool pool = batchClient.PoolOperations.CreatePool(
                    poolId: Constants.BatchVariableNames.PoolId,
                    targetDedicatedComputeNodes: Constants.BatchVariableNames.PoolNodeCount,
                    virtualMachineSize: Constants.BatchVariableNames.PoolVMSize,
                    virtualMachineConfiguration: vmConfiguration);

                pool.ApplicationPackageReferences = new List<ApplicationPackageReference>
                {
                    new ApplicationPackageReference
                    {
                        ApplicationId = Constants.BatchVariableNames.AppPackageId,
                        Version = Constants.BatchVariableNames.AppPackageVersion
                    }
                };

                pool.Commit();
            }
            catch (BatchException be)
            {
                // Accept the specific error code PoolExists as that is expected if the pool already exists
                if (be.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.PoolExists)
                {
                    Console.WriteLine("The pool {0} already existed when we tried to create it", Constants.BatchVariableNames.PoolId);
                }
                else
                {
                    throw; // Any other exception is unexpected
                }
            }
        }

        internal static VirtualMachineConfiguration CreateVirtualMachineConfiguration(ImageReference imageReference)
        {
            return new VirtualMachineConfiguration(
                imageReference: imageReference,
                nodeAgentSkuId: "batch.node.windows amd64");
        }

        internal static ImageReference CreateImageReference()
        {
            return new ImageReference(
                publisher: "MicrosoftWindowsServer",
                offer: "WindowsServer",
                sku: "2016-datacenter-smalldisk",
                version: "latest");
        }
    }
}
