using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using IEBatchWrapper.Helpers;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Storage.Blob;

namespace IEBatchWrapper
{
    public class Program
    {
        // This assumes you have already created a Batch Account and using your own Azure storage.
        // Save the credentials in AKV or another secret management library and NOT plain text. This is just for demo purposes.

        #region credentials
        // Batch account credentials
        private const string BatchAccountUrl = "";
        private const string BatchAccountName = "";
        private const string BatchAccountKey = "";

        // StorageHelpers account credentials for an account that has no firewall rules and Hierarchial namespace disabled.
        private const string StorageAccountName = "";
        private const string StorageAccountKey = "";

        // Defaults
        private const int DefaultMaxWallClockTime = 12;
        private const int DefaultMaxTaskRetryCount = 3;

        #endregion

        public static void Usage()
        {
            Console.WriteLine("Usage: IEBatchWrapper.exe fileName MaxWallClockTime MaxTaskRetryCount");
            Console.WriteLine("fileName <string>: The Import or Export json file.");
            Console.WriteLine("MaxWallClockTime <int>: The number of hours the job is allowed to run for. Defaults to 12.");
            Console.WriteLine("MaxTaskRetryCount <int>: The number of retries a task is allowed to rerun in case of failures. Defaults to 3.");
        }

        static void Main(string[] args)
        {
            string parameterFile = string.Empty;
            TimeSpan maxWallClockTime;
            int maxTaskRetryCount;

            try
            {
                if (args == null || args.Length != 3)
                {
                    Usage();
                    throw new ArgumentException("Arguments are missing. Aborting");
                }
                else
                {
                    parameterFile = args[0];
                    maxWallClockTime = Int32.TryParse(args[1], out int numTimeoutHours) ? TimeSpan.FromHours(numTimeoutHours) : TimeSpan.FromHours(DefaultMaxWallClockTime);
                    maxTaskRetryCount = Int32.TryParse(args[2], out maxTaskRetryCount) ? maxTaskRetryCount : DefaultMaxTaskRetryCount;
                    Console.WriteLine("File name = " + parameterFile);
                    Console.WriteLine("maxWallClockTime = " + maxWallClockTime);
                    Console.WriteLine("maxTaskRetryCount = " + maxTaskRetryCount);
                }

                // Verify Batch/StorageHelpers account credentials have been included.
                // This can be expanded to verify that the credentials are correct by attempting a connection to Batch and checking if we can read/write to the storage account.
                if (!InputValidation.ValidateAccounts(BatchAccountName, BatchAccountKey, BatchAccountUrl, StorageAccountName, StorageAccountKey))
                {
                    throw new InvalidOperationException("One or more account credential strings have not been populated. Please ensure that your Batch and StorageHelpers account credentials have been specified.");
                }

                // Extract parameters
                string jsonString = File.ReadAllText(parameterFile);
                SqlPackageParameters p = JsonSerializer.Deserialize<SqlPackageParameters>(jsonString);

                if (p.Action.Equals("Export", StringComparison.InvariantCultureIgnoreCase))
                {
                    InputValidation.ValidateExportInput(p);
                    PerformExportOperation(p, maxWallClockTime, maxTaskRetryCount);
                }
                else if (p.Action.Equals("Import", StringComparison.InvariantCultureIgnoreCase))
                {
                    InputValidation.ValidateImportInput(p);
                    PerformImportOperation(p, maxWallClockTime, maxTaskRetryCount);
                }
                else
                {
                    throw new InvalidOperationException("Only Import and Export are allowed as actions.");
                }
            }
            catch(Exception e)
            {
                Console.WriteLine("Exception was thrown, message = " + e.Message);
            }
           
        }

        private static void PerformExportOperation(SqlPackageParameters parameters, TimeSpan maxWallClockTime, int maxTaskRetryCount)
        {
            try
            {
                // Create the blob client, for use in obtaining references to blob storage containers.
                CloudBlobClient blobClient = StorageHelpers.CreateCloudBlobClient(StorageAccountName, StorageAccountKey);

                // Use the blob client to create the output container in Azure StorageHelpers which will hold the bacpac.
                CloudBlobContainer container = StorageHelpers.CreateContainerForExportIfNotExists(parameters.TargetContainerName);
                string containerSasUrl = StorageHelpers.GetExportContainerSasUrl(container);

                // Get a Batch client using account credentials.
                // This can be replaced with authentication via a certificate or by giving the app permissions to authenticate against the Batch account. Doc at https://docs.microsoft.com/en-us/azure/batch/batch-aad-auth
                BatchSharedKeyCredentials credentials = new BatchSharedKeyCredentials(BatchAccountUrl, BatchAccountName, BatchAccountKey);

                using (BatchClient batchClient = BatchClient.Open(credentials))
                {
                    // Create a Windows Server image, VM configuration, Batch pool
                    ImageReference imageReference = AzureBatchHelpers.CreateImageReference();
                    VirtualMachineConfiguration vmConfiguration = AzureBatchHelpers.CreateVirtualMachineConfiguration(imageReference);
                    AzureBatchHelpers.CreateBatchPool(batchClient, vmConfiguration);

                    // Create a Batch job
                    Console.WriteLine("Creating job [{0}]...", Constants.BatchVariableNames.JobId);

                    try
                    {
                        CloudJob job = batchClient.JobOperations.CreateJob();
                        job.Id = Constants.BatchVariableNames.JobId;
                        job.PoolInformation = new PoolInformation { PoolId = Constants.BatchVariableNames.PoolId };

                        job.Constraints = new JobConstraints
                        {
                            MaxWallClockTime = maxWallClockTime,
                            MaxTaskRetryCount = maxTaskRetryCount
                        };

                        job.Commit();
                    }
                    catch (BatchException be)
                    {
                        // Accept the specific error code JobExists as that is expected if the job already exists
                        if (be.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.JobExists)
                        {
                            Console.WriteLine("The job {0} already existed when we tried to create it", Constants.BatchVariableNames.JobId);
                        }
                        else
                        {
                            throw;
                        }
                    }

                    // Create a collection to hold the tasks that we'll be adding to the job
                    List<CloudTask> tasks = new List<CloudTask>();

                    // Create an export task 
                    string command = $"cmd /c %AZ_BATCH_APP_PACKAGE_{Constants.BatchVariableNames.AppPackageId.ToUpper()}#{Constants.BatchVariableNames.AppPackageVersion}%\\sqlpackage.exe ";
                    string arguments = $"/a:Export /ssn:{parameters.ServerName} /sdn:{parameters.DatabaseName} /su:{parameters.SqlServerAdmin} /sp:{parameters.SqlServerAdminPassword} /tf:{parameters.TargetFileName}";

                    string fullcmd = command + arguments;
                    
                    // Creating one cloud task here but multiple can be created if necessary.
                    CloudTask task = new CloudTask("export", fullcmd)
                    {
                        OutputFiles = new List<OutputFile>
                        {
                            new OutputFile(
                                filePattern: parameters.TargetFileName,
                                destination: new OutputFileDestination(
                            new OutputFileBlobContainerDestination(
                                        containerUrl: containerSasUrl,
                                        path: parameters.TargetFileName )),
                                uploadOptions: new OutputFileUploadOptions(
                                uploadCondition: OutputFileUploadCondition.TaskCompletion))
                        }
                    };
                    task.ApplicationPackageReferences = new List<ApplicationPackageReference>
                    {
                        new ApplicationPackageReference
                        {
                            ApplicationId = Constants.BatchVariableNames.AppPackageId,
                            Version = Constants.BatchVariableNames.AppPackageVersion
                        }
                    };

                    tasks.Add(task);

                    batchClient.JobOperations.AddTask(Constants.BatchVariableNames.JobId, tasks);

                    // Monitor task success/failure, specifying a maximum amount of time to wait for the tasks to complete.
                    TimeSpan timeout = TimeSpan.FromMinutes(5);
                    Console.WriteLine("Monitoring all tasks for 'Completed' state, timeout in {0}...", timeout);

                    IEnumerable<CloudTask> addedTasks = batchClient.JobOperations.ListTasks(Constants.BatchVariableNames.JobId);
                    batchClient.Utilities.CreateTaskStateMonitor().WaitAll(addedTasks, TaskState.Completed, timeout);

                    Console.WriteLine("All tasks reached state Completed.");

                    // Print task output
                    Console.WriteLine();
                    Console.WriteLine("Printing task output...");

                    IEnumerable<CloudTask> completedtasks = batchClient.JobOperations.ListTasks(Constants.BatchVariableNames.JobId);

                    // If cleanup is needed, it can be accomplished as follows
                    // to clean up the job, 
                    // batchClient.JobOperations.DeleteJob(Constants.BatchVariableNames.JobId);
                    // to clean up the pool, 
                    // batchClient.PoolOperations.DeletePool(Constants.BatchVariableNames.PoolId);
                }
            }
            catch(Exception e)
            {
                throw e;
            }
        }

        private static void PerformImportOperation(SqlPackageParameters parameters, TimeSpan maxWallClockTime, int maxTaskRetryCount)
        {
            try
            {
                // Create the blob client, for use in obtaining references to blob storage containers.
                CloudBlobClient blobClient = StorageHelpers.CreateCloudBlobClient(StorageAccountName, StorageAccountKey);

                // Verify container and bacpac exist in the storage account.
                if (!StorageHelpers.ValidateImportBacpacExists(parameters.SourceContainerName, parameters.SourceFileName))
                {
                    throw new Exception("bacpac does not already exist in the storage account so an import cannot be performed.");
                }

                ResourceFile file = StorageHelpers.GetImportBacpac(parameters.SourceContainerName, parameters.SourceFileName);
                var inputFiles = new List<ResourceFile>();
                inputFiles.Add(file);

                // Get a Batch client using account credentials
                BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(BatchAccountUrl, BatchAccountName, BatchAccountKey);

                using (BatchClient batchClient = BatchClient.Open(cred))
                {
                    // Create a Windows Server image, VM configuration, Batch pool
                    ImageReference imageReference = AzureBatchHelpers.CreateImageReference();
                    VirtualMachineConfiguration vmConfiguration = AzureBatchHelpers.CreateVirtualMachineConfiguration(imageReference);
                    AzureBatchHelpers.CreateBatchPool(batchClient, vmConfiguration);

                    // Create a Batch job
                    Console.WriteLine("Creating job [{0}]...", Constants.BatchVariableNames.JobId);

                    try
                    {
                        CloudJob job = batchClient.JobOperations.CreateJob();
                        job.Id = Constants.BatchVariableNames.JobId;
                        job.PoolInformation = new PoolInformation { PoolId = Constants.BatchVariableNames.PoolId };

                        job.Constraints = new JobConstraints
                        {
                            MaxWallClockTime = TimeSpan.FromHours(12),
                            MaxTaskRetryCount = 2
                        };

                        job.Commit();
                    }
                    catch (BatchException be)
                    {
                        // Accept the specific error code JobExists as that is expected if the job already exists
                        if (be.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.JobExists)
                        {
                            Console.WriteLine("The job {0} already existed when we tried to create it", Constants.BatchVariableNames.JobId);
                        }
                        else
                        {
                            throw;
                        }
                    }

                    // Create a collection to hold the tasks that we'll be adding to the job
                    List<CloudTask> tasks = new List<CloudTask>();

                    // Create an export task 
                    string command = $"cmd /c %AZ_BATCH_APP_PACKAGE_{Constants.BatchVariableNames.AppPackageId.ToUpper()}#{Constants.BatchVariableNames.AppPackageVersion}%\\sqlpackage.exe ";
                    string arguments = $"/a:Import /tsn:{parameters.ServerName} /tdn:{parameters.DatabaseName} /tu:{parameters.SqlServerAdmin} /tp:{parameters.SqlServerAdminPassword} /sf:{inputFiles[0].FilePath + @"\" + parameters.SourceFileName}";
                    // string arguments = "/a:import /tsn:batchtestdeema.database.windows.net /tdn:importedDb /tu:cloudsa /tp:Yukon900! /sf:" + inputFiles[0].FilePath + @"\export.bacpac";
                    string fullcmd = command + arguments;
                    //CloudTask task = new CloudTask("export", fullcmd);
                    CloudTask task = new CloudTask("import", fullcmd)
                    {
                        ResourceFiles = new List<ResourceFile>
                        {
                            inputFiles[0]
                        }
                    };
                    task.ApplicationPackageReferences = new List<ApplicationPackageReference>
                    {
                        new ApplicationPackageReference
                        {
                            ApplicationId = Constants.BatchVariableNames.AppPackageId,
                            Version = Constants.BatchVariableNames.AppPackageVersion
                        }
                    };

                    tasks.Add(task);

                    batchClient.JobOperations.AddTask(Constants.BatchVariableNames.JobId, tasks);


                    // Monitor task success/failure, specifying a maximum amount of time to wait for the tasks to complete.
                    TimeSpan timeout = TimeSpan.FromMinutes(5);
                    Console.WriteLine("Monitoring all tasks for 'Completed' state, timeout in {0}...", timeout);

                    IEnumerable<CloudTask> addedTasks = batchClient.JobOperations.ListTasks(Constants.BatchVariableNames.JobId);
                    batchClient.Utilities.CreateTaskStateMonitor().WaitAll(addedTasks, TaskState.Completed, timeout);
                    Console.WriteLine("All tasks reached state Completed.");

                    // Print task output
                    Console.WriteLine();
                    Console.WriteLine("Printing task output...");

                    IEnumerable<CloudTask> completedtasks = batchClient.JobOperations.ListTasks(Constants.BatchVariableNames.JobId);

                    // If cleanup is needed, it can be accomplished as follows
                    // to clean up the job, 
                    // batchClient.JobOperations.DeleteJob(Constants.BatchVariableNames.JobId);
                    // to clean up the pool, 
                    // batchClient.PoolOperations.DeletePool(Constants.BatchVariableNames.PoolId);
                }
            }
            catch (Exception e)
            {
                throw e;
            }
        }
    }
}
