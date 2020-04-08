namespace IEBatchWrapper
{
    /// <summary>
    /// Constants for the batch wrapper.
    /// </summary>
    public static class Constants
    {
        /// <summary>
        /// Azure Batch variables.
        /// </summary>
        public static class BatchVariableNames
        {
            /// <summary>
            /// Pool Variables
            /// </summary>
            internal const string PoolId = "DeemaPool";
            internal const int PoolNodeCount = 1;
            internal const string PoolVMSize = "STANDARD_D3_V2";

            /// <summary>
            /// Job ID
            /// </summary>
            internal const string JobId = "importexport";

            /// <summary>
            /// SQL package to use
            /// </summary>
            internal const string AppPackageId = "sqlpackagenetcore";
            internal const string AppPackageVersion = "15.0.4630.1";
        }

        /// <summary>
        /// Environment variable names present or needed during the batch task execution.
        /// </summary>
        public static class EnvironmentVariableNames
        {
            /// <summary>
            /// Path to the directory containing the sqlpackage exe.
            /// </summary>
            internal const string SqlPackageLocation = "AZ_BATCH_APP_PACKAGE_";
        }
    }
}
