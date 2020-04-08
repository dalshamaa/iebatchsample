namespace IEBatchWrapper
{
    public class SqlPackageParameters
    {
        // Action can be Export or Import.
        public string Action { get; set; }

        // Common Parameters for Import and Export
        // The server name with the full domain.
        public string ServerName { get; set; }
        
        // Database Name
        public string DatabaseName { get; set; }

        // SqlServer admin user name
        public string SqlServerAdmin { get; set; }

        // SqlServer admin password
        public string SqlServerAdminPassword { get; set; }

        // Export only parameters
        // The bacpac file to export to.
        public string TargetFileName { get; set; }

        // The container name that contains the bacpac file to export to.
        public string TargetContainerName { get; set; }

        // Import only parameters
        // The bacpac file to import from.
        public string SourceFileName { get; set; }

        // The container name that contains the bacpac file to import from.
        public string SourceContainerName { get; set; }
    }
}
