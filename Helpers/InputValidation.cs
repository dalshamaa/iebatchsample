using System;

namespace IEBatchWrapper.Helpers
{
    internal class InputValidation
    {
        private static string publicServerDomain = ".database.windows.net";

        internal static void ValidateExportInput(SqlPackageParameters p)
        {
            if(p == null)
            {
                throw new ArgumentNullException("sqlpackage parameters were not parsable from the json");
            }
            if(string.IsNullOrEmpty(p.Action))
            {
                throw new ArgumentNullException("Action was not specified. Accepted values are: [Export/Import]");
            }
            if(string.IsNullOrEmpty(p.ServerName) | !p.ServerName.EndsWith(InputValidation.publicServerDomain))
            {
                throw new ArgumentNullException("The server was either not specified or it's missing the domain name.");
            }
            if(string.IsNullOrEmpty(p.DatabaseName))
            {
                throw new ArgumentNullException("The database name was not specified.");
            }
            if(string.IsNullOrEmpty(p.TargetContainerName))
            {
                throw new ArgumentNullException("The target container name to save the bacpac to was not specified.");
            }
            if(string.IsNullOrEmpty(p.TargetFileName))
            {
                throw new ArgumentNullException("The target bacpac file name was not specified.");
            }
        }

        internal static void ValidateImportInput(SqlPackageParameters p)
        {
            if (p == null)
            {
                throw new ArgumentNullException("sqlpackage parameters were not parsable from the json");
            }
            if (string.IsNullOrEmpty(p.Action))
            {
                throw new ArgumentNullException("Action was not specified. Accepted values are: [Export/Import]");
            }
            if (string.IsNullOrEmpty(p.ServerName) | !p.ServerName.EndsWith(publicServerDomain))
            {
                throw new ArgumentNullException("The server was either not specified or it's missing the domain name.");
            }
            if (string.IsNullOrEmpty(p.DatabaseName))
            {
                throw new ArgumentNullException("The database name was not specified.");
            }
            if (string.IsNullOrEmpty(p.SourceContainerName))
            {
                throw new ArgumentNullException("The source container name containging the bacpac file was not specified.");
            }
            if (string.IsNullOrEmpty(p.SourceFileName))
            {
                throw new ArgumentNullException("The source bacpac file name was not specified.");
            }
        }
        internal static bool ValidateAccounts(string batchAccountName, string batchAccountKey, string batchAccountUrl, string storageAccountName, string storageAccountKey)
        {
            if (String.IsNullOrEmpty(batchAccountName) ||
                String.IsNullOrEmpty(batchAccountKey) ||
                String.IsNullOrEmpty(batchAccountUrl) ||
                String.IsNullOrEmpty(storageAccountName) ||
                String.IsNullOrEmpty(storageAccountKey))
            {
                return false;
            }

            return true;
        }
    }
}
