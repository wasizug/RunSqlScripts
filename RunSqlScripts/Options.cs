using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CommandLine;

namespace RunSqlScripts
{
    public class Options
    {
        [Option('c', "connectionstring", Required = false, HelpText = "ConnectionString")]
        public string ConnectionString { get; set; }

        [Option('s', "server", Required = false, HelpText = "Server (set or overwrite \"Server\" in \"connectionstring\" if configured)")]
        public string Server { get; set; }

        [Option('d', "database", Required = false, HelpText = "Database  (set or overwrite \"Database\" or \"Initial Catalog\" in \"connectionstring\" if configured)")]
        public string Database { get; set; }

        [Option('u', "user", Required = false, HelpText = "User ID (set or overwrite \"User ID\" in \"connectionstring\" if configured)")]
        public string UserId { get; set; }

        [Option('p', "password", Required = false, HelpText = "Password (set or overwrite \"Password\" in \"connectionstring\" if configured)")]
        public string Password { get; set; }

        [Option('t', "trustedconnection", Required = false, HelpText = "Trusted_Connection (set 1/0 for true/false) (set or overwrite \"Trusted_Connection\" in \"connectionstring\" if configured)")]
        public string TrustedConnection { get; set; }

        [Option('a', "applicationname", Required = false, DefaultValue = "RunSqlScripts", HelpText = "Application Name (set or overwrite \"Application Name\" in \"connectionstring\" if configured)")]
        public string ApplicationName { get; set; }

        [Option('i', "persistsecurityinfo", Required = false, HelpText = "Persist Security Info (set 1/0 for true/false) (set or overwrite \"Persist Security Info\" in \"connectionstring\" if configured)")]
        public string PersistentSecurityInfo { get; set; }

        [Option('f', "folders", Required = true, HelpText = "Folders to get the SQL-Script-Files. Syntax: \"Foldername&stopOnError(1/0)&recursiveFolders(1/0)\" (each folder-item is separated bei \";\") / Example: \"C:\\folder1&1&0;..\\folder2&0&1\"")]
        public string Folders { get; set; }
        
        [Option('l', "logfile", Required = false, HelpText = "Logfile")]
        public string logFile { get; set; }

        [Option('o', "output", Required = false, HelpText = "Output in Window (Console.Write / default true)")]
        public string Output { get; set; }
        [Option("stop", Required = false, HelpText = "")]
        public string Stop { get; set; }
    }
}
