using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net.Http.Headers;
using System.Reflection.Emit;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Xsl;
using CommandLine;



namespace RunSqlScripts
{
    class Program
    {
        //private static ParserResult<Options> _args;
        private static OptionArgs _args = new OptionArgs();
        private static List<string> _errorList = new List<string>();
        private static Dictionary<string, List<string>> _connectionStringAliasses = new Dictionary<string, List<string>>(StringComparer.InvariantCultureIgnoreCase);

        static void Main(string[] args)
        {
        
            WriteConsoleAndLog("START RunSqlScripts");

            var arguments = Parser.Default.ParseArguments<Options>(args);
            if (!arguments.Errors.Any())
            {
                Init(arguments);

                if (_errorList.Any())
                {
                    WriteConsoleAndLog(_errorList);
                }
                else
                {
                    Run();
                    WriteConsoleAndLog("FINISH RunSqlScripts");
                    if (_args.Stop)
                    {
                        Console.ReadLine();
                    }
                }

            }
        }




        private static void Run()
        {
            var filesToRun = new Dictionary<string, bool>(StringComparer.InvariantCultureIgnoreCase);
            _args.Folders.ForEach(folder =>
            {
                GetFilesToRun(folder).ToList().ForEach(file =>
                {
                    filesToRun.Add(file.Key, file.Value);
                });
            });
            WriteConsoleAndLog($"{filesToRun.Count} Files to execute");

            var connectionString = string.Join(";", _args.ConnectionString.Select(x => x.Key + "=" + x.Value).ToArray());
            using (var con = new SqlConnection(connectionString))
            {
                try
                {
                    con.Open();
                    using (var comm = new SqlCommand())
                    {
                        comm.Connection = con;
                        ExecuteFiles(filesToRun, comm);
                    }
                }
                catch (Exception ex)
                {
                    WriteConsoleAndLog($"ERROR database: {ex.Message}");
                }
            }
        }

        private static void ExecuteFiles(Dictionary<string, bool> filesToRun, SqlCommand comm)
        {
            var filesCount = filesToRun.Count;

            var currentCount = 0;
            foreach (var fileItem in filesToRun)
            {
                currentCount++;
                var encoding = GetEncoding(fileItem.Key);
                var content = File.ReadAllText(fileItem.Key, encoding);
                try
                {
                    comm.CommandText = $"USE {_args.ConnectionString["database"]}";
                    comm.ExecuteNonQuery();

                    content = RemoveComments(content);
                    var scripts = RemoveGoStatement(content);
                    scripts.ToList().ForEach(script =>
                    {
                        var sql = script;
                        if (script.Substring(script.Length - 2).ToUpper() == "GO")
                        {
                            sql = sql.Substring(0, content.Length - 2);
                        }

                        comm.CommandText = sql;
                        comm.ExecuteNonQuery();
                    });
                }
                catch (Exception ex)
                {
                    WriteConsoleAndLog($"ERROR Execute SQL: {ex.Message} -- {fileItem.Key}");
                    if (fileItem.Value)
                    {
                        WriteConsoleAndLog("Task is STOPPED !!!");
                        WriteConsoleAndLog($"ERROR in {fileItem.Key}");
                        break;
                    }
                }
                WriteConsoleAndLog($"{(int)(currentCount * 100 / (decimal)filesCount)}% {fileItem.Key} (Encoding: {encoding.BodyName})");
            }
        }

        private static string RemoveComments(string content)
        {
            var ret = "";
            var startPos = 0;
            // when failed, we return the original content:
            try
            {
                while (content?.IndexOf("/*", startPos) > -1)
                {
                    var endPos = content.IndexOf("/*", startPos);
                    var startPosAfterComment = content.IndexOf("*/", startPos);
                    var contentBefore = content.Substring(startPos, endPos - startPos);
                    var contentAfter = content.Substring(startPosAfterComment + 2);
                    ret += $"{contentBefore}{Environment.NewLine}{contentAfter}";
                    startPos = endPos + 1;
                }
            }
            catch (Exception ex)
            {
                WriteConsoleAndLog("SoftFail: ", ex.Message);
                ret = content;
            }

            return ret;
        }

        private static Dictionary<string, bool> GetFilesToRun(FolderList directory, string path = null)
        {
            WriteConsoleAndLog($"Read files from {path ?? directory.Folder}");
            var fileList = new Dictionary<string, bool>(StringComparer.InvariantCultureIgnoreCase);
            try
            {
                Directory.GetFiles(path ?? directory.Folder, "*.sql").OrderBy(Path.GetFileNameWithoutExtension).ToList().ForEach(file =>
                {
                    fileList.Add(file, directory.StopOnError);
                });
                if (directory.RecursiveFolders)
                {
                    Directory.GetDirectories(path ?? directory.Folder).ToList().ForEach(subFolder =>
                    {
                        GetFilesToRun(directory, subFolder).ToList().ForEach(file =>
                        {
                            fileList.Add(file.Key, file.Value);
                        });
                    });
                }
            }
            catch (Exception ex)
            {
                WriteConsoleAndLog($"ERROR Folder: {ex.Message}");
            }
            return fileList;
        }


        private static void Init(ParserResult<Options> args)
        {
            _connectionStringAliasses.Add("Application Name", new List<string> { "Application Name" });
            _connectionStringAliasses.Add("Async", new List<string> { "Async" });
            _connectionStringAliasses.Add("AttachDBFilename", new List<string> { "AttachDBFilename", "extended properties", "Initial File Name" });
            _connectionStringAliasses.Add("Connect Timeout", new List<string> { "Connect Timeout", "Connection Timeout" });
            _connectionStringAliasses.Add("Connection Lifetime", new List<string> { "Connection Lifetime" });
            _connectionStringAliasses.Add("Context Connection", new List<string> { "Context Connection" });
            _connectionStringAliasses.Add("Connection Reset", new List<string> { "Connection Reset" });
            _connectionStringAliasses.Add("Current Language", new List<string> { "Current Language" });
            _connectionStringAliasses.Add("Data Source", new List<string> { "Data Source", "Server", "Address", "Addr", "Network Address" });
            _connectionStringAliasses.Add("Encrypt", new List<string> { "Encrypt" });
            _connectionStringAliasses.Add("Enlist", new List<string> { "Enlist" });
            _connectionStringAliasses.Add("Failover Partner", new List<string> { "Failover Partner" });
            _connectionStringAliasses.Add("Initial Catalog", new List<string> { "Initial Catalog", "Database" });
            _connectionStringAliasses.Add("Load Balance Timeout", new List<string> { "Load Balance Timeout" });
            _connectionStringAliasses.Add("MultipleActiveResultSets", new List<string> { "MultipleActiveResultSets" });
            _connectionStringAliasses.Add("Integrated Security", new List<string> { "Integrated Security", "Trusted_Connection" });
            _connectionStringAliasses.Add("Max Pool Size", new List<string> { "Max Pool Size	" });
            _connectionStringAliasses.Add("Min Pool Size", new List<string> { "Min Pool Size" });
            _connectionStringAliasses.Add("Network Library", new List<string> { "Network Library", "Net" });
            _connectionStringAliasses.Add("Packet Size", new List<string> { "Packet Size" });
            _connectionStringAliasses.Add("Password", new List<string> { "Password", "Pwd" });
            _connectionStringAliasses.Add("Persist Security Info	", new List<string> { "Persist Security Info" });
            _connectionStringAliasses.Add("Pooling", new List<string> { "Pooling" });
            _connectionStringAliasses.Add("Replication", new List<string> { "Replication" });
            _connectionStringAliasses.Add("Transaction Binding", new List<string> { "Transaction Binding" });
            _connectionStringAliasses.Add("TrustServerCertificate", new List<string> { "TrustServerCertificate" });
            _connectionStringAliasses.Add("Type System Version", new List<string> { "Type System Version" });
            _connectionStringAliasses.Add("User ID", new List<string> { "User ID" });
            _connectionStringAliasses.Add("User Instance", new List<string> { "User Instance" });
            _connectionStringAliasses.Add("Workstation ID", new List<string> { "Workstation ID" });

            _args.Server = args.Value.Server;
            _args.Database = args.Value.Database;
            _args.User = args.Value.UserId;
            _args.Password = args.Value.Password;
            _args.TrustedConnection = string.IsNullOrEmpty(args.Value.TrustedConnection) ? (bool?)null : args.Value.TrustedConnection != "0";
            _args.ApplicationName = args.Value.ApplicationName;
            _args.PersistSecurityInfo = string.IsNullOrEmpty(args.Value.PersistentSecurityInfo) ? (bool?)null : args.Value.PersistentSecurityInfo == "1";
            _args.Logfile = args.Value.logFile;
            _args.Output = args.Value.Output != "0";
            _args.Stop = args.Value.Stop == "1";

            // Connection String:
            _args.ConnectionString = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
            if (!string.IsNullOrEmpty(args.Value.ConnectionString))
            {
                args.Value.ConnectionString.Split(';').ToList().ForEach(item =>
                {
                    var splitted = item.Split('=');
                    if (splitted.Length == 2)
                    {
                        var key = _connectionStringAliasses.FirstOrDefault(d => Enumerable.Any<string>(d.Value, v => v.ToLower() == splitted[0].ToLower())).Key;
                        if (key == null)
                        {
                            key = splitted[0];
                        }
                        var value = splitted[1];

                        if (string.IsNullOrEmpty(key))
                        {
                            _errorList.Add($"ConnectionString: The item \"{item}\" has an empty key.");
                        }
                        else if (_args.ConnectionString.ContainsKey(key))
                        {
                            _errorList.Add($"ConnectionString: The key \"{key}\" is already declared.");
                        }
                        else
                        {
                            _args.ConnectionString.Add(key, value);
                        }
                    }
                    else
                    {
                        _errorList.Add($"ConnectionString: The item \"{item}\" has no value.");
                    }
                });
            }
            else
            {
                var dictConStr = new Dictionary<string, string>
                {
                    {"Server", _args.Server},
                    {"Database", _args.Database},
                    {"User ID", _args.User},
                    {"Password", _args.Password},
                    {"Application Name", _args.ApplicationName},
                    {"Persist Security Info", _args.PersistSecurityInfo == null ? string.Empty : (_args.PersistSecurityInfo == true ? "true" : "false")},
                    {"Integrated Security", _args.TrustedConnection == null ? string.Empty : (_args.TrustedConnection == true ? "true" : "false")},
                };
                dictConStr.ToList().ForEach(dic =>
                {
                    if (!string.IsNullOrEmpty(dic.Key))
                    {
                        _args.ConnectionString.Add(dic.Key, dic.Value);
                    }
                });
            }

            // Folders:
            _args.Folders = new List<FolderList>();
            args.Value.Folders.Split(';').ToList().ForEach(folder =>
            {
                var splitted = folder.Split(new[] { "&", "^&" }, StringSplitOptions.None);
                var item = new FolderList
                {
                    Folder = splitted[0],
                    StopOnError = splitted.Length > 1 && splitted[1] == "1",
                    RecursiveFolders = splitted.Length <= 2 || (splitted[2] != "0"),
                };
                if (string.IsNullOrEmpty(item.Folder))
                {
                    item.ErrorMessage = $"Foldername can not be null or empty.";
                }
                _args.Folders.Add(item);
            });

            // Errors:
            _args.Folders.Where(fl => !string.IsNullOrEmpty(fl.ErrorMessage)).ToList().ForEach(fl =>
            {
                _errorList.Add(fl.ErrorMessage);
            });
            if (!_args.ConnectionString.Any() && (string.IsNullOrEmpty(_args.Server) || string.IsNullOrEmpty(_args.Database)))
            {
                _errorList.Add("ConnectionString or (Server and Database) is required.");
            }

            // set or overwrite Connection String
            if (!_errorList.Any())
            {
                var dict = new Dictionary<string, string>
                {
                    {"Application Name", _args.ApplicationName},
                    {"Data Source", _args.Server},
                    {"Initial Catalog", _args.Database},
                    {"Persist Security Info", _args.PersistSecurityInfo == null ? string.Empty : (_args.PersistSecurityInfo == true ? "true" : "false")},
                    {"User ID", _args.User},
                    {"Password", _args.Password},
                    {"Integrated Security", _args.TrustedConnection == null ? string.Empty : (_args.TrustedConnection == true ? "true" : "false")}
                };
                dict.ToList().ForEach(d =>
                {
                    if (!string.IsNullOrEmpty(d.Value))
                    {
                        if (_args.ConnectionString.ContainsKey(d.Key))
                        {
                            _args.ConnectionString[d.Key] = d.Value;
                        }
                        else
                        {
                            _args.ConnectionString.Add(d.Key, d.Value);
                        }
                    }
                });
            }

        }


        private static void WriteConsoleAndLog(IEnumerable<string> messageList, string logfile = null)
        {
            WriteConsoleAndLog(string.Join(Environment.NewLine, messageList), logfile);
        }

        private static void WriteConsoleAndLog(string message, string logfile = null)
        {
            var now = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            try
            {
                if (string.IsNullOrEmpty(logfile))
                {
                    logfile = _args.Logfile;
                }
                if (!string.IsNullOrEmpty(logfile))
                {
                    message.Split(new[] { Environment.NewLine }, StringSplitOptions.None).ToList().ForEach(line =>
                      {
                          File.AppendAllText(logfile, $"{now} {line}{Environment.NewLine}");
                      });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{now} ERROR on logging:");
                ex.Message.Split(new[] { Environment.NewLine }, StringSplitOptions.None).ToList().ForEach(line =>
                 {
                     Console.WriteLine($"{now} {line}");
                 });
            }

            message.Split(new[] { Environment.NewLine }, StringSplitOptions.None).ToList().ForEach(line =>
              {
                  Console.WriteLine($"{now} {line}");
              });
        }

        private static List<string> RemoveGoStatement(string sqlScript)
        {
            var statements = Regex.Split(sqlScript, @"[(\n\r)\t ]GO[(\n\r)\t ]", RegexOptions.IgnorePatternWhitespace | RegexOptions.IgnoreCase | RegexOptions.Multiline);
            return statements.Where(stat => !string.IsNullOrWhiteSpace(stat)).Select(stat => stat.Trim(' ', '\r', '\n')).ToList();
        }

        public static Encoding GetEncoding(string filename)
        {
            // Read the BOM
            var bom = new byte[4];
            using (var file = new FileStream(filename, FileMode.Open, FileAccess.Read))
            {
                file.Read(bom, 0, 4);
            }

            // Analyze the BOM
            if (bom[0] == 0x2b && bom[1] == 0x2f && bom[2] == 0x76) return Encoding.UTF7;
            if (bom[0] == 0xef && bom[1] == 0xbb && bom[2] == 0xbf) return Encoding.UTF8;
            if (bom[0] == 0xff && bom[1] == 0xfe && bom[2] == 0 && bom[3] == 0) return Encoding.UTF32; //UTF-32LE
            if (bom[0] == 0xff && bom[1] == 0xfe) return Encoding.Unicode; //UTF-16LE
            if (bom[0] == 0xfe && bom[1] == 0xff) return Encoding.BigEndianUnicode; //UTF-16BE
            if (bom[0] == 0 && bom[1] == 0 && bom[2] == 0xfe && bom[3] == 0xff) return new UTF32Encoding(true, true);  //UTF-32BE

            // We actually have no idea what the encoding is if we reach this point, so
            // you may wish to return null instead of defaulting to ASCII
            return Encoding.Default;
        }

    }

    internal class FolderList
    {
        public string Folder { get; set; }
        public bool StopOnError { get; set; }
        public bool RecursiveFolders { get; set; }
        public string ErrorMessage { get; set; }
    }

    internal class OptionArgs
    {
        public Dictionary<string, string> ConnectionString { get; set; }
        public string Server { get; set; }
        public string Database { get; set; }
        public string User { get; set; }
        public string Password { get; set; }
        public bool? TrustedConnection { get; set; }
        public string ApplicationName { get; set; }
        public bool? PersistSecurityInfo { get; set; }
        public List<FolderList> Folders { get; set; }
        public string Logfile { get; set; }
        public bool Output { get; set; }
        public bool Stop { get; set; }
    }

}
