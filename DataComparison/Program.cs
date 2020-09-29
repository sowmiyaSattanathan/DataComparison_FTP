using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;
using WinSCP;

namespace DataComparison
{
    class Program
    {
        public static DataTable _dtDifferences = new DataTable();
        public static DataTable _dtMissedRows = new DataTable();

        static void Main(string[] args)
        {

            _dtMissedRows.Columns.Add("RowID", typeof(string));
            _dtMissedRows.Columns.Add("Status", typeof(string));

            _dtDifferences.Columns.Add("RowID", typeof(string));
            _dtDifferences.Columns.Add("SourceColumnName", typeof(string));
            _dtDifferences.Columns.Add("SourceColumnValue", typeof(string));
            _dtDifferences.Columns.Add("TargetColumnName", typeof(string));
            _dtDifferences.Columns.Add("TargetColumnValue", typeof(string));

            var sourceFolderDirectory = ConfigurationManager.AppSettings["SourceFolderDirectory"];
            var targetFolderDirectory = ConfigurationManager.AppSettings["TargetFolderDirectory"];
            var mappingFolderDirectory = ConfigurationManager.AppSettings["MappingFolderDirectory"];
            var processedFolderDirectory = ConfigurationManager.AppSettings["ProcessedFolderDirectory"];
            var toAddress = ConfigurationManager.AppSettings["ToAddress"];
            var fileNamesToCompare = ConfigurationManager.AppSettings["FileNamesToCompare"].Split(',').ToList();
            var listSourceFiles = ListFiles(sourceFolderDirectory);
            var listTargetFiles = ListFiles(targetFolderDirectory);
            var listMappingFiles = ListFiles(mappingFolderDirectory);
            if (listSourceFiles != null && listTargetFiles != null)
            {
                Console.WriteLine("Data comparison started");
                foreach (var fileName in fileNamesToCompare)
                {
                    if (listSourceFiles.Where(x => x.ToUpper().Contains(fileName.ToUpper())).FirstOrDefault() != null && listTargetFiles.Where(x => x.ToUpper().Contains(fileName.ToUpper())).FirstOrDefault() != null && listMappingFiles.Where(x => x.ToUpper().Contains(fileName.ToUpper())).FirstOrDefault() != null)
                    {
                        var sourceFilePath = listSourceFiles.Where(x => x.ToUpper().Contains(fileName.ToUpper())).FirstOrDefault();
                        var targetFilePath = listTargetFiles.Where(x => x.ToUpper().Contains(fileName.ToUpper())).FirstOrDefault();
                        var mappingFilePath = listMappingFiles.Where(x => x.ToUpper().Contains(fileName.ToUpper())).FirstOrDefault();
                        var dtSourceExcel = ReadFile(sourceFolderDirectory + sourceFilePath);
                        var dtTargetExcel = ReadFile(targetFolderDirectory + targetFilePath);
                        var dtMappingExcel = ReadFile(mappingFolderDirectory + mappingFilePath);
                        CompareTables(dtSourceExcel, dtTargetExcel, dtMappingExcel);
                        if (_dtMissedRows.Rows.Count >= 0 || _dtDifferences.Rows.Count >= 0)
                        {
                            Console.WriteLine("Sending Reports to Email...");
                            SendEmail(toAddress, "FTP_Data Comparison Report", "", _dtDifferences, _dtMissedRows, Path.GetFileNameWithoutExtension(sourceFilePath), "", "");
                            Console.WriteLine("Email Report Sent");
                            _dtMissedRows.Clear();
                            _dtDifferences.Clear();

                        }
                        else
                        {
                            SendEmail(toAddress, "FTP_Data Comparison Report", "There was no difference in the"+ Path.GetFileNameWithoutExtension(sourceFilePath)+" feeds.", _dtDifferences, _dtMissedRows, Path.GetFileNameWithoutExtension(sourceFilePath), "", "");
                        }
                        MoveFileFTP(sourceFolderDirectory + sourceFilePath, processedFolderDirectory + sourceFilePath);
                        MoveFileFTP(targetFolderDirectory + targetFilePath, processedFolderDirectory + targetFilePath);
                    }
                }
            }
            Console.WriteLine("Data comparison completed");



        }


        #region ComparisonLogic

        private static void CompareTables(DataTable dtSourceTable, DataTable dtTargetTable, DataTable dtMappingTable)
        {
            try
            {
                using (var dtPrimaryKeys = EnumerableRowCollectionExtensions.Where(dtMappingTable.AsEnumerable(), r => DataRowExtensions.Field<string>(r, "Mapping Rules") == "Primary Key")
                    .CopyToDataTable())
                {
                    // int progressBarPercentage = 10;
                    int sourceTableRowCount = dtSourceTable.Rows.Count;
                    foreach (DataRow drSource in dtSourceTable.Rows)
                    {
                        var row1Id = dtPrimaryKeys.Rows.Cast<DataRow>().Aggregate(string.Empty, (current, primaryKeyColumn) =>
                            $"{current}{primaryKeyColumn["Source File"]}:{drSource[primaryKeyColumn["Source File"].ToString()]}:");
                        row1Id = row1Id.TrimEnd(':');
                        var isMatched = false;
                        foreach (DataRow drTarget in dtTargetTable.Rows)
                        {
                            var row2Id = dtPrimaryKeys.Rows.Cast<DataRow>().Aggregate(string.Empty, (current, primaryKeyColumn) =>
                                $"{current}{primaryKeyColumn["Target File"]}:{drTarget[primaryKeyColumn["Target File"].ToString()]}:");
                            row2Id = row2Id.TrimEnd(':');
                            if (row1Id.Equals(row2Id))
                            {
                                CompareTwoRows(drSource, drTarget, dtMappingTable, $"{row1Id}");
                                isMatched = true;
                                drTarget.Delete();
                                dtTargetTable.AcceptChanges();
                                break;
                            }
                        }
                        if (!isMatched)
                        {

                            if (row1Id.IndexOf(':') != -1)
                            {
                                _dtMissedRows.Rows.Add(row1Id, "Missing row in Target Sheet");
                            }
                        }

                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private static void CompareTwoRows(DataRow drSourceRow, DataRow drTargetRow, DataTable dtMappingTable, string rowId)
        {
            using (var dtCompareKeys = dtMappingTable.AsEnumerable().Where(r => DataRowExtensions.Field<string>(r, "Mapping Rules") == null || DataRowExtensions.Field<string>(r, "Mapping Rules") == "Compare")
                .CopyToDataTable())
            {
                foreach (DataRow compareKey in dtCompareKeys.Rows)
                {
                    string sourceColumnName = compareKey["Source File"].ToString();
                    string targetColumnName = compareKey["Target File"].ToString();
                    string sourceColumnValue = Convert.ToString(drSourceRow[columnName: sourceColumnName]);
                    string targetColumnValue = Convert.ToString(drTargetRow[columnName: targetColumnName]);
                    sourceColumnValue = SourceValueConversion(sourceColumnValue);
                    targetColumnValue = TargetValueConversion(targetColumnValue);
                    if (!sourceColumnValue.Equals(targetColumnValue, StringComparison.OrdinalIgnoreCase))
                    {
                        _dtDifferences.Rows.Add(rowId, sourceColumnName, sourceColumnValue, targetColumnName, targetColumnValue);
                    }
                }
            }
        }

        #endregion

        private static string SourceValueConversion(string origValue)
        {
            string returnValue = string.Empty;

            decimal value;
            DateTime dt;
            if (origValue == "0")
            {
                returnValue = "false";
            }
            else if (origValue == "1")
            {
                returnValue = "true";
            }
            else if (Decimal.TryParse(origValue, out value))
            {
                value = Math.Round(value, 1);
                returnValue = value.ToString();
            }
            else if (DateTime.TryParse(origValue, out dt))
            {
                returnValue = dt.ToString("MM/dd/yyyy HH:mm", CultureInfo.InvariantCulture);
            }
            else
            {
                returnValue = origValue;
            }
            return returnValue;
        }
        private static string TargetValueConversion(string origValue)
        {
            string returnValue = string.Empty;
            DateTime dt;
            decimal value;
            if (origValue == "NULL")
            {
                returnValue = string.Empty;
            }
            else if (origValue == "0")
            {
                returnValue = "false";
            }
            else if (origValue == "1")
            {
                returnValue = "true";
            }
            else if (Decimal.TryParse(origValue, out value))
            {
                value = Math.Round(value, 1);
                returnValue = value.ToString();
            }
            else if (DateTime.TryParse(origValue, out dt))
            {
                returnValue = dt.ToString("MM/dd/yyyy HH:mm", CultureInfo.InvariantCulture);
            }
            else
            {
                returnValue = origValue;
            }
            return returnValue;
        }
        private static List<string> ListFiles(string folderName)
        {
            string[] downloadFiles;
            StringBuilder result = new StringBuilder();
            FtpWebRequest reqFTP;
            var host = ConfigurationManager.AppSettings["FTPHost"] + folderName;
            var username = ConfigurationManager.AppSettings["FTPUsername"];
            var password = ConfigurationManager.AppSettings["FTPPassword"];
            try
            {
                reqFTP = (FtpWebRequest)FtpWebRequest.Create(new Uri(
                          host));
                reqFTP.UseBinary = true;
                reqFTP.Credentials = new NetworkCredential(username,
                                                           password);
                reqFTP.Method = WebRequestMethods.Ftp.ListDirectory;
                WebResponse response = reqFTP.GetResponse();
                StreamReader reader = new StreamReader(response
                                                .GetResponseStream());

                string line = reader.ReadLine();
                while (line != null)
                {
                    result.Append(line);
                    result.Append("\n");
                    line = reader.ReadLine();
                }
                // to remove the trailing '\n'
                if (!string.IsNullOrWhiteSpace(Convert.ToString(result)))
                {
                    result.Remove(result.ToString().LastIndexOf('\n'), 1);
                    reader.Close();
                    response.Close();

                    return result.ToString().Split('\n').ToList();
                }

                return null;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private static DataTable ReadFile(string filePath)
        {

            string localDestination = Path.GetTempPath() + "\\" + Path.GetFileName(filePath);
            DownloadFileFromFTP(filePath, localDestination);
            var fileExt = Path.GetExtension(filePath);
            var dtDetails = new DataTable();
            switch (fileExt)
            {
                case ".xlsx":
                    dtDetails = ReadExcelFile(localDestination);
                    break;
                case ".csv":
                    dtDetails = ReadCsvFile(localDestination);
                    break;
                case ".psv":
                    dtDetails = ReadPsvFile(localDestination);
                    break;
            }

            return dtDetails;
        }
        private static void DownloadFileFromFTP(string ftpSourceFilePath, string localDestinationFilePath)
        {
            try
            {
                var host = ConfigurationManager.AppSettings["FTPHost"];
                var username = ConfigurationManager.AppSettings["FTPUsername"];
                var password = ConfigurationManager.AppSettings["FTPPassword"];

                string ftpFullFilePath = host + "/" + ftpSourceFilePath;

                //Create FTP Request.
                FtpWebRequest request = (FtpWebRequest)WebRequest.Create(ftpFullFilePath);
                request.Method = WebRequestMethods.Ftp.DownloadFile;

                //Enter FTP Server credentials.
                request.Credentials = new NetworkCredential(username, password);
                request.UsePassive = true;
                request.UseBinary = true;
                request.EnableSsl = false;

                FtpWebResponse response = (FtpWebResponse)request.GetResponse();

                Stream responseStream = response.GetResponseStream();
                StreamReader reader = new StreamReader(responseStream);

                using (FileStream writer = new FileStream(localDestinationFilePath, FileMode.Create))
                {

                    long length = response.ContentLength;
                    int bufferSize = 2048;
                    int readCount;
                    byte[] buffer = new byte[2048];

                    readCount = responseStream.Read(buffer, 0, bufferSize);
                    while (readCount > 0)
                    {
                        writer.Write(buffer, 0, readCount);
                        readCount = responseStream.Read(buffer, 0, bufferSize);
                    }
                }

                reader.Close();
                response.Close();

            }
            catch (Exception ex)
            {
                throw ex;
            }


        }

        private static void MoveFileFTP(string ftpSourceFilePath, string ftpTargetFilePath)
        {
            try
            {
                var host = ConfigurationManager.AppSettings["FTPHost"];
                var username = ConfigurationManager.AppSettings["FTPUsername"];
                var password = ConfigurationManager.AppSettings["FTPPassword"];

                string ftpFullFilePath = host + "/" + ftpSourceFilePath;

                //Create FTP Request.
                FtpWebRequest request = (FtpWebRequest)WebRequest.Create(ftpFullFilePath);
                request.Method = WebRequestMethods.Ftp.Rename;
                request.RenameTo = ftpTargetFilePath;
                //Enter FTP Server credentials.
                request.Credentials = new NetworkCredential(username, password);
                request.UsePassive = true;
                request.UseBinary = true;
                request.EnableSsl = false;
                //Fetch the Response and read it into a MemoryStream object.
                FtpWebResponse response = (FtpWebResponse)request.GetResponse();
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        #region FileReader

        private static DataTable ReadExcelFile(string path, bool hasHeader = true)
        {
            ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
            using (var pck = new ExcelPackage())
            {
                using (var stream = File.OpenRead(path))
                {
                    pck.Load(stream);
                }
                var ws = pck.Workbook.Worksheets.First();
                DataTable tbl = new DataTable();
                foreach (var firstRowCell in ws.Cells[1, 1, 1, ws.Dimension.End.Column])
                {
                    tbl.Columns.Add(hasHeader ? firstRowCell.Text : string.Format("Column {0}", firstRowCell.Start.Column));
                }
                var startRow = hasHeader ? 2 : 1;
                for (int rowNum = startRow; rowNum <= ws.Dimension.End.Row; rowNum++)
                {
                    var wsRow = ws.Cells[rowNum, 1, rowNum, ws.Dimension.End.Column];
                    DataRow row = tbl.Rows.Add();
                    foreach (var cell in wsRow)
                    {
                        row[cell.Start.Column - 1] = cell.Text;
                    }
                }
                return tbl;
            }
        }

        private static DataTable ReadCsvFile(String FileName)
        {

            DataTable dtCsv = new DataTable();
            string Fulltext;
            if (File.Exists(FileName))
            {
                using (StreamReader sr = new StreamReader(FileName))
                {
                    while (!sr.EndOfStream)
                    {
                        Fulltext = sr.ReadToEnd().ToString(); //read full file text  
                        string[] rows = Fulltext.Split('\n'); //split full file text into rows  
                        for (int i = 0; i < rows.Count(); i++)
                        {
                            string[] rowValues = rows[i].Split(','); //split each row with comma to get individual values  
                            {
                                if (i == 0)
                                {
                                    for (int j = 0; j < rowValues.Count(); j++)
                                    {
                                        dtCsv.Columns.Add(rowValues[j]); //add headers  
                                    }
                                }
                                else
                                {
                                    DataRow dr = dtCsv.NewRow();
                                    for (int k = 0; k < rowValues.Count(); k++)
                                    {
                                        dr[k] = rowValues[k].ToString().Replace("\r", "");
                                    }
                                    dtCsv.Rows.Add(dr); //add other rows  
                                }
                            }
                        }
                    }
                }
            }
            return dtCsv;
        }

        private static DataTable ReadPsvFile(String FileName)
        {

            DataTable dtCsv = new DataTable();
            string Fulltext;
            if (File.Exists(FileName))
            {
                using (StreamReader sr = new StreamReader(FileName))
                {
                    while (!sr.EndOfStream)
                    {
                        Fulltext = sr.ReadToEnd().ToString(); //read full file text  
                        string[] rows = Fulltext.Split('\n'); //split full file text into rows  
                        for (int i = 0; i < rows.Count(); i++)
                        {
                            string[] rowValues = rows[i].Split('|'); //split each row with pipe to get individual values  
                            {
                                if (i == 0)
                                {
                                    for (int j = 0; j < rowValues.Count(); j++)
                                    {
                                        dtCsv.Columns.Add(rowValues[j]); //add headers  
                                    }
                                }
                                else
                                {
                                    DataRow dr = dtCsv.NewRow();
                                    for (int k = 0; k < rowValues.Count(); k++)
                                    {
                                        dr[k] = Convert.ToString(rowValues[k]).Replace("\r", "");
                                    }
                                    dtCsv.Rows.Add(dr); //add other rows  
                                }
                            }
                        }
                    }
                }
            }
            return dtCsv;
        }

        #endregion

        public static bool SendEmail(string toAddress, string subject, string body, DataTable dtDifferences, DataTable dtMissed, string fileName, string CCAddress = "", string BCCAddress = "")
        {
            bool isSent = true;
            try
            {
                using (var mailmessage = new MailMessage())
                {
                    toAddress = ConfigurationManager.AppSettings["ToAddress"];
                    string serviceEmailId = ConfigurationManager.AppSettings["ServiceEmailId"];
                    string serviceEmailPassword = ConfigurationManager.AppSettings["ServiceEmailPassword"];
                    string smtpHostName = ConfigurationManager.AppSettings["SmtpHostName"];
                    int smtpPortNumber = Convert.ToInt32(ConfigurationManager.AppSettings["SmtpPortNumber"]);

                    mailmessage.From = new MailAddress(serviceEmailId);
                    mailmessage.Subject = subject;
                    mailmessage.Body = body;
                    mailmessage.IsBodyHtml = true;
                    if (dtDifferences != null && dtDifferences.Rows.Count > 0)
                        mailmessage.Attachments.Add(GetAttachment(dtDifferences, fileName + "_DataMismatch"));
                    if (dtMissed != null && dtMissed.Rows.Count > 0)
                        mailmessage.Attachments.Add(GetAttachment(dtMissed, fileName + "_RowsMissed"));
                    //Add To Address For Sending Email
                    if (!toAddress.Contains(','))
                    {
                        mailmessage.To.Add(new MailAddress(toAddress));
                    }
                    else
                    {
                        string[] emails = toAddress.Split(',');
                        foreach (string currentEmail in emails)
                        {
                            if (currentEmail != " ")
                            {
                                mailmessage.To.Add(new MailAddress(currentEmail));
                            }
                        }
                    }

                    //Add CC Address If Available For Sending Email
                    if (!string.IsNullOrWhiteSpace(CCAddress))
                    {
                        if (!CCAddress.Contains(','))
                        {
                            mailmessage.CC.Add(new MailAddress(CCAddress));

                        }
                        else
                        {
                            string[] emails = CCAddress.Split(',');
                            foreach (string currentEmail in emails)
                            {
                                if (currentEmail != " ")
                                {
                                    mailmessage.CC.Add(new MailAddress(currentEmail));
                                }
                            }
                        }
                    }

                    //Add BCC Address If Available For Sending Email
                    if (!string.IsNullOrWhiteSpace(BCCAddress))
                    {
                        if (!BCCAddress.Contains(','))
                        {
                            mailmessage.Bcc.Add(new MailAddress(BCCAddress));

                        }
                        else
                        {
                            string[] emails = BCCAddress.Split(',');
                            foreach (string currentEmail in emails)
                            {
                                if (currentEmail != " ")
                                {
                                    mailmessage.Bcc.Add(new MailAddress(currentEmail));
                                }
                            }
                        }
                    }

                    SmtpClient smtp = new SmtpClient();
                    smtp.Host = smtpHostName;
                    smtp.Credentials = new System.Net.NetworkCredential(serviceEmailId, serviceEmailPassword);
                    smtp.Port = smtpPortNumber;
                    smtp.EnableSsl = true;
                    smtp.Send(mailmessage);
                    isSent = true;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return isSent;
        }
        public static Attachment GetAttachment(DataTable dataTable, string FileName)
        {
            MemoryStream outputStream = new MemoryStream();
            using (ExcelPackage package = new ExcelPackage(outputStream))
            {
                ExcelWorksheet facilityWorksheet = package.Workbook.Worksheets.Add(FileName);
                facilityWorksheet.Cells.LoadFromDataTable(dataTable, true);
                facilityWorksheet.Row(1).Style.Font.Bold = true;
                package.Save();
            }
            FileName = $"{FileName}.xlsx";
            outputStream.Position = 0;
            Attachment attachment = new Attachment(outputStream, FileName, "application/vnd.ms-excel");

            return attachment;
        }
    }
}
