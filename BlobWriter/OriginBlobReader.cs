using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;

namespace FileTransformer
{
    public static class BlobReader
    {
        [FunctionName("BlobReader")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log, ExecutionContext context)
        {

            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);

            //Reading original Blob Path from post data
            string originBlobName, originContainerName;
            try
            {
                originBlobName = data.originBlobName;
                originContainerName = data.originContainerName;
            }
            catch
            {
                return CrossUtils.ReturnException("Bad parameters", log);
            }


            string fileContent;
            var blobConnectionString = config["blobConnectionString"];
            var writerFunctionCode = config["writerFunctionCode"];
            WebClient wc = new WebClient();
            wc.Headers.Add("code", writerFunctionCode);

            List<string> bills = null;
            int batchSize = 0;
            Uri writerFunctionURI = null;
            try
            {
                writerFunctionURI = new Uri(config["writerFunctionURL"]);
                batchSize = int.Parse(config["batchSize"]);
            }
            catch
            {
                return CrossUtils.ReturnException("Invalid parameters", log);
            }



            if (CloudStorageAccount.TryParse(blobConnectionString, out CloudStorageAccount cloudStorageAccount))
            {
                var blobClient = cloudStorageAccount.CreateCloudBlobClient();


                //Getting origin blob content
                try
                {
                    var originContainer = blobClient.GetContainerReference(originContainerName);
                    var originBlob = originContainer.GetBlockBlobReference(originBlobName);
                    fileContent = await originBlob.DownloadTextAsync();
                    bills = new List<string>(fileContent.Split("FINDOCUM"));
                    for (int i = 0; i < bills.Count; i += batchSize)
                    {
                        var batchDataList = bills.GetRange(i, batchSize);
                        var batchDataString = JsonConvert.SerializeObject(batchDataList);
                        wc.UploadStringAsync(writerFunctionURI, batchDataString);
                    }
                }
                catch
                {
                    return CrossUtils.ReturnException("Problem reading origin blob", log);
                }

            }
            else
            {
                return CrossUtils.ReturnException("Bad connection string", log);
            }

            return new OkObjectResult($"{bills?.Count} bills processed");
        }

    }
}
