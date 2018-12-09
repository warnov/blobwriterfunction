using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace FileTransformer
{
    public static class BlobReader
    {

        /*
         * POST must be like:
         * {
         *     "originBlobName" : "acme081218.select",
               "originContainerName" : "acme",
               "destinyContainerName" : "acme_batchs",
               "destinyBlobRootName" : "acmebatch"
           }
           */

        [FunctionName("BlobReader")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log, ExecutionContext context)
        {

            #region Reading conf and params
        
            var config = new ConfigurationBuilder()
                        .SetBasePath(context.FunctionAppDirectory)
                        .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                        .AddEnvironmentVariables()
                        .Build();


            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);

            //Reading parameters from post data
            string originBlobName, originContainerName, destinyContainerName, destinyBlobRootName;
            try
            {
                originBlobName = data.originBlobName;
                originContainerName = data.originContainerName;
                destinyContainerName = data.destinyContainerName;
                destinyBlobRootName = data.destinyBlobRootName;
            }
            catch
            {
                return CrossUtils.ReturnException("Bad parameters", log);
            }
           
            int batchSize = 0;            
            try
            {
                //writerFunctionURI = new Uri(config["writerFunctionURL"]);
                batchSize = int.Parse(config["batchSize"]);
            }
            catch
            {
                return CrossUtils.ReturnException("Invalid parameters", log);
            }

            var blobConnectionString = config["blobConnectionString"];
            var billsCount = 0;
            #endregion


            if (CloudStorageAccount.TryParse(blobConnectionString, out CloudStorageAccount cloudStorageAccount))
            {
                var blobClient = cloudStorageAccount.CreateCloudBlobClient();


                //Processing origin blob into batches of lists
                try
                {
                    var originContainer = blobClient.GetContainerReference(originContainerName);
                    var originBlob = originContainer.GetBlockBlobReference(originBlobName);
                    BlobContinuationToken blobContinuationToken = null;
                    var listb = await originContainer.ListBlobsSegmentedAsync(null, blobContinuationToken);
                    blobContinuationToken = listb.ContinuationToken;
                    var fileContent = await originBlob.DownloadTextAsync();
                    var bills = new List<string>(fileContent.Split("FINDOCUM"));
                    billsCount = bills.Count;
                    for (int i = 0, batchNumber = 0; i < billsCount; i += batchSize, batchNumber++)
                    {
                        var offset = i + batchSize > billsCount ? billsCount - i : batchSize;
                        var batchDataList = bills.GetRange(i, offset);
                        var batchDataJsonString = JsonConvert.SerializeObject(batchDataList);
                        ProcessBatchDataString(batchDataJsonString, batchNumber,
                            blobClient, destinyContainerName, destinyBlobRootName);
                    }                    
                }
                catch(Exception exc)
                {
                    return CrossUtils.ReturnException("Problem reading origin blob", log);
                }
            }
            else
            {
                return CrossUtils.ReturnException("Bad connection string", log);
            }

            return new OkObjectResult($"{billsCount} bills processed");
        }

        private static async void ProcessBatchDataString(string batchDataJsonString, int batchNumber, 
            CloudBlobClient blobClient, string destinyContainerName, string destinyBlobRootName)
        {
            var container = blobClient.GetContainerReference(destinyContainerName);
            await container.CreateIfNotExistsAsync();
            var blobName = $"{batchNumber}-{destinyBlobRootName}.json";
            var blob = container.GetBlockBlobReference(blobName);
            _ = blob.UploadTextAsync(batchDataJsonString);
        }
    }
}
