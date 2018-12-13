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
                var destinyContainer = blobClient.GetContainerReference(destinyContainerName);
                try
                {
                    await destinyContainer.CreateIfNotExistsAsync();
                }
                catch (Exception exc)
                {
                    return CrossUtils.ReturnException($"Problem creating destiny container: {exc.Message}", log);
                }

                //Processing origin blob into batches of lists
                var fileContent = string.Empty;
                try
                {
                    var originContainer = blobClient.GetContainerReference(originContainerName);
                    var originBlob = originContainer.GetBlockBlobReference(originBlobName);
                    fileContent = await originBlob.DownloadTextAsync();
                }
                catch
                {
                    return CrossUtils.ReturnException("Problem reading origin blob", log);
                }

                //Creating a blob for each batch of elements in the list
                var bills = new List<string>(fileContent.Split("FINDOCUM"));
                billsCount = bills.Count;
                var batchCount = (int)Math.Ceiling((double)billsCount / (double)batchSize);
                Parallel.For(0, batchCount, i =>
                {
                    var currentPos = i * batchSize;
                    var offset = currentPos + batchSize > billsCount ? billsCount - currentPos : batchSize;
                    var batchDataList = bills.GetRange(currentPos, offset);
                    var batchDataJsonString = JsonConvert.SerializeObject(batchDataList);
                    var destinyBlobName = $"{i}-{destinyBlobRootName}.json";
                    var blob = destinyContainer.GetBlockBlobReference(destinyBlobName);
                    _ = blob.UploadTextAsync(batchDataJsonString);
                });
            }
            else
            {
                return CrossUtils.ReturnException("Bad connection string", log);
            }
            return new OkObjectResult($"{billsCount} bills processed");
        }

    }
    /* private static async void ProcessBatchDataString(string batchDataJsonString, int batchNumber, 
         CloudBlobContainer destinyContainer, string destinyBlobRootName)
     {
         var blobName = $"{batchNumber}-{destinyBlobRootName}.json";
         var blob = destinyContainer.GetBlockBlobReference(blobName);
         _ = blob.UploadTextAsync(batchDataJsonString);
     }*/

}
