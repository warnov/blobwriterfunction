using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using System.Web.Http;

namespace BlobWriter
{
    public static class BlobWriter
    {
        [FunctionName("BlobWriter")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log, ExecutionContext context)
        {

            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            string fileContent;
            #region FileContent
            fileContent = @"E|NUM_FACTURA|1000048832
E | SUCURSAL | 1
E | FECHA | 26 / 09 / 2018
E | FECHA_VENCIMIENTO | 12 / 10 / 2018
E | FECHA_SUSPENSION | 24 / 10 / 2018
E | PERIODO_FACTURACION | 25 / 08 / 2018 - 25 / 09 / 2018
E | HORA_EMISION | 20:06:17
E | MONEDA_DOCUMENTO | COP
E | TIPO_FACTURA | 1
E | FACTURA_CONTINGENCIA | 0
E | NUMERO_CUENTA | 8925424139
E | NUMERO_PEDIDO |
E | CANAL_VENTAS |
E | TRM | 0
E | REF_PAGO | 8925424139
E | FECHA_TRM |
E | TOTAL_MONEDA_EXTRANJERA | 0
E | DESCRIPCION | Factura Servicios
E | OBSERVACIONES |
E | IDENTIFICACION_OBLIGADO | 8301149211
E | NOMBRE_OBLIGADO | COLOMBIA MOVIL S.A E.S.P
E | IDENTIFICACION_ADQUIRIENTE | 22407931
E | NOMBRE_ADQUIRIENTE |
E | ADQUIRIENTE_PN | GARCES SEPULVEDA | LIA EDELMIRA |
      E | TIPO_IDENTIFICACION_ADQUIRIENTE | 13
E | TIPO_PERSONA_ADQUIRIENTE | 2
E | TIPO_REGIMEN_ADQUIRIENTE | 0
E | EMAIL_ADQUIRIENTE |
E | DIRECCION_ADQUIRIENTE | KR 8 22 31
E | SUBDIVISION_ADQUIRIENTE | LA APARTADA
E | CIUDAD_ADQUIRIENTE | LA APARTADA
E | DEPTO_ADQUIRIENTE |
E | PAIS_ADQUIRIENTE | CO
E | NUM_RESOLUCION | 18762010427240
E | RANGO_INI_FACTURACION | 1000000000
E | RANGO_FIN_FACTURACION | 1099999999
E | PREFIJO_FACTURACION | BI
E | FECHA_INI_RESOLUCION | 25 / 09 / 2018
E | FECHA_FIN_RESOLUCION | 25 / 03 / 2019
E | COD_BARRAS1 | (415)7702138000015(8020)8925424139(3900)00000000057900(96)20181012 | EAN128
I | BASE_EXCLUIDA_IVA | 0
I | IMPUESTO |||| 0.00 | 0
T | TOTAL_IVA | 0.00
T | TOTAL_DESCUENTO | 0.00
T | TOTAL_NETO | 0.00
T | VALOR_DOCUMENTO | 0.00
T | BASE_GRAVABLE_IVA | 0.00
T | MONTO_TOTAL | 0.00
T | TOTAL_ICA | 0.00
T | TOTAL_IMPUESTO_CONSUMO | 0.00
T | TOTAL_CONTRIBUCIONES |
T | TOTAL_RETENCIONES_RENTA | 0.00
T | TOTAL_RETENCIONES_IVA | 0.00
T | TOTAL_RETENCIONES_ICA | 0.00
C | CTC | 73f5787ec29d7f2446b84f9754c60e713f462917
C | ENVIO | 01000
C | LINEARESUMEN | BI1000048832201809262006170.00010.00020.00030.000.008301149211322407931d218853f6039f2a9629d846fd0ddd631d21f1c8d69e647cc28fc7f05a6bca979";
            #endregion

            var blobConnectionString = config["blobConnectionString"];
            var containerName = config["containerName"];
            var blobRootName = config["blobRootName"];
            int filesCount;

            try
            {
                filesCount = int.Parse(config["filesCount"]);
            }
            catch
            {                
                return ReturnException("Bad filesCount parameter", log);
            }

            if (CloudStorageAccount.TryParse(blobConnectionString, out CloudStorageAccount cloudStorageAccount))
            {
                var blobClient = cloudStorageAccount.CreateCloudBlobClient();
                var container = blobClient.GetContainerReference(containerName);
                await container.CreateIfNotExistsAsync();

                for (int i = 0; i < filesCount; i++)
                {
                    var content = $"{i}: {fileContent}";
                    var blobName = $"{i}-{blobRootName}.txt";
                    var blob = container.GetBlockBlobReference(blobName);
                    _ = blob.UploadTextAsync(content);
                    log.LogInformation($"File {blobName} written");
                }
            }
            else
            {
                return ReturnException("Bad connection string", log);                
            }

            return new OkObjectResult($"{filesCount} Files written");
        }

        private static IActionResult ReturnException(string msg, ILogger log)
        {           
            log.LogError(msg);
            return new ExceptionResult(new Exception(msg), true);
        }
    }
}
