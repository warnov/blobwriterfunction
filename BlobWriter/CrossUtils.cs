using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Web.Http;

namespace FileTransformer
{
    public static class CrossUtils
    {
        public static IActionResult ReturnException(string msg, ILogger log)
        {
            log.LogError(msg);
            return new ExceptionResult(new Exception(msg), true);
        }
    }
}
