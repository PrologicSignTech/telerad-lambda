using Amazon.Lambda.APIGatewayEvents;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using TeleRad.Shared.Model;

namespace TeleRad.Shared;

public static class FunctionBase
{
    public static APIGatewayProxyResponse Ok(object? data, string message = "Success")
        => Build(200, new ApiResponse { StatusCode = 200, StatusMessage = message, Data = data });

    public static APIGatewayProxyResponse BadRequest(string message)
        => Build(400, new ApiResponse { StatusCode = 400, StatusMessage = message });

    public static APIGatewayProxyResponse Unauthorized(string message)
        => Build(401, new ApiResponse { StatusCode = 401, StatusMessage = message });

    public static APIGatewayProxyResponse Forbidden(string message)
        => Build(403, new ApiResponse { StatusCode = 403, StatusMessage = message });

    public static APIGatewayProxyResponse NotFound(string message)
        => Build(404, new ApiResponse { StatusCode = 404, StatusMessage = message });

    public static APIGatewayProxyResponse ServerError(string message)
        => Build(500, new ApiResponse { StatusCode = 500, StatusMessage = message });

    private static readonly JsonSerializerSettings _camelCase = new()
    {
        ContractResolver = new CamelCasePropertyNamesContractResolver()
    };

    private static APIGatewayProxyResponse Build(int statusCode, object body)
        => new()
        {
            StatusCode = statusCode,
            Body       = JsonConvert.SerializeObject(body, _camelCase),
            Headers    = new Dictionary<string, string>
            {
                { "Content-Type",                 "application/json" },
                { "Access-Control-Allow-Origin",  "*"                },
                { "Access-Control-Allow-Headers", "Content-Type,Authorization" },
                { "Access-Control-Allow-Methods", "POST,OPTIONS"     }
            }
        };
}
