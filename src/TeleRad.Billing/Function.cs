using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using MySqlConnector;
using Newtonsoft.Json;
using TeleRad.Shared;
using TeleRad.Shared.Model;
using TeleRad.Shared.Utility;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace TeleRad.Billing;

public class Function
{
    private static readonly string ConnectionString =
        Environment.GetEnvironmentVariable("cs")
        ?? "Server=PLACEHOLDER_HOST;Port=3306;User Id=PLACEHOLDER_USER;Password=PLACEHOLDER_PASSWORD;Database=PLACEHOLDER_DB;Pooling=false;";

    public async Task<APIGatewayProxyResponse> Handler(APIGatewayProxyRequest request, ILambdaContext context)
    {
        try
        {
            if (request.HttpMethod?.ToUpper() == "OPTIONS")
                return FunctionBase.Ok(null, "OK");

            var wrapper = JsonConvert.DeserializeObject<LambdaRequest>(request.Body ?? "{}");
            if (wrapper == null)
                return FunctionBase.BadRequest("Invalid request body.");

            var trigger = (wrapper.Trigger ?? string.Empty).Trim();

            var authToken = wrapper.AuthToken
                ?? (request.Headers != null && request.Headers.TryGetValue("Authorization", out var ah) ? ah?.Replace("Bearer ", "") : null)
                ?? string.Empty;

            if (string.IsNullOrWhiteSpace(authToken))
                return FunctionBase.Unauthorized("Authorization token is required.");

            var token = await ValidateTokenAsync(authToken);
            if (!token.IsValid)
                return FunctionBase.Unauthorized("Token Not Authorized.");

            var helper = new BillingHelper(ConnectionString, token);

            return trigger switch
            {
                // ── Billing ──────────────────────────────────────────────────
                TriggerHelper.CreateTranscriberInvoice      => await helper.CreateTranscriberInvoiceAsync(wrapper.Body),
                TriggerHelper.CreateTranscriberInvoiceStep2 => await helper.CreateTranscriberInvoiceStep2Async(wrapper.Body),
                TriggerHelper.SaveTranscriberInvoice        => await helper.SaveTranscriberInvoiceAsync(wrapper.Body),
                TriggerHelper.CreateClientInvoice           => await helper.CreateClientInvoiceAsync(wrapper.Body),
                TriggerHelper.SaveClientInvoice             => await helper.SaveClientInvoiceAsync(wrapper.Body),
                TriggerHelper.GetInvoicePayments            => await helper.GetInvoicePaymentsAsync(wrapper.Body),
                TriggerHelper.GetClientPayments             => await helper.GetClientPaymentsAsync(wrapper.Body),
                TriggerHelper.GetTranscriberAnalytics       => await helper.GetTranscriberAnalyticsAsync(wrapper.Body),
                TriggerHelper.GetClientAnalytics            => await helper.GetClientAnalyticsAsync(wrapper.Body),
                TriggerHelper.GetBilledAmount               => await helper.GetBilledAmountAsync(wrapper.Body),

                // ── Price Management ─────────────────────────────────────────
                TriggerHelper.GetPriceManagement            => await helper.GetPriceManagementAsync(wrapper.Body),
                TriggerHelper.SaveModalityPrice             => await helper.SaveModalityPriceAsync(wrapper.Body),
                TriggerHelper.SavePerPagePrice              => await helper.SavePerPagePriceAsync(wrapper.Body),
                TriggerHelper.SavePerCharPrice              => await helper.SavePerCharPriceAsync(wrapper.Body),

                _                                           => FunctionBase.BadRequest($"Unknown trigger: {trigger}")
            };
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[TeleRad.Billing] Unhandled exception: {ex}");
            return FunctionBase.ServerError(ex.Message);
        }
    }

    private async Task<TokenValidationResult> ValidateTokenAsync(string token)
    {
        try
        {
            await using var conn = new MySqlConnection(ConnectionString);
            await conn.OpenAsync();
            await using var cmd    = QueryHelper.TokenValidation(conn, token);
            await using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                return new TokenValidationResult
                {
                    IsValid  = true,
                    UserId   = reader.GetInt32("userid"),
                    Username = reader.GetString("username"),
                    UserType = reader.IsDBNull(reader.GetOrdinal("usertype")) ? 0 : reader.GetInt32("usertype")
                };
            }
        }
        catch (Exception ex) { Console.WriteLine($"[TeleRad.Billing] Token validation error: {ex.Message}"); }
        return new TokenValidationResult { IsValid = false };
    }
}
