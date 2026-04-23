using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using MySqlConnector;
using Newtonsoft.Json;
using TeleRad.Shared;
using TeleRad.Shared.Model;
using TeleRad.Shared.Utility;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace TeleRad.Admin;

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

            var helper = new AdminHelper(ConnectionString, token);

            return trigger switch
            {
                // ── Audit ────────────────────────────────────────────────────
                TriggerHelper.GetAuditAll                   => await helper.GetAuditAllAsync(wrapper.Body),

                // ── Fax ──────────────────────────────────────────────────────
                TriggerHelper.SendFax                       => await helper.SendFaxAsync(wrapper.Body),
                TriggerHelper.ViewFaxes                     => await helper.ViewFaxesAsync(wrapper.Body),
                TriggerHelper.GetInboundFaxes               => await helper.GetInboundFaxesAsync(wrapper.Body),
                TriggerHelper.GetIncomingFaxes              => await helper.GetIncomingFaxesAsync(wrapper.Body),
                TriggerHelper.RenameFax                     => await helper.RenameFaxAsync(wrapper.Body),
                TriggerHelper.MoveInboundFax                => await helper.MoveInboundFaxAsync(wrapper.Body),
                TriggerHelper.GetFaxStatus                  => await helper.GetFaxStatusAsync(wrapper.Body),
                TriggerHelper.GetUrgentNotifications        => await helper.GetUrgentNotificationsAsync(wrapper.Body),

                // ── Order Status ─────────────────────────────────────────────
                TriggerHelper.GetOrderStatus                => await helper.GetOrderStatusAsync(wrapper.Body),
                TriggerHelper.GetOrderStatusV2              => await helper.GetOrderStatusV2Async(wrapper.Body),
                TriggerHelper.GetAuditDetailOrderStatus     => await helper.GetAuditDetailOrderStatusAsync(wrapper.Body),
                TriggerHelper.ReassignTranscriber           => await helper.ReassignTranscriberAsync(wrapper.Body),

                // ── Modalities ───────────────────────────────────────────────
                TriggerHelper.GetModalities                 => await helper.GetModalitiesAsync(),
                TriggerHelper.ToggleModalityStatus          => await helper.ToggleModalityStatusAsync(wrapper.Body),

                // ── Non-DICOM ────────────────────────────────────────────────
                TriggerHelper.GetExamsByModality            => await helper.GetExamsByModalityAsync(wrapper.Body),
                TriggerHelper.GetNonDicomAccounts           => await helper.GetNonDicomAccountsAsync(wrapper.Body),
                TriggerHelper.StoreNonDicomEntry            => await helper.StoreNonDicomEntryAsync(wrapper.Body),
                TriggerHelper.DeleteNonDicomEntry           => await helper.DeleteNonDicomEntryAsync(wrapper.Body),
                TriggerHelper.GetNonDicomEntry              => await helper.GetNonDicomEntryAsync(wrapper.Body),
                TriggerHelper.InterpretNonDicomEntry        => await helper.InterpretNonDicomEntryAsync(wrapper.Body),

                // ── Standard Reports ─────────────────────────────────────────
                TriggerHelper.GetStandardReports            => await helper.GetStandardReportsAsync(wrapper.Body),
                TriggerHelper.CreateStandardReport          => await helper.CreateStandardReportAsync(wrapper.Body),
                TriggerHelper.UpdateStandardReport          => await helper.UpdateStandardReportAsync(wrapper.Body),
                TriggerHelper.DeleteStandardReport          => await helper.DeleteStandardReportAsync(wrapper.Body),

                // ── Site Management ──────────────────────────────────────────
                TriggerHelper.GetMiscSettings               => await helper.GetMiscSettingsAsync(),
                TriggerHelper.UpdateMiscSettings            => await helper.UpdateMiscSettingsAsync(wrapper.Body),
                TriggerHelper.GetOsrix                      => await helper.GetOsrixAsync(wrapper.Body),
                TriggerHelper.CreateOsrixUser               => await helper.CreateOsrixUserAsync(wrapper.Body),
                TriggerHelper.UpdateOsrixUser               => await helper.UpdateOsrixUserAsync(wrapper.Body),
                TriggerHelper.DeleteOsrixUser               => await helper.DeleteOsrixUserAsync(wrapper.Body),
                TriggerHelper.GetOsrixInstitutions          => await helper.GetOsrixInstitutionsAsync(wrapper.Body),
                TriggerHelper.CreateOsrixInstitution        => await helper.CreateOsrixInstitutionAsync(wrapper.Body),
                TriggerHelper.UpdateOsrixInstitution        => await helper.UpdateOsrixInstitutionAsync(wrapper.Body),
                TriggerHelper.DeleteOsrixInstitution        => await helper.DeleteOsrixInstitutionAsync(wrapper.Body),
                TriggerHelper.UpdateDefaultCharges          => await helper.UpdateDefaultChargesAsync(wrapper.Body),
                TriggerHelper.GetDefaultCharges             => await helper.GetDefaultChargesAsync(),
                TriggerHelper.GetRadiologists               => await helper.GetRadiologistsAsync(wrapper.Body),

                // ── Quickbooks ───────────────────────────────────────────────
                TriggerHelper.GetQbSettings                 => await helper.GetQbSettingsAsync(wrapper.Body),
                TriggerHelper.SaveQbSettings                => await helper.SaveQbSettingsAsync(wrapper.Body),
                TriggerHelper.GetQbModalityMappings         => await helper.GetQbModalityMappingsAsync(wrapper.Body),
                TriggerHelper.SaveQbModalityMapping         => await helper.SaveQbModalityMappingAsync(wrapper.Body),
                TriggerHelper.PushQbModalities              => await helper.PushQbModalitiesAsync(wrapper.Body),
                TriggerHelper.PullQbModalities              => await helper.PullQbModalitiesAsync(wrapper.Body),
                TriggerHelper.GetQbClientMappings           => await helper.GetQbClientMappingsAsync(wrapper.Body),
                TriggerHelper.SaveQbClientMapping           => await helper.SaveQbClientMappingAsync(wrapper.Body),
                TriggerHelper.PushQbClients                 => await helper.PushQbClientsAsync(wrapper.Body),
                TriggerHelper.PullQbClients                 => await helper.PullQbClientsAsync(wrapper.Body),
                TriggerHelper.GetQbTranscriberMappings      => await helper.GetQbTranscriberMappingsAsync(wrapper.Body),
                TriggerHelper.SaveQbTranscriberMapping      => await helper.SaveQbTranscriberMappingAsync(wrapper.Body),
                TriggerHelper.PushQbTranscribers            => await helper.PushQbTranscribersAsync(wrapper.Body),
                TriggerHelper.PullQbTranscribers            => await helper.PullQbTranscribersAsync(wrapper.Body),

                _                                           => FunctionBase.BadRequest($"Unknown trigger: {trigger}")
            };
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[TeleRad.Admin] Unhandled exception: {ex}");
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
        catch (Exception ex) { Console.WriteLine($"[TeleRad.Admin] Token validation error: {ex.Message}"); }
        return new TokenValidationResult { IsValid = false };
    }
}
