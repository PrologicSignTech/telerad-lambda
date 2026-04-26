using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using MySqlConnector;
using Newtonsoft.Json;
using TeleRadLambda.Model;
using TeleRadLambda.Utility;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace TeleRadLambda;

public class Function
{
    private static readonly string ConnectionString =
        Environment.GetEnvironmentVariable("cs")
        ?? "Server=PLACEHOLDER_HOST;Port=3306;User Id=PLACEHOLDER_USER;Password=PLACEHOLDER_PASSWORD;Database=PLACEHOLDER_DB;Pooling=false;";

    public async Task<APIGatewayProxyResponse> Handler(APIGatewayProxyRequest request, ILambdaContext context)
    {
        try
        {
            var wrapper = JsonConvert.DeserializeObject<LambdaRequest>(request.Body ?? "{}");
            if (wrapper == null)
                return BadRequest("Invalid request body.");

            var trigger = (wrapper.Trigger ?? string.Empty).Trim();

            // Login is the only unauthenticated trigger
            if (trigger == TriggerHelper.Login)
            {
                var h = new Helper(ConnectionString);
                return await h.LoginAsync(wrapper.Body);
            }

            // All other triggers require valid token
            var authToken = wrapper.AuthToken
                ?? (request.Headers != null && request.Headers.TryGetValue("Authorization", out var ah) ? ah?.Replace("Bearer ", "") : null)
                ?? string.Empty;

            if (string.IsNullOrWhiteSpace(authToken))
                return Unauthorized("Authorization token is required.");

            var token = await ValidateTokenAsync(authToken);
            if (!token.IsValid)
                return Unauthorized("Token Not Authorized.");

            var helper = new Helper(ConnectionString, token);

            return trigger switch
            {
                // ── Auth ─────────────────────────────────────────────────────
                TriggerHelper.Logout                        => await helper.LogoutAsync(authToken),

                // ── Studies ──────────────────────────────────────────────────
                TriggerHelper.GetStudies                    => await helper.GetStudiesAsync(wrapper.Body),
                TriggerHelper.GetPendingBatch               => await helper.GetPendingBatchAsync(wrapper.Body),
                TriggerHelper.GetDailyWorkflowData          => await helper.GetDailyWorkflowDataAsync(wrapper.Body),
                TriggerHelper.UpdateStudyStatus             => await helper.UpdateStudyStatusAsync(wrapper.Body),
                TriggerHelper.UpdateStudyStatusAuto         => await helper.UpdateStudyStatusAutoAsync(wrapper.Body),
                TriggerHelper.DeleteStudy                   => await helper.DeleteStudyAsync(wrapper.Body),
                TriggerHelper.MarkAsNewStudy                => await helper.MarkAsNewStudyAsync(wrapper.Body),
                TriggerHelper.RestoreStudy                  => await helper.RestoreStudyAsync(wrapper.Body),
                TriggerHelper.UpdateStudy                   => await helper.UpdateStudyAsync(wrapper.Body),
                TriggerHelper.MarkStat                      => await helper.MarkStatAsync(wrapper.Body),
                TriggerHelper.UnmarkStat                    => await helper.UnmarkStatAsync(wrapper.Body),
                TriggerHelper.CloneStudy                    => await helper.CloneStudyAsync(wrapper.Body),
                TriggerHelper.GetStudyAudit                 => await helper.GetStudyAuditAsync(wrapper.Body),
                TriggerHelper.GetFinalReportUrl             => await helper.GetFinalReportUrlAsync(wrapper.Body),
                TriggerHelper.GetStudyPreview               => await helper.GetStudyPreviewAsync(wrapper.Body),
                TriggerHelper.GetViewerToken                => await helper.GetViewerTokenAsync(wrapper.Body),
                TriggerHelper.SaveAudit                     => await helper.SaveAuditAsync(wrapper.Body),
                TriggerHelper.UpdateStudyReport             => await helper.UpdateStudyReportAsync(wrapper.Body),
                TriggerHelper.PdfReviewed                   => await helper.PdfReviewedAsync(wrapper.Body),
                TriggerHelper.CheckForDownloadAudio         => await helper.CheckForDownloadAudioAsync(wrapper.Body),
                TriggerHelper.DownloadAudioZip              => await helper.DownloadAudioZipAsync(wrapper.Body),
                TriggerHelper.DeleteTempZip                 => await helper.DeleteTempZipAsync(wrapper.Body),
                TriggerHelper.GetStudyDocument              => await helper.GetStudyDocumentAsync(wrapper.Body),
                TriggerHelper.GetPresignedUrl               => await helper.GetPresignedUrlAsync(wrapper.Body),
                TriggerHelper.GetVerificationSheet          => await helper.GetVerificationSheetAsync(wrapper.Body),
                TriggerHelper.GetExamHistory                => await helper.GetExamHistoryAsync(wrapper.Body),
                TriggerHelper.GetNewStudy                   => await helper.GetNewStudyAsync(wrapper.Body),
                TriggerHelper.GetTransNewMessages           => await helper.GetTransNewMessagesAsync(wrapper.Body),
                TriggerHelper.GetTransReportsOnHold         => await helper.GetTransReportsOnHoldAsync(wrapper.Body),
                TriggerHelper.GetRadPendingSignature        => await helper.GetRadPendingSignatureAsync(wrapper.Body),
                TriggerHelper.GetReview                     => await helper.GetReviewAsync(wrapper.Body),
                TriggerHelper.GetPendingReports             => await helper.GetPendingReportsAsync(wrapper.Body),
                TriggerHelper.GetPendingModalities          => await helper.GetPendingModalitiesAsync(wrapper.Body),
                TriggerHelper.GetExamsByModality            => await helper.GetExamsByModalityAsync(wrapper.Body),

                // ── Notes ────────────────────────────────────────────────────
                TriggerHelper.GetNotes                      => await helper.GetNotesAsync(wrapper.Body),
                TriggerHelper.AddNote                       => await helper.AddNoteAsync(wrapper.Body),
                TriggerHelper.UpdateNote                    => await helper.UpdateNoteAsync(wrapper.Body),

                // ── Templates ────────────────────────────────────────────────
                TriggerHelper.GetAllTemplates               => await helper.GetAllTemplatesAsync(wrapper.Body),
                TriggerHelper.SearchTemplates               => await helper.SearchTemplatesAsync(wrapper.Body),
                TriggerHelper.CreateTemplate                => await helper.CreateTemplateAsync(wrapper.Body),
                TriggerHelper.UpdateTemplate                => await helper.UpdateTemplateAsync(wrapper.Body),
                TriggerHelper.DeleteTemplate                => await helper.DeleteTemplateAsync(wrapper.Body),
                TriggerHelper.GetUserTemplates              => await helper.GetUserTemplatesAsync(wrapper.Body),
                TriggerHelper.SearchReferrers               => await helper.SearchReferrersAsync(wrapper.Body),

                // ── Users ────────────────────────────────────────────────────
                TriggerHelper.GetUsers                      => await helper.GetUsersAsync(wrapper.Body),
                TriggerHelper.CreateUser                    => await helper.CreateUserAsync(wrapper.Body),
                TriggerHelper.UpdateUser                    => await helper.UpdateUserAsync(wrapper.Body),
                TriggerHelper.UpdateUserPermission          => await helper.UpdateUserPermissionAsync(wrapper.Body),
                TriggerHelper.UpdateMedicalFee              => await helper.UpdateMedicalFeeAsync(wrapper.Body),
                TriggerHelper.ResetPassword                 => await helper.ResetPasswordAsync(wrapper.Body),
                TriggerHelper.ShowPassword                  => await helper.ShowPasswordAsync(wrapper.Body),
                TriggerHelper.ChangeUserStatus              => await helper.ChangeUserStatusAsync(wrapper.Body),
                TriggerHelper.ChangeBillingStatus           => await helper.ChangeBillingStatusAsync(wrapper.Body),
                TriggerHelper.UserSearch                    => await helper.UserSearchAsync(wrapper.Body),
                TriggerHelper.SearchUserByType              => await helper.SearchUserByTypeAsync(wrapper.Body),
                TriggerHelper.LoginAsAnotherUser            => await helper.LoginAsAnotherUserAsync(wrapper.Body),
                TriggerHelper.BackToAccount                 => await helper.BackToAccountAsync(wrapper.Body),
                TriggerHelper.GetClientLookup               => await helper.GetClientLookupAsync(wrapper.Body),
                TriggerHelper.UpdateClientInfo              => await helper.UpdateClientInfoAsync(wrapper.Body),
                TriggerHelper.ConfirmDefaultTemplate        => await helper.ConfirmDefaultTemplateAsync(wrapper.Body),
                TriggerHelper.GetUserTypes                  => await helper.GetUserTypesAsync(wrapper.Body),
                TriggerHelper.GetCountries                  => await helper.GetCountriesAsync(wrapper.Body),
                TriggerHelper.GetStates                     => await helper.GetStatesAsync(wrapper.Body),
                TriggerHelper.GetCities                     => await helper.GetCitiesAsync(wrapper.Body),
                TriggerHelper.GetTranscribers               => await helper.GetTranscribersAsync(wrapper.Body),
                TriggerHelper.GetUsersByDisplayName         => await helper.GetUsersByDisplayNameAsync(wrapper.Body),
                TriggerHelper.UpdateEmployees               => await helper.UpdateEmployeesAsync(wrapper.Body),

                // ── Attachments / Audio ──────────────────────────────────────
                TriggerHelper.AddAttachment                 => await helper.AddAttachmentAsync(wrapper.Body),
                TriggerHelper.AddMultipleAttachments        => await helper.AddMultipleAttachmentsAsync(wrapper.Body),
                TriggerHelper.GetAttachedFiles              => await helper.GetAttachedFilesAsync(wrapper.Body),
                TriggerHelper.SendAudioFiles                => await helper.SendAudioFilesAsync(wrapper.Body),
                TriggerHelper.GetSentAudioFiles             => await helper.GetSentAudioFilesAsync(wrapper.Body),

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

                // ── Final Reports ────────────────────────────────────────────
                TriggerHelper.GetFinalReports               => await helper.GetFinalReportsAsync(wrapper.Body),
                TriggerHelper.UpdateFinalReport             => await helper.UpdateFinalReportAsync(wrapper.Body),

                // ── Dashboard / Charts ───────────────────────────────────────
                TriggerHelper.GetChartData                  => await helper.GetChartDataAsync(wrapper.Body),
                TriggerHelper.GetLastFifteenRecords         => await helper.GetLastFifteenRecordsAsync(wrapper.Body),
                TriggerHelper.GetHoldForComparison          => await helper.GetStudiesByStatusAsync("hold_for_comparison"),
                TriggerHelper.GetMissingPaperwork           => await helper.GetStudiesByStatusAsync("missing_paperwork"),
                TriggerHelper.GetSpeakToTech                => await helper.GetStudiesByStatusAsync("speak_to_tech"),
                TriggerHelper.GetMissingImages              => await helper.GetStudiesByStatusAsync("missing_images"),
                TriggerHelper.GetHoldReport                 => await helper.GetStudiesByStatusAsync("rad_reports_on_hold"),
                TriggerHelper.GetFinalizedData              => await helper.GetFinalizedDataAsync(wrapper.Body),
                TriggerHelper.GetRefFinalizedData           => await helper.GetRefFinalizedDataAsync(wrapper.Body),

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

                // ── Order Status ─────────────────────────────────────────────
                TriggerHelper.GetOrderStatus                => await helper.GetOrderStatusAsync(wrapper.Body),
                TriggerHelper.GetOrderStatusV2              => await helper.GetOrderStatusV2Async(wrapper.Body),
                TriggerHelper.GetAuditDetailOrderStatus     => await helper.GetAuditDetailOrderStatusAsync(wrapper.Body),
                TriggerHelper.ReassignTranscriber           => await helper.ReassignTranscriberAsync(wrapper.Body),

                // ── Modalities ───────────────────────────────────────────────
                TriggerHelper.GetModalities                 => await helper.GetModalitiesAsync(),
                TriggerHelper.ToggleModalityStatus          => await helper.ToggleModalityStatusAsync(wrapper.Body),

                // ── Non-DICOM ────────────────────────────────────────────────
                TriggerHelper.GetNonDicomAccounts           => await helper.GetNonDicomAccountsAsync(wrapper.Body),
                TriggerHelper.StoreNonDicomEntry            => await helper.StoreNonDicomEntryAsync(wrapper.Body),
                TriggerHelper.DeleteNonDicomEntry           => await helper.DeleteNonDicomEntryAsync(wrapper.Body),
                TriggerHelper.GetNonDicomEntry              => await helper.GetNonDicomEntryAsync(wrapper.Body),

                // ── Standard Reports ─────────────────────────────────────────
                TriggerHelper.GetStandardReports            => await helper.GetStandardReportsAsync(wrapper.Body),
                TriggerHelper.CreateStandardReport          => await helper.CreateStandardReportAsync(wrapper.Body),
                TriggerHelper.UpdateStandardReport          => await helper.UpdateStandardReportAsync(wrapper.Body),
                TriggerHelper.DeleteStandardReport          => await helper.DeleteStandardReportAsync(wrapper.Body),

                // ── Roles ────────────────────────────────────────────────────
                TriggerHelper.GetRoles                      => await helper.GetRolesAsync(),
                TriggerHelper.CreateRole                    => await helper.CreateRoleAsync(wrapper.Body),
                TriggerHelper.UpdateRole                    => await helper.UpdateRoleAsync(wrapper.Body),
                TriggerHelper.DeleteRole                    => await helper.DeleteRoleAsync(wrapper.Body),

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

                // ── Region — Countries ───────────────────────────────────────
                TriggerHelper.GetAllCountries               => await helper.GetAllCountriesAsync(wrapper.Body),
                TriggerHelper.CreateCountry                 => await helper.CreateCountryAsync(wrapper.Body),
                TriggerHelper.UpdateCountry                 => await helper.UpdateCountryAsync(wrapper.Body),
                TriggerHelper.DeleteCountry                 => await helper.DeleteCountryAsync(wrapper.Body),

                // ── Region — States ──────────────────────────────────────────
                TriggerHelper.GetAllStates                  => await helper.GetAllStatesAsync(wrapper.Body),
                TriggerHelper.CreateState                   => await helper.CreateStateAsync(wrapper.Body),
                TriggerHelper.UpdateState                   => await helper.UpdateStateAsync(wrapper.Body),
                TriggerHelper.DeleteState                   => await helper.DeleteStateAsync(wrapper.Body),

                // ── Region — Cities ──────────────────────────────────────────
                TriggerHelper.GetAllCities                  => await helper.GetAllCitiesAsync(wrapper.Body),
                TriggerHelper.CreateCity                    => await helper.CreateCityAsync(wrapper.Body),
                TriggerHelper.UpdateCity                    => await helper.UpdateCityAsync(wrapper.Body),
                TriggerHelper.DeleteCity                    => await helper.DeleteCityAsync(wrapper.Body),

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

                _                                           => BadRequest($"Unknown trigger: {trigger}")
            };
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[TeleRad] Unhandled exception: {ex}");
            return ServerError(ex.Message);
        }
    }

    // ── Token validation ──────────────────────────────────────────────────────

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
        catch (Exception ex)
        {
            Console.WriteLine($"[TeleRad] Token validation error: {ex.Message}");
        }

        return new TokenValidationResult { IsValid = false };
    }

    // ── Response builders ─────────────────────────────────────────────────────

    internal static APIGatewayProxyResponse Ok(object? data, string message = "Success")
        => Build(200, new ApiResponse { StatusCode = 200, StatusMessage = message, Data = data });

    internal static APIGatewayProxyResponse BadRequest(string message)
        => Build(400, new ApiResponse { StatusCode = 400, StatusMessage = message });

    internal static APIGatewayProxyResponse Unauthorized(string message)
        => Build(401, new ApiResponse { StatusCode = 401, StatusMessage = message });

    internal static APIGatewayProxyResponse Forbidden(string message)
        => Build(403, new ApiResponse { StatusCode = 403, StatusMessage = message });

    internal static APIGatewayProxyResponse NotFound(string message)
        => Build(404, new ApiResponse { StatusCode = 404, StatusMessage = message });

    internal static APIGatewayProxyResponse ServerError(string message)
        => Build(500, new ApiResponse { StatusCode = 500, StatusMessage = message });

    private static APIGatewayProxyResponse Build(int statusCode, object body)
        => new()
        {
            StatusCode = statusCode,
            Body       = JsonConvert.SerializeObject(body),
            Headers    = new Dictionary<string, string>
            {
                { "Content-Type",                 "application/json" },
                { "Access-Control-Allow-Origin",  "*"                },
                { "Access-Control-Allow-Headers", "Content-Type,Authorization" },
                { "Access-Control-Allow-Methods", "POST,OPTIONS"     }
            }
        };
}
