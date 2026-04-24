using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using MySqlConnector;
using Newtonsoft.Json;
using TeleRad.Shared;
using TeleRad.Shared.Model;
using TeleRad.Shared.Utility;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace TeleRad.Studies;

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
            if (trigger == TriggerHelper.Login)
            {
                var h = new StudiesHelper(ConnectionString);
                return await h.LoginAsync(wrapper.Body);
            }

            var authToken = wrapper.AuthToken
                ?? (request.Headers != null && request.Headers.TryGetValue("Authorization", out var ah) ? ah?.Replace("Bearer ", "") : null)
                ?? string.Empty;

            if (string.IsNullOrWhiteSpace(authToken))
                return FunctionBase.Unauthorized("Authorization token is required.");

            var token = await ValidateTokenAsync(authToken);
            if (!token.IsValid)
                return FunctionBase.Unauthorized("Token Not Authorized.");

            var helper = new StudiesHelper(ConnectionString, token);

            return trigger switch
            {
                // ── Auth ─────────────────────────────────────────────────────
                TriggerHelper.Logout                        => await helper.LogoutAsync(authToken),

                // ── Studies ──────────────────────────────────────────────────
                TriggerHelper.GetStudies                    => await helper.GetStudiesAsync(wrapper.Body),
                TriggerHelper.GetDailyWorkflowData          => await helper.GetDailyWorkflowDataAsync(wrapper.Body),
                TriggerHelper.UpdateStudyStatus             => await helper.UpdateStudyStatusAsync(wrapper.Body),
                TriggerHelper.UpdateStudyStatusAuto         => await helper.UpdateStudyStatusAutoAsync(wrapper.Body),
                TriggerHelper.UpdateStudy                   => await helper.UpdateStudyAsync(wrapper.Body),
                TriggerHelper.DeleteStudy                   => await helper.DeleteStudyAsync(wrapper.Body),
                TriggerHelper.MarkStat                      => await helper.MarkStatAsync(wrapper.Body),
                TriggerHelper.UnmarkStat                    => await helper.UnmarkStatAsync(wrapper.Body),
                TriggerHelper.CloneStudy                    => await helper.CloneStudyAsync(wrapper.Body),
                TriggerHelper.GetStudyAudit                 => await helper.GetStudyAuditAsync(wrapper.Body),
                TriggerHelper.GetFinalReportUrl             => await helper.GetFinalReportUrlAsync(wrapper.Body),
                TriggerHelper.SaveAudit                     => await helper.SaveAuditAsync(wrapper.Body),
                TriggerHelper.UpdateStudyReport             => await helper.UpdateStudyReportAsync(wrapper.Body),
                TriggerHelper.PdfReviewed                   => await helper.PdfReviewedAsync(wrapper.Body),
                TriggerHelper.CheckForDownloadAudio         => await helper.CheckForDownloadAudioAsync(wrapper.Body),
                TriggerHelper.DownloadAudioZip              => await helper.DownloadAudioZipAsync(wrapper.Body),
                TriggerHelper.DeleteTempZip                 => await helper.DeleteTempZipAsync(wrapper.Body),
                TriggerHelper.GetStudyDocument              => await helper.GetStudyDocumentAsync(wrapper.Body),
                TriggerHelper.GetStudyPreview               => await helper.GetStudyPreviewAsync(wrapper.Body),
                TriggerHelper.GetVerificationSheet          => await helper.GetVerificationSheetAsync(wrapper.Body),
                TriggerHelper.GetExamHistory                => await helper.GetExamHistoryAsync(wrapper.Body),

                // ── Notes ────────────────────────────────────────────────────
                TriggerHelper.GetNotes                      => await helper.GetNotesAsync(wrapper.Body),
                TriggerHelper.AddNote                       => await helper.AddNoteAsync(wrapper.Body),
                TriggerHelper.UpdateNote                    => await helper.UpdateNoteAsync(wrapper.Body),

                // ── Attachments / Audio ──────────────────────────────────────
                TriggerHelper.AddAttachment                 => await helper.AddAttachmentAsync(wrapper.Body),
                TriggerHelper.AddMultipleAttachments        => await helper.AddMultipleAttachmentsAsync(wrapper.Body),
                TriggerHelper.GetAttachedFiles              => await helper.GetAttachedFilesAsync(wrapper.Body),
                TriggerHelper.GetPresignedUrl               => await helper.GetPresignedUrlAsync(wrapper.Body),
                TriggerHelper.GetPresignedUploadUrl         => await helper.GetPresignedUploadUrlAsync(wrapper.Body),
                TriggerHelper.UploadAudioAttachment         => await helper.UploadAudioAttachmentAsync(wrapper.Body),
                TriggerHelper.AddKeyImage                   => await helper.AddKeyImageAsync(wrapper.Body),
                TriggerHelper.DeleteKeyImage                => await helper.DeleteKeyImageAsync(wrapper.Body),

                TriggerHelper.GetViewerToken                => await helper.GetViewerTokenAsync(wrapper.Body),
                TriggerHelper.GetIncomingFaxes              => await helper.GetIncomingFaxesAsync(wrapper.Body),
                TriggerHelper.SendAudioFiles                => await helper.SendAudioFilesAsync(wrapper.Body),
                TriggerHelper.GetSentAudioFiles             => await helper.GetSentAudioFilesAsync(wrapper.Body),

                // ── Final Reports ────────────────────────────────────────────
                TriggerHelper.GetFinalReports               => await helper.GetFinalReportsAsync(wrapper.Body),
                TriggerHelper.UpdateFinalReport             => await helper.UpdateFinalReportAsync(wrapper.Body),

                // ── Dashboard / Charts ───────────────────────────────────────
                TriggerHelper.GetChartData                  => await helper.GetChartDataAsync(wrapper.Body),
                TriggerHelper.GetLastFifteenRecords         => await helper.GetLastFifteenRecordsAsync(wrapper.Body),
                TriggerHelper.GetHoldForComparison          => await helper.GetStudiesByStatusAsync("hold_for_comparison", wrapper.Body),
                TriggerHelper.GetMissingPaperwork           => await helper.GetStudiesByStatusAsync("missing_paperwork", wrapper.Body),
                TriggerHelper.GetSpeakToTech                => await helper.GetStudiesByStatusAsync("speak_to_tech", wrapper.Body),
                TriggerHelper.GetMissingImages              => await helper.GetStudiesByStatusAsync("missing_images", wrapper.Body),
                TriggerHelper.GetHoldReport                 => await helper.GetStudiesByStatusAsync("rad_reports_on_hold", wrapper.Body),
                TriggerHelper.GetNewStudy                   => await helper.GetStudiesByStatusAsync("new study", wrapper.Body),
                TriggerHelper.GetTransNewMessages           => await helper.GetStudiesByStatusAsync("trans new messages", wrapper.Body),
                TriggerHelper.GetTransReportsOnHold         => await helper.GetStudiesByStatusAsync("trans reports on hold", wrapper.Body),
                TriggerHelper.GetRadPendingSignature        => await helper.GetStudiesByStatusAsync("rad reports pending signature", wrapper.Body),
                TriggerHelper.GetReview                     => await helper.GetStudiesByStatusAsync("review", wrapper.Body),
                TriggerHelper.GetPendingBatch               => await helper.GetPendingBatchAsync(wrapper.Body),
                TriggerHelper.GetPendingModalities          => await helper.GetPendingModalitiesAsync(),
                TriggerHelper.GetFinalizedData              => await helper.GetFinalizedDataAsync(wrapper.Body),
                TriggerHelper.GetRefFinalizedData           => await helper.GetRefFinalizedDataAsync(wrapper.Body),

                _                                           => FunctionBase.BadRequest($"Unknown trigger: {trigger}")
            };
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[TeleRad.Studies] Unhandled exception: {ex}");
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
        catch (Exception ex) { Console.WriteLine($"[TeleRad.Studies] Token validation error: {ex.Message}"); }
        return new TokenValidationResult { IsValid = false };
    }
}
