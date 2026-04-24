using Amazon.Lambda.APIGatewayEvents;
using Amazon.S3;
using Amazon.S3.Model;
using MySqlConnector;
using TeleRad.Shared;
using TeleRad.Shared.Model;
using TeleRad.Shared.Utility;

namespace TeleRad.Studies;

public class StudiesHelper : HelperBase
{
    public StudiesHelper(string connectionString, TokenValidationResult? token = null)
        : base(connectionString, token) { }

    // ══════════════════════════════════════════════════════════════════════════
    // AUTH
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> LoginAsync(string? body)
    {
        var req = Parse<LoginRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.Username) || string.IsNullOrWhiteSpace(req.Password))
            return FunctionBase.BadRequest("Username and password are required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.FindUserForLogin(conn, req.Username);
            await using var reader = await cmd.ExecuteReaderAsync();

            if (!await reader.ReadAsync())
                return FunctionBase.Unauthorized("Invalid credentials.");

            var storedHash = reader.GetString("password");
            if (!BCrypt.Net.BCrypt.Verify(req.Password, storedHash))
                return FunctionBase.Unauthorized("Invalid credentials.");

            var userId   = reader.GetInt32("id");
            var name     = reader.GetString("name");
            var email    = reader.GetString("email");
            var username = reader.GetString("username");
            var userType = reader.GetInt32("usertype");
            await reader.CloseAsync();

            await using var permCmd    = QueryHelper.GetUserPermissions(conn, userId);
            await using var permReader = await permCmd.ExecuteReaderAsync();
            var perms = new List<string>();
            while (await permReader.ReadAsync()) perms.Add(permReader.GetString(0));
            await permReader.CloseAsync();

            var token = GenerateToken();
            await using var upsertCmd = QueryHelper.UpsertToken(conn, userId, username, token);
            await upsertCmd.ExecuteNonQueryAsync();

            return FunctionBase.Ok(new LoginResponse
            {
                Id = userId, Name = name, Email = email,
                Username = username, UserType = userType,
                Token = token, Permissions = perms
            });
        }
        catch (Exception ex) { return Err("Login", ex); }
    }

    public async Task<APIGatewayProxyResponse> LogoutAsync(string token)
    {
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.DeleteToken(conn, token);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Logged out successfully.");
        }
        catch (Exception ex) { return Err("Logout", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // STUDIES
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetStudiesAsync(string? body)
    {
        var req = Parse<GetStudiesRequest>(body) ?? new GetStudiesRequest();
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.GetStudies(conn, req.Dos, req.ClientName, req.ClientNameExcept,
                req.Search, req.StatusList, req.ModalityList, _token.UserId, _token.UserType);
            return FunctionBase.Ok(await ReadStudies(cmd));
        }
        catch (Exception ex) { return Err("GetStudies", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetDailyWorkflowDataAsync(string? body)
    {
        var req = Parse<GetStudiesRequest>(body) ?? new GetStudiesRequest();
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.GetStudies(conn, req.Dos, req.ClientName, req.ClientNameExcept,
                req.Search, req.StatusList, req.ModalityList, _token.UserId, _token.UserType);
            return FunctionBase.Ok(await ReadStudies(cmd));
        }
        catch (Exception ex) { return Err("GetDailyWorkflowData", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateStudyStatusAsync(string? body)
    {
        var req = Parse<UpdateStudyStatusRequest>(body);
        if (req == null || req.StudyId <= 0 || string.IsNullOrWhiteSpace(req.Status))
            return FunctionBase.BadRequest("StudyId and Status are required.");

        // Allowed statuses — supports both DB (spaces) and API (underscores) formats
        var allowed = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            // DB values (with spaces)
            "cancel exam","canceled exam","missing images","missing paperwork",
            "new study","new  study","rad final report","rad non dicom accunts",
            "rad reports pending signature","review","speak to tech",
            "trans new message","trans new messages","trans reports on hold",
            "rad reports on hold","hold for comparison",
            // API / underscore equivalents
            "new_study","trans_new_messages","trans_reports_on_hold","rad_reports_on_hold",
            "rad_reports_pending_signature","hold_for_comparison","missing_paperwork",
            "speak_to_tech","missing_images","rad_final_report","finalized",
            "no_audio","in_progress","cancelled","cancel_exam","canceled_exam"
        };
        if (!allowed.Contains(req.Status.ToLower()))
            return FunctionBase.BadRequest($"Invalid status: {req.Status}");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateStudyStatus(conn, req.StudyId, req.Status);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Status updated.");
        }
        catch (Exception ex) { return Err("UpdateStudyStatus", ex); }
    }

    /// <summary>
    /// Mirrors Laravel's updateStatus() — auto-advances status based on business rules:
    /// rad_final_report → new_study or trans_new_messages (depending on audio attachment).
    /// cancel_exam       → new_study or trans_new_messages.
    /// Resets delete=0 in all cases.
    /// </summary>
    public async Task<APIGatewayProxyResponse> UpdateStudyStatusAutoAsync(string? body)
    {
        var req = Parse<UpdateStudyStatusRequest>(body);
        if (req == null || req.StudyId <= 0)
            return FunctionBase.BadRequest("StudyId is required.");

        try
        {
            await using var conn = await OpenAsync();

            // 1. Get current study type + delete flag
            string currentType = string.Empty;
            int    deleteFlag  = 0;
            await using (var infoCmd = new MySqlConnector.MySqlCommand(
                "SELECT type, `delete` FROM tran_typewordlist WHERE id = @id", conn))
            {
                infoCmd.Parameters.AddWithValue("@id", req.StudyId);
                await using var rd = await infoCmd.ExecuteReaderAsync();
                if (await rd.ReadAsync())
                {
                    currentType = rd.IsDBNull(0) ? string.Empty : rd.GetString(0);
                    deleteFlag  = rd.IsDBNull(1) ? 0 : rd.GetInt32(1);
                }
            }

            // 2. Check for audio attachment
            bool hasAudio = false;
            await using (var audioCmd = new MySqlConnector.MySqlCommand(
                "SELECT COUNT(*) FROM tran_attachment WHERE exam_id = @id AND attachment_type = 'Audio File'", conn))
            {
                audioCmd.Parameters.AddWithValue("@id", req.StudyId);
                var cnt = await audioCmd.ExecuteScalarAsync();
                hasAudio = Convert.ToInt64(cnt) > 0;
            }

            // 3. Apply business rules (mirrors Laravel StudyController@updateStatus)
            string newType    = string.Empty;
            string message    = string.Empty;
            var    t          = currentType.ToLower().Trim();

            if (t == "rad final report")
            {
                // For rad final report we auto-route back into workflow
                newType = hasAudio ? "trans new messages" : "new study";
                message = hasAudio
                    ? $"Study [#{req.StudyId}] sent to transcription (audio attached)."
                    : $"Study [#{req.StudyId}] reset to new study.";
            }
            else if (t == "cancel exam")
            {
                newType = hasAudio ? "trans new messages" : "new study";
                message = hasAudio
                    ? $"Study [#{req.StudyId}] sent to transcription (audio attached)."
                    : $"Study [#{req.StudyId}] reset to new study.";
            }
            else
            {
                // Other statuses: nothing to auto-advance — return message
                return FunctionBase.Ok(new { message = $"No auto-advance rule for status '{currentType}'." }, "No change needed.");
            }

            // 4. Apply update — set new type + reset delete=0
            await using (var updCmd = new MySqlConnector.MySqlCommand(
                "UPDATE tran_typewordlist SET type = @type, `delete` = 0 WHERE id = @id", conn))
            {
                updCmd.Parameters.AddWithValue("@type", newType);
                updCmd.Parameters.AddWithValue("@id",   req.StudyId);
                await updCmd.ExecuteNonQueryAsync();
            }

            return FunctionBase.Ok(new { newStatus = newType, message }, "Status auto-updated.");
        }
        catch (Exception ex) { return Err("UpdateStudyStatusAuto", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateStudyAsync(string? body)
    {
        var req = Parse<UpdateStudyRequest>(body);
        if (req == null || req.StudyId <= 0)
            return FunctionBase.BadRequest("StudyId is required.");

        try
        {
            // ── Laravel-matching report_text logic ─────────────────────────────
            string? reportText      = req.ReportText;
            string? impressionText  = null;
            string? autoStatus      = null;

            if (!string.IsNullOrWhiteSpace(reportText))
            {
                // 1. Sanitize quotes (matches Laravel str_replace)
                reportText = reportText.Replace("'", "`").Replace("&#39;", "&#96;");

                // 2. Split impression — everything after last "Impression" keyword
                var impIdx = reportText.LastIndexOf("Impression", StringComparison.OrdinalIgnoreCase);
                if (impIdx >= 0)
                {
                    impressionText = "IMPRESSION" + reportText.Substring(impIdx + "Impression".Length);
                    reportText     = reportText.Substring(0, impIdx) + " Impression ";
                }
                else
                {
                    impressionText = string.Empty;
                }

                // 3. Auto-advance status: trans new messages → trans reports on hold
                //    (only if the study currently has an empty report_text)
                await using var connCheck = await OpenAsync();
                await using var chkCmd    = QueryHelper.GetStudyCurrentReportText(connCheck, req.StudyId);
                await using var chkReader = await chkCmd.ExecuteReaderAsync();
                if (await chkReader.ReadAsync())
                {
                    var existingReport = chkReader.IsDBNull(0) ? "" : chkReader.GetString(0);
                    var currentType    = chkReader.IsDBNull(1) ? "" : chkReader.GetString(1);
                    if (existingReport == "" && currentType == "trans new messages")
                        autoStatus = "trans reports on hold";
                }
            }

            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateStudy(conn, req.StudyId,
                req.PatientName, req.PatientFirstName, req.PatientLastName,
                req.PatientId, req.PatientDob, req.Dos, req.Modality,
                req.Description, req.OrderingPhysician, req.AccessionNumber,
                req.Status ?? autoStatus, req.TranscriberId, req.RadId, req.TemplateId, req.ClientId,
                reportText, impressionText);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Study updated.");
        }
        catch (Exception ex) { return Err("UpdateStudy", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetStudyPreviewAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0)
            return FunctionBase.BadRequest("StudyId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetStudyPreview(conn, req.StudyId);
            await using var reader = await cmd.ExecuteReaderAsync();

            if (!await reader.ReadAsync())
                return FunctionBase.NotFound("Study not found.");

            var rawDod = SafeDateStr(reader, "dod");
            var rawDot = SafeDateStr(reader, "dot");
            var rawTrans = Str(reader, "transcriber_name");
            Console.WriteLine($"[GetStudyPreview] id={reader.GetInt32("id")} trans='{rawTrans}' dod='{rawDod}' dot='{rawDot}'");

            return FunctionBase.Ok(new
            {
                id                = reader.GetInt32("id"),
                patientName       = Str(reader, "patient_name"),
                firstName         = Str(reader, "firstname"),
                lastName          = Str(reader, "lastname"),
                dob               = SafeDateStr(reader, "dob"),
                dos               = SafeDateStr(reader, "dos"),
                idNumber          = Str(reader, "idnumber"),
                accessNumber      = Str(reader, "access_number"),
                orderingPhysician = Str(reader, "ordering_physician"),
                modality          = Str(reader, "modality"),
                exam              = Str(reader, "exam"),
                status            = Str(reader, "status"),
                reportText        = Str(reader, "report_text"),
                impressionText    = Str(reader, "impression_text"),
                reportKeyImage    = Str(reader, "report_key_image"),
                isAddendum        = !reader.IsDBNull(reader.GetOrdinal("is_addendum")) && reader.GetInt32("is_addendum") == 1,
                pdfReport         = Str(reader, "pdf_report"),
                pdfPresignedUrl   = GeneratePresignedUrl(Str(reader, "pdf_report")),
                radName           = Str(reader, "rad_name"),
                radSignature      = Str(reader, "rad_signature"),
                transcriberName   = rawTrans,
                clientName        = Str(reader, "client_name"),
                clientAddress     = Str(reader, "client_address"),
                clientPhone       = Str(reader, "client_phone"),
                clientFax         = Str(reader, "client_fax"),
                headerImage       = GetS3ImageAsBase64(Str(reader, "header_image")),
                footerImage       = GetS3ImageAsBase64(Str(reader, "footer_image")),
                headingText       = Str(reader, "heading_text"),
                signatureText     = Str(reader, "signature_text"),
                dod               = rawDod,
                dot               = rawDot,
            });
        }
        catch (Exception ex) { return Err("GetStudyPreview", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteStudyAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0)
            return FunctionBase.BadRequest("StudyId is required.");
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.DeleteStudy(conn, req.StudyId);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Study deleted.");
        }
        catch (Exception ex) { return Err("DeleteStudy", ex); }
    }

    public async Task<APIGatewayProxyResponse> MarkStatAsync(string? body)
    {
        var req = Parse<MarkStatRequest>(body);
        if (req == null || req.StudyIds.Count == 0)
            return FunctionBase.BadRequest("StudyIds list is required.");
        if (req.StudyIds.Any(id => id <= 0))
            return FunctionBase.BadRequest("All StudyIds must be positive integers.");

        try
        {
            var idParams = string.Join(",", req.StudyIds);
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.MarkStudiesStat(conn, idParams);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Studies marked as STAT.");
        }
        catch (Exception ex) { return Err("MarkStat", ex); }
    }

    public async Task<APIGatewayProxyResponse> UnmarkStatAsync(string? body)
    {
        var req = Parse<MarkStatRequest>(body);
        if (req == null || req.StudyIds.Count == 0)
            return FunctionBase.BadRequest("StudyIds list is required.");
        if (req.StudyIds.Any(id => id <= 0))
            return FunctionBase.BadRequest("All StudyIds must be positive integers.");

        try
        {
            var idParams = string.Join(",", req.StudyIds);
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UnmarkStudiesStat(conn, idParams);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "STAT removed from studies.");
        }
        catch (Exception ex) { return Err("UnmarkStat", ex); }
    }

    public async Task<APIGatewayProxyResponse> CloneStudyAsync(string? body)
    {
        var req = Parse<CloneStudyRequest>(body);
        if (req == null || req.StudyId <= 0)
            return FunctionBase.BadRequest("StudyId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.CloneStudy(conn, req.StudyId);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(new { newStudyId = cmd.LastInsertedId }, "Study cloned.");
        }
        catch (Exception ex) { return Err("CloneStudy", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetStudyAuditAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0)
            return FunctionBase.BadRequest("StudyId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetStudyAudit(conn, req.StudyId);
            await using var reader = await cmd.ExecuteReaderAsync();
            var audits = new List<AuditResponse>();
            while (await reader.ReadAsync())
                audits.Add(new AuditResponse
                {
                    Id = reader.GetInt32("id"), StudyId = req.StudyId,
                    Action = Str(reader, "action"), Description = Str(reader, "description"),
                    Username = Str(reader, "username"), CreatedAt = DateStr(reader, "created_at")
                });
            return FunctionBase.Ok(audits);
        }
        catch (Exception ex) { return Err("GetStudyAudit", ex); }
    }

    private static readonly string S3Bucket =
        Environment.GetEnvironmentVariable("S3_BUCKET")
        ?? Environment.GetEnvironmentVariable("AWS_BUCKET")
        ?? "pacsmst";

    private static readonly string S3AudioBucket = S3Bucket;

    // Reads S3 object and returns data URI string (base64), same as Laravel's Storage::get() + base64_encode()
    // Falls back through "ris/", "images/", and "php/template/" prefixes if direct key not found.
    private static string? GetS3ImageAsBase64(string s3Key)
    {
        if (string.IsNullOrWhiteSpace(s3Key)) return null;
        Console.WriteLine($"[GetS3ImageAsBase64] Bucket='{S3Bucket}' Key='{s3Key}'");
        try
        {
            using var s3  = new AmazonS3Client();
            var cleanKey  = s3Key.TrimStart('/');
            var resolvedKey = cleanKey;

            var candidates = new[]
            {
                cleanKey,
                "ris/"          + cleanKey,
                "images/"       + cleanKey,
                // If DB stores just filename without a folder, try images/ + basename
                "images/"       + System.IO.Path.GetFileName(cleanKey),
            };

            string? found = null;
            foreach (var candidate in candidates)
            {
                if (S3KeyExistsAsync(s3, candidate).GetAwaiter().GetResult())
                { found = candidate; break; }
            }
            if (found == null)
            {
                Console.WriteLine($"[GetS3ImageAsBase64] Not found in S3 for any candidate of key='{s3Key}'");
                return null;
            }
            resolvedKey = found;
            Console.WriteLine($"[GetS3ImageAsBase64] Resolved to key='{resolvedKey}'");

            using var resp   = s3.GetObjectAsync(new Amazon.S3.Model.GetObjectRequest { BucketName = S3Bucket, Key = resolvedKey }).GetAwaiter().GetResult();
            using var ms     = new System.IO.MemoryStream();
            resp.ResponseStream.CopyTo(ms);
            var bytes   = ms.ToArray();
            var ext     = System.IO.Path.GetExtension(resolvedKey).TrimStart('.').ToLower();
            var mime    = ext switch { "png" => "image/png", "gif" => "image/gif", "svg" => "image/svg+xml", _ => "image/jpeg" };
            return $"data:{mime};base64,{Convert.ToBase64String(bytes)}";
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[GetS3ImageAsBase64] Error for key '{s3Key}': {ex.Message}");
            return null;
        }
    }

    // Checks if an S3 key exists using HeadObject — returns true if found
    private static async Task<bool> S3KeyExistsAsync(AmazonS3Client s3, string key)
    {
        try
        {
            await s3.GetObjectMetadataAsync(new Amazon.S3.Model.GetObjectMetadataRequest
            {
                BucketName = S3Bucket,
                Key        = key,
            });
            return true;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    // Generates a 30-min presigned URL.
    // Fallback logic (matches Laravel storage path variants):
    //   1. Try key as-is  (e.g. "php/pdf_reports/NAME_123.pdf")
    //   2. Try "ris/" + key  (e.g. "ris/php/pdf_reports/NAME_123.pdf")
    private static string? GeneratePresignedUrl(string s3Key, int expiryMinutes = 30)
    {
        if (string.IsNullOrWhiteSpace(s3Key)) return null;
        try
        {
            using var s3      = new AmazonS3Client();
            var cleanKey      = s3Key.TrimStart('/');
            var fallbackKey   = "ris/" + cleanKey;

            // Synchronous check — Lambda warm path is fast enough
            var existsDirect  = S3KeyExistsAsync(s3, cleanKey).GetAwaiter().GetResult();
            var resolvedKey   = existsDirect ? cleanKey : fallbackKey;

            if (!existsDirect)
            {
                var existsFallback = S3KeyExistsAsync(s3, fallbackKey).GetAwaiter().GetResult();
                if (!existsFallback)
                {
                    Console.WriteLine($"[PresignedUrl] Key not found in S3: '{cleanKey}' or '{fallbackKey}'");
                    return null;
                }
                Console.WriteLine($"[PresignedUrl] Using fallback key: '{fallbackKey}'");
            }

            var request = new GetPreSignedUrlRequest
            {
                BucketName = S3Bucket,
                Key        = resolvedKey,
                Expires    = DateTime.UtcNow.AddMinutes(expiryMinutes),
                Verb       = HttpVerb.GET,
            };
            var url = s3.GetPreSignedURL(request);
            Console.WriteLine($"[PresignedUrl] Generated for key '{resolvedKey}'");
            return url;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[PresignedUrl] Error for key '{s3Key}': {ex.Message}");
            return null;
        }
    }

    public async Task<APIGatewayProxyResponse> GetFinalReportUrlAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0)
            return FunctionBase.BadRequest("StudyId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetStudyById(conn, req.StudyId);
            await using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync())
                return FunctionBase.NotFound("Study not found.");

            var pdfPath      = Str(reader, "pdf_path");
            var presignedUrl = !string.IsNullOrWhiteSpace(pdfPath)
                ? GeneratePresignedUrl(pdfPath)
                : null;

            return FunctionBase.Ok(new { pdfPath, presignedUrl, studyId = req.StudyId });
        }
        catch (Exception ex) { return Err("GetFinalReportUrl", ex); }
    }

    public async Task<APIGatewayProxyResponse> SaveAuditAsync(string? body)
    {
        var req = Parse<SaveAuditRequest>(body);
        if (req == null || req.StudyId <= 0 || string.IsNullOrWhiteSpace(req.Action))
            return FunctionBase.BadRequest("StudyId and Action are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertAudit(conn, req.StudyId, _token.UserId, req.Action, req.Description);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Audit saved.");
        }
        catch (Exception ex) { return Err("SaveAudit", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateStudyReportAsync(string? body)
    {
        var req = Parse<UpdateStudyReportRequest>(body);
        if (req == null || req.StudyId <= 0)
            return FunctionBase.BadRequest("StudyId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateStudyReport(conn, req.StudyId,
                req.PatientName, req.Dob, req.Dos, req.Modality, req.Description);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Study report updated.");
        }
        catch (Exception ex) { return Err("UpdateStudyReport", ex); }
    }

    public async Task<APIGatewayProxyResponse> PdfReviewedAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0)
            return FunctionBase.BadRequest("StudyId is required.");

        try
        {
            await using var conn     = await OpenAsync();
            await using var checkCmd = QueryHelper.AuditAlreadyExists(conn, req.StudyId, "PDF Reviewed");
            var count = Convert.ToInt32(await checkCmd.ExecuteScalarAsync());
            if (count == 0)
            {
                await using var insertCmd = QueryHelper.InsertAudit(conn, req.StudyId, _token.UserId, "PDF Reviewed", null);
                await insertCmd.ExecuteNonQueryAsync();
            }
            return FunctionBase.Ok(null, "PDF reviewed logged.");
        }
        catch (Exception ex) { return Err("PdfReviewed", ex); }
    }

    public async Task<APIGatewayProxyResponse> CheckForDownloadAudioAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0)
            return FunctionBase.BadRequest("StudyId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetAttachedFiles(conn, req.StudyId, "Audio File");
            await using var reader = await cmd.ExecuteReaderAsync();
            var files = new List<AttachmentResponse>();
            while (await reader.ReadAsync())
                files.Add(new AttachmentResponse
                {
                    Id = reader.GetInt32("id"), StudyId = req.StudyId,
                    FileName = Str(reader, "file_name"), FileType = Str(reader, "type"),
                    FilePath = Str(reader, "file_path"), CreatedAt = DateStr(reader, "created_at")
                });
            return FunctionBase.Ok(new { hasAudio = files.Count > 0, files });
        }
        catch (Exception ex) { return Err("CheckForDownloadAudio", ex); }
    }

    // ── Download Audio ZIP ────────────────────────────────────────────────────
    // Accepts { studyIds: [1,2,3], orderNumbers: {"1":"564830","2":"503469"} }
    // Fetches all audio files per study, zips them into per-study folders,
    // uploads the ZIP to S3 temp/ and returns a presigned download URL.
    public async Task<APIGatewayProxyResponse> DownloadAudioZipAsync(string? body)
    {
        var req = Parse<DownloadAudioZipRequest>(body);
        if (req == null || req.StudyIds == null || req.StudyIds.Count == 0)
            return FunctionBase.BadRequest("studyIds is required.");

        try
        {
            using var s3  = new AmazonS3Client();
            using var ms  = new System.IO.MemoryStream();
            using (var zip = new System.IO.Compression.ZipArchive(ms, System.IO.Compression.ZipArchiveMode.Create, leaveOpen: true))
            {
                await using var conn = await OpenAsync();

                foreach (var studyId in req.StudyIds)
                {
                    req.OrderNumbers.TryGetValue(studyId.ToString(), out var orderNum);
                    var folderName = string.IsNullOrWhiteSpace(orderNum) ? studyId.ToString() : orderNum;

                    await using var cmd    = QueryHelper.GetAttachedFiles(conn, studyId, "Audio File");
                    await using var reader = await cmd.ExecuteReaderAsync();
                    var paths = new List<string>();
                    while (await reader.ReadAsync())
                    {
                        var fp = Str(reader, "file_path");
                        if (!string.IsNullOrWhiteSpace(fp)) paths.Add(fp);
                    }

                    foreach (var filePath in paths)
                    {
                        var cleanKey = filePath.TrimStart('/');
                        // resolve key (direct or ris/ prefix)
                        string? resolvedKey = null;
                        foreach (var candidate in new[] { cleanKey, "ris/" + cleanKey })
                        {
                            if (await S3KeyExistsAsync(s3, candidate)) { resolvedKey = candidate; break; }
                        }
                        if (resolvedKey == null) continue;

                        try
                        {
                            var getReq = new Amazon.S3.Model.GetObjectRequest { BucketName = S3Bucket, Key = resolvedKey };
                            using var getResp = await s3.GetObjectAsync(getReq);
                            var fileName = System.IO.Path.GetFileName(resolvedKey);
                            var entry    = zip.CreateEntry($"{folderName}/{fileName}", System.IO.Compression.CompressionLevel.Fastest);
                            await using var entryStream = entry.Open();
                            await getResp.ResponseStream.CopyToAsync(entryStream);
                        }
                        catch (Exception ex) { Console.WriteLine($"[DownloadAudioZip] Skip file {filePath}: {ex.Message}"); }
                    }
                }
            }

            ms.Position = 0;
            if (ms.Length == 0)
                return FunctionBase.BadRequest("No audio files found for the requested studies.");

            // Upload ZIP to S3 temp/ prefix
            var zipKey  = $"temp/audio_zip/{Guid.NewGuid()}.zip";
            var putReq  = new Amazon.S3.Model.PutObjectRequest
            {
                BucketName  = S3Bucket,
                Key         = zipKey,
                InputStream = ms,
                ContentType = "application/zip",
            };
            await s3.PutObjectAsync(putReq);

            // Generate presigned URL (30 min)
            var presigned = s3.GetPreSignedURL(new GetPreSignedUrlRequest
            {
                BucketName = S3Bucket,
                Key        = zipKey,
                Expires    = DateTime.UtcNow.AddMinutes(30),
                Verb       = HttpVerb.GET,
            });

            return FunctionBase.Ok(new { zipUrl = presigned, zipKey });
        }
        catch (Exception ex) { return Err("DownloadAudioZip", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteTempZipAsync(string? body)
    {
        var req = Parse<DeleteTempZipRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.ZipKey))
            return FunctionBase.BadRequest("zipKey is required.");
        try
        {
            // Safety: only allow deleting under temp/ prefix
            var key = req.ZipKey.TrimStart('/');
            if (!key.StartsWith("temp/"))
                return FunctionBase.BadRequest("Invalid key — only temp/ files can be deleted.");
            using var s3 = new AmazonS3Client();
            await s3.DeleteObjectAsync(new Amazon.S3.Model.DeleteObjectRequest { BucketName = S3Bucket, Key = key });
            return FunctionBase.Ok(null, "Temp ZIP deleted.");
        }
        catch (Exception ex) { return Err("DeleteTempZip", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetStudyDocumentAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0)
            return FunctionBase.BadRequest("StudyId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetAttachedFiles(conn, req.StudyId, "document");
            await using var reader = await cmd.ExecuteReaderAsync();
            var docs = new List<AttachmentResponse>();
            while (await reader.ReadAsync())
                docs.Add(new AttachmentResponse
                {
                    Id = reader.GetInt32("id"), StudyId = req.StudyId,
                    FileName = Str(reader, "file_name"), FileType = Str(reader, "type"),
                    FilePath = Str(reader, "file_path"), CreatedAt = DateStr(reader, "created_at")
                });
            return FunctionBase.Ok(docs);
        }
        catch (Exception ex) { return Err("GetStudyDocument", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetVerificationSheetAsync(string? body)
        => await GetStudyDocumentAsync(body);

    public async Task<APIGatewayProxyResponse> GetExamHistoryAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0)
            return FunctionBase.BadRequest("StudyId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetPatientExamHistory(conn, req.StudyId);
            await using var reader = await cmd.ExecuteReaderAsync();
            var history = new List<object>();
            while (await reader.ReadAsync())
                history.Add(new
                {
                    id       = reader.GetInt32("id"),
                    modality = Str(reader, "modality"),
                    exam     = Str(reader, "exam"),
                    dos      = DateStr(reader, "dos"),
                    status   = Str(reader, "status"),
                    pdfPath  = Str(reader, "pdf_path"),
                    radName  = Str(reader, "rad_name"),
                });
            return FunctionBase.Ok(history);
        }
        catch (Exception ex) { return Err("GetExamHistory", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // NOTES
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetNotesAsync(string? body)
    {
        var req = Parse<GetNotesRequest>(body);
        if (req == null || req.StudyId <= 0)
            return FunctionBase.BadRequest("StudyId is required.");

        try
        {
            await using var conn = await OpenAsync();
            if (req.AutoView)
            {
                await using var auditCmd = QueryHelper.InsertAudit(conn, req.StudyId, _token.UserId, "Notes Viewed", null);
                await auditCmd.ExecuteNonQueryAsync();
            }
            await using var cmd    = QueryHelper.GetNotes(conn, req.StudyId);
            await using var reader = await cmd.ExecuteReaderAsync();
            var notes = new List<NoteResponse>();
            while (await reader.ReadAsync())
                notes.Add(new NoteResponse
                {
                    Id = reader.GetInt32("id"), UserId = SafeInt(reader, "userid") ?? 0,
                    Username = Str(reader, "username"),
                    Notes = Str(reader, "notes"), Date = DateStr(reader, "created_at")
                });
            return FunctionBase.Ok(notes);
        }
        catch (Exception ex) { return Err("GetNotes", ex); }
    }

    public async Task<APIGatewayProxyResponse> AddNoteAsync(string? body)
    {
        var req = Parse<AddNoteRequest>(body);
        if (req == null || req.StudyId <= 0 || string.IsNullOrWhiteSpace(req.Notes))
            return FunctionBase.BadRequest("StudyId and Notes are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertNote(conn, req.StudyId, _token.UserId, req.Notes);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Note added.");
        }
        catch (Exception ex) { return Err("AddNote", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateNoteAsync(string? body)
    {
        var req = Parse<UpdateNoteRequest>(body);
        if (req == null || req.NoteId <= 0 || string.IsNullOrWhiteSpace(req.Notes))
            return FunctionBase.BadRequest("NoteId and Notes are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateNote(conn, req.NoteId, req.Notes);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Note updated.");
        }
        catch (Exception ex) { return Err("UpdateNote", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // ATTACHMENTS / AUDIO
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> AddAttachmentAsync(string? body)
    {
        var req = Parse<AddAttachmentRequest>(body);
        if (req == null || req.StudyId <= 0 || string.IsNullOrWhiteSpace(req.FileName))
            return FunctionBase.BadRequest("StudyId and FileName are required.");

        try
        {
            var filePath = $"attachments/{req.StudyId}/{req.FileName}";
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertAttachment(conn, req.StudyId, req.FileName, filePath, req.FileType);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(new { filePath }, "Attachment saved.");
        }
        catch (Exception ex) { return Err("AddAttachment", ex); }
    }

    public async Task<APIGatewayProxyResponse> AddMultipleAttachmentsAsync(string? body)
    {
        var req = Parse<AddMultipleAttachmentsRequest>(body);
        if (req == null || req.StudyIds.Count == 0 || string.IsNullOrWhiteSpace(req.FileName))
            return FunctionBase.BadRequest("StudyIds and FileName are required.");

        try
        {
            await using var conn = await OpenAsync();
            foreach (var sid in req.StudyIds)
            {
                var filePath = $"attachments/{sid}/{req.FileName}";
                await using var cmd = QueryHelper.InsertAttachment(conn, sid, req.FileName, filePath, "document");
                await cmd.ExecuteNonQueryAsync();
            }
            return FunctionBase.Ok(null, $"Attachment added to {req.StudyIds.Count} studies.");
        }
        catch (Exception ex) { return Err("AddMultipleAttachments", ex); }
    }

    public Task<APIGatewayProxyResponse> GetPresignedUrlAsync(string? body)
    {
        try
        {
            var req = Parse<GetPresignedUrlRequest>(body);
            if (req == null || string.IsNullOrWhiteSpace(req.FilePath))
                return Task.FromResult(FunctionBase.BadRequest("FilePath is required."));

            var key = req.FilePath.TrimStart('/');
            using var s3  = new AmazonS3Client();
            var urlReq    = new Amazon.S3.Model.GetPreSignedUrlRequest
            {
                BucketName = S3Bucket,
                Key        = key,
                Expires    = DateTime.UtcNow.AddMinutes(30),
                Verb       = Amazon.S3.HttpVerb.GET,
            };
            var url = s3.GetPreSignedURL(urlReq);
            return Task.FromResult(FunctionBase.Ok(new { url }));
        }
        catch (Exception ex) { return Task.FromResult(Err("GetPresignedUrl", ex)); }
    }

    public Task<APIGatewayProxyResponse> GetPresignedUploadUrlAsync(string? body)
    {
        try
        {
            var req = Parse<GetPresignedUploadUrlRequest>(body);
            if (req == null || req.StudyId <= 0 || string.IsNullOrWhiteSpace(req.FileName))
                return Task.FromResult(FunctionBase.BadRequest("StudyId and FileName are required."));

            var key = $"attachments/{req.StudyId}/{req.FileName}";
            using var s3 = new AmazonS3Client();
            var urlReq   = new Amazon.S3.Model.GetPreSignedUrlRequest
            {
                BucketName  = S3Bucket,
                Key         = key,
                Expires     = DateTime.UtcNow.AddMinutes(15),
                Verb        = Amazon.S3.HttpVerb.PUT,
                ContentType = req.ContentType,
            };
            var uploadUrl = s3.GetPreSignedURL(urlReq);
            return Task.FromResult(FunctionBase.Ok(new { uploadUrl, filePath = key }));
        }
        catch (Exception ex) { return Task.FromResult(Err("GetPresignedUploadUrl", ex)); }
    }

    public async Task<APIGatewayProxyResponse> UploadAudioAttachmentAsync(string? body)
    {
        try
        {
            var req = Parse<UploadAudioAttachmentRequest>(body);
            if (req == null || req.StudyId <= 0 || string.IsNullOrWhiteSpace(req.FileName) || string.IsNullOrWhiteSpace(req.Data))
                return FunctionBase.BadRequest("StudyId, FileName and Data are required.");

            var bytes   = Convert.FromBase64String(req.Data);
            var key     = $"attachments/{req.StudyId}/{req.FileName}";

            using var s3     = new AmazonS3Client();
            using var stream = new System.IO.MemoryStream(bytes);
            var putReq = new Amazon.S3.Model.PutObjectRequest
            {
                BucketName  = S3AudioBucket,
                Key         = key,
                InputStream = stream,
                ContentType = "audio/wav",
            };
            await s3.PutObjectAsync(putReq);

            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertAttachment(conn, req.StudyId, req.FileName, key, "Audio File");
            await cmd.ExecuteNonQueryAsync();

            return FunctionBase.Ok(new { filePath = key }, "Audio uploaded.");
        }
        catch (Exception ex) { return Err("UploadAudioAttachment", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetViewerTokenAsync(string? body)
    {
        try
        {
            var req = Parse<GetViewerTokenRequest>(body);
            if (req == null || string.IsNullOrWhiteSpace(req.Sid))
                return FunctionBase.BadRequest("sid is required.");

            // Match Laravel edit.blade.php exactly: studies is an object, not an array
            var payload = System.Text.Json.JsonSerializer.Serialize(new
            {
                items = new[]
                {
                    new
                    {
                        studies = new { study = req.Sid, storage = "Pacs" }
                    }
                }
            });

            using var http    = new System.Net.Http.HttpClient { Timeout = TimeSpan.FromSeconds(10) };
            using var content = new System.Net.Http.StringContent(payload, System.Text.Encoding.UTF8, "application/json");
            var response      = await http.PostAsync("http://18.214.76.136:8088/v2/generate", content);
            var token         = (await response.Content.ReadAsStringAsync()).Trim().Trim('"');

            if (string.IsNullOrWhiteSpace(token))
                return FunctionBase.ServerError("PACS server returned empty token.");

            return FunctionBase.Ok(new { token });
        }
        catch (Exception ex) { return Err("GetViewerToken", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetAttachedFilesAsync(string? body)
    {
        var req = Parse<GetAttachedFilesRequest>(body);
        if (req == null || req.StudyId <= 0)
            return FunctionBase.BadRequest("StudyId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetAttachedFiles(conn, req.StudyId, req.FileType);
            await using var reader = await cmd.ExecuteReaderAsync();
            var files = new List<AttachmentResponse>();
            while (await reader.ReadAsync())
                files.Add(new AttachmentResponse
                {
                    Id = reader.GetInt32("id"), StudyId = req.StudyId,
                    FileName = Str(reader, "file_name"), FileType = Str(reader, "type"),
                    FilePath = Str(reader, "file_path"), CreatedAt = DateStr(reader, "created_at")
                });
            return FunctionBase.Ok(files);
        }
        catch (Exception ex) { return Err("GetAttachedFiles", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetIncomingFaxesAsync(string? body)
    {
        var req = Parse<GetIncomingFaxesRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.ClientUsername))
            return FunctionBase.BadRequest("ClientUsername is required.");
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetIncomingFaxesByClient(conn, req.ClientUsername);
            await using var reader = await cmd.ExecuteReaderAsync();
            var faxes = new List<object>();
            while (await reader.ReadAsync())
                faxes.Add(new
                {
                    id         = reader.GetInt32("id"),
                    fileName   = Str(reader, "file_name"),
                    filePath   = Str(reader, "file_name"), // media_file is just the filename
                    createdAt  = SafeDateStr(reader, "created_at"),
                    fileType   = "fax"
                });
            return FunctionBase.Ok(faxes);
        }
        catch (Exception ex) { return Err("GetIncomingFaxes", ex); }
    }

    public async Task<APIGatewayProxyResponse> SendAudioFilesAsync(string? body)
    {
        var req = Parse<SendAudioFilesRequest>(body);
        if (req == null || req.StudyId <= 0 || req.TranscriberId <= 0)
            return FunctionBase.BadRequest("StudyId and TranscriberId are required.");

        try
        {
            await using var conn      = await OpenAsync();
            var filePath              = $"audio/{req.StudyId}/{req.FileName}";
            await using var attCmd    = QueryHelper.InsertAttachment(conn, req.StudyId, req.FileName, filePath, "audio");
            await attCmd.ExecuteNonQueryAsync();
            await using var statusCmd = QueryHelper.UpdateStudyStatus(conn, req.StudyId, "trans_new_messages");
            await statusCmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Audio file sent to transcriber.");
        }
        catch (Exception ex) { return Err("SendAudioFiles", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetSentAudioFilesAsync(string? body)
    {
        var req = Parse<GetSentAudioFilesRequest>(body) ?? new GetSentAudioFilesRequest();
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.GetSentAudioFiles(conn, req.Dos, req.ClientId, _token.UserId);
            return FunctionBase.Ok(await ReadStudies(cmd));
        }
        catch (Exception ex) { return Err("GetSentAudioFiles", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // FINAL REPORTS
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetFinalReportsAsync(string? body)
    {
        var req     = Parse<GetFinalReportsRequest>(body) ?? new GetFinalReportsRequest();
        var page    = Math.Max(1, req.Page ?? 1);
        var perPage = req.PerPage ?? 20;

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetFinalReports(conn, req.DateFrom, req.DateTo, req.RadId,
                (page - 1) * perPage, perPage);
            await using var reader = await cmd.ExecuteReaderAsync();
            var reports = new List<FinalReportResponse>();
            while (await reader.ReadAsync())
                reports.Add(new FinalReportResponse
                {
                    Id = reader.GetInt32("id"), PatientName = Str(reader, "patient_name"),
                    Modality = Str(reader, "modality"), Dos = DateStr(reader, "dos"),
                    ReportText = Str(reader, "report_text"), Impression = Str(reader, "impression"),
                    Status = Str(reader, "status"), ClientName = Str(reader, "client_name"),
                    PdfUrl = Str(reader, "pdf_path"), RadName = Str(reader, "rad_name"),
                    UpdatedAt = DateStr(reader, "updated_at")
                });
            return FunctionBase.Ok(reports);
        }
        catch (Exception ex) { return Err("GetFinalReports", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateFinalReportAsync(string? body)
    {
        var req = Parse<UpdateFinalReportRequest>(body);
        if (req == null || req.StudyId <= 0 || string.IsNullOrWhiteSpace(req.ReportText))
            return FunctionBase.BadRequest("StudyId and ReportText are required.");

        try
        {
            await using var conn     = await OpenAsync();
            await using var cmd      = QueryHelper.UpdateFinalReport(conn, req.StudyId, req.ReportText, req.Impression, req.IsAddendum);
            await cmd.ExecuteNonQueryAsync();
            await using var auditCmd = QueryHelper.InsertAudit(conn, req.StudyId, _token.UserId,
                req.IsAddendum ? "Addendum Created" : "Final Report Updated", null);
            await auditCmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Final report updated.");
        }
        catch (Exception ex) { return Err("UpdateFinalReport", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // DASHBOARD / CHARTS
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetChartDataAsync(string? body)
    {
        var req = Parse<GetChartDataRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.ReportType))
            return FunctionBase.BadRequest("ReportType is required.");

        try
        {
            await using var conn = await OpenAsync();
            var (df, dt) = ResolveDateRange(req.DateRange);

            MySqlCommand cmd = req.ReportType.ToLower() switch
            {
                "pending-reports"                              => QueryHelper.GetChartPendingReports(conn, req.RadId),
                "no-audio-file"                                => QueryHelper.GetChartNoAudioFile(conn, req.RadId),
                "records-pending-per-ranscriber"
                or "records-pending-per-transcriber"           => QueryHelper.GetChartPendingByTranscriber(conn, req.RadId),
                "records-on-hold-ranscriber"
                or "records-on-hold-transcriber"               => QueryHelper.GetChartOnHoldByTranscriber(conn, req.RadId),
                "records-on-hold-radiologist"                  => QueryHelper.GetChartOnHoldByRadiologist(conn, req.RadId),
                "records-pending-signature-per-radiologist"    => QueryHelper.GetChartPendingSignatureByRadiologist(conn, req.RadId),
                "records-finalized-per-radiologist"            => QueryHelper.GetChartFinalizedPerRadiologist(conn, req.RadId, df, dt),
                "earning-by-modality"                          => QueryHelper.GetChartEarningByModality(conn, req.RadId, df, dt),
                _                                              => QueryHelper.GetChartPendingReports(conn, req.RadId)
            };

            await using var reader = await cmd.ExecuteReaderAsync();
            var items = new List<ChartDataItem>();
            while (await reader.ReadAsync())
                items.Add(new ChartDataItem { Label = Str(reader, "label"), Total = Convert.ToDecimal(reader["total"]) });
            return FunctionBase.Ok(new ChartResponse { TotalRecords = items.Sum(i => i.Total), ChartData = items });
        }
        catch (Exception ex) { return Err("GetChartData", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetLastFifteenRecordsAsync(string? body)
    {
        var req    = Parse<GetFifteenRecordsRequest>(body) ?? new GetFifteenRecordsRequest();
        var status = req.Status ?? "new_study";
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.GetStudiesByStatus(conn, status, _token.UserId, _token.UserType, 0, 15);
            return FunctionBase.Ok(await ReadStudies(cmd));
        }
        catch (Exception ex) { return Err("GetLastFifteenRecords", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetStudiesByStatusAsync(string status, string? body = null)
    {
        try
        {
            var req      = Parse<PagedRequest>(body);
            var page     = Math.Max(1, req?.Page     ?? 1);
            var pageSize = Math.Max(1, req?.PageSize ?? 20);
            var offset   = (page - 1) * pageSize;

            await using var conn      = await OpenAsync();
            await using var countCmd  = QueryHelper.GetStudiesByStatusCount(conn, status, _token.UserId, _token.UserType);
            var total = Convert.ToInt32(await countCmd.ExecuteScalarAsync());

            await using var cmd     = QueryHelper.GetStudiesByStatus(conn, status, _token.UserId, _token.UserType, offset, pageSize);
            var studies = await ReadStudies(cmd);

            return FunctionBase.Ok(new {
                studies,
                total,
                page,
                pageSize,
                hasMore = (offset + studies.Count) < total
            });
        }
        catch (Exception ex) { return Err("GetStudiesByStatus", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetPendingBatchAsync(string? body)
    {
        try
        {
            var req      = Parse<PendingBatchRequest>(body);
            if (req == null || req.Statuses == null || req.Statuses.Count == 0)
                return FunctionBase.BadRequest("statuses array is required.");

            var page     = Math.Max(1, req.Page     > 0 ? req.Page     : 1);
            var pageSize = Math.Max(1, req.PageSize > 0 ? req.PageSize : 10);
            var offset   = (page - 1) * pageSize;

            await using var conn      = await OpenAsync();
            await using var countCmd  = QueryHelper.GetStudiesBatchCount(conn, req.Statuses, _token.UserId, _token.UserType);
            var total = Convert.ToInt32(await countCmd.ExecuteScalarAsync());

            await using var cmd     = QueryHelper.GetStudiesBatch(conn, req.Statuses, _token.UserId, _token.UserType, offset, pageSize);
            var studies = await ReadStudies(cmd);

            return FunctionBase.Ok(new {
                studies,
                total,
                page,
                pageSize,
                hasMore = (offset + studies.Count) < total
            });
        }
        catch (Exception ex) { return Err("GetPendingBatch", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetPendingModalitiesAsync()
    {
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetPendingModalities(conn);
            await using var reader = await cmd.ExecuteReaderAsync();
            var list = new List<string>();
            while (await reader.ReadAsync())
            {
                var m = reader.IsDBNull(0) ? null : reader.GetString(0);
                if (!string.IsNullOrWhiteSpace(m)) list.Add(m);
            }
            return FunctionBase.Ok(list);
        }
        catch (Exception ex) { return Err("GetPendingModalities", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetFinalizedDataAsync(string? body)
    {
        var req = Parse<GetFinalizedDataRequest>(body) ?? new GetFinalizedDataRequest();
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.GetFinalizedData(conn, req.RadId, req.TranscriberId, req.DateFrom, req.DateTo);
            return FunctionBase.Ok(await ReadStudies(cmd));
        }
        catch (Exception ex) { return Err("GetFinalizedData", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetRefFinalizedDataAsync(string? body)
        => await GetFinalizedDataAsync(body);
}
