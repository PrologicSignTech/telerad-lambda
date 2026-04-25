using Amazon.Lambda.APIGatewayEvents;
using Amazon.S3;
using Amazon.S3.Model;
using MySqlConnector;
using Newtonsoft.Json;
using TeleRadLambda.Model;
using TeleRadLambda.Utility;

namespace TeleRadLambda;

public class Helper
{
    private readonly string _cs;
    private readonly TokenValidationResult _token;
    private static readonly string S3Bucket =
        Environment.GetEnvironmentVariable("S3_BUCKET") ?? "pacsmst";

    public Helper(string connectionString, TokenValidationResult? token = null)
    {
        _cs    = connectionString;
        _token = token ?? new TokenValidationResult();
    }

    // ══════════════════════════════════════════════════════════════════════════
    // AUTH
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> LoginAsync(string? body)
    {
        var req = Parse<LoginRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.Username) || string.IsNullOrWhiteSpace(req.Password))
            return Function.BadRequest("Username and password are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd = QueryHelper.FindUserForLogin(conn, req.Username);
            await using var reader = await cmd.ExecuteReaderAsync();

            if (!await reader.ReadAsync())
                return Function.Unauthorized("Invalid credentials.");

            var storedHash = reader.GetString("password");
            if (!BCrypt.Net.BCrypt.Verify(req.Password, storedHash))
                return Function.Unauthorized("Invalid credentials.");

            var userId   = reader.GetInt32("id");
            var name     = reader.GetString("name");
            var email    = reader.GetString("email");
            var username = reader.GetString("username");
            var userType = reader.GetInt32("usertype");
            await reader.CloseAsync();

            // Load permissions
            await using var permCmd    = QueryHelper.GetUserPermissions(conn, userId);
            await using var permReader = await permCmd.ExecuteReaderAsync();
            var perms = new List<string>();
            while (await permReader.ReadAsync()) perms.Add(permReader.GetString(0));
            await permReader.CloseAsync();

            var token = GenerateToken();
            await using var upsertCmd = QueryHelper.UpsertToken(conn, userId, username, token);
            await upsertCmd.ExecuteNonQueryAsync();

            return Function.Ok(new LoginResponse
            {
                Id          = userId,
                Name        = name,
                Email       = email,
                Username    = username,
                UserType    = userType,
                Token       = token,
                Permissions = perms
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
            return Function.Ok(null, "Logged out successfully.");
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
            return Function.Ok(await ReadStudies(cmd));
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
            return Function.Ok(await ReadStudies(cmd));
        }
        catch (Exception ex) { return Err("GetDailyWorkflowData", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateStudyStatusAsync(string? body)
    {
        var req = Parse<UpdateStudyStatusRequest>(body);
        if (req == null || req.StudyId <= 0 || string.IsNullOrWhiteSpace(req.Status))
            return Function.BadRequest("StudyId and Status are required.");

        var allowed = new HashSet<string>
        {
            "new_study","trans_new_messages","trans_reports_on_hold","rad_reports_on_hold",
            "rad_reports_pending_signature","hold_for_comparison","missing_paperwork",
            "speak_to_tech","missing_images","review","finalized","no_audio","in_progress","cancelled"
        };
        if (!allowed.Contains(req.Status.ToLower()))
            return Function.BadRequest($"Invalid status: {req.Status}");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateStudyStatus(conn, req.StudyId, req.Status);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Status updated.");
        }
        catch (Exception ex) { return Err("UpdateStudyStatus", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateStudyAsync(string? body)
    {
        var req = Parse<UpdateStudyRequest>(body);
        if (req == null || req.StudyId <= 0)
            return Function.BadRequest("StudyId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateStudy(conn, req.StudyId,
                req.PatientName, req.PatientDob, req.Dos, req.Modality,
                req.Description, req.Status, req.TranscriberId, req.RadId, req.TemplateId);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Study updated.");
        }
        catch (Exception ex) { return Err("UpdateStudy", ex); }
    }

    public async Task<APIGatewayProxyResponse> MarkStatAsync(string? body)
    {
        var req = Parse<MarkStatRequest>(body);
        if (req == null || req.StudyIds.Count == 0)
            return Function.BadRequest("StudyIds list is required.");
        if (req.StudyIds.Any(id => id <= 0))
            return Function.BadRequest("All StudyIds must be positive integers.");

        try
        {
            var idParams = string.Join(",", req.StudyIds);
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.MarkStudiesStat(conn, idParams);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Studies marked as STAT.");
        }
        catch (Exception ex) { return Err("MarkStat", ex); }
    }

    public async Task<APIGatewayProxyResponse> UnmarkStatAsync(string? body)
    {
        var req = Parse<MarkStatRequest>(body);
        if (req == null || req.StudyIds.Count == 0)
            return Function.BadRequest("StudyIds list is required.");
        if (req.StudyIds.Any(id => id <= 0))
            return Function.BadRequest("All StudyIds must be positive integers.");

        try
        {
            var idParams = string.Join(",", req.StudyIds);
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UnmarkStudiesStat(conn, idParams);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "STAT removed from studies.");
        }
        catch (Exception ex) { return Err("UnmarkStat", ex); }
    }

    public async Task<APIGatewayProxyResponse> CloneStudyAsync(string? body)
    {
        var req = Parse<CloneStudyRequest>(body);
        if (req == null || req.StudyId <= 0)
            return Function.BadRequest("StudyId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.CloneStudy(conn, req.StudyId);
            await cmd.ExecuteNonQueryAsync();
            var newId = cmd.LastInsertedId;
            return Function.Ok(new { newStudyId = newId }, "Study cloned.");
        }
        catch (Exception ex) { return Err("CloneStudy", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetStudyAuditAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0)
            return Function.BadRequest("StudyId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetStudyAudit(conn, req.StudyId);
            await using var reader = await cmd.ExecuteReaderAsync();
            var audits = new List<AuditResponse>();
            while (await reader.ReadAsync())
            {
                audits.Add(new AuditResponse
                {
                    Id          = reader.GetInt32("id"),
                    StudyId     = req.StudyId,
                    Action      = Str(reader, "action"),
                    Description = Str(reader, "description"),
                    Username    = Str(reader, "username"),
                    CreatedAt   = DateStr(reader, "created_at")
                });
            }
            return Function.Ok(audits);
        }
        catch (Exception ex) { return Err("GetStudyAudit", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetFinalReportUrlAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0)
            return Function.BadRequest("StudyId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetStudyById(conn, req.StudyId);
            await using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync())
                return Function.NotFound("Study not found.");

            var pdfPath = Str(reader, "pdf_path");
            return Function.Ok(new { pdfPath, studyId = req.StudyId });
        }
        catch (Exception ex) { return Err("GetFinalReportUrl", ex); }
    }

    public async Task<APIGatewayProxyResponse> SaveAuditAsync(string? body)
    {
        var req = Parse<SaveAuditRequest>(body);
        if (req == null || req.StudyId <= 0 || string.IsNullOrWhiteSpace(req.Action))
            return Function.BadRequest("StudyId and Action are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertAudit(conn, req.StudyId, _token.UserId, req.Action, req.Description);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Audit saved.");
        }
        catch (Exception ex) { return Err("SaveAudit", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateStudyReportAsync(string? body)
    {
        var req = Parse<UpdateStudyReportRequest>(body);
        if (req == null || req.StudyId <= 0)
            return Function.BadRequest("StudyId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateStudyReport(conn, req.StudyId,
                req.PatientName, req.Dob, req.Dos, req.Modality, req.Description);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Study report updated.");
        }
        catch (Exception ex) { return Err("UpdateStudyReport", ex); }
    }

    public async Task<APIGatewayProxyResponse> PdfReviewedAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0)
            return Function.BadRequest("StudyId is required.");

        try
        {
            await using var conn      = await OpenAsync();
            await using var checkCmd  = QueryHelper.AuditAlreadyExists(conn, req.StudyId, "PDF Reviewed");
            var count = Convert.ToInt32(await checkCmd.ExecuteScalarAsync());
            if (count == 0)
            {
                await using var insertCmd = QueryHelper.InsertAudit(conn, req.StudyId, _token.UserId, "PDF Reviewed", null);
                await insertCmd.ExecuteNonQueryAsync();
            }
            return Function.Ok(null, "PDF reviewed logged.");
        }
        catch (Exception ex) { return Err("PdfReviewed", ex); }
    }

    public async Task<APIGatewayProxyResponse> CheckForDownloadAudioAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0)
            return Function.BadRequest("StudyId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetAttachedFiles(conn, req.StudyId, "audio");
            await using var reader = await cmd.ExecuteReaderAsync();
            var files = new List<AttachmentResponse>();
            while (await reader.ReadAsync())
            {
                files.Add(new AttachmentResponse
                {
                    Id        = reader.GetInt32("id"),
                    StudyId   = req.StudyId,
                    FileName  = Str(reader, "file_name"),
                    FileType  = Str(reader, "type"),
                    FilePath  = Str(reader, "file_path"),
                    CreatedAt = DateStr(reader, "created_at")
                });
            }
            return Function.Ok(new { hasAudio = files.Count > 0, files });
        }
        catch (Exception ex) { return Err("CheckForDownloadAudio", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetStudyDocumentAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0)
            return Function.BadRequest("StudyId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetAttachedFiles(conn, req.StudyId, "document");
            await using var reader = await cmd.ExecuteReaderAsync();
            var docs = new List<AttachmentResponse>();
            while (await reader.ReadAsync())
            {
                docs.Add(new AttachmentResponse
                {
                    Id = reader.GetInt32("id"), StudyId = req.StudyId,
                    FileName = Str(reader, "file_name"), FileType = Str(reader, "type"),
                    FilePath = Str(reader, "file_path"), CreatedAt = DateStr(reader, "created_at")
                });
            }
            return Function.Ok(docs);
        }
        catch (Exception ex) { return Err("GetStudyDocument", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetVerificationSheetAsync(string? body)
        => await GetStudyDocumentAsync(body); // Same query, different context

    public async Task<APIGatewayProxyResponse> GetExamHistoryAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0)
            return Function.BadRequest("StudyId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetStudyAudit(conn, req.StudyId);
            await using var reader = await cmd.ExecuteReaderAsync();
            var history = new List<AuditResponse>();
            while (await reader.ReadAsync())
            {
                history.Add(new AuditResponse
                {
                    Id = reader.GetInt32("id"), StudyId = req.StudyId,
                    Action = Str(reader, "action"), Description = Str(reader, "description"),
                    Username = Str(reader, "username"), CreatedAt = DateStr(reader, "created_at")
                });
            }
            return Function.Ok(history);
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
            return Function.BadRequest("StudyId is required.");

        try
        {
            await using var conn   = await OpenAsync();

            if (req.AutoView)
            {
                await using var auditCmd = QueryHelper.InsertAudit(conn, req.StudyId, _token.UserId, "Notes Viewed", null);
                await auditCmd.ExecuteNonQueryAsync();
            }

            await using var cmd    = QueryHelper.GetNotes(conn, req.StudyId);
            await using var reader = await cmd.ExecuteReaderAsync();
            var notes = new List<NoteResponse>();
            while (await reader.ReadAsync())
            {
                notes.Add(new NoteResponse
                {
                    Id       = reader.GetInt32("id"),
                    Username = Str(reader, "username"),
                    Notes    = Str(reader, "notes"),
                    Date     = DateStr(reader, "created_at")
                });
            }
            return Function.Ok(notes);
        }
        catch (Exception ex) { return Err("GetNotes", ex); }
    }

    public async Task<APIGatewayProxyResponse> AddNoteAsync(string? body)
    {
        var req = Parse<AddNoteRequest>(body);
        if (req == null || req.StudyId <= 0 || string.IsNullOrWhiteSpace(req.Notes))
            return Function.BadRequest("StudyId and Notes are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertNote(conn, req.StudyId, _token.UserId, req.Notes);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Note added.");
        }
        catch (Exception ex) { return Err("AddNote", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateNoteAsync(string? body)
    {
        var req = Parse<UpdateNoteRequest>(body);
        if (req == null || req.NoteId <= 0 || string.IsNullOrWhiteSpace(req.Notes))
            return Function.BadRequest("NoteId and Notes are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateNote(conn, req.NoteId, req.Notes);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Note updated.");
        }
        catch (Exception ex) { return Err("UpdateNote", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // TEMPLATES
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetAllTemplatesAsync(string? body)
    {
        var req = Parse<GetTemplatesRequest>(body) ?? new GetTemplatesRequest();
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetTemplates(conn, req.Search, null);
            await using var reader = await cmd.ExecuteReaderAsync();
            return Function.Ok(await ReadTemplates(reader));
        }
        catch (Exception ex) { return Err("GetAllTemplates", ex); }
    }

    public async Task<APIGatewayProxyResponse> SearchTemplatesAsync(string? body)
    {
        var req = Parse<SearchTemplatesRequest>(body) ?? new SearchTemplatesRequest();
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetTemplates(conn, req.Query, req.UserId);
            await using var reader = await cmd.ExecuteReaderAsync();
            return Function.Ok(await ReadTemplates(reader));
        }
        catch (Exception ex) { return Err("SearchTemplates", ex); }
    }

    public async Task<APIGatewayProxyResponse> CreateTemplateAsync(string? body)
    {
        var req = Parse<CreateTemplateRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.Name) || string.IsNullOrWhiteSpace(req.BodyText))
            return Function.BadRequest("Name and BodyText are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertTemplate(conn, req.Name, req.BodyText, req.UserId, req.Modality);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(new { id = cmd.LastInsertedId }, "Template created.");
        }
        catch (Exception ex) { return Err("CreateTemplate", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateTemplateAsync(string? body)
    {
        var req = Parse<UpdateTemplateRequest>(body);
        if (req == null || req.TemplateId <= 0)
            return Function.BadRequest("TemplateId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateTemplate(conn, req.TemplateId, req.Name, req.BodyText, req.Modality);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Template updated.");
        }
        catch (Exception ex) { return Err("UpdateTemplate", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteTemplateAsync(string? body)
    {
        var req = Parse<DeleteTemplateRequest>(body);
        if (req == null || req.TemplateId <= 0)
            return Function.BadRequest("TemplateId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.DeleteTemplate(conn, req.TemplateId);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Template deleted.");
        }
        catch (Exception ex) { return Err("DeleteTemplate", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetUserTemplatesAsync(string? body)
    {
        var req = Parse<UserIdRequest>(body);
        if (req == null || req.UserId <= 0)
            return Function.BadRequest("UserId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetTemplates(conn, null, req.UserId);
            await using var reader = await cmd.ExecuteReaderAsync();
            return Function.Ok(await ReadTemplates(reader));
        }
        catch (Exception ex) { return Err("GetUserTemplates", ex); }
    }

    public async Task<APIGatewayProxyResponse> SearchReferrersAsync(string? body)
    {
        var req = Parse<SearchRequest>(body) ?? new SearchRequest();
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetUsers(conn, 4, req.Query);
            await using var reader = await cmd.ExecuteReaderAsync();
            var users = new List<UserResponse>();
            while (await reader.ReadAsync()) users.Add(ReadUser(reader));
            return Function.Ok(users);
        }
        catch (Exception ex) { return Err("SearchReferrers", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // USERS
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetUsersAsync(string? body)
    {
        var req = Parse<GetUsersRequest>(body) ?? new GetUsersRequest();
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetUsers(conn, req.UserType, req.Search);
            await using var reader = await cmd.ExecuteReaderAsync();
            var users = new List<UserResponse>();
            while (await reader.ReadAsync()) users.Add(ReadUser(reader));
            return Function.Ok(users);
        }
        catch (Exception ex) { return Err("GetUsers", ex); }
    }

    public async Task<APIGatewayProxyResponse> CreateUserAsync(string? body)
    {
        var req = Parse<CreateUserRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.Name) || string.IsNullOrWhiteSpace(req.Email) ||
            string.IsNullOrWhiteSpace(req.Username) || string.IsNullOrWhiteSpace(req.Password))
            return Function.BadRequest("Name, Email, Username, and Password are required.");

        try
        {
            var hash = BCrypt.Net.BCrypt.HashPassword(req.Password);
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertUser(conn, req.Name, req.Email, req.Username, hash,
                req.UserType, req.Phone, req.Address, req.CountryId, req.StateId, req.CityId);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(new { id = cmd.LastInsertedId }, "User created.");
        }
        catch (Exception ex) { return Err("CreateUser", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateUserAsync(string? body)
    {
        var req = Parse<UpdateUserRequest>(body);
        if (req == null || req.UserId <= 0)
            return Function.BadRequest("UserId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateUser(conn, req.UserId, req.Name, req.Email,
                req.Username, req.Phone, req.Address, req.CountryId, req.StateId, req.CityId);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "User updated.");
        }
        catch (Exception ex) { return Err("UpdateUser", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateUserPermissionAsync(string? body)
    {
        var req = Parse<UpdateUserPermissionRequest>(body);
        if (req == null || req.UserId <= 0)
            return Function.BadRequest("UserId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var delCmd = QueryHelper.DeleteUserPermissions(conn, req.UserId);
            await delCmd.ExecuteNonQueryAsync();

            foreach (var perm in req.Permissions)
            {
                await using var getCmd = QueryHelper.GetPermissionByName(conn, perm);
                var permId = await getCmd.ExecuteScalarAsync();
                if (permId != null)
                {
                    await using var insCmd = QueryHelper.InsertUserPermission(conn, req.UserId, Convert.ToInt32(permId));
                    await insCmd.ExecuteNonQueryAsync();
                }
            }
            return Function.Ok(null, "Permissions updated.");
        }
        catch (Exception ex) { return Err("UpdateUserPermission", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateMedicalFeeAsync(string? body)
    {
        var req = Parse<UpdateMedicalFeeRequest>(body);
        if (req == null || req.UserId <= 0)
            return Function.BadRequest("UserId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateMedicalFee(conn, req.UserId, req.MedicalDirector, req.Fee);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Medical fee updated.");
        }
        catch (Exception ex) { return Err("UpdateMedicalFee", ex); }
    }

    public async Task<APIGatewayProxyResponse> ResetPasswordAsync(string? body)
    {
        var req = Parse<PasswordRequest>(body);
        if (req == null || req.UserId <= 0)
            return Function.BadRequest("UserId is required.");

        try
        {
            var newPassword = GenerateRandomPassword();
            var hash        = BCrypt.Net.BCrypt.HashPassword(newPassword);
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateUserPassword(conn, req.UserId, hash);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(new { temporaryPassword = newPassword }, "Password reset.");
        }
        catch (Exception ex) { return Err("ResetPassword", ex); }
    }

    public async Task<APIGatewayProxyResponse> ShowPasswordAsync(string? body)
    {
        // Admin-only: returns stored hash (plaintext unavailable with bcrypt)
        var req = Parse<PasswordRequest>(body);
        if (req == null || req.UserId <= 0)
            return Function.BadRequest("UserId is required.");
        if (_token.UserType != 1)
            return Function.Forbidden("Admin access required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetUserPassword(conn, req.UserId);
            var hash               = await cmd.ExecuteScalarAsync();
            return Function.Ok(new { passwordHash = hash?.ToString() });
        }
        catch (Exception ex) { return Err("ShowPassword", ex); }
    }

    public async Task<APIGatewayProxyResponse> ChangeUserStatusAsync(string? body)
    {
        var req = Parse<ChangeUserStatusRequest>(body);
        if (req == null || req.UserId <= 0)
            return Function.BadRequest("UserId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.ChangeUserStatus(conn, req.UserId, req.Status);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "User status updated.");
        }
        catch (Exception ex) { return Err("ChangeUserStatus", ex); }
    }

    public async Task<APIGatewayProxyResponse> ChangeBillingStatusAsync(string? body)
    {
        var req = Parse<ChangeUserStatusRequest>(body);
        if (req == null || req.UserId <= 0)
            return Function.BadRequest("UserId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.ChangeBillingStatus(conn, req.UserId, req.Status);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Billing status updated.");
        }
        catch (Exception ex) { return Err("ChangeBillingStatus", ex); }
    }

    public async Task<APIGatewayProxyResponse> UserSearchAsync(string? body)
    {
        var req = Parse<SearchRequest>(body) ?? new SearchRequest();
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetUsers(conn, null, req.Query);
            await using var reader = await cmd.ExecuteReaderAsync();
            var users = new List<UserResponse>();
            while (await reader.ReadAsync()) users.Add(ReadUser(reader));
            return Function.Ok(users);
        }
        catch (Exception ex) { return Err("UserSearch", ex); }
    }

    public async Task<APIGatewayProxyResponse> SearchUserByTypeAsync(string? body)
    {
        var req = Parse<GetUsersRequest>(body) ?? new GetUsersRequest();
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetUsers(conn, req.UserType, req.Search);
            await using var reader = await cmd.ExecuteReaderAsync();
            var users = new List<UserResponse>();
            while (await reader.ReadAsync()) users.Add(ReadUser(reader));
            return Function.Ok(users);
        }
        catch (Exception ex) { return Err("SearchUserByType", ex); }
    }

    public async Task<APIGatewayProxyResponse> LoginAsAnotherUserAsync(string? body)
    {
        var req = Parse<LoginAsRequest>(body);
        if (req == null || req.TargetUserId <= 0)
            return Function.BadRequest("TargetUserId is required.");
        if (_token.UserType != 1)
            return Function.Forbidden("Admin access required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetUserById(conn, req.TargetUserId);
            await using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync())
                return Function.NotFound("User not found.");

            var name     = Str(reader, "name");
            var email    = Str(reader, "email");
            var username = Str(reader, "username");
            var userType = reader.GetInt32("usertype");
            await reader.CloseAsync();

            var newToken = GenerateToken();
            await using var upsert = QueryHelper.UpsertToken(conn, req.TargetUserId, username, newToken);
            await upsert.ExecuteNonQueryAsync();

            return Function.Ok(new LoginResponse
            {
                Id = req.TargetUserId, Name = name, Email = email,
                Username = username, UserType = userType, Token = newToken
            }, "Logged in as user.");
        }
        catch (Exception ex) { return Err("LoginAsAnotherUser", ex); }
    }

    public async Task<APIGatewayProxyResponse> BackToAccountAsync(string? body)
    {
        // The frontend should simply discard the impersonation token and restore the original
        return await Task.FromResult(Function.Ok(null, "Back to original account."));
    }

    public async Task<APIGatewayProxyResponse> GetClientLookupAsync(string? body)
    {
        var req     = Parse<GetClientLookupRequest>(body) ?? new GetClientLookupRequest();
        var page    = Math.Max(1, req.Page ?? 1);
        var perPage = 15;
        var offset  = (page - 1) * perPage;

        try
        {
            await using var conn   = await OpenAsync();
            await using var cntCmd = QueryHelper.CountClientLookup(conn, req.Search);
            var total              = Convert.ToInt32(await cntCmd.ExecuteScalarAsync());

            await using var cmd    = QueryHelper.GetClientLookup(conn, req.Search, offset, perPage);
            await using var reader = await cmd.ExecuteReaderAsync();
            var users = new List<UserResponse>();
            while (await reader.ReadAsync()) users.Add(ReadUser(reader));

            return Function.Ok(new PaginatedResponse<UserResponse>
            {
                Data    = users,
                Total   = total,
                Page    = page,
                PerPage = perPage
            });
        }
        catch (Exception ex) { return Err("GetClientLookup", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateClientInfoAsync(string? body)
    {
        var req = Parse<UpdateClientInfoRequest>(body);
        if (req == null || req.ClientId <= 0)
            return Function.BadRequest("ClientId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateUser(conn, req.ClientId, req.Name, req.Email, null, req.Phone, req.Address, null, null, null);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Client info updated.");
        }
        catch (Exception ex) { return Err("UpdateClientInfo", ex); }
    }

    public async Task<APIGatewayProxyResponse> ConfirmDefaultTemplateAsync(string? body)
    {
        var req = Parse<UserIdRequest>(body);
        if (req == null || req.UserId <= 0)
            return Function.BadRequest("UserId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetTemplates(conn, null, req.UserId);
            await using var reader = await cmd.ExecuteReaderAsync();
            return Function.Ok(await ReadTemplates(reader));
        }
        catch (Exception ex) { return Err("ConfirmDefaultTemplate", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetUserTypesAsync(string? body)
    {
        var req = Parse<SearchRequest>(body) ?? new SearchRequest();
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetUserTypes(conn, req.Query);
            await using var reader = await cmd.ExecuteReaderAsync();
            var types = new List<object>();
            while (await reader.ReadAsync())
                types.Add(new { id = reader.GetInt32("id"), name = reader.GetString("name") });
            return Function.Ok(types);
        }
        catch (Exception ex) { return Err("GetUserTypes", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetCountriesAsync(string? body)
    {
        var req = Parse<SearchRequest>(body) ?? new SearchRequest();
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetCountries(conn, req.Query);
            await using var reader = await cmd.ExecuteReaderAsync();
            var items = new List<object>();
            while (await reader.ReadAsync())
                items.Add(new { id = reader.GetInt32("id"), name = reader.GetString("name") });
            return Function.Ok(items);
        }
        catch (Exception ex) { return Err("GetCountries", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetStatesAsync(string? body)
    {
        var req = Parse<GetStatesRequest>(body) ?? new GetStatesRequest();
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetStates(conn, req.CountryId, req.Query);
            await using var reader = await cmd.ExecuteReaderAsync();
            var items = new List<object>();
            while (await reader.ReadAsync())
                items.Add(new { id = reader.GetInt32("id"), name = reader.GetString("name") });
            return Function.Ok(items);
        }
        catch (Exception ex) { return Err("GetStates", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetCitiesAsync(string? body)
    {
        var req = Parse<GetCitiesRequest>(body) ?? new GetCitiesRequest();
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetCities(conn, req.StateId, req.Query);
            await using var reader = await cmd.ExecuteReaderAsync();
            var items = new List<object>();
            while (await reader.ReadAsync())
                items.Add(new { id = reader.GetInt32("id"), name = reader.GetString("name") });
            return Function.Ok(items);
        }
        catch (Exception ex) { return Err("GetCities", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetTranscribersAsync(string? body)
    {
        var req = Parse<SearchRequest>(body) ?? new SearchRequest();
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetUsers(conn, 3, req.Query);
            await using var reader = await cmd.ExecuteReaderAsync();
            var users = new List<UserResponse>();
            while (await reader.ReadAsync()) users.Add(ReadUser(reader));
            return Function.Ok(users);
        }
        catch (Exception ex) { return Err("GetTranscribers", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetUsersByDisplayNameAsync(string? body)
    {
        var req = Parse<SearchRequest>(body) ?? new SearchRequest();
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetUsers(conn, null, req.Query);
            await using var reader = await cmd.ExecuteReaderAsync();
            var users = new List<UserResponse>();
            while (await reader.ReadAsync()) users.Add(ReadUser(reader));
            return Function.Ok(users);
        }
        catch (Exception ex) { return Err("GetUsersByDisplayName", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateEmployeesAsync(string? body)
    {
        var req = Parse<UpdateEmployeesRequest>(body);
        if (req == null || req.TranscriberAdminId <= 0)
            return Function.BadRequest("TranscriberAdminId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var delCmd = QueryHelper.DeleteUserEmployees(conn, req.TranscriberAdminId);
            await delCmd.ExecuteNonQueryAsync();

            foreach (var eid in req.EmployeeIds)
            {
                await using var insCmd = QueryHelper.InsertUserEmployee(conn, req.TranscriberAdminId, eid);
                await insCmd.ExecuteNonQueryAsync();
            }
            return Function.Ok(null, "Employees updated.");
        }
        catch (Exception ex) { return Err("UpdateEmployees", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // ATTACHMENTS / AUDIO
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> AddAttachmentAsync(string? body)
    {
        var req = Parse<AddAttachmentRequest>(body);
        if (req == null || req.StudyId <= 0 || string.IsNullOrWhiteSpace(req.FileName))
            return Function.BadRequest("StudyId and FileName are required.");

        try
        {
            // File storage is handled externally (S3); we record the path in DB
            var filePath = $"attachments/{req.StudyId}/{req.FileName}";
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertAttachment(conn, req.StudyId, req.FileName, filePath, req.FileType);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(new { filePath }, "Attachment saved.");
        }
        catch (Exception ex) { return Err("AddAttachment", ex); }
    }

    public async Task<APIGatewayProxyResponse> AddMultipleAttachmentsAsync(string? body)
    {
        var req = Parse<AddMultipleAttachmentsRequest>(body);
        if (req == null || req.StudyIds.Count == 0 || string.IsNullOrWhiteSpace(req.FileName))
            return Function.BadRequest("StudyIds and FileName are required.");

        try
        {
            await using var conn = await OpenAsync();
            foreach (var sid in req.StudyIds)
            {
                var filePath = $"attachments/{sid}/{req.FileName}";
                await using var cmd = QueryHelper.InsertAttachment(conn, sid, req.FileName, filePath, "document");
                await cmd.ExecuteNonQueryAsync();
            }
            return Function.Ok(null, $"Attachment added to {req.StudyIds.Count} studies.");
        }
        catch (Exception ex) { return Err("AddMultipleAttachments", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetAttachedFilesAsync(string? body)
    {
        var req = Parse<GetAttachedFilesRequest>(body);
        if (req == null || req.StudyId <= 0)
            return Function.BadRequest("StudyId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetAttachedFiles(conn, req.StudyId, req.FileType);
            await using var reader = await cmd.ExecuteReaderAsync();
            var files = new List<AttachmentResponse>();
            while (await reader.ReadAsync())
            {
                files.Add(new AttachmentResponse
                {
                    Id = reader.GetInt32("id"), StudyId = req.StudyId,
                    FileName = Str(reader, "file_name"), FileType = Str(reader, "type"),
                    FilePath = Str(reader, "file_path"), CreatedAt = DateStr(reader, "created_at")
                });
            }
            return Function.Ok(files);
        }
        catch (Exception ex) { return Err("GetAttachedFiles", ex); }
    }

    public async Task<APIGatewayProxyResponse> SendAudioFilesAsync(string? body)
    {
        var req = Parse<SendAudioFilesRequest>(body);
        if (req == null || req.StudyId <= 0 || req.TranscriberId <= 0)
            return Function.BadRequest("StudyId and TranscriberId are required.");

        try
        {
            await using var conn     = await OpenAsync();
            var filePath             = $"audio/{req.StudyId}/{req.FileName}";
            await using var attCmd   = QueryHelper.InsertAttachment(conn, req.StudyId, req.FileName, filePath, "audio");
            await attCmd.ExecuteNonQueryAsync();
            await using var statusCmd = QueryHelper.UpdateStudyStatus(conn, req.StudyId, "trans_new_messages");
            await statusCmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Audio file sent to transcriber.");
        }
        catch (Exception ex) { return Err("SendAudioFiles", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetSentAudioFilesAsync(string? body)
    {
        var req = Parse<GetSentAudioFilesRequest>(body) ?? new GetSentAudioFilesRequest();
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetSentAudioFiles(conn, req.Dos, req.ClientId, _token.UserId);
            await using var reader = await cmd.ExecuteReaderAsync();
            return Function.Ok(await ReadStudies(cmd));
        }
        catch (Exception ex) { return Err("GetSentAudioFiles", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // AUDIT (management)
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetAuditAllAsync(string? body)
    {
        var req     = Parse<GetAuditAllRequest>(body) ?? new GetAuditAllRequest();
        var page    = Math.Max(1, req.Page ?? 1);
        var perPage = req.PerPage ?? 20;
        var offset  = (page - 1) * perPage;

        try
        {
            await using var conn   = await OpenAsync();
            await using var cntCmd = QueryHelper.CountAuditAll(conn, req.Search, req.DateFrom, req.DateTo);
            var total              = Convert.ToInt32(await cntCmd.ExecuteScalarAsync());

            await using var cmd    = QueryHelper.GetAuditAll(conn, req.Search, req.DateFrom, req.DateTo, offset, perPage);
            await using var reader = await cmd.ExecuteReaderAsync();
            var audits = new List<AuditResponse>();
            while (await reader.ReadAsync())
            {
                audits.Add(new AuditResponse
                {
                    Id          = reader.GetInt32("id"),
                    StudyId     = reader.IsDBNull(reader.GetOrdinal("typewordlist_id")) ? 0 : reader.GetInt32("typewordlist_id"),
                    Action      = Str(reader, "action"),
                    Description = Str(reader, "description"),
                    Username    = Str(reader, "username"),
                    CreatedAt   = DateStr(reader, "created_at")
                });
            }
            return Function.Ok(new PaginatedResponse<AuditResponse>
            {
                Data = audits, Total = total, Page = page, PerPage = perPage
            });
        }
        catch (Exception ex) { return Err("GetAuditAll", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // FAX
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> SendFaxAsync(string? body)
    {
        var req = Parse<SendFaxRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.FaxNumber))
            return Function.BadRequest("FaxNumber is required.");

        // ── Dev/demo guard ────────────────────────────────────────────────────
        // When FAX_SEND_ENABLED=false in template.yaml the fax is NOT dispatched.
        // Return a suppressed response so the UI behaves normally without sending.
        if (!AppConfig.FaxSendEnabled)
            return Function.Ok(
                new { faxId = 0, status = "dev-suppressed" },
                $"Fax sending is disabled on this environment (Stage={AppConfig.Stage}). Set FAX_SEND_ENABLED=true in template.yaml to enable.");

        try
        {
            // Actual Twilio/Globalsender call would go here; we record the intent
            var filePath = $"faxes/outbound/{req.FileName}";
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertFaxSent(conn, req.FaxNumber, req.FileName, req.StudyId);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(new { faxId = cmd.LastInsertedId, status = "pending" }, "Fax queued.");
        }
        catch (Exception ex) { return Err("SendFax", ex); }
    }

    public async Task<APIGatewayProxyResponse> ViewFaxesAsync(string? body)
    {
        var req     = Parse<PaginatedRequest>(body) ?? new PaginatedRequest();
        var page    = Math.Max(1, req.Page ?? 1);
        var perPage = req.PerPage ?? 20;
        var offset  = (page - 1) * perPage;

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetSentFaxes(conn, offset, perPage);
            await using var reader = await cmd.ExecuteReaderAsync();
            var faxes = new List<FaxResponse>();
            while (await reader.ReadAsync())
            {
                faxes.Add(new FaxResponse
                {
                    Id          = reader.GetInt32("id"),
                    FaxNumber   = Str(reader, "fax_number"),
                    FileName    = Str(reader, "file_name"),
                    Status      = Str(reader, "status"),
                    SentAt      = DateStr(reader, "created_at"),
                    DeliveredAt = DateStr(reader, "delevered_at")
                });
            }
            return Function.Ok(faxes);
        }
        catch (Exception ex) { return Err("ViewFaxes", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetInboundFaxesAsync(string? body)
    {
        // Mirror Laravel inboundFax():
        // 1. List PDF files directly in php/faxesin/ (non-recursive, root only)
        // 2. For each file, look up fax_receiveds by media_file
        // 3. If no DB record, still show file using S3 LastModified as date
        try
        {
            // --- Step 1: list S3 root-level PDFs in php/faxesin/ ---
            using var s3 = new AmazonS3Client();
            var s3Files = new List<S3Object>();
            string? continuationToken = null;
            do
            {
                var listReq = new ListObjectsV2Request
                {
                    BucketName        = S3Bucket,
                    Prefix            = "php/faxesin/",
                    Delimiter         = "/",
                    ContinuationToken = continuationToken,
                    MaxKeys           = 1000
                };
                var listResp = await s3.ListObjectsV2Async(listReq);
                s3Files.AddRange(listResp.S3Objects.Where(o =>
                    o.Key.EndsWith(".pdf", StringComparison.OrdinalIgnoreCase)));
                continuationToken = listResp.IsTruncated ? listResp.NextContinuationToken : null;
            } while (continuationToken != null);

            if (s3Files.Count == 0)
                return Function.Ok(new List<InboundFaxResponse>());

            // --- Step 2: bulk DB lookup by media_file paths ---
            await using var conn = await OpenAsync();
            var keys = s3Files.Select(f => f.Key).ToList();
            await using var cmd = QueryHelper.GetInboundFaxesByPaths(conn, keys);
            await using var reader = await cmd.ExecuteReaderAsync();
            var dbMap = new Dictionary<string, (int id, string from, string createdAt, string modifiedAt)>();
            while (await reader.ReadAsync())
            {
                var path = Str(reader, "media_file");
                dbMap[path] = (
                    reader.GetInt32("id"),
                    Str(reader, "from"),
                    DateStr(reader, "created_at"),
                    DateStr(reader, "modified_at")
                );
            }

            // --- Step 3: build response (S3 is source of truth) ---
            var faxes = new List<InboundFaxResponse>();
            foreach (var s3f in s3Files.OrderByDescending(f => f.LastModified))
            {
                if (dbMap.TryGetValue(s3f.Key, out var db))
                {
                    var mod = string.IsNullOrEmpty(db.modifiedAt)
                        ? s3f.LastModified.ToString("yyyy-MM-dd HH:mm:ss")
                        : db.modifiedAt;
                    faxes.Add(new InboundFaxResponse
                    {
                        Id         = db.id,
                        FileName   = s3f.Key,
                        FileUrl    = s3f.Key,
                        FaxNumber  = db.from,
                        ReceivedAt = db.createdAt,
                        ModifiedAt = mod
                    });
                }
                else
                {
                    var dt = s3f.LastModified.ToString("yyyy-MM-dd HH:mm:ss");
                    faxes.Add(new InboundFaxResponse
                    {
                        Id         = 0,
                        FileName   = s3f.Key,
                        FileUrl    = s3f.Key,
                        FaxNumber  = string.Empty,
                        ReceivedAt = dt,
                        ModifiedAt = dt
                    });
                }
            }
            return Function.Ok(faxes);
        }
        catch (Exception ex) { return Err("GetInboundFaxes", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetIncomingFaxesAsync(string? body)
    {
        var req = Parse<GetIncomingFaxesRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.ClientUsername))
            return Function.BadRequest("ClientUsername is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetIncomingFaxesByClient(conn, req.ClientUsername);
            await using var reader = await cmd.ExecuteReaderAsync();
            var faxes = new List<InboundFaxResponse>();
            while (await reader.ReadAsync())
            {
                faxes.Add(new InboundFaxResponse
                {
                    Id         = reader.GetInt32("id"),
                    FileName   = Str(reader, "file_name"),
                    FileUrl    = Str(reader, "file_name"),
                    FaxNumber  = Str(reader, "from"),
                    IsRead     = false,
                    ReceivedAt = DateStr(reader, "created_at")
                });
            }
            return Function.Ok(faxes);
        }
        catch (Exception ex) { return Err("GetIncomingFaxes", ex); }
    }

    public async Task<APIGatewayProxyResponse> RenameFaxAsync(string? body)
    {
        var req = Parse<RenameFaxRequest>(body);
        if (req == null || req.FaxId <= 0 || string.IsNullOrWhiteSpace(req.NewName))
            return Function.BadRequest("FaxId and NewName are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.RenameFax(conn, req.FaxId, req.NewName);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Fax renamed.");
        }
        catch (Exception ex) { return Err("RenameFax", ex); }
    }

    public async Task<APIGatewayProxyResponse> MoveInboundFaxAsync(string? body)
    {
        var req = Parse<MoveInboundFaxRequest>(body);
        if (req == null || req.FaxIds.Count == 0 || req.ClientId <= 0)
            return Function.BadRequest("FaxIds and ClientId are required.");

        try
        {
            await using var conn = await OpenAsync();
            foreach (var faxId in req.FaxIds)
            {
                var newPath = $"faxes/clients/{req.ClientId}/{faxId}";
                await using var cmd = QueryHelper.MoveFax(conn, faxId, req.ClientId);
                await cmd.ExecuteNonQueryAsync();
            }
            return Function.Ok(null, $"{req.FaxIds.Count} faxes moved.");
        }
        catch (Exception ex) { return Err("MoveInboundFax", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetFaxStatusAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0)
            return Function.BadRequest("FaxId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetSentFaxes(conn, 0, 1);
            await using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                return Function.Ok(new FaxResponse
                {
                    Id        = reader.GetInt32("id"),
                    FaxNumber = Str(reader, "fax_number"),
                    FileName  = Str(reader, "file_name"),
                    Status    = Str(reader, "status"),
                    SentAt    = DateStr(reader, "created_at")
                });
            }
            return Function.NotFound("Fax not found.");
        }
        catch (Exception ex) { return Err("GetFaxStatus", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetUrgentNotificationsAsync(string? body)
    {
        var req     = Parse<PaginatedRequest>(body) ?? new PaginatedRequest();
        var page    = Math.Max(1, req.Page ?? 1);
        var perPage = req.PerPage ?? 20;

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetUrgentNotifications(conn, (page - 1) * perPage, perPage);
            await using var reader = await cmd.ExecuteReaderAsync();
            var items = new List<object>();
            while (await reader.ReadAsync())
            {
                items.Add(new
                {
                    id          = reader.GetInt32("id"),
                    filePath    = Str(reader, "file_path"),
                    isRead      = !reader.IsDBNull(reader.GetOrdinal("is_read")) && reader.GetInt32("is_read") == 1,
                    patientName = Str(reader, "patient_name"),
                    accession   = Str(reader, "accession_number"),
                    createdAt   = DateStr(reader, "created_at")
                });
            }
            return Function.Ok(items);
        }
        catch (Exception ex) { return Err("GetUrgentNotifications", ex); }
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
            {
                reports.Add(new FinalReportResponse
                {
                    Id          = reader.GetInt32("id"),
                    PatientName = Str(reader, "patient_name"),
                    Modality    = Str(reader, "modality"),
                    Dos         = DateStr(reader, "dos"),
                    ReportText  = Str(reader, "report_text"),
                    Impression  = Str(reader, "impression"),
                    Status      = Str(reader, "status"),
                    ClientName  = Str(reader, "client_name"),
                    PdfUrl      = Str(reader, "pdf_path"),
                    RadName     = Str(reader, "rad_name"),
                    UpdatedAt   = DateStr(reader, "updated_at")
                });
            }
            return Function.Ok(reports);
        }
        catch (Exception ex) { return Err("GetFinalReports", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateFinalReportAsync(string? body)
    {
        var req = Parse<UpdateFinalReportRequest>(body);
        if (req == null || req.StudyId <= 0 || string.IsNullOrWhiteSpace(req.ReportText))
            return Function.BadRequest("StudyId and ReportText are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateFinalReport(conn, req.StudyId, req.ReportText, req.Impression, req.IsAddendum);
            await cmd.ExecuteNonQueryAsync();
            await using var auditCmd = QueryHelper.InsertAudit(conn, req.StudyId, _token.UserId,
                req.IsAddendum ? "Addendum Created" : "Final Report Updated", null);
            await auditCmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Final report updated.");
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
            return Function.BadRequest("ReportType is required.");

        try
        {
            await using var conn = await OpenAsync();
            var parts = req.DateRange?.Split('|');
            var df    = parts?.Length >= 1 ? parts[0].Trim() : null;
            var dt    = parts?.Length == 2 ? parts[1].Trim() : null;

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
            {
                items.Add(new ChartDataItem
                {
                    Label = Str(reader, "label"),
                    Total = reader.GetInt32("total")
                });
            }
            return Function.Ok(new ChartResponse { TotalRecords = items.Sum(i => i.Total), ChartData = items });
        }
        catch (Exception ex) { return Err("GetChartData", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetLastFifteenRecordsAsync(string? body)
    {
        var req    = Parse<GetFifteenRecordsRequest>(body) ?? new GetFifteenRecordsRequest();
        var status = req.Status ?? "new_study";
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetStudiesByStatus(conn, status, _token.UserId, _token.UserType, 0, 15);
            return Function.Ok(await ReadStudies(cmd));
        }
        catch (Exception ex) { return Err("GetLastFifteenRecords", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetStudiesByStatusAsync(string status)
    {
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetStudiesByStatus(conn, status, _token.UserId, _token.UserType, 0, 50);
            return Function.Ok(await ReadStudies(cmd));
        }
        catch (Exception ex) { return Err("GetStudiesByStatus", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetFinalizedDataAsync(string? body)
    {
        var req = Parse<GetFinalizedDataRequest>(body) ?? new GetFinalizedDataRequest();
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetFinalizedData(conn, req.RadId, req.TranscriberId, req.DateFrom, req.DateTo);
            return Function.Ok(await ReadStudies(cmd));
        }
        catch (Exception ex) { return Err("GetFinalizedData", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetRefFinalizedDataAsync(string? body)
    {
        var req = Parse<GetFinalizedDataRequest>(body) ?? new GetFinalizedDataRequest();
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetFinalizedData(conn, req.RadId, req.TranscriberId, req.DateFrom, req.DateTo);
            return Function.Ok(await ReadStudies(cmd));
        }
        catch (Exception ex) { return Err("GetRefFinalizedData", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // BILLING
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> CreateTranscriberInvoiceAsync(string? body)
    {
        var req = Parse<CreateTranscriberInvoiceRequest>(body);
        if (req == null || req.TranscriberId <= 0 || req.ClientId <= 0)
            return Function.BadRequest("TranscriberId and ClientId are required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetStudiesForInvoice(conn, req.ClientId, req.TranscriberId, req.DateFrom, req.DateTo);
            await using var reader = await cmd.ExecuteReaderAsync();
            var studies = new List<object>();
            var groups  = new Dictionary<string, int>();
            while (await reader.ReadAsync())
            {
                var mod = Str(reader, "modality");
                groups[mod] = groups.GetValueOrDefault(mod) + 1;
            }
            return Function.Ok(new { groups, dateFrom = req.DateFrom, dateTo = req.DateTo });
        }
        catch (Exception ex) { return Err("CreateTranscriberInvoice", ex); }
    }

    public async Task<APIGatewayProxyResponse> CreateTranscriberInvoiceStep2Async(string? body)
    {
        var req = Parse<CreateTranscriberInvoiceRequest>(body);
        if (req == null)
            return Function.BadRequest("Request body is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetStudiesForInvoice(conn, req.ClientId, req.TranscriberId, req.DateFrom, req.DateTo);
            await using var reader = await cmd.ExecuteReaderAsync();
            var lineItems = new List<InvoiceLineItemResponse>();
            var groups    = new Dictionary<string, int>();
            while (await reader.ReadAsync())
            {
                var mod = Str(reader, "modality");
                groups[mod] = groups.GetValueOrDefault(mod) + 1;
            }
            await reader.CloseAsync();

            decimal total = 0;
            foreach (var (mod, count) in groups)
            {
                await using var priceCmd    = QueryHelper.GetModalityPriceForUser(conn, req.TranscriberId, "transcriber", mod);
                var priceObj                = await priceCmd.ExecuteScalarAsync();
                var price                   = priceObj == null ? 0m : Convert.ToDecimal(priceObj);
                var lineTotal               = price * count;
                total                      += lineTotal;
                lineItems.Add(new InvoiceLineItemResponse { Modality = mod, Count = count, UnitPrice = price, Total = lineTotal });
            }
            return Function.Ok(new { lineItems, total, dateFrom = req.DateFrom, dateTo = req.DateTo });
        }
        catch (Exception ex) { return Err("CreateTranscriberInvoiceStep2", ex); }
    }

    public async Task<APIGatewayProxyResponse> SaveTranscriberInvoiceAsync(string? body)
    {
        var req = Parse<SaveTranscriberInvoiceRequest>(body);
        if (req == null || req.TranscriberId <= 0 || string.IsNullOrWhiteSpace(req.InvoiceNumber))
            return Function.BadRequest("TranscriberId and InvoiceNumber are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertTranscriberInvoice(conn, req.TranscriberId, req.ClientId,
                req.DateFrom, req.DateTo, req.TotalAmount, req.InvoiceNumber);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(new { invoiceId = cmd.LastInsertedId }, "Transcriber invoice saved.");
        }
        catch (Exception ex) { return Err("SaveTranscriberInvoice", ex); }
    }

    public async Task<APIGatewayProxyResponse> CreateClientInvoiceAsync(string? body)
    {
        var req = Parse<SaveClientInvoiceRequest>(body);
        if (req == null || req.ClientId <= 0)
            return Function.BadRequest("ClientId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetStudiesForInvoice(conn, req.ClientId, null, req.DateFrom, req.DateTo);
            await using var reader = await cmd.ExecuteReaderAsync();
            var groups = new Dictionary<string, int>();
            while (await reader.ReadAsync())
            {
                var mod = Str(reader, "modality");
                groups[mod] = groups.GetValueOrDefault(mod) + 1;
            }
            return Function.Ok(new { groups, dateFrom = req.DateFrom, dateTo = req.DateTo });
        }
        catch (Exception ex) { return Err("CreateClientInvoice", ex); }
    }

    public async Task<APIGatewayProxyResponse> SaveClientInvoiceAsync(string? body)
    {
        var req = Parse<SaveClientInvoiceRequest>(body);
        if (req == null || req.ClientId <= 0 || string.IsNullOrWhiteSpace(req.InvoiceNumber))
            return Function.BadRequest("ClientId and InvoiceNumber are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertClientInvoice(conn, req.ClientId,
                req.DateFrom, req.DateTo, req.TotalAmount, req.InvoiceNumber);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(new { invoiceId = cmd.LastInsertedId }, "Client invoice saved.");
        }
        catch (Exception ex) { return Err("SaveClientInvoice", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetInvoicePaymentsAsync(string? body)
    {
        var req     = Parse<PaginatedRequest>(body) ?? new PaginatedRequest();
        var page    = Math.Max(1, req.Page ?? 1);
        var perPage = req.PerPage ?? 20;

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetInvoicePayments(conn, null, (page - 1) * perPage, perPage);
            await using var reader = await cmd.ExecuteReaderAsync();
            var invoices = new List<InvoiceResponse>();
            while (await reader.ReadAsync())
            {
                invoices.Add(new InvoiceResponse
                {
                    Id            = reader.GetInt32("id"),
                    InvoiceNumber = Str(reader, "invoice_number"),
                    DateFrom      = DateStr(reader, "date_from"),
                    DateTo        = DateStr(reader, "date_to"),
                    TotalAmount   = reader.IsDBNull(reader.GetOrdinal("total_amount")) ? 0 : reader.GetDecimal("total_amount"),
                    Status        = Str(reader, "status"),
                    CreatedAt     = DateStr(reader, "created_at")
                });
            }
            return Function.Ok(invoices);
        }
        catch (Exception ex) { return Err("GetInvoicePayments", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetClientPaymentsAsync(string? body)
        => await GetInvoicePaymentsAsync(body);

    public async Task<APIGatewayProxyResponse> GetTranscriberAnalyticsAsync(string? body)
    {
        var req = Parse<GetFinalizedDataRequest>(body) ?? new GetFinalizedDataRequest();
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.GetChartPendingByTranscriber(conn, null);
            await using var r    = await cmd.ExecuteReaderAsync();
            var items = new List<ChartDataItem>();
            while (await r.ReadAsync()) items.Add(new ChartDataItem { Label = Str(r, "label"), Total = r.GetInt32("total") });
            return Function.Ok(new ChartResponse { TotalRecords = items.Sum(i => i.Total), ChartData = items });
        }
        catch (Exception ex) { return Err("GetTranscriberAnalytics", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetClientAnalyticsAsync(string? body)
        => await GetTranscriberAnalyticsAsync(body);

    public async Task<APIGatewayProxyResponse> GetBilledAmountAsync(string? body)
    {
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.GetInvoicePayments(conn, null, 0, 1000);
            await using var r    = await cmd.ExecuteReaderAsync();
            decimal total = 0;
            while (await r.ReadAsync())
                total += r.IsDBNull(r.GetOrdinal("total_amount")) ? 0 : r.GetDecimal("total_amount");
            return Function.Ok(new { totalBilled = total });
        }
        catch (Exception ex) { return Err("GetBilledAmount", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // PRICE MANAGEMENT
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetPriceManagementAsync(string? body)
    {
        var req = Parse<GetPriceManagementRequest>(body);
        if (req == null || req.UserId <= 0)
            return Function.BadRequest("UserId and UserCategory are required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetAllModalityPricesForUser(conn, req.UserId, req.UserCategory);
            await using var reader = await cmd.ExecuteReaderAsync();
            var prices = new List<ModalityPriceResponse>();
            while (await reader.ReadAsync())
            {
                prices.Add(new ModalityPriceResponse
                {
                    Modality = Str(reader, "modality"),
                    Price    = reader.GetDecimal("price")
                });
            }
            return Function.Ok(new PriceManagementResponse
            {
                UserId        = req.UserId,
                UserCategory  = req.UserCategory,
                ModalityPrices = prices
            });
        }
        catch (Exception ex) { return Err("GetPriceManagement", ex); }
    }

    public async Task<APIGatewayProxyResponse> SaveModalityPriceAsync(string? body)
    {
        var req = Parse<SaveModalityPriceRequest>(body);
        if (req == null || req.UserId <= 0 || string.IsNullOrWhiteSpace(req.Modality))
            return Function.BadRequest("UserId, UserCategory, and Modality are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpsertModalityPrice(conn, req.UserId, req.UserCategory, req.Modality, req.Price);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Modality price saved.");
        }
        catch (Exception ex) { return Err("SaveModalityPrice", ex); }
    }

    public async Task<APIGatewayProxyResponse> SavePerPagePriceAsync(string? body)
    {
        var req = Parse<SavePerPagePriceRequest>(body);
        if (req == null || req.UserId <= 0)
            return Function.BadRequest("UserId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpsertPerPagePrice(conn, req.UserId, req.PricePerPage, req.Modality);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Per-page price saved.");
        }
        catch (Exception ex) { return Err("SavePerPagePrice", ex); }
    }

    public async Task<APIGatewayProxyResponse> SavePerCharPriceAsync(string? body)
    {
        var req = Parse<SavePerCharPriceRequest>(body);
        if (req == null || req.UserId <= 0)
            return Function.BadRequest("UserId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpsertPerCharPrice(conn, req.UserId, req.PricePerChar);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Per-character price saved.");
        }
        catch (Exception ex) { return Err("SavePerCharPrice", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // ORDER STATUS
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetOrderStatusAsync(string? body)
    {
        var req     = Parse<GetOrderStatusRequest>(body) ?? new GetOrderStatusRequest();
        var page    = Math.Max(1, req.Page ?? 1);
        var perPage = req.PerPage ?? 20;

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetOrderStatus(conn, req.Search, req.DateFrom, req.DateTo,
                (page - 1) * perPage, perPage);
            await using var reader = await cmd.ExecuteReaderAsync();
            var results = new List<OrderStatusResponse>();
            while (await reader.ReadAsync())
            {
                results.Add(new OrderStatusResponse
                {
                    Id              = reader.GetInt32("id"),
                    PatientName     = Str(reader, "patient_name"),
                    AccessionNumber = Str(reader, "accession_number"),
                    Modality        = Str(reader, "modality"),
                    Status          = Str(reader, "status"),
                    Dos             = DateStr(reader, "dos"),
                    ClientName      = Str(reader, "client_name"),
                    TranscriberName = Str(reader, "transcriber_name"),
                    RadName         = Str(reader, "rad_name"),
                    LastAuditAction = Str(reader, "last_audit_action"),
                    LastAuditAt     = DateStr(reader, "last_audit_at")
                });
            }
            return Function.Ok(results);
        }
        catch (Exception ex) { return Err("GetOrderStatus", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetOrderStatusV2Async(string? body)
        => await GetOrderStatusAsync(body);

    public async Task<APIGatewayProxyResponse> GetAuditDetailOrderStatusAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0)
            return Function.BadRequest("StudyId is required.");
        return await GetStudyAuditAsync(body);
    }

    public async Task<APIGatewayProxyResponse> ReassignTranscriberAsync(string? body)
    {
        var req = Parse<ReassignTranscriberRequest>(body);
        if (req == null || req.StudyId <= 0 || req.TranscriberId <= 0)
            return Function.BadRequest("StudyId and TranscriberId are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.ReassignTranscriber(conn, req.StudyId, req.TranscriberId);
            var affected         = await cmd.ExecuteNonQueryAsync();
            if (affected == 0)
                return Function.BadRequest("Cannot reassign — study is already finalized.");

            await using var auditCmd = QueryHelper.InsertAudit(conn, req.StudyId, _token.UserId, "Transcriber Reassigned", null);
            await auditCmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Transcriber reassigned.");
        }
        catch (Exception ex) { return Err("ReassignTranscriber", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MODALITIES
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetModalitiesAsync()
    {
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetModalities(conn);
            await using var reader = await cmd.ExecuteReaderAsync();
            var items = new List<ModalityResponse>();
            while (await reader.ReadAsync())
            {
                items.Add(new ModalityResponse
                {
                    Id     = reader.GetInt32("id"),
                    Name   = Str(reader, "name"),
                    Status = reader.GetInt32("status")
                });
            }
            return Function.Ok(items);
        }
        catch (Exception ex) { return Err("GetModalities", ex); }
    }

    public async Task<APIGatewayProxyResponse> ToggleModalityStatusAsync(string? body)
    {
        var req = Parse<ToggleModalityRequest>(body);
        if (req == null || req.ModalityId <= 0)
            return Function.BadRequest("ModalityId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.ToggleModalityStatusCmd(conn, req.ModalityId);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Modality status toggled.");
        }
        catch (Exception ex) { return Err("ToggleModalityStatus", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // NON-DICOM
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetNonDicomAccountsAsync(string? body)
    {
        var req     = Parse<GetNonDicomAccountsRequest>(body) ?? new GetNonDicomAccountsRequest();
        var page    = Math.Max(1, req.Page ?? 1);
        var perPage = 20;

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetNonDicomAccounts(conn, req.Search, (page - 1) * perPage, perPage, _token.UserId, _token.UserType);
            return Function.Ok(await ReadNonDicomStudies(cmd));
        }
        catch (Exception ex) { return Err("GetNonDicomAccounts", ex); }
    }

    public async Task<APIGatewayProxyResponse> StoreNonDicomEntryAsync(string? body)
    {
        var req = Parse<StoreNonDicomEntryRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.PatientName))
            return Function.BadRequest("PatientName is required.");

        try
        {
            await using var conn = await OpenAsync();
            if (req.StudyId.HasValue && req.StudyId > 0)
            {
                // Update existing
                await using var cmd = QueryHelper.UpdateStudy(conn, req.StudyId.Value,
                    req.PatientName, req.Dob, req.Dos, req.Modality, req.Description, null, null, null, null);
                await cmd.ExecuteNonQueryAsync();
                return Function.Ok(new { studyId = req.StudyId }, "Non-DICOM entry updated.");
            }
            else
            {
                // Insert new
                await using var cmd = QueryHelper.InsertNonDicomEntry(conn, req.PatientName, req.Dob, req.Dos,
                    req.Modality, req.ExamType, req.Description, req.ClientId, req.PhysicianId);
                await cmd.ExecuteNonQueryAsync();
                return Function.Ok(new { studyId = cmd.LastInsertedId }, "Non-DICOM entry created.");
            }
        }
        catch (Exception ex) { return Err("StoreNonDicomEntry", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteNonDicomEntryAsync(string? body)
    {
        var req = Parse<DeleteNonDicomEntryRequest>(body);
        if (req == null || req.StudyId <= 0)
            return Function.BadRequest("StudyId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.DeleteNonDicomEntry(conn, req.StudyId);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Non-DICOM entry deleted.");
        }
        catch (Exception ex) { return Err("DeleteNonDicomEntry", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetNonDicomEntryAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0)
            return Function.BadRequest("StudyId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetStudyById(conn, req.StudyId);
            await using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync())
                return Function.NotFound("Entry not found.");
            return Function.Ok(MapStudy(reader));
        }
        catch (Exception ex) { return Err("GetNonDicomEntry", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // STANDARD REPORTS
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetStandardReportsAsync(string? body)
    {
        var req     = Parse<GetStandardReportsRequest>(body) ?? new GetStandardReportsRequest();
        var page    = Math.Max(1, req.Page ?? 1);
        var perPage = req.PerPage ?? 20;

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetStandardReports(conn, req.RadId, (page - 1) * perPage, perPage);
            await using var reader = await cmd.ExecuteReaderAsync();
            var reports = new List<StandardReportResponse>();
            while (await reader.ReadAsync())
            {
                reports.Add(new StandardReportResponse
                {
                    Id           = reader.GetInt32("id"),
                    Label        = Str(reader, "label"),
                    ReportText   = Str(reader, "report_text"),
                    RadId        = reader.IsDBNull(reader.GetOrdinal("rad_id")) ? 0 : reader.GetInt32("rad_id"),
                    RadName      = Str(reader, "rad_name"),
                    ModalityId   = reader.IsDBNull(reader.GetOrdinal("modality_id")) ? 0 : reader.GetInt32("modality_id"),
                    ModalityName = Str(reader, "modality_name"),
                    CreatedAt    = DateStr(reader, "created_at")
                });
            }
            return Function.Ok(reports);
        }
        catch (Exception ex) { return Err("GetStandardReports", ex); }
    }

    public async Task<APIGatewayProxyResponse> CreateStandardReportAsync(string? body)
    {
        var req = Parse<CreateStandardReportRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.Label) || req.RadId <= 0 || req.ModalityId <= 0)
            return Function.BadRequest("Label, RadId, and ModalityId are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertStandardReport(conn, req.Label, req.ReportText, req.RadId, req.ModalityId);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(new { id = cmd.LastInsertedId }, "Standard report created.");
        }
        catch (Exception ex) { return Err("CreateStandardReport", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateStandardReportAsync(string? body)
    {
        var req = Parse<UpdateStandardReportRequest>(body);
        if (req == null || req.ReportId <= 0)
            return Function.BadRequest("ReportId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateStandardReport(conn, req.ReportId, req.Label, req.ReportText, req.RadId, req.ModalityId);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Standard report updated.");
        }
        catch (Exception ex) { return Err("UpdateStandardReport", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteStandardReportAsync(string? body)
    {
        var req = Parse<DeleteStandardReportRequest>(body);
        if (req == null || req.ReportId <= 0)
            return Function.BadRequest("ReportId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.DeleteStandardReport(conn, req.ReportId);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Standard report deleted.");
        }
        catch (Exception ex) { return Err("DeleteStandardReport", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // ROLES
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetRolesAsync()
    {
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetRoles(conn);
            await using var reader = await cmd.ExecuteReaderAsync();
            var roles = new List<RoleResponse>();
            while (await reader.ReadAsync())
                roles.Add(new RoleResponse { Id = reader.GetInt32("id"), Name = reader.GetString("name") });
            return Function.Ok(roles);
        }
        catch (Exception ex) { return Err("GetRoles", ex); }
    }

    public async Task<APIGatewayProxyResponse> CreateRoleAsync(string? body)
    {
        var req = Parse<CreateRoleRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.Name))
            return Function.BadRequest("Role name is required.");

        try
        {
            await using var conn    = await OpenAsync();
            await using var insCmd  = QueryHelper.InsertRole(conn, req.Name);
            await insCmd.ExecuteNonQueryAsync();
            var roleId = (int)insCmd.LastInsertedId;

            foreach (var perm in req.Permissions)
            {
                await using var getCmd = QueryHelper.GetPermissionByName(conn, perm);
                var permId = await getCmd.ExecuteScalarAsync();
                if (permId != null)
                {
                    await using var rpCmd = QueryHelper.InsertRolePermission(conn, roleId, Convert.ToInt32(permId));
                    await rpCmd.ExecuteNonQueryAsync();
                }
            }
            return Function.Ok(new { roleId }, "Role created.");
        }
        catch (Exception ex) { return Err("CreateRole", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateRoleAsync(string? body)
    {
        var req = Parse<UpdateRoleRequest>(body);
        if (req == null || req.RoleId <= 0)
            return Function.BadRequest("RoleId is required.");

        try
        {
            await using var conn = await OpenAsync();
            if (!string.IsNullOrWhiteSpace(req.Name))
            {
                await using var updCmd = QueryHelper.UpdateRole(conn, req.RoleId, req.Name);
                await updCmd.ExecuteNonQueryAsync();
            }

            await using var delCmd = QueryHelper.DeleteRolePermissions(conn, req.RoleId);
            await delCmd.ExecuteNonQueryAsync();

            foreach (var perm in req.Permissions)
            {
                await using var getCmd = QueryHelper.GetPermissionByName(conn, perm);
                var permId = await getCmd.ExecuteScalarAsync();
                if (permId != null)
                {
                    await using var rpCmd = QueryHelper.InsertRolePermission(conn, req.RoleId, Convert.ToInt32(permId));
                    await rpCmd.ExecuteNonQueryAsync();
                }
            }
            return Function.Ok(null, "Role updated.");
        }
        catch (Exception ex) { return Err("UpdateRole", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteRoleAsync(string? body)
    {
        var req = Parse<DeleteRoleRequest>(body);
        if (req == null || req.RoleId <= 0)
            return Function.BadRequest("RoleId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.DeleteRole(conn, req.RoleId);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Role deleted.");
        }
        catch (Exception ex) { return Err("DeleteRole", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // SITE MANAGEMENT
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetMiscSettingsAsync()
    {
        try
        {
            await using var conn      = await OpenAsync();
            await using var wmCmd     = QueryHelper.GetSetting(conn, "welcome_message");
            var welcomeMsg            = (await wmCmd.ExecuteScalarAsync())?.ToString();
            await using var logoCmd   = QueryHelper.GetSetting(conn, "logo_path");
            var logoPath              = (await logoCmd.ExecuteScalarAsync())?.ToString();
            await using var nameCmd   = QueryHelper.GetSetting(conn, "site_name");
            var siteName              = (await nameCmd.ExecuteScalarAsync())?.ToString();

            return Function.Ok(new MiscSettingsResponse
            {
                WelcomeMessage = welcomeMsg,
                LogoUrl        = logoPath,
                SiteName       = siteName
            });
        }
        catch (Exception ex) { return Err("GetMiscSettings", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateMiscSettingsAsync(string? body)
    {
        var req = Parse<UpdateMiscSettingsRequest>(body);
        if (req == null)
            return Function.BadRequest("Request body is required.");

        try
        {
            await using var conn = await OpenAsync();
            if (req.WelcomeMessage != null)
            {
                await using var cmd = QueryHelper.UpsertSetting(conn, "welcome_message", req.WelcomeMessage);
                await cmd.ExecuteNonQueryAsync();
            }
            if (req.SiteName != null)
            {
                await using var cmd = QueryHelper.UpsertSetting(conn, "site_name", req.SiteName);
                await cmd.ExecuteNonQueryAsync();
            }
            if (req.LogoFileName != null)
            {
                var logoPath = $"logos/{req.LogoFileName}";
                await using var cmd = QueryHelper.UpsertSetting(conn, "logo_path", logoPath);
                await cmd.ExecuteNonQueryAsync();
            }
            return Function.Ok(null, "Settings updated.");
        }
        catch (Exception ex) { return Err("UpdateMiscSettings", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetOsrixAsync(string? body)
    {
        var req     = Parse<PaginatedRequest>(body) ?? new PaginatedRequest();
        var page    = Math.Max(1, req.Page ?? 1);
        var perPage = req.PerPage ?? 20;

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetOsrix(conn, (page - 1) * perPage, perPage);
            await using var reader = await cmd.ExecuteReaderAsync();
            var items = new List<OsrixUserResponse>();
            while (await reader.ReadAsync())
            {
                items.Add(new OsrixUserResponse
                {
                    Id       = reader.GetInt32("id"),
                    Username = Str(reader, "username"),
                    RadId    = reader.IsDBNull(reader.GetOrdinal("rad_id")) ? 0 : reader.GetInt32("rad_id"),
                    RadName  = Str(reader, "rad_name")
                });
            }
            return Function.Ok(items);
        }
        catch (Exception ex) { return Err("GetOsrix", ex); }
    }

    public async Task<APIGatewayProxyResponse> CreateOsrixUserAsync(string? body)
    {
        var req = Parse<OsrixUserRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.Username) || req.RadId <= 0)
            return Function.BadRequest("Username and RadId are required.");

        try
        {
            var hash = BCrypt.Net.BCrypt.HashPassword(req.Password);
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertOsrixUser(conn, req.Username, hash, req.RadId);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(new { id = cmd.LastInsertedId }, "Osrix user created.");
        }
        catch (Exception ex) { return Err("CreateOsrixUser", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateOsrixUserAsync(string? body)
    {
        var req = Parse<OsrixUserRequest>(body);
        if (req == null || req.Id <= 0)
            return Function.BadRequest("Id is required.");

        try
        {
            string? hash = null;
            if (!string.IsNullOrWhiteSpace(req.Password))
                hash = BCrypt.Net.BCrypt.HashPassword(req.Password);

            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateOsrixUser(conn, req.Id.Value, req.Username, hash, req.RadId);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Osrix user updated.");
        }
        catch (Exception ex) { return Err("UpdateOsrixUser", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteOsrixUserAsync(string? body)
    {
        var req = Parse<UserIdRequest>(body);
        if (req == null || req.UserId <= 0)
            return Function.BadRequest("UserId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.DeleteOsrixUser(conn, req.UserId);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Osrix user deleted.");
        }
        catch (Exception ex) { return Err("DeleteOsrixUser", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetOsrixInstitutionsAsync(string? body)
    {
        var req     = Parse<PaginatedRequest>(body) ?? new PaginatedRequest();
        var page    = Math.Max(1, req.Page ?? 1);
        var perPage = req.PerPage ?? 20;

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetOsrixInstitutions(conn, (page - 1) * perPage, perPage);
            await using var reader = await cmd.ExecuteReaderAsync();
            var items = new List<OsrixInstitutionResponse>();
            while (await reader.ReadAsync())
            {
                items.Add(new OsrixInstitutionResponse
                {
                    Id          = reader.GetInt32("id"),
                    Name        = Str(reader, "name"),
                    Description = Str(reader, "description")
                });
            }
            return Function.Ok(items);
        }
        catch (Exception ex) { return Err("GetOsrixInstitutions", ex); }
    }

    public async Task<APIGatewayProxyResponse> CreateOsrixInstitutionAsync(string? body)
    {
        var req = Parse<OsrixInstitutionRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.Name))
            return Function.BadRequest("Name is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertOsrixInstitution(conn, req.Name, req.Description);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(new { id = cmd.LastInsertedId }, "Institution created.");
        }
        catch (Exception ex) { return Err("CreateOsrixInstitution", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateOsrixInstitutionAsync(string? body)
    {
        var req = Parse<OsrixInstitutionRequest>(body);
        if (req == null || req.Id <= 0)
            return Function.BadRequest("Id is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateOsrixInstitution(conn, req.Id.Value, req.Name, req.Description);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Institution updated.");
        }
        catch (Exception ex) { return Err("UpdateOsrixInstitution", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteOsrixInstitutionAsync(string? body)
    {
        var req = Parse<UserIdRequest>(body);
        if (req == null || req.UserId <= 0)
            return Function.BadRequest("Id is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.DeleteOsrixInstitution(conn, req.UserId);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Institution deleted.");
        }
        catch (Exception ex) { return Err("DeleteOsrixInstitution", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetDefaultChargesAsync()
    {
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.GetSetting(conn, "default_charges");
            var json             = (await cmd.ExecuteScalarAsync())?.ToString() ?? "{}";
            var charges          = JsonConvert.DeserializeObject<Dictionary<string, decimal>>(json) ?? new();
            return Function.Ok(new DefaultChargesResponse { ModalityCharges = charges });
        }
        catch (Exception ex) { return Err("GetDefaultCharges", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateDefaultChargesAsync(string? body)
    {
        var req = Parse<UpdateDefaultChargesRequest>(body);
        if (req == null)
            return Function.BadRequest("Request body is required.");

        try
        {
            var json             = JsonConvert.SerializeObject(req.ModalityCharges);
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpsertSetting(conn, "default_charges", json);
            await cmd.ExecuteNonQueryAsync();
            return Function.Ok(null, "Default charges updated.");
        }
        catch (Exception ex) { return Err("UpdateDefaultCharges", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetRadiologistsAsync(string? body)
    {
        var req = Parse<SearchRequest>(body) ?? new SearchRequest();
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetRadiologists(conn, req.Query);
            await using var reader = await cmd.ExecuteReaderAsync();
            var rads = new List<object>();
            while (await reader.ReadAsync())
                rads.Add(new { id = reader.GetInt32("id"), name = reader.GetString("name"), email = reader.GetString("email") });
            return Function.Ok(rads);
        }
        catch (Exception ex) { return Err("GetRadiologists", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // INTERNAL HELPERS
    // ══════════════════════════════════════════════════════════════════════════

    private async Task<MySqlConnection> OpenAsync()
    {
        var conn = new MySqlConnection(_cs);
        await conn.OpenAsync();
        return conn;
    }

    private static T? Parse<T>(string? json) where T : class
    {
        if (string.IsNullOrWhiteSpace(json)) return null;
        try { return JsonConvert.DeserializeObject<T>(json); }
        catch { return null; }
    }

    private static string Str(MySqlDataReader r, string col)
    {
        var o = r.GetOrdinal(col);
        return r.IsDBNull(o) ? string.Empty : r.GetString(o);
    }

    private static string DateStr(MySqlDataReader r, string col)
    {
        var o = r.GetOrdinal(col);
        if (r.IsDBNull(o)) return string.Empty;
        return r.GetDateTime(o).ToString("yyyy-MM-dd HH:mm:ss");
    }

    private static APIGatewayProxyResponse Err(string method, Exception ex)
    {
        Console.WriteLine($"[TeleRad:{method}] {ex}");
        return Function.ServerError($"{method} failed: {ex.Message}");
    }

    private static StudyResponse MapStudy(MySqlDataReader r)
        => new()
        {
            Id              = r.GetInt32("id"),
            PatientName     = Str(r, "patient_name"),
            AccessionNumber = Str(r, "accession_number"),
            OrderNumber     = Str(r, "order_number"),
            Modality        = Str(r, "modality"),
            Status          = Str(r, "status"),
            Dos             = DateStr(r, "dos"),
            ClientName      = Str(r, "client_name"),
            ReferrerName    = Str(r, "referrer_name"),
            RadId           = r.IsDBNull(r.GetOrdinal("rad_id")) ? null : r.GetInt32("rad_id"),
            TranscriberId   = r.IsDBNull(r.GetOrdinal("transcriber_id")) ? null : r.GetInt32("transcriber_id"),
            IsStat          = !r.IsDBNull(r.GetOrdinal("is_stat")) && r.GetInt32("is_stat") == 1,
            CreatedAt       = DateStr(r, "created_at"),
            UpdatedAt       = DateStr(r, "updated_at")
        };

    private static async Task<List<StudyResponse>> ReadStudies(MySqlCommand cmd)
    {
        var list = new List<StudyResponse>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) list.Add(MapStudy(reader));
        return list;
    }

    private static NonDicomResponse MapNonDicom(MySqlDataReader r) => new()
    {
        Id              = r.GetInt32("id"),
        PatientName     = StrOpt(r, "patient_name")  ?? string.Empty,
        FirstName       = StrOpt(r, "firstname")      ?? string.Empty,
        LastName        = StrOpt(r, "lastname")       ?? string.Empty,
        IdNumber        = StrOpt(r, "idnumber")       ?? string.Empty,
        Dos             = StrOpt(r, "dos")            ?? string.Empty,
        Modality        = StrOpt(r, "modality")       ?? string.Empty,
        Exam            = StrOpt(r, "exam")           ?? string.Empty,
        Status          = StrOpt(r, "status")         ?? string.Empty,
        ClientName      = StrOpt(r, "client_name")    ?? string.Empty,
        TranscriberName = StrOpt(r, "transcriber_name") ?? string.Empty,
    };

    private static async Task<List<NonDicomResponse>> ReadNonDicomStudies(MySqlCommand cmd)
    {
        var list = new List<NonDicomResponse>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) list.Add(MapNonDicom(reader));
        return list;
    }

    private static string? StrOpt(MySqlDataReader r, string col)
    {
        try { var o = r.GetOrdinal(col); return r.IsDBNull(o) ? null : r.GetString(o); }
        catch { return null; }
    }

    private static UserResponse ReadUser(MySqlDataReader r)
        => new()
        {
            Id        = r.GetInt32("id"),
            Name      = Str(r, "name"),
            Email     = Str(r, "email"),
            Username  = Str(r, "username"),
            FirstName = StrOpt(r, "firstname"),
            LastName  = StrOpt(r, "lastname"),
            UserType  = r.GetInt32("usertype"),
            Status    = r.GetInt32("status"),
            Phone     = StrOpt(r, "phone"),
            Fax       = StrOpt(r, "fax"),
            Address   = StrOpt(r, "address")
        };

    private static async Task<List<TemplateResponse>> ReadTemplates(MySqlDataReader reader)
    {
        var list = new List<TemplateResponse>();
        while (await reader.ReadAsync())
        {
            list.Add(new TemplateResponse
            {
                Id        = reader.GetInt32("id"),
                Name      = Str(reader, "name"),
                BodyText  = Str(reader, "body_text"),
                Modality  = Str(reader, "modality"),
                UserId    = reader.IsDBNull(reader.GetOrdinal("user_id")) ? null : reader.GetInt32("user_id"),
                UserName  = Str(reader, "user_name"),
                CreatedAt = DateStr(reader, "created_at")
            });
        }
        return list;
    }

    private static string GenerateToken()
        => Convert.ToBase64String(Guid.NewGuid().ToByteArray())
           + Convert.ToBase64String(Guid.NewGuid().ToByteArray());

    private static string GenerateRandomPassword()
    {
        const string chars = "ABCDEFGHJKMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789!@#$";
        var rng = new Random();
        return new string(Enumerable.Repeat(chars, 10).Select(s => s[rng.Next(s.Length)]).ToArray());
    }

    // ══════════════════════════════════════════════════════════════════════════
    // REGION — COUNTRIES
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetAllCountriesAsync(string? body)
    {
        var req = Parse<GetAllRegionRequest>(body) ?? new GetAllRegionRequest();
        try
        {
            await using var conn  = await OpenAsync();
            var total = Convert.ToInt32(await QueryHelper.CountCountries(conn, req.Search).ExecuteScalarAsync());
            await using var cmd   = QueryHelper.GetAllCountries(conn, req.Search, req.Start, req.Length);
            await using var r     = await cmd.ExecuteReaderAsync();
            var rows = new List<CountryResponse>();
            while (await r.ReadAsync())
                rows.Add(new CountryResponse { Id = r.GetInt32("id"), Name = Str(r, "name") });
            return Function.Ok(new DatatableResponse<CountryResponse>
            {
                Draw = req.Draw, RecordsTotal = total, RecordsFiltered = total, AaData = rows
            });
        }
        catch (Exception ex) { return Err("GetAllCountries", ex); }
    }

    public async Task<APIGatewayProxyResponse> CreateCountryAsync(string? body)
    {
        var req = Parse<CreateCountryRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.Name))
            return Function.BadRequest("Name is required.");
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertCountry(conn, req.Name.Trim());
            var newId = Convert.ToInt32(await cmd.ExecuteScalarAsync());
            return Function.Ok(new CountryResponse { Id = newId, Name = req.Name.Trim() });
        }
        catch (Exception ex) { return Err("CreateCountry", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateCountryAsync(string? body)
    {
        var req = Parse<UpdateCountryRequest>(body);
        if (req == null || req.Id <= 0 || string.IsNullOrWhiteSpace(req.Name))
            return Function.BadRequest("Id and Name are required.");
        try
        {
            await using var conn = await OpenAsync();
            await QueryHelper.UpdateCountry(conn, req.Id, req.Name.Trim()).ExecuteNonQueryAsync();
            return Function.Ok(new CountryResponse { Id = req.Id, Name = req.Name.Trim() });
        }
        catch (Exception ex) { return Err("UpdateCountry", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteCountryAsync(string? body)
    {
        var req = Parse<DeleteCountryRequest>(body);
        if (req == null || req.Id <= 0) return Function.BadRequest("Id is required.");
        try
        {
            await using var conn = await OpenAsync();
            await QueryHelper.DeleteCountry(conn, req.Id).ExecuteNonQueryAsync();
            return Function.Ok(null, "Country deleted.");
        }
        catch (Exception ex) { return Err("DeleteCountry", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // REGION — STATES
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetAllStatesAsync(string? body)
    {
        var req = Parse<GetAllRegionRequest>(body) ?? new GetAllRegionRequest();
        try
        {
            await using var conn  = await OpenAsync();
            var total = Convert.ToInt32(await QueryHelper.CountStates(conn, req.Search).ExecuteScalarAsync());
            await using var cmd   = QueryHelper.GetAllStates(conn, req.Search, req.Start, req.Length);
            await using var r     = await cmd.ExecuteReaderAsync();
            var rows = new List<StateResponse>();
            while (await r.ReadAsync())
                rows.Add(new StateResponse
                {
                    Id = r.GetInt32("id"), Name = Str(r,"name"),
                    CountryId = r.GetInt32("countryid"), CountryName = Str(r,"country_name")
                });
            return Function.Ok(new DatatableResponse<StateResponse>
            {
                Draw = req.Draw, RecordsTotal = total, RecordsFiltered = total, AaData = rows
            });
        }
        catch (Exception ex) { return Err("GetAllStates", ex); }
    }

    public async Task<APIGatewayProxyResponse> CreateStateAsync(string? body)
    {
        var req = Parse<CreateStateRequest>(body);
        if (req == null || req.CountryId <= 0 || string.IsNullOrWhiteSpace(req.Name))
            return Function.BadRequest("CountryId and Name are required.");
        try
        {
            await using var conn = await OpenAsync();
            var newId = Convert.ToInt32(await QueryHelper.InsertState(conn, req.CountryId, req.Name.Trim()).ExecuteScalarAsync());
            return Function.Ok(new StateResponse { Id = newId, CountryId = req.CountryId, Name = req.Name.Trim() });
        }
        catch (Exception ex) { return Err("CreateState", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateStateAsync(string? body)
    {
        var req = Parse<UpdateStateRequest>(body);
        if (req == null || req.Id <= 0 || string.IsNullOrWhiteSpace(req.Name))
            return Function.BadRequest("Id and Name are required.");
        try
        {
            await using var conn = await OpenAsync();
            await QueryHelper.UpdateState(conn, req.Id, req.CountryId, req.Name.Trim()).ExecuteNonQueryAsync();
            return Function.Ok(new StateResponse { Id = req.Id, CountryId = req.CountryId, Name = req.Name.Trim() });
        }
        catch (Exception ex) { return Err("UpdateState", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteStateAsync(string? body)
    {
        var req = Parse<DeleteStateRequest>(body);
        if (req == null || req.Id <= 0) return Function.BadRequest("Id is required.");
        try
        {
            await using var conn = await OpenAsync();
            await QueryHelper.DeleteState(conn, req.Id).ExecuteNonQueryAsync();
            return Function.Ok(null, "State deleted.");
        }
        catch (Exception ex) { return Err("DeleteState", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // REGION — CITIES
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetAllCitiesAsync(string? body)
    {
        var req = Parse<GetAllRegionRequest>(body) ?? new GetAllRegionRequest();
        try
        {
            await using var conn  = await OpenAsync();
            var total = Convert.ToInt32(await QueryHelper.CountCities(conn, req.Search).ExecuteScalarAsync());
            await using var cmd   = QueryHelper.GetAllCities(conn, req.Search, req.Start, req.Length);
            await using var r     = await cmd.ExecuteReaderAsync();
            var rows = new List<CityResponse>();
            while (await r.ReadAsync())
                rows.Add(new CityResponse
                {
                    Id = r.GetInt32("id"), Name = Str(r,"name"),
                    StateId = r.GetInt32("stateid"), StateName = Str(r,"state_name")
                });
            return Function.Ok(new DatatableResponse<CityResponse>
            {
                Draw = req.Draw, RecordsTotal = total, RecordsFiltered = total, AaData = rows
            });
        }
        catch (Exception ex) { return Err("GetAllCities", ex); }
    }

    public async Task<APIGatewayProxyResponse> CreateCityAsync(string? body)
    {
        var req = Parse<CreateCityRequest>(body);
        if (req == null || req.StateId <= 0 || string.IsNullOrWhiteSpace(req.Name))
            return Function.BadRequest("StateId and Name are required.");
        try
        {
            await using var conn = await OpenAsync();
            var newId = Convert.ToInt32(await QueryHelper.InsertCity(conn, req.StateId, req.Name.Trim()).ExecuteScalarAsync());
            return Function.Ok(new CityResponse { Id = newId, StateId = req.StateId, Name = req.Name.Trim() });
        }
        catch (Exception ex) { return Err("CreateCity", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateCityAsync(string? body)
    {
        var req = Parse<UpdateCityRequest>(body);
        if (req == null || req.Id <= 0 || string.IsNullOrWhiteSpace(req.Name))
            return Function.BadRequest("Id and Name are required.");
        try
        {
            await using var conn = await OpenAsync();
            await QueryHelper.UpdateCity(conn, req.Id, req.StateId, req.Name.Trim()).ExecuteNonQueryAsync();
            return Function.Ok(new CityResponse { Id = req.Id, StateId = req.StateId, Name = req.Name.Trim() });
        }
        catch (Exception ex) { return Err("UpdateCity", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteCityAsync(string? body)
    {
        var req = Parse<DeleteCityRequest>(body);
        if (req == null || req.Id <= 0) return Function.BadRequest("Id is required.");
        try
        {
            await using var conn = await OpenAsync();
            await QueryHelper.DeleteCity(conn, req.Id).ExecuteNonQueryAsync();
            return Function.Ok(null, "City deleted.");
        }
        catch (Exception ex) { return Err("DeleteCity", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // QUICKBOOKS
    // ══════════════════════════════════════════════════════════════════════════

    private static readonly HttpClient _http = new();
    private static string QbBaseUrl   => Environment.GetEnvironmentVariable("QB_BASE_URL")    ?? "https://quickbooks.api.intuit.com";
    private static string QbClientId  => Environment.GetEnvironmentVariable("QB_CLIENT_ID")   ?? string.Empty;
    private static string QbClientSecret => Environment.GetEnvironmentVariable("QB_CLIENT_SECRET") ?? string.Empty;

    private async Task<(string realmId, string accessToken)?> GetQbConnectionAsync()
    {
        await using var conn = await OpenAsync();
        await using var cmd  = QueryHelper.GetQbConnection(conn);
        await using var r    = await cmd.ExecuteReaderAsync();
        if (!await r.ReadAsync()) return null;
        var realmId      = r.GetString(r.GetOrdinal("realm_id"));
        var accessToken  = r.GetString(r.GetOrdinal("access_token_value"));
        var refreshToken = r.GetString(r.GetOrdinal("refresh_token_value"));
        var expiresAtStr = r.IsDBNull(r.GetOrdinal("access_token_expires_at")) ? "" : r.GetDateTime(r.GetOrdinal("access_token_expires_at")).ToString("o");
        await r.CloseAsync();

        if (DateTime.TryParse(expiresAtStr, out var expiresAt) && DateTime.UtcNow >= expiresAt)
        {
            var creds = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{QbClientId}:{QbClientSecret}"));
            var form  = new FormUrlEncodedContent(new[] {
                new KeyValuePair<string,string>("grant_type","refresh_token"),
                new KeyValuePair<string,string>("refresh_token", refreshToken)
            });
            var req2 = new HttpRequestMessage(HttpMethod.Post, "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer")
            {
                Headers = { {"Authorization", $"Basic {creds}"}, {"Accept","application/json"} },
                Content = form
            };
            var resp = await _http.SendAsync(req2);
            var tok  = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(await resp.Content.ReadAsStringAsync())!;
            accessToken  = (string)tok.access_token;
            refreshToken = (string)tok.refresh_token;
            var newExp   = DateTime.UtcNow.AddSeconds((int)tok.expires_in).ToString("yyyy-MM-dd HH:mm:ss");
            await QueryHelper.UpdateQbToken(conn, accessToken, refreshToken, newExp).ExecuteNonQueryAsync();
        }
        return (realmId, accessToken);
    }

    private async Task<string> QbQueryAsync(string realmId, string token, string query)
    {
        var url = $"{QbBaseUrl}/v3/company/{realmId}/query?query={Uri.EscapeDataString(query)}&minorversion=65";
        var req = new HttpRequestMessage(HttpMethod.Get, url);
        req.Headers.Add("Authorization", $"Bearer {token}"); req.Headers.Add("Accept","application/json");
        return await (await _http.SendAsync(req)).Content.ReadAsStringAsync();
    }

    private async Task<string> QbPostAsync(string realmId, string token, string entity, string jsonBody)
    {
        var url = $"{QbBaseUrl}/v3/company/{realmId}/{entity.ToLower()}?minorversion=65";
        var req = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = new StringContent(jsonBody, System.Text.Encoding.UTF8, "application/json")
        };
        req.Headers.Add("Authorization", $"Bearer {token}"); req.Headers.Add("Accept","application/json");
        return await (await _http.SendAsync(req)).Content.ReadAsStringAsync();
    }

    private static List<QbItemResponse> ParseQbItems(string json) {
        var list = new List<QbItemResponse>();
        try { var d=Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(json); var items=d?.QueryResponse?.Item; if(items!=null) foreach(var i in items) list.Add(new QbItemResponse{Id=(string)i.Id,Name=(string)i.Name}); } catch{}
        return list;
    }
    private static List<QbCustomerResponse> ParseQbCustomers(string json) {
        var list = new List<QbCustomerResponse>();
        try { var d=Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(json); var c=d?.QueryResponse?.Customer; if(c!=null) foreach(var x in c) list.Add(new QbCustomerResponse{Id=(string)x.Id,DisplayName=(string)x.DisplayName}); } catch{}
        return list;
    }
    private static List<QbVendorResponse> ParseQbVendors(string json) {
        var list = new List<QbVendorResponse>();
        try { var d=Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(json); var v=d?.QueryResponse?.Vendor; if(v!=null) foreach(var x in v) list.Add(new QbVendorResponse{Id=(string)x.Id,DisplayName=(string)x.DisplayName}); } catch{}
        return list;
    }
    private static string ParseFirstId(string json, string entity) {
        try { var d=Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(json); var n=d?.QueryResponse?[entity]; if(n!=null&&n.Count>0) return (string)n[0].Id; } catch{}
        return string.Empty;
    }
    private static string ParseCreatedId(string json, string entity) {
        try { var d=Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(json); return (string)(d?[entity]?.Id??string.Empty); } catch{ return string.Empty; }
    }
    private async Task<(int,int,int)> ResolveLocationAsync(MySqlConnection db,string? country,string? state,string? city) {
        int co=0,st=0,ci=0;
        if(!string.IsNullOrEmpty(country)){var r=await QueryHelper.FindCountryByName(db,country).ExecuteScalarAsync();if(r!=null)co=Convert.ToInt32(r);}
        if(co>0&&!string.IsNullOrEmpty(state)){var r=await QueryHelper.FindStateByName(db,state,co).ExecuteScalarAsync();if(r!=null)st=Convert.ToInt32(r);}
        if(st>0&&!string.IsNullOrEmpty(city)){var r=await QueryHelper.FindCityByName(db,city,st).ExecuteScalarAsync();if(r!=null)ci=Convert.ToInt32(r);}
        return (co,st,ci);
    }

    public async Task<APIGatewayProxyResponse> GetQbSettingsAsync(string? body) {
        try {
            await using var conn=await OpenAsync(); await using var cmd=QueryHelper.GetQbConnection(conn); await using var r=await cmd.ExecuteReaderAsync();
            if(!await r.ReadAsync()) return Function.Ok(new QbSettingsResponse{IsConnected=false});
            return Function.Ok(new QbSettingsResponse{IsConnected=!string.IsNullOrEmpty(r.IsDBNull(r.GetOrdinal("realm_id"))?"":r.GetString("realm_id")),CompanyName=r.IsDBNull(r.GetOrdinal("company_name"))?"":r.GetString("company_name"),Country=r.IsDBNull(r.GetOrdinal("country"))?"":r.GetString("country"),CheckAccount=r.IsDBNull(r.GetOrdinal("check_account"))?"":r.GetString("check_account")});
        } catch(Exception ex){return Err("GetQbSettings",ex);}
    }

    public async Task<APIGatewayProxyResponse> SaveQbSettingsAsync(string? body) {
        var req=Parse<SaveQbSettingsRequest>(body); if(req==null) return Function.BadRequest("Request body required.");
        try { await using var conn=await OpenAsync(); await QueryHelper.UpdateQbSettings(conn,req.CheckAccount).ExecuteNonQueryAsync(); return Function.Ok(null,"QB settings saved."); }
        catch(Exception ex){return Err("SaveQbSettings",ex);}
    }

    public async Task<APIGatewayProxyResponse> GetQbModalityMappingsAsync(string? body) {
        try {
            var conn=await GetQbConnectionAsync(); if(conn==null) return Function.BadRequest("QuickBooks not connected.");
            var (realmId,token)=conn.Value;
            await using var db=await OpenAsync();
            var mappings=new Dictionary<string,string>();
            await using var mR=await QueryHelper.GetQbMappings(db,"Modality").ExecuteReaderAsync();
            while(await mR.ReadAsync()) mappings[mR.GetString("system_id")]=mR.GetString("qbo_id"); await mR.CloseAsync();
            var mods=new List<string>();
            await using var dR=await QueryHelper.GetDistinctModalities(db).ExecuteReaderAsync();
            while(await dR.ReadAsync()) mods.Add(Str(dR,"modality")); await dR.CloseAsync();
            mods.Add("MedicalDirector");
            var items=ParseQbItems(await QbQueryAsync(realmId,token,"Select * from Item where Type = 'Service' MAXRESULTS 1000"));
            return Function.Ok(new QbModalityMappingResponse{Mapped=mods.Where(m=>mappings.ContainsKey(m)).ToList(),Unmapped=mods.Where(m=>!mappings.ContainsKey(m)).ToList(),Mappings=mappings,Items=items});
        } catch(Exception ex){return Err("GetQbModalityMappings",ex);}
    }

    public async Task<APIGatewayProxyResponse> SaveQbModalityMappingAsync(string? body) {
        var req=Parse<SaveQbModalityMappingRequest>(body); if(req==null) return Function.BadRequest("Request body required.");
        try {
            await using var db=await OpenAsync();
            foreach(var(mod,qboId) in req.Mappings)
                if(string.IsNullOrEmpty(qboId)) await QueryHelper.DeleteQbMapping(db,mod,"Modality","Item").ExecuteNonQueryAsync();
                else await QueryHelper.UpsertQbMapping(db,mod,"Modality","Item",qboId).ExecuteNonQueryAsync();
            return Function.Ok(null,"Modality mappings saved.");
        } catch(Exception ex){return Err("SaveQbModalityMapping",ex);}
    }

    public async Task<APIGatewayProxyResponse> PushQbModalitiesAsync(string? body) {
        var req=Parse<PushQbModalitiesRequest>(body); if(req==null||req.Modalities.Count==0) return Function.BadRequest("Modalities list required.");
        try {
            var conn=await GetQbConnectionAsync(); if(conn==null) return Function.BadRequest("QuickBooks not connected.");
            var (realmId,token)=conn.Value;
            await using var db=await OpenAsync();
            var mappings=new Dictionary<string,string>();
            await using var mR=await QueryHelper.GetQbMappings(db,"Modality").ExecuteReaderAsync();
            while(await mR.ReadAsync()) mappings[mR.GetString("system_id")]=mR.GetString("qbo_id"); await mR.CloseAsync();
            var pushed=new List<string>(); var errors=new List<string>();
            foreach(var mod in req.Modalities) {
                if(mappings.TryGetValue(mod,out var ex2)&&!string.IsNullOrEmpty(ex2)&&ex2!="InQueue") continue;
                await QueryHelper.UpsertQbMapping(db,mod,"Modality","Item","InQueue").ExecuteNonQueryAsync();
                var acctId=ParseFirstId(await QbQueryAsync(realmId,token,"Select * from Account where AccountSubType = 'SalesOfProductIncome' MAXRESULTS 1"),"Account");
                var itemJson=Newtonsoft.Json.JsonConvert.SerializeObject(new{Name=mod,Sku=mod,UnitPrice=1,Taxable=false,Type="Service",IncomeAccountRef=new{value=acctId}});
                var qboId=ParseCreatedId(await QbPostAsync(realmId,token,"item",itemJson),"Item");
                if(!string.IsNullOrEmpty(qboId)){await QueryHelper.UpsertQbMapping(db,mod,"Modality","Item",qboId).ExecuteNonQueryAsync();pushed.Add(mod);}
                else errors.Add(mod);
            }
            return Function.Ok(new{pushed,errors});
        } catch(Exception ex){return Err("PushQbModalities",ex);}
    }

    public async Task<APIGatewayProxyResponse> PullQbModalitiesAsync(string? body) {
        try {
            var conn=await GetQbConnectionAsync(); if(conn==null) return Function.BadRequest("QuickBooks not connected.");
            var (realmId,token)=conn.Value;
            await using var db=await OpenAsync();
            var mappings=new Dictionary<string,string>();
            await using var mR=await QueryHelper.GetQbMappings(db,"Modality").ExecuteReaderAsync();
            while(await mR.ReadAsync()) mappings[mR.GetString("system_id")]=mR.GetString("qbo_id"); await mR.CloseAsync();
            var items=ParseQbItems(await QbQueryAsync(realmId,token,"Select * from Item where Type = 'Service' MAXRESULTS 1000"));
            var imported=0;
            foreach(var i in items){if(mappings.ContainsValue(i.Id)) continue; await QueryHelper.InsertModality(db,i.Name).ExecuteNonQueryAsync(); await QueryHelper.UpsertQbMapping(db,i.Name,"Modality","Item",i.Id).ExecuteNonQueryAsync(); imported++;}
            return Function.Ok(new{imported});
        } catch(Exception ex){return Err("PullQbModalities",ex);}
    }

    public async Task<APIGatewayProxyResponse> GetQbClientMappingsAsync(string? body) {
        try {
            var conn=await GetQbConnectionAsync(); if(conn==null) return Function.BadRequest("QuickBooks not connected.");
            var (realmId,token)=conn.Value;
            await using var db=await OpenAsync();
            var mappings=new Dictionary<string,string>();
            await using var mR=await QueryHelper.GetQbMappings(db,"Client").ExecuteReaderAsync();
            while(await mR.ReadAsync()) mappings[mR.GetString("system_id")]=mR.GetString("qbo_id"); await mR.CloseAsync();
            await using var cR=await QueryHelper.GetUsers(db,4,null).ExecuteReaderAsync();
            var mapped=new List<UserResponse>(); var unmapped=new List<UserResponse>();
            while(await cR.ReadAsync()){var u=ReadUser(cR);(mappings.ContainsKey(u.Id.ToString())?mapped:unmapped).Add(u);} await cR.CloseAsync();
            var customers=ParseQbCustomers(await QbQueryAsync(realmId,token,"Select * from Customer MAXRESULTS 1000"));
            return Function.Ok(new QbClientMappingResponse{MappedClients=mapped,UnmappedClients=unmapped,Mappings=mappings,Customers=customers});
        } catch(Exception ex){return Err("GetQbClientMappings",ex);}
    }

    public async Task<APIGatewayProxyResponse> SaveQbClientMappingAsync(string? body) {
        var req=Parse<SaveQbClientMappingRequest>(body); if(req==null) return Function.BadRequest("Request body required.");
        try {
            await using var db=await OpenAsync();
            foreach(var(id,qboId) in req.Mappings)
                if(string.IsNullOrEmpty(qboId)) await QueryHelper.DeleteQbMapping(db,id,"Client","Customer").ExecuteNonQueryAsync();
                else await QueryHelper.UpsertQbMapping(db,id,"Client","Customer",qboId).ExecuteNonQueryAsync();
            return Function.Ok(null,"Client mappings saved.");
        } catch(Exception ex){return Err("SaveQbClientMapping",ex);}
    }

    public async Task<APIGatewayProxyResponse> PushQbClientsAsync(string? body) {
        var req=Parse<PushQbClientsRequest>(body); if(req==null||req.ClientIds.Count==0) return Function.BadRequest("ClientIds required.");
        try {
            var conn=await GetQbConnectionAsync(); if(conn==null) return Function.BadRequest("QuickBooks not connected.");
            var (realmId,token)=conn.Value;
            await using var db=await OpenAsync();
            var mappings=new Dictionary<string,string>();
            await using var mR=await QueryHelper.GetQbMappings(db,"Client").ExecuteReaderAsync();
            while(await mR.ReadAsync()) mappings[mR.GetString("system_id")]=mR.GetString("qbo_id"); await mR.CloseAsync();
            await using var uR=await QueryHelper.GetUsersForQbPush(db,req.ClientIds).ExecuteReaderAsync();
            var clients=new List<(int id,string fn,string mn,string ln,string dn,string em,string ph,string off,string ci,string st,string co)>();
            while(await uR.ReadAsync()) clients.Add((uR.GetInt32("id"),Str(uR,"firstname"),Str(uR,"middlename"),Str(uR,"lastname"),Str(uR,"displayname"),Str(uR,"email"),Str(uR,"phone"),Str(uR,"office"),Str(uR,"city_name"),Str(uR,"state_name"),Str(uR,"country_name"))); await uR.CloseAsync();
            var pushed=new List<int>(); var errors=new List<int>();
            foreach(var c in clients) {
                if(mappings.TryGetValue(c.id.ToString(),out var ex2)&&!string.IsNullOrEmpty(ex2)&&ex2!="InQueue") continue;
                await QueryHelper.UpsertQbMapping(db,c.id.ToString(),"Client","Customer","InQueue").ExecuteNonQueryAsync();
                var json=Newtonsoft.Json.JsonConvert.SerializeObject(new{GivenName=c.fn,MiddleName=c.mn,FamilyName=c.ln,DisplayName=c.dn,PrimaryEmailAddr=new{Address=c.em},PrimaryPhone=string.IsNullOrEmpty(c.ph)?null:(object)new{FreeFormNumber=c.ph[..Math.Min(20,c.ph.Length)]},BillAddr=new{Line1=c.off,City=c.ci,CountrySubDivisionCode=c.st,Country=c.co}});
                var qboId=ParseCreatedId(await QbPostAsync(realmId,token,"customer",json),"Customer");
                if(!string.IsNullOrEmpty(qboId)){await QueryHelper.UpsertQbMapping(db,c.id.ToString(),"Client","Customer",qboId).ExecuteNonQueryAsync();pushed.Add(c.id);}else errors.Add(c.id);
            }
            return Function.Ok(new{pushed,errors});
        } catch(Exception ex){return Err("PushQbClients",ex);}
    }

    public async Task<APIGatewayProxyResponse> PullQbClientsAsync(string? body) {
        try {
            var conn=await GetQbConnectionAsync(); if(conn==null) return Function.BadRequest("QuickBooks not connected.");
            var (realmId,token)=conn.Value;
            await using var db=await OpenAsync();
            var mappings=new Dictionary<string,string>();
            await using var mR=await QueryHelper.GetQbMappings(db,"Client").ExecuteReaderAsync();
            while(await mR.ReadAsync()) mappings[mR.GetString("system_id")]=mR.GetString("qbo_id"); await mR.CloseAsync();
            var customers=ParseQbCustomers(await QbQueryAsync(realmId,token,"Select * from Customer ORDERBY Id ASC STARTPOSITION 1 MAXRESULTS 1000"));
            var imported=0;
            foreach(var c in customers){if(mappings.ContainsValue(c.Id)) continue; await using var ins=QueryHelper.InsertUserFromQb(db,c.DisplayName,"","",c.DisplayName,"","",4,0,0,0); var newId=Convert.ToInt32(await ins.ExecuteScalarAsync()); await QueryHelper.UpsertQbMapping(db,newId.ToString(),"Client","Customer",c.Id).ExecuteNonQueryAsync(); imported++;}
            return Function.Ok(new{imported});
        } catch(Exception ex){return Err("PullQbClients",ex);}
    }

    public async Task<APIGatewayProxyResponse> GetQbTranscriberMappingsAsync(string? body) {
        try {
            var conn=await GetQbConnectionAsync(); if(conn==null) return Function.BadRequest("QuickBooks not connected.");
            var (realmId,token)=conn.Value;
            await using var db=await OpenAsync();
            var mappings=new Dictionary<string,string>();
            await using var mR=await QueryHelper.GetQbMappings(db,"Transcriber").ExecuteReaderAsync();
            while(await mR.ReadAsync()) mappings[mR.GetString("system_id")]=mR.GetString("qbo_id"); await mR.CloseAsync();
            await using var tR=await QueryHelper.GetUsers(db,3,null).ExecuteReaderAsync();
            var mapped=new List<UserResponse>(); var unmapped=new List<UserResponse>();
            while(await tR.ReadAsync()){var u=ReadUser(tR);(mappings.ContainsKey(u.Id.ToString())?mapped:unmapped).Add(u);} await tR.CloseAsync();
            var vendors=ParseQbVendors(await QbQueryAsync(realmId,token,"Select Id,DisplayName from Vendor MAXRESULTS 1000"));
            return Function.Ok(new QbTranscriberMappingResponse{MappedTranscribers=mapped,UnmappedTranscribers=unmapped,Mappings=mappings,Vendors=vendors});
        } catch(Exception ex){return Err("GetQbTranscriberMappings",ex);}
    }

    public async Task<APIGatewayProxyResponse> SaveQbTranscriberMappingAsync(string? body) {
        var req=Parse<SaveQbTranscriberMappingRequest>(body); if(req==null) return Function.BadRequest("Request body required.");
        try {
            await using var db=await OpenAsync();
            foreach(var(id,qboId) in req.Mappings)
                if(string.IsNullOrEmpty(qboId)) await QueryHelper.DeleteQbMapping(db,id,"Transcriber","Vendor").ExecuteNonQueryAsync();
                else await QueryHelper.UpsertQbMapping(db,id,"Transcriber","Vendor",qboId).ExecuteNonQueryAsync();
            return Function.Ok(null,"Transcriber mappings saved.");
        } catch(Exception ex){return Err("SaveQbTranscriberMapping",ex);}
    }

    public async Task<APIGatewayProxyResponse> PushQbTranscribersAsync(string? body) {
        var req=Parse<PushQbTranscribersRequest>(body); if(req==null||req.TranscriberIds.Count==0) return Function.BadRequest("TranscriberIds required.");
        try {
            var conn=await GetQbConnectionAsync(); if(conn==null) return Function.BadRequest("QuickBooks not connected.");
            var (realmId,token)=conn.Value;
            await using var db=await OpenAsync();
            var mappings=new Dictionary<string,string>();
            await using var mR=await QueryHelper.GetQbMappings(db,"Transcriber").ExecuteReaderAsync();
            while(await mR.ReadAsync()) mappings[mR.GetString("system_id")]=mR.GetString("qbo_id"); await mR.CloseAsync();
            await using var uR=await QueryHelper.GetUsersForQbPush(db,req.TranscriberIds).ExecuteReaderAsync();
            var trans=new List<(int id,string fn,string mn,string ln,string dn,string em,string ph,string off,string ci,string st,string co)>();
            while(await uR.ReadAsync()) trans.Add((uR.GetInt32("id"),Str(uR,"firstname"),Str(uR,"middlename"),Str(uR,"lastname"),Str(uR,"displayname"),Str(uR,"email"),Str(uR,"phone"),Str(uR,"office"),Str(uR,"city_name"),Str(uR,"state_name"),Str(uR,"country_name"))); await uR.CloseAsync();
            var pushed=new List<int>(); var errors=new List<int>();
            foreach(var t in trans) {
                if(mappings.TryGetValue(t.id.ToString(),out var ex2)&&!string.IsNullOrEmpty(ex2)&&ex2!="InQueue") continue;
                await QueryHelper.UpsertQbMapping(db,t.id.ToString(),"Transcriber","Vendor","InQueue").ExecuteNonQueryAsync();
                var isEmail=System.Text.RegularExpressions.Regex.IsMatch(t.em,@"^[^@\s]+@[^@\s]+\.[^@\s]+$");
                var json=Newtonsoft.Json.JsonConvert.SerializeObject(new{GivenName=t.fn,MiddleName=t.mn,FamilyName=t.ln,DisplayName=t.dn,PrimaryEmailAddr=isEmail?(object)new{Address=t.em}:null,PrimaryPhone=string.IsNullOrEmpty(t.ph)?null:(object)new{FreeFormNumber=t.ph[..Math.Min(20,t.ph.Length)]},BillAddr=new{Line1=t.off,City=t.ci,CountrySubDivisionCode=t.st,Country=t.co}});
                var qboId=ParseCreatedId(await QbPostAsync(realmId,token,"vendor",json),"Vendor");
                if(!string.IsNullOrEmpty(qboId)){await QueryHelper.UpsertQbMapping(db,t.id.ToString(),"Transcriber","Vendor",qboId).ExecuteNonQueryAsync();pushed.Add(t.id);}else errors.Add(t.id);
            }
            return Function.Ok(new{pushed,errors});
        } catch(Exception ex){return Err("PushQbTranscribers",ex);}
    }

    public async Task<APIGatewayProxyResponse> PullQbTranscribersAsync(string? body) {
        try {
            var conn=await GetQbConnectionAsync(); if(conn==null) return Function.BadRequest("QuickBooks not connected.");
            var (realmId,token)=conn.Value;
            await using var db=await OpenAsync();
            var mappings=new Dictionary<string,string>();
            await using var mR=await QueryHelper.GetQbMappings(db,"Transcriber").ExecuteReaderAsync();
            while(await mR.ReadAsync()) mappings[mR.GetString("system_id")]=mR.GetString("qbo_id"); await mR.CloseAsync();
            var vendors=ParseQbVendors(await QbQueryAsync(realmId,token,"Select * from Vendor ORDERBY Id ASC STARTPOSITION 1 MAXRESULTS 1000"));
            var imported=0;
            foreach(var v in vendors){if(mappings.ContainsValue(v.Id)) continue; await using var ins=QueryHelper.InsertUserFromQb(db,v.DisplayName,"","",v.DisplayName,"","",3,0,0,0); var newId=Convert.ToInt32(await ins.ExecuteScalarAsync()); await QueryHelper.UpsertQbMapping(db,newId.ToString(),"Transcriber","Vendor",v.Id).ExecuteNonQueryAsync(); imported++;}
            return Function.Ok(new{imported});
        } catch(Exception ex){return Err("PullQbTranscribers",ex);}
    }

}
