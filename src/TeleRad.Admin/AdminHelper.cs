using Amazon.Lambda.APIGatewayEvents;
using MySqlConnector;
using TeleRad.Shared;
using TeleRad.Shared.Model;
using TeleRad.Shared.Utility;

namespace TeleRad.Admin;

public class AdminHelper : HelperBase
{
    public AdminHelper(string connectionString, TokenValidationResult? token = null)
        : base(connectionString, token) { }

    // ══════════════════════════════════════════════════════════════════════════
    // AUDIT (management)
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetAuditAllAsync(string? body)
    {
        var req     = Parse<GetAuditAllRequest>(body) ?? new GetAuditAllRequest();
        var page    = Math.Max(1, req.Page ?? 1);
        var perPage = req.PerPage ?? 20;

        try
        {
            await using var conn   = await OpenAsync();
            await using var cntCmd = QueryHelper.CountAuditAll(conn, req.Search, req.DateFrom, req.DateTo);
            var total              = Convert.ToInt32(await cntCmd.ExecuteScalarAsync());
            await using var cmd    = QueryHelper.GetAuditAll(conn, req.Search, req.DateFrom, req.DateTo,
                (page - 1) * perPage, perPage);
            await using var reader = await cmd.ExecuteReaderAsync();
            var audits = new List<AuditResponse>();
            while (await reader.ReadAsync())
                audits.Add(new AuditResponse
                {
                    Id          = reader.GetInt32("id"),
                    StudyId     = reader.IsDBNull(reader.GetOrdinal("typewordlist_id")) ? 0 : reader.GetInt32("typewordlist_id"),
                    Action      = Str(reader, "action"),
                    Description = Str(reader, "description"),
                    Username    = Str(reader, "username"),
                    CreatedAt   = DateStr(reader, "created_at")
                });
            return FunctionBase.Ok(new PaginatedResponse<AuditResponse>
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
            return FunctionBase.BadRequest("FaxNumber is required.");

        // ── Dev/demo guard ────────────────────────────────────────────────────
        // When FAX_SEND_ENABLED=false in template.yaml the fax is NOT dispatched.
        // Return a suppressed response so the UI behaves normally without sending.
        if (!AppConfig.FaxSendEnabled)
            return FunctionBase.Ok(
                new { faxId = 0, status = "dev-suppressed" },
                $"Fax sending is disabled on this environment (Stage={AppConfig.Stage}). Set FAX_SEND_ENABLED=true in template.yaml to enable.");

        try
        {
            var filePath = $"faxes/outbound/{req.FileName}";
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertFaxSent(conn, req.FaxNumber, req.FileName, req.StudyId);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(new { faxId = cmd.LastInsertedId, status = "pending" }, "Fax queued.");
        }
        catch (Exception ex) { return Err("SendFax", ex); }
    }

    public async Task<APIGatewayProxyResponse> ViewFaxesAsync(string? body)
    {
        var req     = Parse<PaginatedRequest>(body) ?? new PaginatedRequest();
        var page    = Math.Max(1, req.Page ?? 1);
        var perPage = req.PerPage ?? 20;

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetSentFaxes(conn, (page - 1) * perPage, perPage);
            await using var reader = await cmd.ExecuteReaderAsync();
            var faxes = new List<FaxResponse>();
            while (await reader.ReadAsync())
                faxes.Add(new FaxResponse
                {
                    Id = reader.GetInt32("id"), FaxNumber = Str(reader, "fax_number"),
                    FileName = Str(reader, "file_name"), Status = Str(reader, "status"),
                    SentAt = DateStr(reader, "created_at"),
                    DeliveredAt = SafeDateStr(reader, "delevered_at")
                });
            return FunctionBase.Ok(faxes);
        }
        catch (Exception ex) { return Err("ViewFaxes", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetInboundFaxesAsync(string? body)
    {
        var req     = Parse<PaginatedRequest>(body) ?? new PaginatedRequest();
        var page    = Math.Max(1, req.Page ?? 1);
        var perPage = req.PerPage ?? 20;

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetInboundFaxes(conn, (page - 1) * perPage, perPage);
            await using var reader = await cmd.ExecuteReaderAsync();
            var faxes = new List<InboundFaxResponse>();
            while (await reader.ReadAsync())
                faxes.Add(new InboundFaxResponse
                {
                    Id             = reader.GetInt32("id"),
                    FileName       = Str(reader, "file_name"),
                    FileUrl        = Str(reader, "file_name"),   // same path used for presigned URL
                    IsRead         = false,
                    ClientUsername = Str(reader, "client_username"),
                    ReceivedAt     = DateStr(reader, "created_at")
                });
            return FunctionBase.Ok(faxes);
        }
        catch (Exception ex) { return Err("GetInboundFaxes", ex); }
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
            var faxes = new List<InboundFaxResponse>();
            while (await reader.ReadAsync())
                faxes.Add(new InboundFaxResponse
                {
                    Id         = reader.GetInt32("id"),
                    FileName   = Str(reader, "file_name"),
                    FileUrl    = Str(reader, "file_name"),   // S3 key = file_name
                    IsRead     = !reader.IsDBNull(reader.GetOrdinal("is_read")) && reader.GetInt32("is_read") == 1,
                    ReceivedAt = DateStr(reader, "created_at")
                });
            return FunctionBase.Ok(faxes);
        }
        catch (Exception ex) { return Err("GetIncomingFaxes", ex); }
    }

    public async Task<APIGatewayProxyResponse> RenameFaxAsync(string? body)
    {
        var req = Parse<RenameFaxRequest>(body);
        if (req == null || req.FaxId <= 0 || string.IsNullOrWhiteSpace(req.NewName))
            return FunctionBase.BadRequest("FaxId and NewName are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.RenameFax(conn, req.FaxId, req.NewName);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Fax renamed.");
        }
        catch (Exception ex) { return Err("RenameFax", ex); }
    }

    public async Task<APIGatewayProxyResponse> MoveInboundFaxAsync(string? body)
    {
        var req = Parse<MoveInboundFaxRequest>(body);
        if (req == null || req.FaxIds.Count == 0 || req.ClientId <= 0)
            return FunctionBase.BadRequest("FaxIds and ClientId are required.");

        try
        {
            await using var conn = await OpenAsync();
            foreach (var faxId in req.FaxIds)
            {
                var newPath = $"faxes/clients/{req.ClientId}/{faxId}";
                await using var cmd = QueryHelper.MoveFax(conn, faxId, req.ClientId);
                await cmd.ExecuteNonQueryAsync();
            }
            return FunctionBase.Ok(null, $"{req.FaxIds.Count} faxes moved.");
        }
        catch (Exception ex) { return Err("MoveInboundFax", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetFaxStatusAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0)
            return FunctionBase.BadRequest("FaxId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetSentFaxes(conn, 0, 1);
            await using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
                return FunctionBase.Ok(new FaxResponse
                {
                    Id = reader.GetInt32("id"), FaxNumber = Str(reader, "fax_number"),
                    FileName = Str(reader, "file_name"), Status = Str(reader, "status"),
                    SentAt = DateStr(reader, "created_at"),
                    DeliveredAt = SafeDateStr(reader, "delevered_at")
                });
            return FunctionBase.NotFound("Fax not found.");
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
                items.Add(new
                {
                    id          = reader.GetInt32("id"),
                    filePath    = Str(reader, "file_path"),
                    isRead      = !reader.IsDBNull(reader.GetOrdinal("is_read")) && reader.GetInt32("is_read") == 1,
                    patientName = Str(reader, "patient_name"),
                    accession   = Str(reader, "accession_number"),
                    createdAt   = DateStr(reader, "created_at")
                });
            return FunctionBase.Ok(items);
        }
        catch (Exception ex) { return Err("GetUrgentNotifications", ex); }
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
                (page - 1) * perPage, perPage,
                req.OrderNo, req.FirstName, req.LastName, req.Client, req.IdNumber,
                req.AccNumber, req.Modality, req.Exam, req.ReportText, req.Status);
            await using var reader = await cmd.ExecuteReaderAsync();
            var results = new List<OrderStatusResponse>();
            while (await reader.ReadAsync())
            {
                // Strip HTML from report_text, truncate to 50 chars (matches Laravel)
                var rawReport = Str(reader, "report_text");
                var plainReport = System.Text.RegularExpressions.Regex.Replace(rawReport, "<.*?>", "");
                if (plainReport.Length > 50) plainReport = plainReport.Substring(0, 50) + "…";

                // dob is now DATE_FORMAT'd to 'YYYY-MM-DD' string
                var dobStr = Str(reader, "dob");
                int? age = null;
                if (DateTime.TryParse(dobStr, out var dob))
                    age = (int)((DateTime.Today - dob).TotalDays / 365.25);

                results.Add(new OrderStatusResponse
                {
                    Id              = reader.GetInt32("id"),
                    PatientName     = Str(reader, "patient_name"),
                    FirstName       = Str(reader, "firstname"),
                    LastName        = Str(reader, "lastname"),
                    AccessionNumber = Str(reader, "accession_number"),
                    Modality        = Str(reader, "modality"),
                    Exam            = Str(reader, "exam"),
                    Status          = Str(reader, "status"),
                    Dos             = Str(reader, "dos"),
                    Dob             = dobStr,
                    Age             = age,
                    IdNumber        = Str(reader, "idnumber"),
                    ClientName      = Str(reader, "client_name"),
                    TranscriberName = Str(reader, "transcriber_name"),
                    RadName         = Str(reader, "rad_name"),
                    DotTime         = Str(reader, "dot_time"),
                    DodTime         = Str(reader, "dod_time"),
                    ReportTextSnippet = plainReport,
                    PdfReport       = Str(reader, "pdf_report"),
                    IsDeleted       = !reader.IsDBNull(reader.GetOrdinal("is_deleted")) && reader.GetInt32("is_deleted") == 1,
                });
            }
            return FunctionBase.Ok(results);
        }
        catch (Exception ex) { return Err("GetOrderStatus", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetOrderStatusV2Async(string? body)
        => await GetOrderStatusAsync(body);

    public async Task<APIGatewayProxyResponse> GetAuditDetailOrderStatusAsync(string? body)
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
        catch (Exception ex) { return Err("GetAuditDetailOrderStatus", ex); }
    }

    public async Task<APIGatewayProxyResponse> ReassignTranscriberAsync(string? body)
    {
        var req = Parse<ReassignTranscriberRequest>(body);
        if (req == null || req.StudyId <= 0 || req.TranscriberId <= 0)
            return FunctionBase.BadRequest("StudyId and TranscriberId are required.");

        try
        {
            await using var conn     = await OpenAsync();
            await using var cmd      = QueryHelper.ReassignTranscriber(conn, req.StudyId, req.TranscriberId);
            var affected             = await cmd.ExecuteNonQueryAsync();
            if (affected == 0)
                return FunctionBase.BadRequest("Cannot reassign — study is already finalized.");
            await using var auditCmd = QueryHelper.InsertAudit(conn, req.StudyId, _token.UserId, "Transcriber Reassigned", null);
            await auditCmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Transcriber reassigned.");
        }
        catch (Exception ex) { return Err("ReassignTranscriber", ex); }
    }

    // Mirrors Laravel markAsNewStudy: changes type to 'new study' (blocked for rad final report)
    public async Task<APIGatewayProxyResponse> MarkAsNewStudyAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0) return FunctionBase.BadRequest("StudyId is required.");
        try
        {
            await using var conn = await OpenAsync();
            await using var chk  = new MySqlConnector.MySqlCommand("SELECT type FROM tran_typewordlist WHERE id=@id LIMIT 1", conn);
            chk.Parameters.AddWithValue("@id", req.StudyId);
            await using var chkR = await chk.ExecuteReaderAsync();
            string currentStatus = "";
            if (await chkR.ReadAsync()) currentStatus = chkR.IsDBNull(0) ? "" : chkR.GetString(0);
            await chkR.CloseAsync();
            if (currentStatus == "rad final report")
                return FunctionBase.BadRequest("Cannot recover a finalized report.");
            await using var cmd = new MySqlConnector.MySqlCommand("UPDATE tran_typewordlist SET type='new study' WHERE id=@id", conn);
            cmd.Parameters.AddWithValue("@id", req.StudyId);
            await cmd.ExecuteNonQueryAsync();
            await using var auditCmd = QueryHelper.InsertAudit(conn, req.StudyId, _token.UserId, "Recovered to New Study", null);
            await auditCmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Study recovered.");
        }
        catch (Exception ex) { return Err("MarkAsNewStudy", ex); }
    }

    // Mirrors Laravel getDeleteStore: restores a soft-deleted study
    public async Task<APIGatewayProxyResponse> RestoreStudyAsync(string? body)
    {
        var req = Parse<StudyIdRequest>(body);
        if (req == null || req.StudyId <= 0) return FunctionBase.BadRequest("StudyId is required.");
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = new MySqlConnector.MySqlCommand("UPDATE tran_typewordlist SET `delete`=0 WHERE id=@id", conn);
            cmd.Parameters.AddWithValue("@id", req.StudyId);
            await cmd.ExecuteNonQueryAsync();
            await using var auditCmd = QueryHelper.InsertAudit(conn, req.StudyId, _token.UserId, "Study Restored", null);
            await auditCmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Study restored.");
        }
        catch (Exception ex) { return Err("RestoreStudy", ex); }
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
                // id and status may be stored as VARCHAR in some DB versions
                var idRaw     = reader.GetValue(reader.GetOrdinal("id"));
                var statusRaw = reader.GetValue(reader.GetOrdinal("status"));
                items.Add(new ModalityResponse
                {
                    Id     = idRaw     is int    i ? i : int.TryParse(idRaw?.ToString(),     out var pi)  ? pi  : 0,
                    Name   = Str(reader, "name"),
                    Status = statusRaw is int    s ? s : int.TryParse(statusRaw?.ToString(), out var ps) ? ps : 0,
                });
            }
            return FunctionBase.Ok(items);
        }
        catch (Exception ex) { return Err("GetModalities", ex); }
    }

    public async Task<APIGatewayProxyResponse> ToggleModalityStatusAsync(string? body)
    {
        var req = Parse<ToggleModalityRequest>(body);
        if (req == null || req.ModalityId <= 0)
            return FunctionBase.BadRequest("ModalityId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.ToggleModalityStatusCmd(conn, req.ModalityId);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Modality status toggled.");
        }
        catch (Exception ex) { return Err("ToggleModalityStatus", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // NON-DICOM
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetExamsByModalityAsync(string? body)
    {
        string? mod  = null;
        string? srch = null;
        try
        {
            if (!string.IsNullOrWhiteSpace(body))
            {
                var doc = System.Text.Json.JsonDocument.Parse(body);
                try { mod  = doc.RootElement.GetProperty("modality").GetString(); } catch { }
                try { srch = doc.RootElement.GetProperty("search").GetString();   } catch { }
            }
        }
        catch { }

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetExamsByModality(conn, mod, srch);
            await using var reader = await cmd.ExecuteReaderAsync();
            var list = new List<object>();
            while (await reader.ReadAsync())
            {
                var idVal   = reader.GetValue(reader.GetOrdinal("id"));
                var nameVal = reader.IsDBNull(reader.GetOrdinal("exam")) ? null : reader.GetString("exam");
                if (string.IsNullOrWhiteSpace(nameVal)) continue;
                var id = idVal is int i ? i : int.TryParse(idVal?.ToString(), out var pi) ? pi : 0;
                list.Add(new { id, name = nameVal.Trim() });
            }
            return FunctionBase.Ok(list);
        }
        catch (Exception ex) { return Err("GetExamsByModality", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetNonDicomAccountsAsync(string? body)
    {
        var req     = Parse<GetNonDicomAccountsRequest>(body) ?? new GetNonDicomAccountsRequest();
        var page    = Math.Max(1, req.Page ?? 1);
        var perPage = 20;

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetNonDicomAccounts(conn, req.Search, (page - 1) * perPage, perPage, _token.UserId, _token.UserType);
            await using var reader = await cmd.ExecuteReaderAsync();
            var list = new List<object>();
            while (await reader.ReadAsync())
            {
                list.Add(new
                {
                    id               = SafeInt(reader, "id")               ?? 0,
                    patientName      = StrOpt(reader, "patient_name"),
                    firstName        = StrOpt(reader, "firstname"),
                    lastName         = StrOpt(reader, "lastname"),
                    idNumber         = StrOpt(reader, "idnumber"),
                    dob              = SafeDateStr(reader, "dob"),
                    dos              = SafeDateStr(reader, "dos"),
                    status           = StrOpt(reader, "status"),
                    modality         = StrOpt(reader, "modality"),
                    exam             = StrOpt(reader, "exam"),
                    orderingPhysician= StrOpt(reader, "ordering_physician"),
                    location         = StrOpt(reader, "location"),
                    accessNumber     = StrOpt(reader, "access_number"),
                    radId            = SafeInt(reader, "rad_id"),
                    clientId         = SafeInt(reader, "client_id"),
                    clientName       = StrOpt(reader, "client_name"),
                    transcriberName  = StrOpt(reader, "transcriber_name"),
                });
            }
            return FunctionBase.Ok(list);
        }
        catch (Exception ex) { return Err("GetNonDicomAccounts", ex); }
    }

    public async Task<APIGatewayProxyResponse> StoreNonDicomEntryAsync(string? body)
    {
        var req = Parse<StoreNonDicomEntryRequest>(body);
        if (req == null)
            return FunctionBase.BadRequest("Invalid request.");
        var firstName   = req.FirstName?.Trim()  ?? "";
        var lastName    = req.LastName?.Trim()   ?? "";
        var patientName = string.IsNullOrWhiteSpace(req.PatientName)
            ? $"{firstName} {lastName}".Trim()
            : req.PatientName.Trim();
        if (string.IsNullOrWhiteSpace(patientName))
            return FunctionBase.BadRequest("Patient name is required.");

        try
        {
            await using var conn = await OpenAsync();
            if (req.StudyId.HasValue && req.StudyId > 0)
            {
                await using var cmd = QueryHelper.UpdateStudy(conn, req.StudyId.Value,
                    patientName, null, null, null, req.Dob, req.Dos, req.Modality, req.Description, null, null, null, null, null, null, null);
                await cmd.ExecuteNonQueryAsync();
                return FunctionBase.Ok(new { studyId = req.StudyId }, "Non-DICOM entry updated.");
            }
            else
            {
                await using var cmd = QueryHelper.InsertNonDicomEntry(conn, patientName, firstName, lastName,
                    req.Dob, req.Dos, req.Modality, req.ExamType, req.Description,
                    req.ClientId, req.RadId, req.OrderingPhysician, req.Location, req.AccessNumber, req.IdNumber);
                await cmd.ExecuteNonQueryAsync();
                var newId = cmd.LastInsertedId;
                // Mirror Laravel: set incoming_order_patient_id = incoming_order_exam_id = own id
                await using var upd = QueryHelper.SetNonDicomIncomingIds(conn, newId);
                await upd.ExecuteNonQueryAsync();
                return FunctionBase.Ok(new { studyId = newId }, "Non-DICOM entry created.");
            }
        }
        catch (Exception ex) { return Err("StoreNonDicomEntry", ex); }
    }

    public async Task<APIGatewayProxyResponse> InterpretNonDicomEntryAsync(string? body)
    {
        var req = Parse<InterpretNonDicomEntryRequest>(body);
        if (req == null || req.StudyId <= 0 || req.TransId <= 0)
            return FunctionBase.BadRequest("StudyId and TransId are required.");

        try
        {
            await using var conn = await OpenAsync();

            // 1. Update study: assign transcriber + set type = 'trans new messages'
            await using var upd = QueryHelper.InterpretNonDicomEntry(conn, req.StudyId, req.TransId);
            await upd.ExecuteNonQueryAsync();

            // 2. Save audio attachment if provided (base64 → store path as reference)
            if (!string.IsNullOrWhiteSpace(req.AudioBase64) && !string.IsNullOrWhiteSpace(req.AudioFileName))
            {
                var safeFileName = System.IO.Path.GetFileName(req.AudioFileName);
                var filePath     = $"php/uploads/{safeFileName}";
                await using var ins = QueryHelper.InsertAudioAttachment(conn, req.StudyId, filePath);
                await ins.ExecuteNonQueryAsync();
            }

            return FunctionBase.Ok(new { studyId = req.StudyId }, "Study sent to transcriber.");
        }
        catch (Exception ex) { return Err("InterpretNonDicomEntry", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteNonDicomEntryAsync(string? body)
    {
        var req = Parse<DeleteNonDicomEntryRequest>(body);
        if (req == null || req.StudyId <= 0)
            return FunctionBase.BadRequest("StudyId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.DeleteNonDicomEntry(conn, req.StudyId);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Non-DICOM entry deleted.");
        }
        catch (Exception ex) { return Err("DeleteNonDicomEntry", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetNonDicomEntryAsync(string? body)
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
                return FunctionBase.NotFound("Entry not found.");
            return FunctionBase.Ok(MapStudy(reader));
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
                reports.Add(new StandardReportResponse
                {
                    Id           = SafeInt(reader, "id")         ?? 0,
                    Label        = StrOpt(reader, "label"),
                    ReportText   = StrOpt(reader, "report_text"),
                    RadId        = SafeInt(reader, "rad_id")     ?? 0,
                    RadName      = StrOpt(reader, "rad_name"),
                    ModalityId   = SafeInt(reader, "modality_id") ?? 0,
                    ModalityName = StrOpt(reader, "modality_name"),
                    CreatedAt    = SafeDateStr(reader, "created_at")
                });
            return FunctionBase.Ok(reports);
        }
        catch (Exception ex) { return Err("GetStandardReports", ex); }
    }

    public async Task<APIGatewayProxyResponse> CreateStandardReportAsync(string? body)
    {
        var req = Parse<CreateStandardReportRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.Label) || req.RadId <= 0 || req.ModalityId <= 0)
            return FunctionBase.BadRequest("Label, RadId, and ModalityId are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertStandardReport(conn, req.Label, req.ReportText, req.RadId, req.ModalityId);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(new { id = cmd.LastInsertedId }, "Standard report created.");
        }
        catch (Exception ex) { return Err("CreateStandardReport", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateStandardReportAsync(string? body)
    {
        var req = Parse<UpdateStandardReportRequest>(body);
        if (req == null || req.ReportId <= 0)
            return FunctionBase.BadRequest("ReportId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateStandardReport(conn, req.ReportId, req.Label, req.ReportText, req.RadId, req.ModalityId);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Standard report updated.");
        }
        catch (Exception ex) { return Err("UpdateStandardReport", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteStandardReportAsync(string? body)
    {
        var req = Parse<DeleteStandardReportRequest>(body);
        if (req == null || req.ReportId <= 0)
            return FunctionBase.BadRequest("ReportId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.DeleteStandardReport(conn, req.ReportId);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Standard report deleted.");
        }
        catch (Exception ex) { return Err("DeleteStandardReport", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // SITE MANAGEMENT
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetMiscSettingsAsync()
    {
        try
        {
            await using var conn    = await OpenAsync();
            await using var wmCmd   = QueryHelper.GetSetting(conn, "welcome_message");
            var welcomeMsg          = (await wmCmd.ExecuteScalarAsync())?.ToString();
            await using var logoCmd = QueryHelper.GetSetting(conn, "logo_path");
            var logoPath            = (await logoCmd.ExecuteScalarAsync())?.ToString();
            await using var nameCmd = QueryHelper.GetSetting(conn, "site_name");
            var siteName            = (await nameCmd.ExecuteScalarAsync())?.ToString();
            return FunctionBase.Ok(new MiscSettingsResponse
            {
                WelcomeMessage = welcomeMsg, LogoUrl = logoPath, SiteName = siteName
            });
        }
        catch (Exception ex) { return Err("GetMiscSettings", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateMiscSettingsAsync(string? body)
    {
        var req = Parse<UpdateMiscSettingsRequest>(body);
        if (req == null)
            return FunctionBase.BadRequest("Request body is required.");

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
            return FunctionBase.Ok(null, "Settings updated.");
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
                items.Add(new OsrixUserResponse
                {
                    Id = reader.GetInt32("id"), Username = Str(reader, "username"),
                    RadId = reader.IsDBNull(reader.GetOrdinal("rad_id")) ? 0 : reader.GetInt32("rad_id"),
                    RadName = Str(reader, "rad_name")
                });
            return FunctionBase.Ok(items);
        }
        catch (Exception ex) { return Err("GetOsrix", ex); }
    }

    public async Task<APIGatewayProxyResponse> CreateOsrixUserAsync(string? body)
    {
        var req = Parse<OsrixUserRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.Username) || req.RadId <= 0)
            return FunctionBase.BadRequest("Username and RadId are required.");

        try
        {
            var hash = BCrypt.Net.BCrypt.HashPassword(req.Password);
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertOsrixUser(conn, req.Username, hash, req.RadId);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(new { id = cmd.LastInsertedId }, "Osrix user created.");
        }
        catch (Exception ex) { return Err("CreateOsrixUser", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateOsrixUserAsync(string? body)
    {
        var req = Parse<OsrixUserRequest>(body);
        if (req == null || req.Id <= 0)
            return FunctionBase.BadRequest("Id is required.");

        try
        {
            string? hash = null;
            if (!string.IsNullOrWhiteSpace(req.Password))
                hash = BCrypt.Net.BCrypt.HashPassword(req.Password);
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateOsrixUser(conn, req.Id.Value, req.Username, hash, req.RadId);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Osrix user updated.");
        }
        catch (Exception ex) { return Err("UpdateOsrixUser", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteOsrixUserAsync(string? body)
    {
        var req = Parse<UserIdRequest>(body);
        if (req == null || req.UserId <= 0)
            return FunctionBase.BadRequest("UserId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.DeleteOsrixUser(conn, req.UserId);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Osrix user deleted.");
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
                items.Add(new OsrixInstitutionResponse
                {
                    Id = reader.GetInt32("id"), Name = Str(reader, "name"),
                    Description = Str(reader, "description")
                });
            return FunctionBase.Ok(items);
        }
        catch (Exception ex) { return Err("GetOsrixInstitutions", ex); }
    }

    public async Task<APIGatewayProxyResponse> CreateOsrixInstitutionAsync(string? body)
    {
        var req = Parse<OsrixInstitutionRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.Name))
            return FunctionBase.BadRequest("Name is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertOsrixInstitution(conn, req.Name, req.Description);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(new { id = cmd.LastInsertedId }, "Institution created.");
        }
        catch (Exception ex) { return Err("CreateOsrixInstitution", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateOsrixInstitutionAsync(string? body)
    {
        var req = Parse<OsrixInstitutionRequest>(body);
        if (req == null || req.Id <= 0)
            return FunctionBase.BadRequest("Id is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateOsrixInstitution(conn, req.Id.Value, req.Name, req.Description);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Institution updated.");
        }
        catch (Exception ex) { return Err("UpdateOsrixInstitution", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteOsrixInstitutionAsync(string? body)
    {
        var req = Parse<UserIdRequest>(body);
        if (req == null || req.UserId <= 0)
            return FunctionBase.BadRequest("Id is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.DeleteOsrixInstitution(conn, req.UserId);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Institution deleted.");
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
            var charges          = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, decimal>>(json) ?? new();
            return FunctionBase.Ok(new DefaultChargesResponse { ModalityCharges = charges });
        }
        catch (Exception ex) { return Err("GetDefaultCharges", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateDefaultChargesAsync(string? body)
    {
        var req = Parse<UpdateDefaultChargesRequest>(body);
        if (req == null)
            return FunctionBase.BadRequest("Request body is required.");

        try
        {
            var json             = Newtonsoft.Json.JsonConvert.SerializeObject(req.ModalityCharges);
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpsertSetting(conn, "default_charges", json);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Default charges updated.");
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
            return FunctionBase.Ok(rads);
        }
        catch (Exception ex) { return Err("GetRadiologists", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // QUICKBOOKS — CONNECTION / SETTINGS
    // ══════════════════════════════════════════════════════════════════════════

    private static readonly HttpClient _http = new();

    private static string QbBaseUrl =>
        Environment.GetEnvironmentVariable("QB_BASE_URL") ?? "https://quickbooks.api.intuit.com";

    private static string QbClientId =>
        Environment.GetEnvironmentVariable("QB_CLIENT_ID") ?? string.Empty;

    private static string QbClientSecret =>
        Environment.GetEnvironmentVariable("QB_CLIENT_SECRET") ?? string.Empty;

    /// <summary>
    /// Loads QB credentials from DB, refreshes token if expired, returns (realmId, accessToken).
    /// Returns null if QB is not configured.
    /// </summary>
    private async Task<(string realmId, string accessToken)?> GetQbConnectionAsync()
    {
        await using var conn = await OpenAsync();
        await using var cmd  = QueryHelper.GetQbConnection(conn);
        await using var r    = await cmd.ExecuteReaderAsync();
        if (!await r.ReadAsync()) return null;

        var realmId     = Str(r, "realm_id");
        var accessToken = Str(r, "access_token_value");
        var refreshToken= Str(r, "refresh_token_value");
        var expiresAtStr= Str(r, "access_token_expires_at");
        await r.CloseAsync();

        if (DateTime.TryParse(expiresAtStr, out var expiresAt) && DateTime.UtcNow >= expiresAt)
        {
            var creds   = Convert.ToBase64String(
                System.Text.Encoding.UTF8.GetBytes($"{QbClientId}:{QbClientSecret}"));
            var form    = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string,string>("grant_type",    "refresh_token"),
                new KeyValuePair<string,string>("refresh_token", refreshToken)
            });
            var req = new HttpRequestMessage(HttpMethod.Post,
                "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer")
            {
                Headers = { { "Authorization", $"Basic {creds}" },
                             { "Accept", "application/json" } },
                Content = form
            };
            var resp   = await _http.SendAsync(req);
            var json   = await resp.Content.ReadAsStringAsync();
            var tok    = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(json)!;
            accessToken  = (string)tok.access_token;
            refreshToken = (string)tok.refresh_token;
            var newExpiry= DateTime.UtcNow.AddSeconds((int)tok.expires_in).ToString("yyyy-MM-dd HH:mm:ss");

            await using var upd = QueryHelper.UpdateQbToken(conn, accessToken, refreshToken, newExpiry);
            await upd.ExecuteNonQueryAsync();
        }

        return (realmId, accessToken);
    }

    private async Task<string> QbQueryAsync(string realmId, string accessToken, string query)
    {
        var url  = $"{QbBaseUrl}/v3/company/{realmId}/query?query={Uri.EscapeDataString(query)}&minorversion=65";
        var req  = new HttpRequestMessage(HttpMethod.Get, url);
        req.Headers.Add("Authorization", $"Bearer {accessToken}");
        req.Headers.Add("Accept", "application/json");
        var resp = await _http.SendAsync(req);
        return await resp.Content.ReadAsStringAsync();
    }

    private async Task<string> QbPostAsync(string realmId, string accessToken, string entity, string jsonBody)
    {
        var url  = $"{QbBaseUrl}/v3/company/{realmId}/{entity.ToLower()}?minorversion=65";
        var req  = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = new StringContent(jsonBody, System.Text.Encoding.UTF8, "application/json")
        };
        req.Headers.Add("Authorization", $"Bearer {accessToken}");
        req.Headers.Add("Accept", "application/json");
        var resp = await _http.SendAsync(req);
        return await resp.Content.ReadAsStringAsync();
    }

    public async Task<APIGatewayProxyResponse> GetQbSettingsAsync(string? body)
    {
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.GetQbConnection(conn);
            await using var r    = await cmd.ExecuteReaderAsync();
            if (!await r.ReadAsync())
                return FunctionBase.Ok(new QbSettingsResponse { IsConnected = false });

            return FunctionBase.Ok(new QbSettingsResponse
            {
                IsConnected  = !string.IsNullOrEmpty(Str(r, "realm_id")),
                CompanyName  = Str(r, "company_name"),
                Country      = Str(r, "country"),
                CheckAccount = Str(r, "check_account")
            });
        }
        catch (Exception ex) { return Err("GetQbSettings", ex); }
    }

    public async Task<APIGatewayProxyResponse> SaveQbSettingsAsync(string? body)
    {
        var req = Parse<SaveQbSettingsRequest>(body);
        if (req == null) return FunctionBase.BadRequest("Request body required.");
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateQbSettings(conn, req.CheckAccount);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "QB settings saved.");
        }
        catch (Exception ex) { return Err("SaveQbSettings", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // QUICKBOOKS — MODALITY MAPPINGS
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetQbModalityMappingsAsync(string? body)
    {
        try
        {
            var conn = await GetQbConnectionAsync();
            if (conn == null) return FunctionBase.BadRequest("QuickBooks not connected.");
            var (realmId, accessToken) = conn.Value;

            await using var db = await OpenAsync();

            var mappings = new Dictionary<string, string>();
            await using var mCmd = QueryHelper.GetQbMappings(db, "Modality");
            await using var mR   = await mCmd.ExecuteReaderAsync();
            while (await mR.ReadAsync()) mappings[Str(mR, "system_id")] = Str(mR, "qbo_id");
            await mR.CloseAsync();

            var modalities = new List<string>();
            await using var dCmd = QueryHelper.GetDistinctModalities(db);
            await using var dR   = await dCmd.ExecuteReaderAsync();
            while (await dR.ReadAsync()) modalities.Add(Str(dR, "modality"));
            await dR.CloseAsync();
            modalities.Add("MedicalDirector");

            var qbJson = await QbQueryAsync(realmId, accessToken, "Select * from Item where Type = 'Service' MAXRESULTS 1000");
            var items  = ParseQbItems(qbJson);

            var mapped   = modalities.Where(m => mappings.ContainsKey(m)).ToList();
            var unmapped = modalities.Where(m => !mappings.ContainsKey(m)).ToList();

            return FunctionBase.Ok(new QbModalityMappingResponse
            {
                Mapped = mapped, Unmapped = unmapped, Mappings = mappings, Items = items
            });
        }
        catch (Exception ex) { return Err("GetQbModalityMappings", ex); }
    }

    public async Task<APIGatewayProxyResponse> SaveQbModalityMappingAsync(string? body)
    {
        var req = Parse<SaveQbModalityMappingRequest>(body);
        if (req == null) return FunctionBase.BadRequest("Request body required.");
        try
        {
            await using var db = await OpenAsync();
            foreach (var (modality, qboId) in req.Mappings)
            {
                if (string.IsNullOrEmpty(qboId))
                    await QueryHelper.DeleteQbMapping(db, modality, "Modality", "Item").ExecuteNonQueryAsync();
                else
                    await QueryHelper.UpsertQbMapping(db, modality, "Modality", "Item", qboId).ExecuteNonQueryAsync();
            }
            return FunctionBase.Ok(null, "Modality mappings saved.");
        }
        catch (Exception ex) { return Err("SaveQbModalityMapping", ex); }
    }

    public async Task<APIGatewayProxyResponse> PushQbModalitiesAsync(string? body)
    {
        var req = Parse<PushQbModalitiesRequest>(body);
        if (req == null || req.Modalities.Count == 0)
            return FunctionBase.BadRequest("Modalities list required.");
        try
        {
            var conn = await GetQbConnectionAsync();
            if (conn == null) return FunctionBase.BadRequest("QuickBooks not connected.");
            var (realmId, accessToken) = conn.Value;

            await using var db = await OpenAsync();
            var mappings = new Dictionary<string, string>();
            await using var mCmd = QueryHelper.GetQbMappings(db, "Modality");
            await using var mR   = await mCmd.ExecuteReaderAsync();
            while (await mR.ReadAsync()) mappings[Str(mR, "system_id")] = Str(mR, "qbo_id");
            await mR.CloseAsync();

            var pushed = new List<string>();
            var errors = new List<string>();

            foreach (var modality in req.Modalities)
            {
                if (mappings.TryGetValue(modality, out var existing) &&
                    !string.IsNullOrEmpty(existing) && existing != "InQueue") continue;

                await QueryHelper.UpsertQbMapping(db, modality, "Modality", "Item", "InQueue").ExecuteNonQueryAsync();

                var acctJson = await QbQueryAsync(realmId, accessToken,
                    "Select * from Account where AccountSubType = 'SalesOfProductIncome' MAXRESULTS 1");
                var acctId = ParseFirstId(acctJson, "Account");

                var itemJson = Newtonsoft.Json.JsonConvert.SerializeObject(new
                {
                    Name = modality, Sku = modality, UnitPrice = 1, Taxable = false,
                    Type = "Service",
                    IncomeAccountRef = new { value = acctId }
                });
                var resp   = await QbPostAsync(realmId, accessToken, "item", itemJson);
                var qboId  = ParseCreatedId(resp, "Item");

                if (!string.IsNullOrEmpty(qboId))
                {
                    await QueryHelper.UpsertQbMapping(db, modality, "Modality", "Item", qboId).ExecuteNonQueryAsync();
                    pushed.Add(modality);
                }
                else errors.Add(modality);
            }
            return FunctionBase.Ok(new { pushed, errors });
        }
        catch (Exception ex) { return Err("PushQbModalities", ex); }
    }

    public async Task<APIGatewayProxyResponse> PullQbModalitiesAsync(string? body)
    {
        try
        {
            var conn = await GetQbConnectionAsync();
            if (conn == null) return FunctionBase.BadRequest("QuickBooks not connected.");
            var (realmId, accessToken) = conn.Value;

            await using var db = await OpenAsync();
            var mappings = new Dictionary<string, string>();
            await using var mCmd = QueryHelper.GetQbMappings(db, "Modality");
            await using var mR   = await mCmd.ExecuteReaderAsync();
            while (await mR.ReadAsync()) mappings[Str(mR, "system_id")] = Str(mR, "qbo_id");
            await mR.CloseAsync();

            var json  = await QbQueryAsync(realmId, accessToken, "Select * from Item where Type = 'Service' MAXRESULTS 1000");
            var items = ParseQbItems(json);

            var imported = 0;
            foreach (var item in items)
            {
                if (mappings.ContainsValue(item.Id)) continue;
                await QueryHelper.InsertModality(db, item.Name).ExecuteNonQueryAsync();
                await QueryHelper.UpsertQbMapping(db, item.Name, "Modality", "Item", item.Id).ExecuteNonQueryAsync();
                imported++;
            }
            return FunctionBase.Ok(new { imported });
        }
        catch (Exception ex) { return Err("PullQbModalities", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // QUICKBOOKS — CLIENT (CUSTOMER) MAPPINGS
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetQbClientMappingsAsync(string? body)
    {
        try
        {
            var conn = await GetQbConnectionAsync();
            if (conn == null) return FunctionBase.BadRequest("QuickBooks not connected.");
            var (realmId, accessToken) = conn.Value;

            await using var db = await OpenAsync();
            var mappings = new Dictionary<string, string>();
            await using var mCmd = QueryHelper.GetQbMappings(db, "Client");
            await using var mR   = await mCmd.ExecuteReaderAsync();
            while (await mR.ReadAsync()) mappings[Str(mR, "system_id")] = Str(mR, "qbo_id");
            await mR.CloseAsync();

            await using var cCmd = QueryHelper.GetUsers(db, 4, null);
            await using var cR   = await cCmd.ExecuteReaderAsync();
            var mapped = new List<UserResponse>(); var unmapped = new List<UserResponse>();
            while (await cR.ReadAsync())
            {
                var u = ReadUser(cR);
                (mappings.ContainsKey(u.Id.ToString()) ? mapped : unmapped).Add(u);
            }
            await cR.CloseAsync();

            var qbJson    = await QbQueryAsync(realmId, accessToken, "Select * from Customer MAXRESULTS 1000");
            var customers = ParseQbCustomers(qbJson);

            return FunctionBase.Ok(new QbClientMappingResponse
            {
                MappedClients = mapped, UnmappedClients = unmapped,
                Mappings = mappings, Customers = customers
            });
        }
        catch (Exception ex) { return Err("GetQbClientMappings", ex); }
    }

    public async Task<APIGatewayProxyResponse> SaveQbClientMappingAsync(string? body)
    {
        var req = Parse<SaveQbClientMappingRequest>(body);
        if (req == null) return FunctionBase.BadRequest("Request body required.");
        try
        {
            await using var db = await OpenAsync();
            foreach (var (clientId, qboId) in req.Mappings)
            {
                if (string.IsNullOrEmpty(qboId))
                    await QueryHelper.DeleteQbMapping(db, clientId, "Client", "Customer").ExecuteNonQueryAsync();
                else
                    await QueryHelper.UpsertQbMapping(db, clientId, "Client", "Customer", qboId).ExecuteNonQueryAsync();
            }
            return FunctionBase.Ok(null, "Client mappings saved.");
        }
        catch (Exception ex) { return Err("SaveQbClientMapping", ex); }
    }

    public async Task<APIGatewayProxyResponse> PushQbClientsAsync(string? body)
    {
        var req = Parse<PushQbClientsRequest>(body);
        if (req == null || req.ClientIds.Count == 0)
            return FunctionBase.BadRequest("ClientIds required.");
        try
        {
            var conn = await GetQbConnectionAsync();
            if (conn == null) return FunctionBase.BadRequest("QuickBooks not connected.");
            var (realmId, accessToken) = conn.Value;

            await using var db = await OpenAsync();
            var mappings = new Dictionary<string, string>();
            await using var mCmd = QueryHelper.GetQbMappings(db, "Client");
            await using var mR   = await mCmd.ExecuteReaderAsync();
            while (await mR.ReadAsync()) mappings[Str(mR, "system_id")] = Str(mR, "qbo_id");
            await mR.CloseAsync();

            await using var uCmd = QueryHelper.GetUsersForQbPush(db, req.ClientIds);
            await using var uR   = await uCmd.ExecuteReaderAsync();
            var clients = new List<dynamic>();
            while (await uR.ReadAsync())
                clients.Add(new {
                    id = uR.GetInt32("id"), firstname = Str(uR,"firstname"),
                    middlename = Str(uR,"middlename"), lastname = Str(uR,"lastname"),
                    displayname = Str(uR,"displayname"), email = Str(uR,"email"),
                    phone = Str(uR,"phone"), office = Str(uR,"office"),
                    city = Str(uR,"city_name"), state = Str(uR,"state_name"), country = Str(uR,"country_name")
                });
            await uR.CloseAsync();

            var pushed = new List<int>(); var errors = new List<int>();
            foreach (var c in clients)
            {
                if (mappings.TryGetValue(((int)c.id).ToString(), out var ex2) && !string.IsNullOrEmpty(ex2) && ex2 != "InQueue") continue;
                await QueryHelper.UpsertQbMapping(db, ((int)c.id).ToString(), "Client", "Customer", "InQueue").ExecuteNonQueryAsync();

                var customerJson = Newtonsoft.Json.JsonConvert.SerializeObject(new
                {
                    GivenName = c.firstname, MiddleName = c.middlename, FamilyName = c.lastname,
                    DisplayName = c.displayname,
                    PrimaryEmailAddr = new { Address = c.email },
                    PrimaryPhone = string.IsNullOrEmpty((string)c.phone) ? null : new { FreeFormNumber = ((string)c.phone)[..Math.Min(20, ((string)c.phone).Length)] },
                    BillAddr = new { Line1 = c.office, City = c.city, CountrySubDivisionCode = c.state, Country = c.country }
                });
                var resp  = await QbPostAsync(realmId, accessToken, "customer", customerJson);
                var qboId = ParseCreatedId(resp, "Customer");

                if (!string.IsNullOrEmpty(qboId))
                {
                    await QueryHelper.UpsertQbMapping(db, ((int)c.id).ToString(), "Client", "Customer", qboId).ExecuteNonQueryAsync();
                    pushed.Add((int)c.id);
                }
                else errors.Add((int)c.id);
            }
            return FunctionBase.Ok(new { pushed, errors });
        }
        catch (Exception ex) { return Err("PushQbClients", ex); }
    }

    public async Task<APIGatewayProxyResponse> PullQbClientsAsync(string? body)
    {
        try
        {
            var conn = await GetQbConnectionAsync();
            if (conn == null) return FunctionBase.BadRequest("QuickBooks not connected.");
            var (realmId, accessToken) = conn.Value;

            await using var db = await OpenAsync();
            var mappings = new Dictionary<string, string>();
            await using var mCmd = QueryHelper.GetQbMappings(db, "Client");
            await using var mR   = await mCmd.ExecuteReaderAsync();
            while (await mR.ReadAsync()) mappings[Str(mR, "system_id")] = Str(mR, "qbo_id");
            await mR.CloseAsync();

            var json      = await QbQueryAsync(realmId, accessToken,
                "Select * from Customer ORDERBY Id ASC STARTPOSITION 1 MAXRESULTS 1000");
            var customers = ParseQbCustomers(json);

            var imported = 0;
            foreach (var c in customers)
            {
                if (mappings.ContainsValue(c.Id)) continue;
                var (coId, stId, ciId) = await ResolveLocationAsync(db, null, null, null);
                await using var ins = QueryHelper.InsertUserFromQb(db,
                    c.DisplayName, "", "", c.DisplayName, "", "", 4, coId, stId, ciId);
                var newId = Convert.ToInt32(await ins.ExecuteScalarAsync());
                await QueryHelper.UpsertQbMapping(db, newId.ToString(), "Client", "Customer", c.Id).ExecuteNonQueryAsync();
                imported++;
            }
            return FunctionBase.Ok(new { imported });
        }
        catch (Exception ex) { return Err("PullQbClients", ex); }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // QUICKBOOKS — TRANSCRIBER (VENDOR) MAPPINGS
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> GetQbTranscriberMappingsAsync(string? body)
    {
        try
        {
            var conn = await GetQbConnectionAsync();
            if (conn == null) return FunctionBase.BadRequest("QuickBooks not connected.");
            var (realmId, accessToken) = conn.Value;

            await using var db = await OpenAsync();
            var mappings = new Dictionary<string, string>();
            await using var mCmd = QueryHelper.GetQbMappings(db, "Transcriber");
            await using var mR   = await mCmd.ExecuteReaderAsync();
            while (await mR.ReadAsync()) mappings[Str(mR, "system_id")] = Str(mR, "qbo_id");
            await mR.CloseAsync();

            await using var tCmd = QueryHelper.GetUsers(db, 3, null);
            await using var tR   = await tCmd.ExecuteReaderAsync();
            var mapped = new List<UserResponse>(); var unmapped = new List<UserResponse>();
            while (await tR.ReadAsync())
            {
                var u = ReadUser(tR);
                (mappings.ContainsKey(u.Id.ToString()) ? mapped : unmapped).Add(u);
            }
            await tR.CloseAsync();

            var qbJson  = await QbQueryAsync(realmId, accessToken, "Select Id,DisplayName from Vendor MAXRESULTS 1000");
            var vendors = ParseQbVendors(qbJson);

            return FunctionBase.Ok(new QbTranscriberMappingResponse
            {
                MappedTranscribers = mapped, UnmappedTranscribers = unmapped,
                Mappings = mappings, Vendors = vendors
            });
        }
        catch (Exception ex) { return Err("GetQbTranscriberMappings", ex); }
    }

    public async Task<APIGatewayProxyResponse> SaveQbTranscriberMappingAsync(string? body)
    {
        var req = Parse<SaveQbTranscriberMappingRequest>(body);
        if (req == null) return FunctionBase.BadRequest("Request body required.");
        try
        {
            await using var db = await OpenAsync();
            foreach (var (transId, qboId) in req.Mappings)
            {
                if (string.IsNullOrEmpty(qboId))
                    await QueryHelper.DeleteQbMapping(db, transId, "Transcriber", "Vendor").ExecuteNonQueryAsync();
                else
                    await QueryHelper.UpsertQbMapping(db, transId, "Transcriber", "Vendor", qboId).ExecuteNonQueryAsync();
            }
            return FunctionBase.Ok(null, "Transcriber mappings saved.");
        }
        catch (Exception ex) { return Err("SaveQbTranscriberMapping", ex); }
    }

    public async Task<APIGatewayProxyResponse> PushQbTranscribersAsync(string? body)
    {
        var req = Parse<PushQbTranscribersRequest>(body);
        if (req == null || req.TranscriberIds.Count == 0)
            return FunctionBase.BadRequest("TranscriberIds required.");
        try
        {
            var conn = await GetQbConnectionAsync();
            if (conn == null) return FunctionBase.BadRequest("QuickBooks not connected.");
            var (realmId, accessToken) = conn.Value;

            await using var db = await OpenAsync();
            var mappings = new Dictionary<string, string>();
            await using var mCmd = QueryHelper.GetQbMappings(db, "Transcriber");
            await using var mR   = await mCmd.ExecuteReaderAsync();
            while (await mR.ReadAsync()) mappings[Str(mR, "system_id")] = Str(mR, "qbo_id");
            await mR.CloseAsync();

            await using var uCmd = QueryHelper.GetUsersForQbPush(db, req.TranscriberIds);
            await using var uR   = await uCmd.ExecuteReaderAsync();
            var transcribers = new List<dynamic>();
            while (await uR.ReadAsync())
                transcribers.Add(new {
                    id = uR.GetInt32("id"), firstname = Str(uR,"firstname"),
                    middlename = Str(uR,"middlename"), lastname = Str(uR,"lastname"),
                    displayname = Str(uR,"displayname"), email = Str(uR,"email"),
                    phone = Str(uR,"phone"), office = Str(uR,"office"),
                    city = Str(uR,"city_name"), state = Str(uR,"state_name"), country = Str(uR,"country_name")
                });
            await uR.CloseAsync();

            var pushed = new List<int>(); var errors = new List<int>();
            foreach (var t in transcribers)
            {
                if (mappings.TryGetValue(((int)t.id).ToString(), out var ex2) && !string.IsNullOrEmpty(ex2) && ex2 != "InQueue") continue;
                await QueryHelper.UpsertQbMapping(db, ((int)t.id).ToString(), "Transcriber", "Vendor", "InQueue").ExecuteNonQueryAsync();

                var email = (string)t.email;
                var vendorJson = Newtonsoft.Json.JsonConvert.SerializeObject(new
                {
                    GivenName = t.firstname, MiddleName = t.middlename, FamilyName = t.lastname,
                    DisplayName = t.displayname,
                    PrimaryEmailAddr = System.Text.RegularExpressions.Regex.IsMatch(email, @"^[^@\s]+@[^@\s]+\.[^@\s]+$")
                        ? new { Address = email } : null,
                    PrimaryPhone = string.IsNullOrEmpty((string)t.phone) ? null : new { FreeFormNumber = ((string)t.phone)[..Math.Min(20,((string)t.phone).Length)] },
                    BillAddr = new { Line1 = t.office, City = t.city, CountrySubDivisionCode = t.state, Country = t.country }
                });
                var resp  = await QbPostAsync(realmId, accessToken, "vendor", vendorJson);
                var qboId = ParseCreatedId(resp, "Vendor");

                if (!string.IsNullOrEmpty(qboId))
                {
                    await QueryHelper.UpsertQbMapping(db, ((int)t.id).ToString(), "Transcriber", "Vendor", qboId).ExecuteNonQueryAsync();
                    pushed.Add((int)t.id);
                }
                else errors.Add((int)t.id);
            }
            return FunctionBase.Ok(new { pushed, errors });
        }
        catch (Exception ex) { return Err("PushQbTranscribers", ex); }
    }

    public async Task<APIGatewayProxyResponse> PullQbTranscribersAsync(string? body)
    {
        try
        {
            var conn = await GetQbConnectionAsync();
            if (conn == null) return FunctionBase.BadRequest("QuickBooks not connected.");
            var (realmId, accessToken) = conn.Value;

            await using var db = await OpenAsync();
            var mappings = new Dictionary<string, string>();
            await using var mCmd = QueryHelper.GetQbMappings(db, "Transcriber");
            await using var mR   = await mCmd.ExecuteReaderAsync();
            while (await mR.ReadAsync()) mappings[Str(mR, "system_id")] = Str(mR, "qbo_id");
            await mR.CloseAsync();

            var json    = await QbQueryAsync(realmId, accessToken,
                "Select * from Vendor ORDERBY Id ASC STARTPOSITION 1 MAXRESULTS 1000");
            var vendors = ParseQbVendors(json);

            var imported = 0;
            foreach (var v in vendors)
            {
                if (mappings.ContainsValue(v.Id)) continue;
                await using var ins = QueryHelper.InsertUserFromQb(db,
                    v.DisplayName, "", "", v.DisplayName, "", "", 3, 0, 0, 0);
                var newId = Convert.ToInt32(await ins.ExecuteScalarAsync());
                await QueryHelper.UpsertQbMapping(db, newId.ToString(), "Transcriber", "Vendor", v.Id).ExecuteNonQueryAsync();
                imported++;
            }
            return FunctionBase.Ok(new { imported });
        }
        catch (Exception ex) { return Err("PullQbTranscribers", ex); }
    }

    // ── QB response parsing helpers ───────────────────────────────────────────

    private static List<QbItemResponse> ParseQbItems(string json)
    {
        var list = new List<QbItemResponse>();
        try
        {
            var d = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(json);
            var items = d?.QueryResponse?.Item;
            if (items == null) return list;
            foreach (var item in items)
                list.Add(new QbItemResponse { Id = (string)item.Id, Name = (string)item.Name });
        }
        catch { /* empty on parse error */ }
        return list;
    }

    private static List<QbCustomerResponse> ParseQbCustomers(string json)
    {
        var list = new List<QbCustomerResponse>();
        try
        {
            var d = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(json);
            var customers = d?.QueryResponse?.Customer;
            if (customers == null) return list;
            foreach (var c in customers)
                list.Add(new QbCustomerResponse { Id = (string)c.Id, DisplayName = (string)c.DisplayName });
        }
        catch { }
        return list;
    }

    private static List<QbVendorResponse> ParseQbVendors(string json)
    {
        var list = new List<QbVendorResponse>();
        try
        {
            var d = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(json);
            var vendors = d?.QueryResponse?.Vendor;
            if (vendors == null) return list;
            foreach (var v in vendors)
                list.Add(new QbVendorResponse { Id = (string)v.Id, DisplayName = (string)v.DisplayName });
        }
        catch { }
        return list;
    }

    private static string ParseFirstId(string json, string entity)
    {
        try
        {
            var d    = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(json);
            var node = d?.QueryResponse?[entity];
            if (node != null && node.Count > 0) return (string)node[0].Id;
        }
        catch { }
        return string.Empty;
    }

    private static string ParseCreatedId(string json, string entity)
    {
        try
        {
            var d = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(json);
            return (string)(d?[entity]?.Id ?? string.Empty);
        }
        catch { return string.Empty; }
    }

    private async Task<(int countryId, int stateId, int cityId)> ResolveLocationAsync(
        MySqlConnection db, string? country, string? state, string? city)
    {
        int coId = 0, stId = 0, ciId = 0;
        if (!string.IsNullOrEmpty(country))
        {
            var c = await QueryHelper.FindCountryByName(db, country).ExecuteScalarAsync();
            if (c != null) coId = Convert.ToInt32(c);
        }
        if (coId > 0 && !string.IsNullOrEmpty(state))
        {
            var s = await QueryHelper.FindStateByName(db, state, coId).ExecuteScalarAsync();
            if (s != null) stId = Convert.ToInt32(s);
        }
        if (stId > 0 && !string.IsNullOrEmpty(city))
        {
            var ci = await QueryHelper.FindCityByName(db, city, stId).ExecuteScalarAsync();
            if (ci != null) ciId = Convert.ToInt32(ci);
        }
        return (coId, stId, ciId);
    }
}
