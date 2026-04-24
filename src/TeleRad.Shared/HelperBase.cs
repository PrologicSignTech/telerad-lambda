using Amazon.Lambda.APIGatewayEvents;
using Amazon.S3;
using MySqlConnector;
using Newtonsoft.Json;
using TeleRad.Shared.Model;
using TeleRad.Shared.Utility;

namespace TeleRad.Shared;

public abstract class HelperBase
{
    protected readonly string _cs;
    protected readonly TokenValidationResult _token;

    protected HelperBase(string connectionString, TokenValidationResult? token = null)
    {
        _cs    = connectionString;
        _token = token ?? new TokenValidationResult();
    }

    // ── DB ────────────────────────────────────────────────────────────────────

    protected async Task<MySqlConnection> OpenAsync()
    {
        var conn = new MySqlConnection(_cs);
        await conn.OpenAsync();
        return conn;
    }

    // ── Parsing ───────────────────────────────────────────────────────────────

    protected static T? Parse<T>(string? json) where T : class
    {
        if (string.IsNullOrWhiteSpace(json)) return null;
        try { return JsonConvert.DeserializeObject<T>(json); }
        catch { return null; }
    }

    // ── Reader helpers ────────────────────────────────────────────────────────

    protected static string Str(MySqlDataReader r, string col)
    {
        var o = r.GetOrdinal(col);
        return r.IsDBNull(o) ? string.Empty : r.GetString(o);
    }

    /// <summary>Safe read — returns empty string when the column is not present in the result set.</summary>
    protected static string StrOpt(MySqlDataReader r, string col)
    {
        try { var o = r.GetOrdinal(col); return r.IsDBNull(o) ? string.Empty : r.GetString(o); }
        catch { return string.Empty; }
    }

    protected static int? SafeInt(MySqlDataReader r, string col)
    {
        try
        {
            var o = r.GetOrdinal(col);
            if (r.IsDBNull(o)) return null;
            var val = r.GetValue(o);
            if (val is int i)     return i;
            if (val is long l)    return (int)l;
            if (val is uint ui)   return (int)ui;
            if (val is ulong ul)  return (int)ul;
            if (val is short s)   return (int)s;
            if (val is ushort us) return (int)us;
            if (val is byte b)    return (int)b;
            if (val is sbyte sb)  return (int)sb;
            return int.TryParse(val?.ToString(), out var parsed) ? parsed : (int?)null;
        }
        catch { return null; }
    }

    private static readonly string[] _dateFormats =
    {
        "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm", "yyyy-MM-dd",
        "MM-dd-yyyy HH:mm:ss", "MM-dd-yyyy HH:mm", "MM-dd-yyyy",
        "MM/dd/yyyy HH:mm:ss", "MM/dd/yyyy HH:mm", "MM/dd/yyyy"
    };

    protected static string SafeDateStr(MySqlDataReader r, string col)
    {
        try
        {
            var o = r.GetOrdinal(col);
            if (r.IsDBNull(o)) return string.Empty;
            // Try native DateTime first (works for proper DATETIME columns)
            try { return r.GetDateTime(o).ToString("yyyy-MM-dd HH:mm:ss"); }
            catch { /* fall through to string parse */ }
            // Column is stored as varchar — parse with known formats
            var raw = r.GetString(o)?.Trim();
            if (string.IsNullOrEmpty(raw)) return string.Empty;
            if (DateTime.TryParseExact(raw, _dateFormats,
                    System.Globalization.CultureInfo.InvariantCulture,
                    System.Globalization.DateTimeStyles.None, out var dt))
                return dt.ToString("yyyy-MM-dd HH:mm:ss");
            return raw; // return as-is if unparseable
        }
        catch { return string.Empty; }
    }

    protected static string DateStr(MySqlDataReader r, string col) => SafeDateStr(r, col);

    // ── Mappers ───────────────────────────────────────────────────────────────

    protected static StudyResponse MapStudy(MySqlDataReader r)
        => new()
        {
            Id                = r.GetInt32("id"),
            PatientName       = StrOpt(r, "patient_name"),
            PatientFirstName  = StrOpt(r, "patient_first_name") ?? StrOpt(r, "patient_firstname"),
            PatientLastName   = StrOpt(r, "patient_last_name")  ?? StrOpt(r, "patient_lastname"),
            PatientId         = StrOpt(r, "patient_id"),
            PatientDob        = SafeDateStr(r, "patient_dob"),
            AccessionNumber   = StrOpt(r, "accession_number"),
            OrderNumber       = StrOpt(r, "order_number"),
            Modality          = StrOpt(r, "modality"),
            Exam              = StrOpt(r, "exam"),
            OrderingPhysician = StrOpt(r, "ordering_physician"),
            Status            = StrOpt(r, "status"),
            Dos               = SafeDateStr(r, "dos"),
            Sid               = StrOpt(r, "sid"),
            Client            = StrOpt(r, "client") ?? StrOpt(r, "client_name"),
            ClientName        = StrOpt(r, "client_name") ?? StrOpt(r, "client"),
            ClientUsername    = StrOpt(r, "client_username") ?? string.Empty,
            HeaderImage       = PresignedUrl(StrOpt(r, "header_image")),
            HeadingText       = StrOpt(r, "heading_text"),
            RadName           = StrOpt(r, "rad_name"),
            RadSignature      = StrOpt(r, "rad_signature"),
            ReportText        = StrOpt(r, "report_text"),
            ImpressionText    = StrOpt(r, "impression_text"),
            PdfPage           = SafeInt(r, "pdf_page") is int pdfP && pdfP > 0 ? pdfP : 1,
            ClientAddress     = StrOpt(r, "client_address"),
            ClientPhone       = StrOpt(r, "client_phone"),
            ClientFax         = StrOpt(r, "client_fax"),
            ReferrerName      = StrOpt(r, "referrer_name"),
            TranscriberName   = StrOpt(r, "transcriber_name"),
            RadId             = SafeInt(r, "rad_id"),
            TranscriberId     = SafeInt(r, "transcriber_id"),
            IsStat            = SafeInt(r, "is_stat") == 1,
            Dod               = SafeDateStr(r, "dod"),
            Dot               = SafeDateStr(r, "dot"),
            CreatedAt         = SafeDateStr(r, "created_at"),
            UpdatedAt         = SafeDateStr(r, "updated_at")
        };

    protected static async Task<List<StudyResponse>> ReadStudies(MySqlCommand cmd)
    {
        var list = new List<StudyResponse>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) list.Add(MapStudy(reader));
        return list;
    }

    protected static UserResponse ReadUser(MySqlDataReader r)
    {
        var schema  = r.GetColumnSchema();
        bool HasCol(string n) => schema.Any(c => c.ColumnName.Equals(n, StringComparison.OrdinalIgnoreCase));
        string? OptStr(string n) => HasCol(n) ? (r.IsDBNull(r.GetOrdinal(n)) ? null : r.GetString(n)) : null;
        return new()
        {
            Id        = r.GetInt32("id"),
            Name      = Str(r, "name"),
            Email     = Str(r, "email"),
            Username  = Str(r, "username"),
            FirstName = OptStr("firstname"),
            LastName  = OptStr("lastname"),
            UserType  = r.GetInt32("usertype"),
            Status    = r.GetInt32("status"),
            Phone     = OptStr("phone"),
            Fax       = OptStr("fax"),
            Address   = OptStr("address"),
        };
    }

    protected static async Task<List<TemplateResponse>> ReadTemplates(MySqlDataReader reader)
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

    // ── S3 Presigned URL (fast — no existence check, suitable for list views) ──

    private static readonly string _s3Bucket =
        Environment.GetEnvironmentVariable("S3_BUCKET")
        ?? Environment.GetEnvironmentVariable("AWS_BUCKET")
        ?? "rispacs-static-content";

    /// <summary>
    /// Generates a 30-min presigned URL without an S3 existence check.
    /// Returns empty string for blank/null keys.
    /// </summary>
    protected static string PresignedUrl(string? s3Key, int expiryMinutes = 30)
    {
        if (string.IsNullOrWhiteSpace(s3Key)) return string.Empty;
        try
        {
            using var s3  = new Amazon.S3.AmazonS3Client();
            var cleanKey  = s3Key.TrimStart('/');
            var req = new Amazon.S3.Model.GetPreSignedUrlRequest
            {
                BucketName = _s3Bucket,
                Key        = cleanKey,
                Expires    = DateTime.UtcNow.AddMinutes(expiryMinutes),
                Verb       = Amazon.S3.HttpVerb.GET,
            };
            return s3.GetPreSignedURL(req);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[PresignedUrl] Error for key '{s3Key}': {ex.Message}");
            return string.Empty;
        }
    }

    // ── Token ─────────────────────────────────────────────────────────────────

    protected static string GenerateToken()
        => Convert.ToBase64String(Guid.NewGuid().ToByteArray())
           + Convert.ToBase64String(Guid.NewGuid().ToByteArray());

    protected static string GenerateRandomPassword()
    {
        const string chars = "ABCDEFGHJKMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789!@#$";
        var rng = new Random();
        return new string(Enumerable.Repeat(chars, 10).Select(s => s[rng.Next(s.Length)]).ToArray());
    }

    // ── Date range resolver ───────────────────────────────────────────────────

    /// <summary>
    /// Converts named ranges (Today, ThisWeek, ThisMonth, LastMonth, ThisYear)
    /// or pipe-separated "YYYY-MM-DD|YYYY-MM-DD" into (dateFrom, dateTo) strings
    /// suitable for MySQL DATE comparisons.
    /// </summary>
    protected static (string? df, string? dt) ResolveDateRange(string? dateRange)
    {
        if (string.IsNullOrWhiteSpace(dateRange)) return (null, null);

        var today = DateTime.UtcNow.Date;
        return dateRange.Trim().ToLower() switch
        {
            "today"     => (today.ToString("yyyy-MM-dd"), today.ToString("yyyy-MM-dd")),
            "yesterday" => (today.AddDays(-1).ToString("yyyy-MM-dd"), today.AddDays(-1).ToString("yyyy-MM-dd")),
            "thisweek"  => (today.AddDays(-(int)today.DayOfWeek).ToString("yyyy-MM-dd"), today.ToString("yyyy-MM-dd")),
            "lastweek"  => (today.AddDays(-(int)today.DayOfWeek - 7).ToString("yyyy-MM-dd"),
                            today.AddDays(-(int)today.DayOfWeek - 1).ToString("yyyy-MM-dd")),
            "thismonth" => (new DateTime(today.Year, today.Month, 1).ToString("yyyy-MM-dd"), today.ToString("yyyy-MM-dd")),
            "lastmonth" => (new DateTime(today.Year, today.Month, 1).AddMonths(-1).ToString("yyyy-MM-dd"),
                            new DateTime(today.Year, today.Month, 1).AddDays(-1).ToString("yyyy-MM-dd")),
            "thisyear"  => (new DateTime(today.Year, 1, 1).ToString("yyyy-MM-dd"), today.ToString("yyyy-MM-dd")),
            _ => dateRange.Contains('|')
                    ? (dateRange.Split('|')[0].Trim(), dateRange.Split('|')[1].Trim())
                    : (null, null)
        };
    }

    // ── Error ─────────────────────────────────────────────────────────────────

    protected static APIGatewayProxyResponse Err(string method, Exception ex)
    {
        Console.WriteLine($"[TeleRad] {method} error: {ex}");
        return FunctionBase.ServerError(ex.Message);
    }
}
