namespace TeleRad.Shared.Model;

// ── Generic envelope ─────────────────────────────────────────────────────────
public class ApiResponse
{
    public int StatusCode { get; set; }
    public string StatusMessage { get; set; } = string.Empty;
    public object? Data { get; set; }
}

public class TokenValidationResult
{
    public bool IsValid { get; set; }
    public int UserId { get; set; }
    public int UserType { get; set; }
    public string Username { get; set; } = string.Empty;
}

// ── Auth ──────────────────────────────────────────────────────────────────────
public class LoginResponse
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public int UserType { get; set; }
    public string Token { get; set; } = string.Empty;
    public List<string> Permissions { get; set; } = new();
}

// ── Study ─────────────────────────────────────────────────────────────────────
public class StudyResponse
{
    public int Id { get; set; }
    public string PatientName { get; set; } = string.Empty;
    public string PatientFirstName { get; set; } = string.Empty;
    public string PatientLastName { get; set; } = string.Empty;
    public string PatientId { get; set; } = string.Empty;       // idnumber
    public string PatientDob { get; set; } = string.Empty;      // dob
    public string AccessionNumber { get; set; } = string.Empty;
    public string OrderNumber { get; set; } = string.Empty;
    public string Modality { get; set; } = string.Empty;
    public string Exam { get; set; } = string.Empty;            // description (procedure)
    public string OrderingPhysician { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public string Dos { get; set; } = string.Empty;
    public string Sid { get; set; } = string.Empty;    // DICOM study instance UID
    public string Client { get; set; } = string.Empty; // = ClientName, matches Laravel 'clientname'
    public string ClientName { get; set; } = string.Empty;
    public string ClientUsername { get; set; } = string.Empty; // referrer username — used for fax lookup
    public string HeaderImage { get; set; } = string.Empty;   // presigned S3 URL of template header logo
    public string HeadingText { get; set; } = string.Empty;   // template heading/client name text
    public string RadSignature { get; set; } = string.Empty;  // radiologist signature text
    public string ClientAddress { get; set; } = string.Empty;
    public string ClientPhone { get; set; } = string.Empty;
    public string ClientFax { get; set; } = string.Empty;
    public string ReferrerName { get; set; } = string.Empty;
    public string RadName { get; set; } = string.Empty;
    public string TranscriberName { get; set; } = string.Empty;
    public int? RadId { get; set; }
    public int? TranscriberId { get; set; }
    public int? TemplateId { get; set; }
    public bool IsStat { get; set; }
    public bool DictationReceived { get; set; }
    public int AttachedDocuments { get; set; }
    public string Dot { get; set; } = string.Empty;
    public string Dod { get; set; } = string.Empty;
    public string CreatedAt { get; set; } = string.Empty;
    public string UpdatedAt { get; set; } = string.Empty;
}

// ── Note ──────────────────────────────────────────────────────────────────────
public class NoteResponse
{
    public int Id { get; set; }
    public string Username { get; set; } = string.Empty;
    public string Notes { get; set; } = string.Empty;
    public string Date { get; set; } = string.Empty;
}

// ── Template ──────────────────────────────────────────────────────────────────
public class TemplateResponse
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string BodyText { get; set; } = string.Empty;
    public string? Modality { get; set; }
    public int? UserId { get; set; }
    public string? UserName { get; set; }
    public string CreatedAt { get; set; } = string.Empty;
}

// ── User ──────────────────────────────────────────────────────────────────────
public class UserResponse
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
    public int UserType { get; set; }
    public int Status { get; set; }
    public string? Phone { get; set; }
    public string? Fax { get; set; }
    public string? Address { get; set; }
    public string? Role { get; set; }
    public List<string> Permissions { get; set; } = new();
}

// ── Modality ──────────────────────────────────────────────────────────────────
public class ModalityResponse
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Status { get; set; }
}

// ── Attachment ────────────────────────────────────────────────────────────────
public class AttachmentResponse
{
    public int Id { get; set; }
    public int StudyId { get; set; }
    public string FileName { get; set; } = string.Empty;
    public string FileType { get; set; } = string.Empty;
    public string FilePath { get; set; } = string.Empty;
    public string CreatedAt { get; set; } = string.Empty;
}

// ── Audit ─────────────────────────────────────────────────────────────────────
public class AuditResponse
{
    public int Id { get; set; }
    public int StudyId { get; set; }
    public string Action { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public string CreatedAt { get; set; } = string.Empty;
}

// ── Fax ───────────────────────────────────────────────────────────────────────
public class FaxResponse
{
    public int Id { get; set; }
    public string FaxNumber { get; set; } = string.Empty;
    public string FileName { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public string SentAt { get; set; } = string.Empty;
    public string StatusUrl { get; set; } = string.Empty;
}

public class InboundFaxResponse
{
    public int Id { get; set; }
    public string FileName { get; set; } = string.Empty;
    public string FileUrl { get; set; } = string.Empty;
    public string ReceivedAt { get; set; } = string.Empty;
    public bool IsRead { get; set; }
    public string? ClientUsername { get; set; }
}

// ── Final Report ──────────────────────────────────────────────────────────────
public class FinalReportResponse
{
    public int Id { get; set; }
    public string PatientName { get; set; } = string.Empty;
    public string Modality { get; set; } = string.Empty;
    public string Dos { get; set; } = string.Empty;
    public string ReportText { get; set; } = string.Empty;
    public string Impression { get; set; } = string.Empty;
    public string RadName { get; set; } = string.Empty;
    public string ClientName { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public string PdfUrl { get; set; } = string.Empty;
    public string UpdatedAt { get; set; } = string.Empty;
}

// ── Chart ─────────────────────────────────────────────────────────────────────
public class ChartResponse
{
    public decimal TotalRecords { get; set; }
    public List<ChartDataItem> ChartData { get; set; } = new();
}

public class ChartDataItem
{
    public string Label { get; set; } = string.Empty;
    public decimal Total { get; set; }
}

// ── Invoice ───────────────────────────────────────────────────────────────────
public class InvoiceResponse
{
    public int Id { get; set; }
    public string InvoiceNumber { get; set; } = string.Empty;
    public string DateFrom { get; set; } = string.Empty;
    public string DateTo { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public string Status { get; set; } = string.Empty;
    public string CreatedAt { get; set; } = string.Empty;
    public List<InvoiceLineItemResponse> LineItems { get; set; } = new();
}

public class InvoiceLineItemResponse
{
    public string Modality { get; set; } = string.Empty;
    public int Count { get; set; }
    public decimal UnitPrice { get; set; }
    public decimal Total { get; set; }
}

// ── Price Management ──────────────────────────────────────────────────────────
public class PriceManagementResponse
{
    public int UserId { get; set; }
    public string UserName { get; set; } = string.Empty;
    public string UserCategory { get; set; } = string.Empty;
    public List<ModalityPriceResponse> ModalityPrices { get; set; } = new();
    public decimal? PricePerPage { get; set; }
    public decimal? PricePerChar { get; set; }
}

public class ModalityPriceResponse
{
    public string Modality { get; set; } = string.Empty;
    public decimal Price { get; set; }
}

// ── Order Status ──────────────────────────────────────────────────────────────
public class OrderStatusResponse
{
    public int Id { get; set; }
    public string PatientName { get; set; } = string.Empty;
    public string AccessionNumber { get; set; } = string.Empty;
    public string Modality { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public string Dos { get; set; } = string.Empty;
    public string ClientName { get; set; } = string.Empty;
    public string TranscriberName { get; set; } = string.Empty;
    public string RadName { get; set; } = string.Empty;
    public string LastAuditAction { get; set; } = string.Empty;
    public string LastAuditAt { get; set; } = string.Empty;
}

// ── Standard Report ───────────────────────────────────────────────────────────
public class StandardReportResponse
{
    public int Id { get; set; }
    public string Label { get; set; } = string.Empty;
    public string ReportText { get; set; } = string.Empty;
    public int RadId { get; set; }
    public string RadName { get; set; } = string.Empty;
    public int ModalityId { get; set; }
    public string ModalityName { get; set; } = string.Empty;
    public string CreatedAt { get; set; } = string.Empty;
}

// ── Role ──────────────────────────────────────────────────────────────────────
public class RoleResponse
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public List<string> Permissions { get; set; } = new();
}

// ── Site Management ───────────────────────────────────────────────────────────
public class MiscSettingsResponse
{
    public string? WelcomeMessage { get; set; }
    public string? LogoUrl { get; set; }
    public string? SiteName { get; set; }
}

public class OsrixUserResponse
{
    public int Id { get; set; }
    public string Username { get; set; } = string.Empty;
    public int RadId { get; set; }
    public string RadName { get; set; } = string.Empty;
}

public class OsrixInstitutionResponse
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? Description { get; set; }
}

public class DefaultChargesResponse
{
    public Dictionary<string, decimal> ModalityCharges { get; set; } = new();
}

// ── Pagination wrapper ────────────────────────────────────────────────────────
public class PaginatedResponse<T>
{
    public List<T> Data { get; set; } = new();
    public int Total { get; set; }
    public int Page { get; set; }
    public int PerPage { get; set; }
    public int LastPage => PerPage > 0 ? (int)Math.Ceiling((double)Total / PerPage) : 1;
}

// ── DataTables envelope (mirrors Laravel getAllCountries/States/Cities) ────────
public class DatatableResponse<T>
{
    public int Draw            { get; set; }
    public int RecordsTotal    { get; set; }
    public int RecordsFiltered { get; set; }
    public List<T> AaData      { get; set; } = new();
}

// ── Region ────────────────────────────────────────────────────────────────────
public class CountryResponse
{
    public int    Id   { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class StateResponse
{
    public int    Id          { get; set; }
    public string Name        { get; set; } = string.Empty;
    public int    CountryId   { get; set; }
    public string CountryName { get; set; } = string.Empty;
}

public class CityResponse
{
    public int    Id        { get; set; }
    public string Name      { get; set; } = string.Empty;
    public int    StateId   { get; set; }
    public string StateName { get; set; } = string.Empty;
}

// ── Quickbooks ────────────────────────────────────────────────────────────────
public class QbSettingsResponse
{
    public bool    IsConnected  { get; set; }
    public string? CompanyName  { get; set; }
    public string? Country      { get; set; }
    public string? CheckAccount { get; set; }
}

public class QbModalityMappingResponse
{
    public List<string>              Mapped    { get; set; } = new();
    public List<string>              Unmapped  { get; set; } = new();
    public Dictionary<string,string> Mappings  { get; set; } = new();
    public List<QbItemResponse>      Items     { get; set; } = new();
}

public class QbClientMappingResponse
{
    public List<UserResponse>        MappedClients   { get; set; } = new();
    public List<UserResponse>        UnmappedClients { get; set; } = new();
    public Dictionary<string,string> Mappings        { get; set; } = new();
    public List<QbCustomerResponse>  Customers       { get; set; } = new();
}

public class QbTranscriberMappingResponse
{
    public List<UserResponse>        MappedTranscribers   { get; set; } = new();
    public List<UserResponse>        UnmappedTranscribers { get; set; } = new();
    public Dictionary<string,string> Mappings             { get; set; } = new();
    public List<QbVendorResponse>    Vendors              { get; set; } = new();
}

public class QbItemResponse     { public string Id { get; set; } = ""; public string Name { get; set; } = ""; }
public class QbCustomerResponse { public string Id { get; set; } = ""; public string DisplayName { get; set; } = ""; }
public class QbVendorResponse   { public string Id { get; set; } = ""; public string DisplayName { get; set; } = ""; }
