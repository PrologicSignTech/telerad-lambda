namespace TeleRad.Shared.Model;

// ── Wrapper ───────────────────────────────────────────────────────────────────
public class LambdaRequest
{
    public string Trigger { get; set; } = string.Empty;
    public string? Body { get; set; }
    public string? Origin { get; set; }
    public string? AuthToken { get; set; }
    public string? UserId { get; set; }
    public string? ClientType { get; set; }
}

// ── Auth ──────────────────────────────────────────────────────────────────────
public class LoginRequest
{
    public string Username { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
}

// ── Studies ───────────────────────────────────────────────────────────────────
public class GetStudiesRequest
{
    public string? Dos { get; set; }
    public string? ClientName { get; set; }
    public string? ClientNameExcept { get; set; }
    public string? Search { get; set; }
    /// <summary>Comma-separated statuses</summary>
    public string? StatusList { get; set; }
    /// <summary>Comma-separated modalities</summary>
    public string? ModalityList { get; set; }
    public string? Type { get; set; }
}

public class UpdateStudyStatusRequest
{
    public int StudyId { get; set; }
    public string Status { get; set; } = string.Empty;
}

public class UpdateStudyRequest
{
    public int StudyId { get; set; }
    public string? PatientName { get; set; }       // pname (full name)
    public string? PatientFirstName { get; set; }  // firstname
    public string? PatientLastName { get; set; }   // lastname
    public string? PatientId { get; set; }         // idnumber
    public string? PatientDob { get; set; }
    public string? Dos { get; set; }
    public string? Modality { get; set; }
    public string? Description { get; set; }       // exam
    public string? OrderingPhysician { get; set; }
    public string? AccessionNumber { get; set; }
    public string? Status { get; set; }
    public int? TranscriberId { get; set; }
    public int? RadId { get; set; }
    public int? TemplateId { get; set; }
    public int? ClientId { get; set; }             // ref_id
    public string? ReportText { get; set; }
}

public class MarkStatRequest
{
    public List<int> StudyIds { get; set; } = new();
}

public class CloneStudyRequest
{
    public int StudyId { get; set; }
}

public class StudyIdRequest
{
    public int StudyId { get; set; }
}

public class SaveAuditRequest
{
    public int StudyId { get; set; }
    public string Action { get; set; } = string.Empty;
    public string? Description { get; set; }
}

public class UpdateStudyReportRequest
{
    public int StudyId { get; set; }
    public string? PatientName { get; set; }
    public string? Dob { get; set; }
    public string? Dos { get; set; }
    public string? Modality { get; set; }
    public string? Description { get; set; }
}

// ── Notes ─────────────────────────────────────────────────────────────────────
public class GetNotesRequest
{
    public int StudyId { get; set; }
    public bool AutoView { get; set; } = false;
}

public class AddNoteRequest
{
    public int StudyId { get; set; }
    public string Notes { get; set; } = string.Empty;
}

public class UpdateNoteRequest
{
    public int NoteId { get; set; }
    public string Notes { get; set; } = string.Empty;
}

// ── Templates ─────────────────────────────────────────────────────────────────
public class GetTemplatesRequest
{
    public string? Search { get; set; }
    public int? Page { get; set; }
    public int? PerPage { get; set; }
}

public class SearchTemplatesRequest
{
    public string? Query { get; set; }
    public int? UserId { get; set; }
}

public class CreateTemplateRequest
{
    public string Name { get; set; } = string.Empty;
    public string BodyText { get; set; } = string.Empty;
    public int? UserId { get; set; }
    public string? Modality { get; set; }
}

public class UpdateTemplateRequest
{
    public int TemplateId { get; set; }
    public string? Name { get; set; }
    public string? BodyText { get; set; }
    public string? Modality { get; set; }
}

public class DeleteTemplateRequest
{
    public int TemplateId { get; set; }
}

public class UserIdRequest
{
    public int UserId { get; set; }
}

public class InterpretNonDicomEntryRequest
{
    public int   StudyId     { get; set; }
    public int   TransId     { get; set; }
    /// <summary>Base64-encoded audio blob (optional). Null = no audio.</summary>
    public string? AudioBase64  { get; set; }
    public string? AudioFileName { get; set; }
}

public class SearchRequest
{
    public string? Query { get; set; }
}

// ── Users ─────────────────────────────────────────────────────────────────────
public class GetUsersRequest
{
    /// <summary>1=Admin 2=Radiologist 3=Transcriber 4=Referrer</summary>
    public int? UserType { get; set; }
    public string? Search { get; set; }
}

public class CreateUserRequest
{
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public int UserType { get; set; }
    public string? Phone { get; set; }
    public string? Address { get; set; }
    public int? CountryId { get; set; }
    public int? StateId { get; set; }
    public int? CityId { get; set; }
    public string? RoleName { get; set; }
}

public class UpdateUserRequest
{
    public int UserId { get; set; }
    public string? Name { get; set; }
    public string? Email { get; set; }
    public string? Username { get; set; }
    public string? Phone { get; set; }
    public string? Address { get; set; }
    public int? CountryId { get; set; }
    public int? StateId { get; set; }
    public int? CityId { get; set; }
    public string? RoleName { get; set; }
}

public class UpdateUserPermissionRequest
{
    public int UserId { get; set; }
    public List<string> Permissions { get; set; } = new();
}

public class UpdateMedicalFeeRequest
{
    public int UserId { get; set; }
    public string? MedicalDirector { get; set; }
    public decimal? Fee { get; set; }
}

public class PasswordRequest
{
    public int UserId { get; set; }
}

public class ChangeUserStatusRequest
{
    public int UserId { get; set; }
    public int Status { get; set; }
}

public class LoginAsRequest
{
    public int TargetUserId { get; set; }
}

public class GetClientLookupRequest
{
    public string? Search { get; set; }
    public int? Page { get; set; }
}

public class UpdateClientInfoRequest
{
    public int     ClientId            { get; set; }
    public string? FirstName           { get; set; }
    public string? LastName            { get; set; }
    public string? Name                { get; set; }   // displayname
    public string? Email               { get; set; }
    public string? Phone               { get; set; }
    public string? Fax                 { get; set; }
    public string? Address             { get; set; }
    public string? Notes               { get; set; }
    public int?    CountryId           { get; set; }
    public int?    StateId             { get; set; }
    public int?    CityId              { get; set; }
    public int?    DefaultTranscriberId { get; set; }
    public string? Username             { get; set; }
    public string? Password             { get; set; }
    public int?    UserTypeId           { get; set; }
    public string? DictationPool        { get; set; }
}

public class GetUserByIdRequest
{
    public int UserId { get; set; }
}

public class GetStatesRequest
{
    public int? CountryId { get; set; }
    public string? Query { get; set; }
}

public class GetCitiesRequest
{
    public int? StateId { get; set; }
    public string? Query { get; set; }
}

public class UpdateEmployeesRequest
{
    public int TranscriberAdminId { get; set; }
    public List<int> EmployeeIds { get; set; } = new();
}

// ── Attachments / Audio ───────────────────────────────────────────────────────
public class AddAttachmentRequest
{
    public int StudyId { get; set; }
    public string FileName { get; set; } = string.Empty;
    public string FileBase64 { get; set; } = string.Empty;
    public string FileType { get; set; } = string.Empty;
}

public class AddMultipleAttachmentsRequest
{
    public List<int> StudyIds { get; set; } = new();
    public string FileName { get; set; } = string.Empty;
    public string FileBase64 { get; set; } = string.Empty;
}

public class GetAttachedFilesRequest
{
    public int StudyId { get; set; }
    public string? FileType { get; set; }
}

public class GetPresignedUrlRequest
{
    public string? FilePath { get; set; }
}

public class GetPresignedUploadUrlRequest
{
    public int StudyId { get; set; }
    public string FileName { get; set; } = string.Empty;
    public string ContentType { get; set; } = "audio/webm";
}

public class UploadAudioAttachmentRequest
{
    public int StudyId { get; set; }
    public string FileName { get; set; } = string.Empty;
    public string Data { get; set; } = string.Empty; // base64-encoded WAV
}

public class GetViewerTokenRequest
{
    public string? Sid { get; set; }
}

public class PagedRequest
{
    public int Page     { get; set; } = 1;
    public int PageSize { get; set; } = 20;
}

public class PendingBatchRequest
{
    public List<string>? Statuses { get; set; }
    public int Page     { get; set; } = 1;
    public int PageSize { get; set; } = 20;
}

public class SendAudioFilesRequest
{
    public int StudyId { get; set; }
    public int TranscriberId { get; set; }
    public string FileName { get; set; } = string.Empty;
    public string FileBase64 { get; set; } = string.Empty;
    public string FileType { get; set; } = string.Empty;
}

public class GetSentAudioFilesRequest
{
    public string? Dos { get; set; }
    public int? ClientId { get; set; }
    public int? TranscriberId { get; set; }
}

// ── Audit ─────────────────────────────────────────────────────────────────────
public class GetAuditAllRequest
{
    public string? Search { get; set; }
    public string? DateFrom { get; set; }
    public string? DateTo { get; set; }
    public int? Page { get; set; }
    public int? PerPage { get; set; }
}

// ── Fax ───────────────────────────────────────────────────────────────────────
public class SendFaxRequest
{
    public string FaxNumber { get; set; } = string.Empty;
    public string FileBase64 { get; set; } = string.Empty;
    public string FileName { get; set; } = string.Empty;
    public int? StudyId { get; set; }
}

public class GetIncomingFaxesRequest
{
    public string? ClientUsername { get; set; }
}

public class RenameFaxRequest
{
    public int FaxId { get; set; }
    public string NewName { get; set; } = string.Empty;
}

public class MoveInboundFaxRequest
{
    public List<int> FaxIds { get; set; } = new();
    public int ClientId { get; set; }
}

public class PaginatedRequest
{
    public int? Page { get; set; }
    public int? PerPage { get; set; }
    public string? Search { get; set; }
}

// ── Final Reports ─────────────────────────────────────────────────────────────
public class GetFinalReportsRequest
{
    public string? DateFrom { get; set; }
    public string? DateTo { get; set; }
    public int? RadId { get; set; }
    public int? Page { get; set; }
    public int? PerPage { get; set; }
}

public class UpdateFinalReportRequest
{
    public int StudyId { get; set; }
    public string ReportText { get; set; } = string.Empty;
    public string? Impression { get; set; }
    public bool IsAddendum { get; set; } = false;
}

// ── Dashboard / Charts ────────────────────────────────────────────────────────
public class GetChartDataRequest
{
    public string ReportType { get; set; } = string.Empty;
    public int? RadId { get; set; }
    public string? DateRange { get; set; }
}

public class GetFifteenRecordsRequest
{
    public string? Status { get; set; }
    public int? Page { get; set; }
}

public class GetFinalizedDataRequest
{
    public int? RadId { get; set; }
    public int? TranscriberId { get; set; }
    public string? DateFrom { get; set; }
    public string? DateTo { get; set; }
}

// ── Billing ───────────────────────────────────────────────────────────────────
public class CreateTranscriberInvoiceRequest
{
    public int TranscriberId { get; set; }
    public int ClientId { get; set; }
    public string DateFrom { get; set; } = string.Empty;
    public string DateTo { get; set; } = string.Empty;
}

public class SaveTranscriberInvoiceRequest
{
    public int TranscriberId { get; set; }
    public int ClientId { get; set; }
    public string DateFrom { get; set; } = string.Empty;
    public string DateTo { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public string InvoiceNumber { get; set; } = string.Empty;
    public List<InvoiceLineItem> LineItems { get; set; } = new();
}

public class SaveClientInvoiceRequest
{
    public int ClientId { get; set; }
    public string DateFrom { get; set; } = string.Empty;
    public string DateTo { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public string InvoiceNumber { get; set; } = string.Empty;
    public List<InvoiceLineItem> LineItems { get; set; } = new();
}

public class InvoiceLineItem
{
    public string Modality { get; set; } = string.Empty;
    public int Count { get; set; }
    public decimal UnitPrice { get; set; }
    public decimal Total { get; set; }
}

// ── Price Management ──────────────────────────────────────────────────────────
public class GetPriceManagementRequest
{
    public int UserId { get; set; }
    public string UserCategory { get; set; } = string.Empty; // "client" or "transcriber"
}

public class SaveModalityPriceRequest
{
    public int UserId { get; set; }
    public string UserCategory { get; set; } = string.Empty;
    public string Modality { get; set; } = string.Empty;
    public decimal Price { get; set; }
}

public class SavePerPagePriceRequest
{
    public int UserId { get; set; }
    public decimal PricePerPage { get; set; }
    public string? Modality { get; set; }
}

public class SavePerCharPriceRequest
{
    public int UserId { get; set; }
    public decimal PricePerChar { get; set; }
}

// ── Order Status ──────────────────────────────────────────────────────────────
public class GetOrderStatusRequest
{
    public string? DateFrom { get; set; }
    public string? DateTo { get; set; }
    public string? Search { get; set; }
    public int? Page { get; set; }
    public int? PerPage { get; set; }
}

public class ReassignTranscriberRequest
{
    public int StudyId { get; set; }
    public int TranscriberId { get; set; }
}

// ── Modalities ────────────────────────────────────────────────────────────────
public class ToggleModalityRequest
{
    public int ModalityId { get; set; }
}

// ── Non-DICOM ─────────────────────────────────────────────────────────────────
public class GetNonDicomAccountsRequest
{
    public string? Search { get; set; }
    public int? Page { get; set; }
}

public class StoreNonDicomEntryRequest
{
    public int? StudyId { get; set; }
    public string? PatientName { get; set; }
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
    public string? Dob { get; set; }
    public string? Dos { get; set; }
    public string? Modality { get; set; }
    public string? ExamType { get; set; }
    public string? Description { get; set; }
    public int? ClientId { get; set; }
    public int? RadId { get; set; }
    public string? OrderingPhysician { get; set; }
    public string? Location { get; set; }
    public string? AccessNumber { get; set; }
    public string? IdNumber { get; set; }
}

public class DeleteNonDicomEntryRequest
{
    public int StudyId { get; set; }
}

// ── Standard Reports ──────────────────────────────────────────────────────────
public class GetStandardReportsRequest
{
    public int? RadId { get; set; }
    public int? Page { get; set; }
    public int? PerPage { get; set; }
}

public class CreateStandardReportRequest
{
    public string Label { get; set; } = string.Empty;
    public string ReportText { get; set; } = string.Empty;
    public int RadId { get; set; }
    public int ModalityId { get; set; }
}

public class UpdateStandardReportRequest
{
    public int ReportId { get; set; }
    public string? Label { get; set; }
    public string? ReportText { get; set; }
    public int? RadId { get; set; }
    public int? ModalityId { get; set; }
}

public class DeleteStandardReportRequest
{
    public int ReportId { get; set; }
}

// ── Roles ─────────────────────────────────────────────────────────────────────
public class CreateRoleRequest
{
    public string Name { get; set; } = string.Empty;
    public List<string> Permissions { get; set; } = new();
}

public class UpdateRoleRequest
{
    public int RoleId { get; set; }
    public string? Name { get; set; }
    public List<string> Permissions { get; set; } = new();
}

public class DeleteRoleRequest
{
    public int RoleId { get; set; }
}

// ── Site Management ───────────────────────────────────────────────────────────
public class UpdateMiscSettingsRequest
{
    public string? WelcomeMessage { get; set; }
    public string? LogoBase64 { get; set; }
    public string? LogoFileName { get; set; }
    public string? SiteName { get; set; }
}

public class OsrixUserRequest
{
    public int? Id { get; set; }
    public string Username { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public int RadId { get; set; }
}

public class OsrixInstitutionRequest
{
    public int? Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? Description { get; set; }
}

public class UpdateDefaultChargesRequest
{
    public Dictionary<string, decimal> ModalityCharges { get; set; } = new();
}

// ── Region Management ─────────────────────────────────────────────────────────
public class GetAllRegionRequest
{
    public string? Search { get; set; }
    public int Start  { get; set; } = 0;
    public int Length { get; set; } = 20;
    public int Draw   { get; set; } = 1;
}

public class CreateCountryRequest { public string Name { get; set; } = string.Empty; }
public class UpdateCountryRequest { public int Id { get; set; }  public string Name { get; set; } = string.Empty; }
public class DeleteCountryRequest { public int Id { get; set; } }

public class CreateStateRequest   { public int CountryId { get; set; } public string Name { get; set; } = string.Empty; }
public class UpdateStateRequest   { public int Id { get; set; } public int CountryId { get; set; } public string Name { get; set; } = string.Empty; }
public class DeleteStateRequest   { public int Id { get; set; } }

public class CreateCityRequest    { public int StateId { get; set; } public string Name { get; set; } = string.Empty; }
public class UpdateCityRequest    { public int Id { get; set; } public int StateId { get; set; } public string Name { get; set; } = string.Empty; }
public class DeleteCityRequest    { public int Id { get; set; } }

// ── Quickbooks ────────────────────────────────────────────────────────────────
public class SaveQbSettingsRequest
{
    public string CheckAccount { get; set; } = string.Empty;
}

public class SaveQbModalityMappingRequest
{
    /// <summary>modality name → QBO item Id (empty string = delete mapping)</summary>
    public Dictionary<string, string> Mappings { get; set; } = new();
}

public class PushQbModalitiesRequest
{
    /// <summary>Modality names to create as Items in QBO</summary>
    public List<string> Modalities { get; set; } = new();
}

public class SaveQbClientMappingRequest
{
    /// <summary>local client id (string key) → QBO customer Id (empty = delete)</summary>
    public Dictionary<string, string> Mappings { get; set; } = new();
}

public class PushQbClientsRequest
{
    public List<int> ClientIds { get; set; } = new();
}

public class SaveQbTranscriberMappingRequest
{
    /// <summary>local transcriber id (string key) → QBO vendor Id (empty = delete)</summary>
    public Dictionary<string, string> Mappings { get; set; } = new();
}

public class PushQbTranscribersRequest
{
    public List<int> TranscriberIds { get; set; } = new();
}

public class DownloadAudioZipRequest
{
    public List<int> StudyIds { get; set; } = new();
    /// <summary>Map of studyId (as string) → order number, used for folder names in the ZIP.</summary>
    public Dictionary<string, string> OrderNumbers { get; set; } = new();
}

public class DeleteTempZipRequest
{
    public string ZipKey { get; set; } = string.Empty;
}

