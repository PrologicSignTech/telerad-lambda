namespace TeleRadLambda.Utility;

public static class TriggerHelper
{
    // ── Auth ──────────────────────────────────────────────────────────────────
    public const string Login                        = "login";
    public const string Logout                       = "logout";

    // ── Studies ───────────────────────────────────────────────────────────────
    public const string GetStudies                   = "getStudies";
    public const string GetDailyWorkflowData         = "getDailyWorkflowData";
    public const string UpdateStudyStatus            = "updateStudyStatus";
    public const string UpdateStudy                  = "updateStudy";
    public const string MarkStat                     = "markStat";
    public const string UnmarkStat                   = "unmarkStat";
    public const string CloneStudy                   = "cloneStudy";
    public const string GetStudyAudit                = "getStudyAudit";
    public const string GetFinalReportUrl            = "getFinalReportUrl";
    public const string SaveAudit                    = "saveAudit";
    public const string UpdateStudyReport            = "updateStudyReport";
    public const string PdfReviewed                  = "pdfReviewed";
    public const string CheckForDownloadAudio        = "checkForDownloadAudio";
    public const string GetStudyDocument             = "getStudyDocument";
    public const string GetVerificationSheet         = "getVerificationSheet";
    public const string GetExamHistory               = "getExamHistory";

    // ── Notes ─────────────────────────────────────────────────────────────────
    public const string GetNotes                     = "getNotes";
    public const string AddNote                      = "addNote";
    public const string UpdateNote                   = "updateNote";

    // ── Templates ─────────────────────────────────────────────────────────────
    public const string GetAllTemplates              = "getAllTemplates";
    public const string SearchTemplates              = "searchTemplates";
    public const string CreateTemplate               = "createTemplate";
    public const string UpdateTemplate               = "updateTemplate";
    public const string DeleteTemplate               = "deleteTemplate";
    public const string GetUserTemplates             = "getUserTemplates";
    public const string SearchReferrers              = "searchReferrers";

    // ── Users ─────────────────────────────────────────────────────────────────
    public const string GetUsers                     = "getUsers";
    public const string CreateUser                   = "createUser";
    public const string UpdateUser                   = "updateUser";
    public const string UpdateUserPermission         = "updateUserPermission";
    public const string UpdateMedicalFee             = "updateMedicalFee";
    public const string ResetPassword                = "resetPassword";
    public const string ShowPassword                 = "showPassword";
    public const string ChangeUserStatus             = "changeUserStatus";
    public const string ChangeBillingStatus          = "changeBillingStatus";
    public const string UserSearch                   = "userSearch";
    public const string SearchUserByType             = "searchUserByType";
    public const string LoginAsAnotherUser           = "loginAsAnotherUser";
    public const string BackToAccount                = "backToAccount";
    public const string GetClientLookup              = "getClientLookup";
    public const string UpdateClientInfo             = "updateClientInfo";
    public const string ConfirmDefaultTemplate       = "confirmDefaultTemplate";
    public const string GetUserTypes                 = "getUserTypes";
    public const string GetCountries                 = "getCountries";
    public const string GetStates                    = "getStates";
    public const string GetCities                    = "getCities";
    public const string GetTranscribers              = "getTranscribers";
    public const string GetUsersByDisplayName        = "getUsersByDisplayName";
    public const string UpdateEmployees              = "updateEmployees";

    // ── Attachments / Audio ───────────────────────────────────────────────────
    public const string AddAttachment                = "addAttachment";
    public const string AddMultipleAttachments       = "addMultipleAttachments";
    public const string GetAttachedFiles             = "getAttachedFiles";
    public const string SendAudioFiles               = "sendAudioFiles";
    public const string GetSentAudioFiles            = "getSentAudioFiles";

    // ── Audit ─────────────────────────────────────────────────────────────────
    public const string GetAuditAll                  = "getAuditAll";

    // ── Fax ───────────────────────────────────────────────────────────────────
    public const string SendFax                      = "sendFax";
    public const string ViewFaxes                    = "viewFaxes";
    public const string GetInboundFaxes              = "getInboundFaxes";
    public const string GetIncomingFaxes             = "getIncomingFaxes";
    public const string RenameFax                    = "renameFax";
    public const string MoveInboundFax               = "moveInboundFax";
    public const string GetFaxStatus                 = "getFaxStatus";
    public const string GetUrgentNotifications       = "getUrgentNotifications";

    // ── Final Reports ─────────────────────────────────────────────────────────
    public const string GetFinalReports              = "getFinalReports";
    public const string UpdateFinalReport            = "updateFinalReport";

    // ── Dashboard / Charts ────────────────────────────────────────────────────
    public const string GetChartData                 = "getChartData";
    public const string GetLastFifteenRecords        = "getLastFifteenRecords";
    public const string GetHoldForComparison         = "getHoldForComparison";
    public const string GetMissingPaperwork          = "getMissingPaperwork";
    public const string GetSpeakToTech               = "getSpeakToTech";
    public const string GetMissingImages             = "getMissingImages";
    public const string GetHoldReport                = "getHoldReport";
    public const string GetFinalizedData             = "getFinalizedData";
    public const string GetRefFinalizedData          = "getRefFinalizedData";

    // ── Billing ───────────────────────────────────────────────────────────────
    public const string CreateTranscriberInvoice     = "createTranscriberInvoice";
    public const string CreateTranscriberInvoiceStep2= "createTranscriberInvoiceStep2";
    public const string SaveTranscriberInvoice       = "saveTranscriberInvoice";
    public const string CreateClientInvoice          = "createClientInvoice";
    public const string SaveClientInvoice            = "saveClientInvoice";
    public const string GetInvoicePayments           = "getInvoicePayments";
    public const string GetClientPayments            = "getClientPayments";
    public const string GetTranscriberAnalytics      = "getTranscriberAnalytics";
    public const string GetClientAnalytics           = "getClientAnalytics";
    public const string GetBilledAmount              = "getBilledAmount";

    // ── Price Management ──────────────────────────────────────────────────────
    public const string GetPriceManagement           = "getPriceManagement";
    public const string SaveModalityPrice            = "saveModalityPrice";
    public const string SavePerPagePrice             = "savePerPagePrice";
    public const string SavePerCharPrice             = "savePerCharPrice";

    // ── Order Status ──────────────────────────────────────────────────────────
    public const string GetOrderStatus               = "getOrderStatus";
    public const string GetOrderStatusV2             = "getOrderStatusV2";
    public const string GetAuditDetailOrderStatus    = "getAuditDetailOrderStatus";
    public const string ReassignTranscriber          = "reassignTranscriber";

    // ── Modalities ────────────────────────────────────────────────────────────
    public const string GetModalities                = "getModalities";
    public const string ToggleModalityStatus         = "toggleModalityStatus";

    // ── Non-DICOM ─────────────────────────────────────────────────────────────
    public const string GetNonDicomAccounts          = "getNonDicomAccounts";
    public const string StoreNonDicomEntry           = "storeNonDicomEntry";
    public const string DeleteNonDicomEntry          = "deleteNonDicomEntry";
    public const string GetNonDicomEntry             = "getNonDicomEntry";

    // ── Standard Reports ──────────────────────────────────────────────────────
    public const string GetStandardReports           = "getStandardReports";
    public const string CreateStandardReport         = "createStandardReport";
    public const string UpdateStandardReport         = "updateStandardReport";
    public const string DeleteStandardReport         = "deleteStandardReport";

    // ── Roles ─────────────────────────────────────────────────────────────────
    public const string GetRoles                     = "getRoles";
    public const string CreateRole                   = "createRole";
    public const string UpdateRole                   = "updateRole";
    public const string DeleteRole                   = "deleteRole";

    // ── Site Management ───────────────────────────────────────────────────────
    public const string GetMiscSettings              = "getMiscSettings";
    public const string UpdateMiscSettings           = "updateMiscSettings";
    public const string GetOsrix                     = "getOsrix";
    public const string CreateOsrixUser              = "createOsrixUser";
    public const string UpdateOsrixUser              = "updateOsrixUser";
    public const string DeleteOsrixUser              = "deleteOsrixUser";
    public const string GetOsrixInstitutions         = "getOsrixInstitutions";
    public const string CreateOsrixInstitution       = "createOsrixInstitution";
    public const string UpdateOsrixInstitution       = "updateOsrixInstitution";
    public const string DeleteOsrixInstitution       = "deleteOsrixInstitution";
    public const string UpdateDefaultCharges         = "updateDefaultCharges";
    public const string GetDefaultCharges            = "getDefaultCharges";
    public const string GetRadiologists              = "getRadiologists";

    // ── Region Management ─────────────────────────────────────────────────────
    public const string GetAllCountries              = "getAllCountries";
    public const string CreateCountry                = "createCountry";
    public const string UpdateCountry                = "updateCountry";
    public const string DeleteCountry                = "deleteCountry";
    public const string GetAllStates                 = "getAllStates";
    public const string CreateState                  = "createState";
    public const string UpdateState                  = "updateState";
    public const string DeleteState                  = "deleteState";
    public const string GetAllCities                 = "getAllCities";
    public const string CreateCity                   = "createCity";
    public const string UpdateCity                   = "updateCity";
    public const string DeleteCity                   = "deleteCity";

    // ── Quickbooks ────────────────────────────────────────────────────────────
    public const string GetQbSettings                = "getQbSettings";
    public const string SaveQbSettings               = "saveQbSettings";
    public const string GetQbModalityMappings        = "getQbModalityMappings";
    public const string SaveQbModalityMapping        = "saveQbModalityMapping";
    public const string PushQbModalities             = "pushQbModalities";
    public const string PullQbModalities             = "pullQbModalities";
    public const string GetQbClientMappings          = "getQbClientMappings";
    public const string SaveQbClientMapping          = "saveQbClientMapping";
    public const string PushQbClients                = "pushQbClients";
    public const string PullQbClients                = "pullQbClients";
    public const string GetQbTranscriberMappings     = "getQbTranscriberMappings";
    public const string SaveQbTranscriberMapping     = "saveQbTranscriberMapping";
    public const string PushQbTranscribers           = "pushQbTranscribers";
    public const string PullQbTranscribers           = "pullQbTranscribers";
}
