namespace TeleRad.Shared;

/// <summary>
/// Reads feature-flag environment variables set in template.yaml (or AWS Lambda console).
/// On dev/demo set FAX_SEND_ENABLED=false to suppress all outbound fax calls without
/// breaking the UI — fax records are still created with status "dev-suppressed".
/// On production set FAX_SEND_ENABLED=true (or omit; default is true).
/// </summary>
public static class AppConfig
{
    /// <summary>
    /// True  → faxes are actually dispatched (production).
    /// False → fax send is suppressed; record written with status "dev-suppressed" (dev/demo).
    /// </summary>
    public static bool FaxSendEnabled =>
        !string.Equals(
            Environment.GetEnvironmentVariable("FAX_SEND_ENABLED"),
            "false",
            StringComparison.OrdinalIgnoreCase
        );

    /// <summary>
    /// The current deployment stage (dev / staging / prod).
    /// Set via the "Stage" environment variable in template.yaml.
    /// </summary>
    public static string Stage =>
        Environment.GetEnvironmentVariable("Stage") ?? "dev";
}
