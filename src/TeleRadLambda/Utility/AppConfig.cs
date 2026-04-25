namespace TeleRadLambda.Utility;

/// <summary>
/// Reads feature-flag environment variables set in template.yaml (or AWS Lambda console).
/// FAX_SEND_ENABLED=false  → suppress all outbound fax dispatch (dev / demo).
/// FAX_SEND_ENABLED=true   → faxes are sent normally (production).
/// </summary>
public static class AppConfig
{
    public static bool FaxSendEnabled =>
        !string.Equals(
            Environment.GetEnvironmentVariable("FAX_SEND_ENABLED"),
            "false",
            StringComparison.OrdinalIgnoreCase
        );

    public static string Stage =>
        Environment.GetEnvironmentVariable("Stage") ?? "dev";
}
