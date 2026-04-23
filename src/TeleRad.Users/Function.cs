using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using MySqlConnector;
using Newtonsoft.Json;
using TeleRad.Shared;
using TeleRad.Shared.Model;
using TeleRad.Shared.Utility;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace TeleRad.Users;

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

            var authToken = wrapper.AuthToken
                ?? (request.Headers != null && request.Headers.TryGetValue("Authorization", out var ah) ? ah?.Replace("Bearer ", "") : null)
                ?? string.Empty;

            if (string.IsNullOrWhiteSpace(authToken))
                return FunctionBase.Unauthorized("Authorization token is required.");

            var token = await ValidateTokenAsync(authToken);
            if (!token.IsValid)
                return FunctionBase.Unauthorized("Token Not Authorized.");

            var helper = new UsersHelper(ConnectionString, token);

            return trigger switch
            {
                // ── Users ────────────────────────────────────────────────────
                TriggerHelper.GetUsers                      => await helper.GetUsersAsync(wrapper.Body),
                TriggerHelper.CreateUser                    => await helper.CreateUserAsync(wrapper.Body),
                TriggerHelper.UpdateUser                    => await helper.UpdateUserAsync(wrapper.Body),
                TriggerHelper.UpdateUserPermission          => await helper.UpdateUserPermissionAsync(wrapper.Body),
                TriggerHelper.UpdateMedicalFee              => await helper.UpdateMedicalFeeAsync(wrapper.Body),
                TriggerHelper.ResetPassword                 => await helper.ResetPasswordAsync(wrapper.Body),
                TriggerHelper.ShowPassword                  => await helper.ShowPasswordAsync(wrapper.Body),
                TriggerHelper.ChangeUserStatus              => await helper.ChangeUserStatusAsync(wrapper.Body),
                TriggerHelper.ChangeBillingStatus           => await helper.ChangeBillingStatusAsync(wrapper.Body),
                TriggerHelper.UserSearch                    => await helper.UserSearchAsync(wrapper.Body),
                TriggerHelper.SearchUserByType              => await helper.SearchUserByTypeAsync(wrapper.Body),
                TriggerHelper.LoginAsAnotherUser            => await helper.LoginAsAnotherUserAsync(wrapper.Body),
                TriggerHelper.BackToAccount                 => await helper.BackToAccountAsync(wrapper.Body),
                TriggerHelper.GetClientLookup               => await helper.GetClientLookupAsync(wrapper.Body),
                TriggerHelper.GetUserById                   => await helper.GetUserByIdAsync(wrapper.Body),
                TriggerHelper.UpdateClientInfo              => await helper.UpdateClientInfoAsync(wrapper.Body),
                TriggerHelper.ConfirmDefaultTemplate        => await helper.ConfirmDefaultTemplateAsync(wrapper.Body),
                TriggerHelper.GetUserTypes                  => await helper.GetUserTypesAsync(wrapper.Body),
                TriggerHelper.GetCountries                  => await helper.GetCountriesAsync(wrapper.Body),
                TriggerHelper.GetStates                     => await helper.GetStatesAsync(wrapper.Body),
                TriggerHelper.GetCities                     => await helper.GetCitiesAsync(wrapper.Body),
                TriggerHelper.GetTranscribers               => await helper.GetTranscribersAsync(wrapper.Body),
                TriggerHelper.GetUsersByDisplayName         => await helper.GetUsersByDisplayNameAsync(wrapper.Body),
                TriggerHelper.UpdateEmployees               => await helper.UpdateEmployeesAsync(wrapper.Body),

                // ── Roles ────────────────────────────────────────────────────
                TriggerHelper.GetRoles                      => await helper.GetRolesAsync(),
                TriggerHelper.CreateRole                    => await helper.CreateRoleAsync(wrapper.Body),
                TriggerHelper.UpdateRole                    => await helper.UpdateRoleAsync(wrapper.Body),
                TriggerHelper.DeleteRole                    => await helper.DeleteRoleAsync(wrapper.Body),

                // ── Templates ────────────────────────────────────────────────
                TriggerHelper.GetAllTemplates               => await helper.GetAllTemplatesAsync(wrapper.Body),
                TriggerHelper.SearchTemplates               => await helper.SearchTemplatesAsync(wrapper.Body),
                TriggerHelper.CreateTemplate                => await helper.CreateTemplateAsync(wrapper.Body),
                TriggerHelper.UpdateTemplate                => await helper.UpdateTemplateAsync(wrapper.Body),
                TriggerHelper.DeleteTemplate                => await helper.DeleteTemplateAsync(wrapper.Body),
                TriggerHelper.GetUserTemplates              => await helper.GetUserTemplatesAsync(wrapper.Body),
                TriggerHelper.SearchReferrers               => await helper.SearchReferrersAsync(wrapper.Body),

                // ── Region — Countries ───────────────────────────────────────
                TriggerHelper.GetAllCountries               => await helper.GetAllCountriesAsync(wrapper.Body),
                TriggerHelper.CreateCountry                 => await helper.CreateCountryAsync(wrapper.Body),
                TriggerHelper.UpdateCountry                 => await helper.UpdateCountryAsync(wrapper.Body),
                TriggerHelper.DeleteCountry                 => await helper.DeleteCountryAsync(wrapper.Body),

                // ── Region — States ──────────────────────────────────────────
                TriggerHelper.GetAllStates                  => await helper.GetAllStatesAsync(wrapper.Body),
                TriggerHelper.CreateState                   => await helper.CreateStateAsync(wrapper.Body),
                TriggerHelper.UpdateState                   => await helper.UpdateStateAsync(wrapper.Body),
                TriggerHelper.DeleteState                   => await helper.DeleteStateAsync(wrapper.Body),

                // ── Region — Cities ──────────────────────────────────────────
                TriggerHelper.GetAllCities                  => await helper.GetAllCitiesAsync(wrapper.Body),
                TriggerHelper.CreateCity                    => await helper.CreateCityAsync(wrapper.Body),
                TriggerHelper.UpdateCity                    => await helper.UpdateCityAsync(wrapper.Body),
                TriggerHelper.DeleteCity                    => await helper.DeleteCityAsync(wrapper.Body),

                _                                           => FunctionBase.BadRequest($"Unknown trigger: {trigger}")
            };
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[TeleRad.Users] Unhandled exception: {ex}");
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
        catch (Exception ex) { Console.WriteLine($"[TeleRad.Users] Token validation error: {ex.Message}"); }
        return new TokenValidationResult { IsValid = false };
    }
}
