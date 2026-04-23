using Amazon.Lambda.APIGatewayEvents;
using TeleRad.Shared;
using TeleRad.Shared.Model;
using TeleRad.Shared.Utility;

namespace TeleRad.Users;

public class UsersHelper : HelperBase
{
    public UsersHelper(string connectionString, TokenValidationResult? token = null)
        : base(connectionString, token) { }

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
            return FunctionBase.Ok(users);
        }
        catch (Exception ex) { return Err("GetUsers", ex); }
    }

    public async Task<APIGatewayProxyResponse> CreateUserAsync(string? body)
    {
        var req = Parse<CreateUserRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.Name) || string.IsNullOrWhiteSpace(req.Email) ||
            string.IsNullOrWhiteSpace(req.Username) || string.IsNullOrWhiteSpace(req.Password))
            return FunctionBase.BadRequest("Name, Email, Username, and Password are required.");

        try
        {
            var hash = BCrypt.Net.BCrypt.HashPassword(req.Password);
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertUser(conn, req.Name, req.Email, req.Username, hash,
                req.UserType, req.Phone, req.Address, req.CountryId, req.StateId, req.CityId);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(new { id = cmd.LastInsertedId }, "User created.");
        }
        catch (Exception ex) { return Err("CreateUser", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateUserAsync(string? body)
    {
        var req = Parse<UpdateUserRequest>(body);
        if (req == null || req.UserId <= 0)
            return FunctionBase.BadRequest("UserId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateUser(conn, req.UserId, req.Name, req.Email,
                req.Username, req.Phone, req.Address, req.CountryId, req.StateId, req.CityId);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "User updated.");
        }
        catch (Exception ex) { return Err("UpdateUser", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateUserPermissionAsync(string? body)
    {
        var req = Parse<UpdateUserPermissionRequest>(body);
        if (req == null || req.UserId <= 0)
            return FunctionBase.BadRequest("UserId is required.");

        try
        {
            await using var conn   = await OpenAsync();
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
            return FunctionBase.Ok(null, "Permissions updated.");
        }
        catch (Exception ex) { return Err("UpdateUserPermission", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateMedicalFeeAsync(string? body)
    {
        var req = Parse<UpdateMedicalFeeRequest>(body);
        if (req == null || req.UserId <= 0)
            return FunctionBase.BadRequest("UserId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateMedicalFee(conn, req.UserId, req.MedicalDirector, req.Fee);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Medical fee updated.");
        }
        catch (Exception ex) { return Err("UpdateMedicalFee", ex); }
    }

    public async Task<APIGatewayProxyResponse> ResetPasswordAsync(string? body)
    {
        var req = Parse<PasswordRequest>(body);
        if (req == null || req.UserId <= 0)
            return FunctionBase.BadRequest("UserId is required.");

        try
        {
            // Match Laravel: rand(100000, 999999) → 6-digit numeric password
            var passwordStr = new Random().Next(100000, 999999).ToString();
            var hash        = BCrypt.Net.BCrypt.HashPassword(passwordStr);

            await using var conn = await OpenAsync();

            // Store hashed password + plain password_string + is_password_reset = 1
            await using var cmd = QueryHelper.UpdateUserPassword(conn, req.UserId, hash, passwordStr);
            await cmd.ExecuteNonQueryAsync();

            // Fetch username to show in the success message
            await using var userCmd    = QueryHelper.GetUserById(conn, req.UserId);
            await using var userReader = await userCmd.ExecuteReaderAsync();
            var username = "";
            if (await userReader.ReadAsync())
                username = userReader["username"]?.ToString() ?? "";

            return FunctionBase.Ok(new { password = passwordStr, username }, "Password reset.");
        }
        catch (Exception ex) { return Err("ResetPassword", ex); }
    }

    public async Task<APIGatewayProxyResponse> ShowPasswordAsync(string? body)
    {
        var req = Parse<PasswordRequest>(body);
        if (req == null || req.UserId <= 0)
            return FunctionBase.BadRequest("UserId is required.");
        if (_token.UserType != 1)
            return FunctionBase.Forbidden("Admin access required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.GetUserPassword(conn, req.UserId);
            var hash             = await cmd.ExecuteScalarAsync();
            return FunctionBase.Ok(new { passwordHash = hash?.ToString() });
        }
        catch (Exception ex) { return Err("ShowPassword", ex); }
    }

    public async Task<APIGatewayProxyResponse> ChangeUserStatusAsync(string? body)
    {
        var req = Parse<ChangeUserStatusRequest>(body);
        if (req == null || req.UserId <= 0)
            return FunctionBase.BadRequest("UserId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.ChangeUserStatus(conn, req.UserId, req.Status);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "User status updated.");
        }
        catch (Exception ex) { return Err("ChangeUserStatus", ex); }
    }

    public async Task<APIGatewayProxyResponse> ChangeBillingStatusAsync(string? body)
    {
        var req = Parse<ChangeUserStatusRequest>(body);
        if (req == null || req.UserId <= 0)
            return FunctionBase.BadRequest("UserId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.ChangeBillingStatus(conn, req.UserId, req.Status);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Billing status updated.");
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
            return FunctionBase.Ok(users);
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
            return FunctionBase.Ok(users);
        }
        catch (Exception ex) { return Err("SearchUserByType", ex); }
    }

    public async Task<APIGatewayProxyResponse> LoginAsAnotherUserAsync(string? body)
    {
        var req = Parse<LoginAsRequest>(body);
        if (req == null || req.TargetUserId <= 0)
            return FunctionBase.BadRequest("TargetUserId is required.");
        if (_token.UserType != 1)
            return FunctionBase.Forbidden("Admin access required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetUserById(conn, req.TargetUserId);
            await using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync())
                return FunctionBase.NotFound("User not found.");

            var name     = Str(reader, "name");
            var email    = Str(reader, "email");
            var username = Str(reader, "username");
            var userType = reader.GetInt32("usertype");
            await reader.CloseAsync();

            var newToken = GenerateToken();
            await using var upsert = QueryHelper.UpsertToken(conn, req.TargetUserId, username, newToken);
            await upsert.ExecuteNonQueryAsync();

            return FunctionBase.Ok(new LoginResponse
            {
                Id = req.TargetUserId, Name = name, Email = email,
                Username = username, UserType = userType, Token = newToken
            }, "Logged in as user.");
        }
        catch (Exception ex) { return Err("LoginAsAnotherUser", ex); }
    }

    public async Task<APIGatewayProxyResponse> BackToAccountAsync(string? body)
        => await Task.FromResult(FunctionBase.Ok(null, "Back to original account."));

    public async Task<APIGatewayProxyResponse> GetClientLookupAsync(string? body)
    {
        var req     = Parse<GetClientLookupRequest>(body) ?? new GetClientLookupRequest();
        var page    = Math.Max(1, req.Page ?? 1);
        var perPage = 15;

        try
        {
            await using var conn   = await OpenAsync();
            await using var cntCmd = QueryHelper.CountClientLookup(conn, req.Search);
            var total              = Convert.ToInt32(await cntCmd.ExecuteScalarAsync());
            await using var cmd    = QueryHelper.GetClientLookup(conn, req.Search, (page - 1) * perPage, perPage);
            await using var reader = await cmd.ExecuteReaderAsync();
            var users = new List<UserResponse>();
            while (await reader.ReadAsync()) users.Add(ReadUser(reader));
            return FunctionBase.Ok(new PaginatedResponse<UserResponse>
            {
                Data = users, Total = total, Page = page, PerPage = perPage
            });
        }
        catch (Exception ex) { return Err("GetClientLookup", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetUserByIdAsync(string? body)
    {
        var req = Parse<GetUserByIdRequest>(body);
        if (req == null || req.UserId <= 0)
            return FunctionBase.BadRequest("UserId is required.");
        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetUserById(conn, req.UserId);
            await using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return FunctionBase.NotFound("User not found.");
            var schema = reader.GetColumnSchema();
            bool Has(string n) => schema.Any(c => c.ColumnName.Equals(n, StringComparison.OrdinalIgnoreCase));
            string? Opt(string n) => Has(n) ? (reader.IsDBNull(reader.GetOrdinal(n)) ? null : reader.GetString(n)) : null;
            int? OptInt(string n) => Has(n) && !reader.IsDBNull(reader.GetOrdinal(n)) ? reader.GetInt32(n) : null;
            return FunctionBase.Ok(new
            {
                id                      = reader.GetInt32("id"),
                username                = Opt("username"),
                email                   = Opt("email"),
                firstName               = Opt("firstname"),
                lastName                = Opt("lastname"),
                displayName             = Opt("displayname"),
                phone                   = Opt("phone"),
                fax                     = Opt("fax"),
                address                 = Opt("office"),
                notes                   = Opt("notes"),
                countryId               = OptInt("country"),
                stateId                 = OptInt("state"),
                cityId                  = OptInt("city"),
                countryName             = Opt("j_country_name"),
                stateName               = Opt("j_state_name"),
                cityName                = Opt("j_city_name"),
                defaultTranscriberId    = OptInt("default_transcriber"),
                defaultTranscriberName  = Opt("j_transcriber_name"),
                dictationPool           = Opt("auto_sign"),
                userType                = reader.GetInt32("usertype"),
                block                   = reader.GetInt32("block"),
            });
        }
        catch (Exception ex) { return Err("GetUserById", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateClientInfoAsync(string? body)
    {
        var req = Parse<UpdateClientInfoRequest>(body);
        if (req == null || req.ClientId <= 0)
            return FunctionBase.BadRequest("ClientId is required.");

        try
        {
            await using var conn = await OpenAsync();
            // Handle password separately (needs BCrypt hash)
            if (!string.IsNullOrWhiteSpace(req.Password))
            {
                var hash = BCrypt.Net.BCrypt.HashPassword(req.Password);
                await using var pwdCmd = QueryHelper.UpdateUserPassword(conn, req.ClientId, hash);
                await pwdCmd.ExecuteNonQueryAsync();
            }
            await using var cmd = QueryHelper.UpdateClientInfo(conn, req.ClientId,
                req.FirstName, req.LastName, req.Name, req.Email,
                req.Phone, req.Fax, req.Address, req.Notes,
                req.CountryId, req.StateId, req.CityId, req.DefaultTranscriberId,
                req.Username, req.UserTypeId, req.DictationPool);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Client info updated.");
        }
        catch (Exception ex) { return Err("UpdateClientInfo", ex); }
    }

    public async Task<APIGatewayProxyResponse> ConfirmDefaultTemplateAsync(string? body)
    {
        var req = Parse<UserIdRequest>(body);
        if (req == null || req.UserId <= 0)
            return FunctionBase.BadRequest("UserId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetTemplates(conn, null, req.UserId);
            await using var reader = await cmd.ExecuteReaderAsync();
            return FunctionBase.Ok(await ReadTemplates(reader));
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
            return FunctionBase.Ok(types);
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
            return FunctionBase.Ok(items);
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
            return FunctionBase.Ok(items);
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
            return FunctionBase.Ok(items);
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
            return FunctionBase.Ok(users);
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
            return FunctionBase.Ok(users);
        }
        catch (Exception ex) { return Err("GetUsersByDisplayName", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateEmployeesAsync(string? body)
    {
        var req = Parse<UpdateEmployeesRequest>(body);
        if (req == null || req.TranscriberAdminId <= 0)
            return FunctionBase.BadRequest("TranscriberAdminId is required.");

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
            return FunctionBase.Ok(null, "Employees updated.");
        }
        catch (Exception ex) { return Err("UpdateEmployees", ex); }
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
            return FunctionBase.Ok(roles);
        }
        catch (Exception ex) { return Err("GetRoles", ex); }
    }

    public async Task<APIGatewayProxyResponse> CreateRoleAsync(string? body)
    {
        var req = Parse<CreateRoleRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.Name))
            return FunctionBase.BadRequest("Role name is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var insCmd = QueryHelper.InsertRole(conn, req.Name);
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
            return FunctionBase.Ok(new { roleId }, "Role created.");
        }
        catch (Exception ex) { return Err("CreateRole", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateRoleAsync(string? body)
    {
        var req = Parse<UpdateRoleRequest>(body);
        if (req == null || req.RoleId <= 0)
            return FunctionBase.BadRequest("RoleId is required.");

        try
        {
            await using var conn   = await OpenAsync();
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
            return FunctionBase.Ok(null, "Role updated.");
        }
        catch (Exception ex) { return Err("UpdateRole", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteRoleAsync(string? body)
    {
        var req = Parse<DeleteRoleRequest>(body);
        if (req == null || req.RoleId <= 0)
            return FunctionBase.BadRequest("RoleId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.DeleteRole(conn, req.RoleId);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Role deleted.");
        }
        catch (Exception ex) { return Err("DeleteRole", ex); }
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
            return FunctionBase.Ok(await ReadTemplates(reader));
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
            return FunctionBase.Ok(await ReadTemplates(reader));
        }
        catch (Exception ex) { return Err("SearchTemplates", ex); }
    }

    public async Task<APIGatewayProxyResponse> CreateTemplateAsync(string? body)
    {
        var req = Parse<CreateTemplateRequest>(body);
        if (req == null || string.IsNullOrWhiteSpace(req.Name) || string.IsNullOrWhiteSpace(req.BodyText))
            return FunctionBase.BadRequest("Name and BodyText are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertTemplate(conn, req.Name, req.BodyText, req.UserId, req.Modality);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(new { id = cmd.LastInsertedId }, "Template created.");
        }
        catch (Exception ex) { return Err("CreateTemplate", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateTemplateAsync(string? body)
    {
        var req = Parse<UpdateTemplateRequest>(body);
        if (req == null || req.TemplateId <= 0)
            return FunctionBase.BadRequest("TemplateId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateTemplate(conn, req.TemplateId, req.Name, req.BodyText, req.Modality);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Template updated.");
        }
        catch (Exception ex) { return Err("UpdateTemplate", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteTemplateAsync(string? body)
    {
        var req = Parse<DeleteTemplateRequest>(body);
        if (req == null || req.TemplateId <= 0)
            return FunctionBase.BadRequest("TemplateId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.DeleteTemplate(conn, req.TemplateId);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Template deleted.");
        }
        catch (Exception ex) { return Err("DeleteTemplate", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetUserTemplatesAsync(string? body)
    {
        var req = Parse<UserIdRequest>(body);
        if (req == null || req.UserId <= 0)
            return FunctionBase.BadRequest("UserId is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetTemplates(conn, null, req.UserId);
            await using var reader = await cmd.ExecuteReaderAsync();
            return FunctionBase.Ok(await ReadTemplates(reader));
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
            return FunctionBase.Ok(users);
        }
        catch (Exception ex) { return Err("SearchReferrers", ex); }
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
            return FunctionBase.Ok(new DatatableResponse<CountryResponse>
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
            return FunctionBase.BadRequest("Name is required.");
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertCountry(conn, req.Name.Trim());
            var newId = Convert.ToInt32(await cmd.ExecuteScalarAsync());
            return FunctionBase.Ok(new CountryResponse { Id = newId, Name = req.Name.Trim() });
        }
        catch (Exception ex) { return Err("CreateCountry", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateCountryAsync(string? body)
    {
        var req = Parse<UpdateCountryRequest>(body);
        if (req == null || req.Id <= 0 || string.IsNullOrWhiteSpace(req.Name))
            return FunctionBase.BadRequest("Id and Name are required.");
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateCountry(conn, req.Id, req.Name.Trim());
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(new CountryResponse { Id = req.Id, Name = req.Name.Trim() });
        }
        catch (Exception ex) { return Err("UpdateCountry", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteCountryAsync(string? body)
    {
        var req = Parse<DeleteCountryRequest>(body);
        if (req == null || req.Id <= 0)
            return FunctionBase.BadRequest("Id is required.");
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.DeleteCountry(conn, req.Id);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Country deleted.");
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
                    Id          = r.GetInt32("id"),
                    Name        = Str(r, "name"),
                    CountryId   = r.GetInt32("countryid"),
                    CountryName = Str(r, "country_name")
                });
            return FunctionBase.Ok(new DatatableResponse<StateResponse>
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
            return FunctionBase.BadRequest("CountryId and Name are required.");
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertState(conn, req.CountryId, req.Name.Trim());
            var newId = Convert.ToInt32(await cmd.ExecuteScalarAsync());
            return FunctionBase.Ok(new StateResponse { Id = newId, CountryId = req.CountryId, Name = req.Name.Trim() });
        }
        catch (Exception ex) { return Err("CreateState", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateStateAsync(string? body)
    {
        var req = Parse<UpdateStateRequest>(body);
        if (req == null || req.Id <= 0 || string.IsNullOrWhiteSpace(req.Name))
            return FunctionBase.BadRequest("Id and Name are required.");
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateState(conn, req.Id, req.CountryId, req.Name.Trim());
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(new StateResponse { Id = req.Id, CountryId = req.CountryId, Name = req.Name.Trim() });
        }
        catch (Exception ex) { return Err("UpdateState", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteStateAsync(string? body)
    {
        var req = Parse<DeleteStateRequest>(body);
        if (req == null || req.Id <= 0)
            return FunctionBase.BadRequest("Id is required.");
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.DeleteState(conn, req.Id);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "State deleted.");
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
                    Id        = r.GetInt32("id"),
                    Name      = Str(r, "name"),
                    StateId   = r.GetInt32("stateid"),
                    StateName = Str(r, "state_name")
                });
            return FunctionBase.Ok(new DatatableResponse<CityResponse>
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
            return FunctionBase.BadRequest("StateId and Name are required.");
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertCity(conn, req.StateId, req.Name.Trim());
            var newId = Convert.ToInt32(await cmd.ExecuteScalarAsync());
            return FunctionBase.Ok(new CityResponse { Id = newId, StateId = req.StateId, Name = req.Name.Trim() });
        }
        catch (Exception ex) { return Err("CreateCity", ex); }
    }

    public async Task<APIGatewayProxyResponse> UpdateCityAsync(string? body)
    {
        var req = Parse<UpdateCityRequest>(body);
        if (req == null || req.Id <= 0 || string.IsNullOrWhiteSpace(req.Name))
            return FunctionBase.BadRequest("Id and Name are required.");
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpdateCity(conn, req.Id, req.StateId, req.Name.Trim());
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(new CityResponse { Id = req.Id, StateId = req.StateId, Name = req.Name.Trim() });
        }
        catch (Exception ex) { return Err("UpdateCity", ex); }
    }

    public async Task<APIGatewayProxyResponse> DeleteCityAsync(string? body)
    {
        var req = Parse<DeleteCityRequest>(body);
        if (req == null || req.Id <= 0)
            return FunctionBase.BadRequest("Id is required.");
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.DeleteCity(conn, req.Id);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "City deleted.");
        }
        catch (Exception ex) { return Err("DeleteCity", ex); }
    }
}
