using Amazon.Lambda.APIGatewayEvents;
using TeleRad.Shared;
using TeleRad.Shared.Model;
using TeleRad.Shared.Utility;

namespace TeleRad.Billing;

public class BillingHelper : HelperBase
{
    public BillingHelper(string connectionString, TokenValidationResult? token = null)
        : base(connectionString, token) { }

    // ══════════════════════════════════════════════════════════════════════════
    // BILLING / INVOICES
    // ══════════════════════════════════════════════════════════════════════════

    public async Task<APIGatewayProxyResponse> CreateTranscriberInvoiceAsync(string? body)
    {
        var req = Parse<CreateTranscriberInvoiceRequest>(body);
        if (req == null || req.TranscriberId <= 0 || req.ClientId <= 0)
            return FunctionBase.BadRequest("TranscriberId and ClientId are required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetStudiesForInvoice(conn, req.ClientId, req.TranscriberId, req.DateFrom, req.DateTo);
            await using var reader = await cmd.ExecuteReaderAsync();
            var groups = new Dictionary<string, int>();
            while (await reader.ReadAsync())
            {
                var mod = Str(reader, "modality");
                groups[mod] = groups.GetValueOrDefault(mod) + 1;
            }
            return FunctionBase.Ok(new { groups, dateFrom = req.DateFrom, dateTo = req.DateTo });
        }
        catch (Exception ex) { return Err("CreateTranscriberInvoice", ex); }
    }

    public async Task<APIGatewayProxyResponse> CreateTranscriberInvoiceStep2Async(string? body)
    {
        var req = Parse<CreateTranscriberInvoiceRequest>(body);
        if (req == null)
            return FunctionBase.BadRequest("Request body is required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetStudiesForInvoice(conn, req.ClientId, req.TranscriberId, req.DateFrom, req.DateTo);
            await using var reader = await cmd.ExecuteReaderAsync();
            var groups = new Dictionary<string, int>();
            while (await reader.ReadAsync())
            {
                var mod = Str(reader, "modality");
                groups[mod] = groups.GetValueOrDefault(mod) + 1;
            }
            await reader.CloseAsync();

            var lineItems = new List<InvoiceLineItemResponse>();
            decimal total = 0;
            foreach (var (mod, count) in groups)
            {
                await using var priceCmd = QueryHelper.GetModalityPriceForUser(conn, req.TranscriberId, "transcriber", mod);
                var priceObj             = await priceCmd.ExecuteScalarAsync();
                var price                = priceObj == null ? 0m : Convert.ToDecimal(priceObj);
                var lineTotal            = price * count;
                total                   += lineTotal;
                lineItems.Add(new InvoiceLineItemResponse { Modality = mod, Count = count, UnitPrice = price, Total = lineTotal });
            }
            return FunctionBase.Ok(new { lineItems, total, dateFrom = req.DateFrom, dateTo = req.DateTo });
        }
        catch (Exception ex) { return Err("CreateTranscriberInvoiceStep2", ex); }
    }

    public async Task<APIGatewayProxyResponse> SaveTranscriberInvoiceAsync(string? body)
    {
        var req = Parse<SaveTranscriberInvoiceRequest>(body);
        if (req == null || req.TranscriberId <= 0 || string.IsNullOrWhiteSpace(req.InvoiceNumber))
            return FunctionBase.BadRequest("TranscriberId and InvoiceNumber are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertTranscriberInvoice(conn, req.TranscriberId, req.ClientId,
                req.DateFrom, req.DateTo, req.TotalAmount, req.InvoiceNumber);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(new { invoiceId = cmd.LastInsertedId }, "Transcriber invoice saved.");
        }
        catch (Exception ex) { return Err("SaveTranscriberInvoice", ex); }
    }

    public async Task<APIGatewayProxyResponse> CreateClientInvoiceAsync(string? body)
    {
        var req = Parse<SaveClientInvoiceRequest>(body);
        if (req == null || req.ClientId <= 0)
            return FunctionBase.BadRequest("ClientId is required.");

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
            return FunctionBase.Ok(new { groups, dateFrom = req.DateFrom, dateTo = req.DateTo });
        }
        catch (Exception ex) { return Err("CreateClientInvoice", ex); }
    }

    public async Task<APIGatewayProxyResponse> SaveClientInvoiceAsync(string? body)
    {
        var req = Parse<SaveClientInvoiceRequest>(body);
        if (req == null || req.ClientId <= 0 || string.IsNullOrWhiteSpace(req.InvoiceNumber))
            return FunctionBase.BadRequest("ClientId and InvoiceNumber are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.InsertClientInvoice(conn, req.ClientId,
                req.DateFrom, req.DateTo, req.TotalAmount, req.InvoiceNumber);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(new { invoiceId = cmd.LastInsertedId }, "Client invoice saved.");
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
            return FunctionBase.Ok(invoices);
        }
        catch (Exception ex) { return Err("GetInvoicePayments", ex); }
    }

    public async Task<APIGatewayProxyResponse> GetClientPaymentsAsync(string? body)
        => await GetInvoicePaymentsAsync(body);

    public async Task<APIGatewayProxyResponse> GetTranscriberAnalyticsAsync(string? body)
    {
        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.GetChartPendingByTranscriber(conn, null);
            await using var r    = await cmd.ExecuteReaderAsync();
            var items = new List<ChartDataItem>();
            while (await r.ReadAsync())
                items.Add(new ChartDataItem { Label = Str(r, "label"), Total = r.GetInt32("total") });
            return FunctionBase.Ok(new ChartResponse { TotalRecords = items.Sum(i => i.Total), ChartData = items });
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
            return FunctionBase.Ok(new { totalBilled = total });
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
            return FunctionBase.BadRequest("UserId and UserCategory are required.");

        try
        {
            await using var conn   = await OpenAsync();
            await using var cmd    = QueryHelper.GetAllModalityPricesForUser(conn, req.UserId, req.UserCategory);
            await using var reader = await cmd.ExecuteReaderAsync();
            var prices = new List<ModalityPriceResponse>();
            while (await reader.ReadAsync())
                prices.Add(new ModalityPriceResponse
                {
                    Modality = Str(reader, "modality"),
                    Price    = reader.GetDecimal("price")
                });
            return FunctionBase.Ok(new PriceManagementResponse
            {
                UserId = req.UserId, UserCategory = req.UserCategory, ModalityPrices = prices
            });
        }
        catch (Exception ex) { return Err("GetPriceManagement", ex); }
    }

    public async Task<APIGatewayProxyResponse> SaveModalityPriceAsync(string? body)
    {
        var req = Parse<SaveModalityPriceRequest>(body);
        if (req == null || req.UserId <= 0 || string.IsNullOrWhiteSpace(req.Modality))
            return FunctionBase.BadRequest("UserId, UserCategory, and Modality are required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpsertModalityPrice(conn, req.UserId, req.UserCategory, req.Modality, req.Price);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Modality price saved.");
        }
        catch (Exception ex) { return Err("SaveModalityPrice", ex); }
    }

    public async Task<APIGatewayProxyResponse> SavePerPagePriceAsync(string? body)
    {
        var req = Parse<SavePerPagePriceRequest>(body);
        if (req == null || req.UserId <= 0)
            return FunctionBase.BadRequest("UserId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpsertPerPagePrice(conn, req.UserId, req.PricePerPage, req.Modality);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Per-page price saved.");
        }
        catch (Exception ex) { return Err("SavePerPagePrice", ex); }
    }

    public async Task<APIGatewayProxyResponse> SavePerCharPriceAsync(string? body)
    {
        var req = Parse<SavePerCharPriceRequest>(body);
        if (req == null || req.UserId <= 0)
            return FunctionBase.BadRequest("UserId is required.");

        try
        {
            await using var conn = await OpenAsync();
            await using var cmd  = QueryHelper.UpsertPerCharPrice(conn, req.UserId, req.PricePerChar);
            await cmd.ExecuteNonQueryAsync();
            return FunctionBase.Ok(null, "Per-character price saved.");
        }
        catch (Exception ex) { return Err("SavePerCharPrice", ex); }
    }
}
