using MySqlConnector;

namespace TeleRadLambda.Utility;

public static class QueryHelper
{
    // ══════════════════════════════════════════════════════════════════════════
    // AUTH / TOKEN
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand TokenValidation(MySqlConnection c, string token)
    {
        var cmd = Cmd(c, @"
            SELECT t.userid, t.username, u.usertype
            FROM tran_token t
            JOIN tran_user u ON u.id = t.userid
            WHERE t.token = @token LIMIT 1");
        cmd.Parameters.AddWithValue("@token", token);
        return cmd;
    }

    public static MySqlCommand FindUserForLogin(MySqlConnection c, string username)
    {
        var cmd = Cmd(c, @"
            SELECT id, displayname AS name, email, username, usertype, password, block AS status
            FROM tran_user
            WHERE (email = @u OR username = @u) AND block = 0 LIMIT 1");
        cmd.Parameters.AddWithValue("@u", username);
        return cmd;
    }

    public static MySqlCommand UpsertToken(MySqlConnection c, int userId, string username, string token)
    {
        var cmd = Cmd(c, @"
            INSERT INTO tran_token (userid, username, token, created_at, updated_at)
            VALUES (@uid, @un, @tok, NOW(), NOW())
            ON DUPLICATE KEY UPDATE token = @tok, updated_at = NOW()");
        cmd.Parameters.AddWithValue("@uid", userId);
        cmd.Parameters.AddWithValue("@un", username);
        cmd.Parameters.AddWithValue("@tok", token);
        return cmd;
    }

    public static MySqlCommand DeleteToken(MySqlConnection c, string token)
    {
        var cmd = Cmd(c, "DELETE FROM tran_token WHERE token = @token");
        cmd.Parameters.AddWithValue("@token", token);
        return cmd;
    }

    public static MySqlCommand GetUserPermissions(MySqlConnection c, int userId)
    {
        var cmd = Cmd(c, @"
            SELECT p.name
            FROM permissions p
            JOIN model_has_permissions mp ON mp.permission_id = p.id
            WHERE mp.model_id = @uid AND mp.model_type = 'App\\User'
            UNION
            SELECT p2.name
            FROM permissions p2
            JOIN role_has_permissions rp ON rp.permission_id = p2.id
            JOIN model_has_roles mr ON mr.role_id = rp.role_id
            WHERE mr.model_id = @uid AND mr.model_type = 'App\\User'");
        cmd.Parameters.AddWithValue("@uid", userId);
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // STUDIES  (tran_typewordlist)
    // Column map: patient_name=pname, patient_dob=dob, accession_number=access_number,
    //             status=type, transcriber_id=trans_id, template_id=templateid,
    //             is_stat=stat, impression=impression_text, pdf_path=pdf_report,
    //             updated_at=report_date (closest available date)
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetStudies(MySqlConnection c,
        string? dos, string? clientName, string? clientNameExcept,
        string? search, string? statusList, string? modalityList,
        int? scopeUserId, int? userType)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = @"
            SELECT s.id, CAST(s.id AS CHAR) AS order_number, s.pname AS patient_name, s.dob AS patient_dob,
                   s.access_number AS accession_number,
                   s.modality, s.type AS status, s.dos,
                   ref_u.displayname AS client_name, s.ref_id, s.rad_id,
                   s.trans_id AS transcriber_id, s.templateid AS template_id,
                   s.stat AS is_stat, s.report_date AS updated_at,
                   r.displayname AS rad_name, t.displayname AS transcriber_name,
                   (SELECT COUNT(*) FROM tran_attachment a
                    WHERE a.exam_id = s.id AND a.attachment_type = 'audio') AS dictation_received,
                   (SELECT COUNT(*) FROM tran_attachment a2
                    WHERE a2.exam_id = s.id) AS attached_documents
            FROM tran_typewordlist s
            LEFT JOIN tran_user r     ON r.id = s.rad_id
            LEFT JOIN tran_user t     ON t.id = s.trans_id
            LEFT JOIN tran_user ref_u ON ref_u.id = s.ref_id
            WHERE s.`delete` = 0";

        if (!string.IsNullOrEmpty(dos))
        {
            sql += " AND DATE(s.dos) = @dos";
            cmd.Parameters.AddWithValue("@dos", dos);
        }
        if (!string.IsNullOrEmpty(clientName))
        {
            sql += " AND ref_u.displayname LIKE @cn";
            cmd.Parameters.AddWithValue("@cn", $"%{clientName}%");
        }
        if (!string.IsNullOrEmpty(clientNameExcept))
        {
            sql += " AND (ref_u.id IS NULL OR ref_u.displayname NOT LIKE @cne)";
            cmd.Parameters.AddWithValue("@cne", $"%{clientNameExcept}%");
        }
        if (!string.IsNullOrEmpty(search))
        {
            sql += " AND (s.pname LIKE @q OR s.access_number LIKE @q)";
            cmd.Parameters.AddWithValue("@q", $"%{search}%");
        }
        if (!string.IsNullOrEmpty(statusList))
        {
            var plist = AddList(cmd, statusList.Split(','), "st");
            sql += $" AND s.type IN ({plist})";
        }
        if (!string.IsNullOrEmpty(modalityList))
        {
            var plist = AddList(cmd, modalityList.Split(','), "md");
            sql += $" AND s.modality IN ({plist})";
        }
        if (userType == 2 && scopeUserId.HasValue)
        {
            sql += " AND s.rad_id = @scopeId";
            cmd.Parameters.AddWithValue("@scopeId", scopeUserId.Value);
        }
        else if (userType == 3 && scopeUserId.HasValue)
        {
            sql += " AND (s.trans_id = @scopeId OR s.trans_id = (SELECT id FROM tran_user WHERE username = 'Dictation' LIMIT 1))";
            cmd.Parameters.AddWithValue("@scopeId", scopeUserId.Value);
        }
        else if (userType == 4 && scopeUserId.HasValue)
        {
            sql += " AND s.ref_id = @scopeId";
            cmd.Parameters.AddWithValue("@scopeId", scopeUserId.Value);
        }

        sql += " ORDER BY s.dos DESC LIMIT 500";
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand GetStudyById(MySqlConnection c, int id)
    {
        var cmd = Cmd(c, @"
            SELECT s.*, r.displayname AS rad_name, t.displayname AS transcriber_name,
                   ref_u.displayname AS client_name
            FROM tran_typewordlist s
            LEFT JOIN tran_user r     ON r.id = s.rad_id
            LEFT JOIN tran_user t     ON t.id = s.trans_id
            LEFT JOIN tran_user ref_u ON ref_u.id = s.ref_id
            WHERE s.id = @id LIMIT 1");
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    public static MySqlCommand UpdateStudyStatus(MySqlConnection c, int studyId, string status)
    {
        var cmd = Cmd(c, "UPDATE tran_typewordlist SET type = @status WHERE id = @id");
        cmd.Parameters.AddWithValue("@status", status);
        cmd.Parameters.AddWithValue("@id", studyId);
        return cmd;
    }

    public static MySqlCommand UpdateStudy(MySqlConnection c, int studyId,
        string? patientName, string? dob, string? dos, string? modality,
        string? description, string? status, int? transcriberId, int? radId, int? templateId)
    {
        var sets = new List<string>();
        var cmd = new MySqlCommand { Connection = c };
        if (patientName != null)   { sets.Add("pname = @pn");         cmd.Parameters.AddWithValue("@pn", patientName); }
        if (dob != null)           { sets.Add("dob = @dob");          cmd.Parameters.AddWithValue("@dob", dob); }
        if (dos != null)           { sets.Add("dos = @dos");          cmd.Parameters.AddWithValue("@dos", dos); }
        if (modality != null)      { sets.Add("modality = @mod");     cmd.Parameters.AddWithValue("@mod", modality); }
        if (description != null)   { sets.Add("description = @desc"); cmd.Parameters.AddWithValue("@desc", description); }
        if (status != null)        { sets.Add("type = @status");      cmd.Parameters.AddWithValue("@status", status); }
        if (transcriberId != null) { sets.Add("trans_id = @tid");     cmd.Parameters.AddWithValue("@tid", transcriberId); }
        if (radId != null)         { sets.Add("rad_id = @rid");       cmd.Parameters.AddWithValue("@rid", radId); }
        if (templateId != null)    { sets.Add("templateid = @tpid");  cmd.Parameters.AddWithValue("@tpid", templateId); }

        if (sets.Count == 0) { cmd.CommandText = "SELECT 1"; return cmd; }
        cmd.CommandText = $"UPDATE tran_typewordlist SET {string.Join(", ", sets)} WHERE id = @sid";
        cmd.Parameters.AddWithValue("@sid", studyId);
        return cmd;
    }

    public static MySqlCommand MarkStudiesStat(MySqlConnection c, string idParams)
    {
        return Cmd(c, $"UPDATE tran_typewordlist SET stat = 1 WHERE id IN ({idParams})");
    }

    public static MySqlCommand UnmarkStudiesStat(MySqlConnection c, string idParams)
    {
        return Cmd(c, $"UPDATE tran_typewordlist SET stat = 0 WHERE id IN ({idParams})");
    }

    public static MySqlCommand CloneStudy(MySqlConnection c, int studyId)
    {
        var cmd = Cmd(c, @"
            INSERT INTO tran_typewordlist
                (pom_id, rad_id, trans_id, type, ordering_physician, location, pname, firstname,
                 middlename, lastname, gender, dob, idnumber, access_number, dos, dos_time_from,
                 dos_time_to, modalityid, examid, cpt, covered, client_location, templateid,
                 document_type, pay_status, report_date, dictation_status, dictating_trans,
                 dictating_time, audio_name, report_date_time, report_text, impression_text,
                 report_text_addendum, report_text_signature, addendum_text_signature, is_addendum,
                 addendum_text_temp, pdf_report, pdf_page, type_rad, transfer_id,
                 incoming_order_patient_id, incoming_order_exam_id, trans_new_message_time,
                 stat, `lock`, ip, `archive`, `delete`, modality, description, sid, ref_id, report_key_image)
            SELECT pom_id, rad_id, trans_id, 'new study', ordering_physician, location, pname, firstname,
                 middlename, lastname, gender, dob, idnumber, access_number, dos, dos_time_from,
                 dos_time_to, modalityid, examid, cpt, covered, client_location, templateid,
                 document_type, pay_status, CURDATE(), dictation_status, dictating_trans,
                 dictating_time, audio_name, '', '', '',
                 '', '', '', 0, '', '', 0, type_rad, 0, 0, 0, NOW(),
                 0, 0, ip, `archive`, 0, modality, description, sid, ref_id, ''
            FROM tran_typewordlist WHERE id = @id");
        cmd.Parameters.AddWithValue("@id", studyId);
        return cmd;
    }

    public static MySqlCommand GetStudyAudit(MySqlConnection c, int studyId)
    {
        var cmd = Cmd(c, @"
            SELECT a.id, a.status_insert AS action, a.status AS description,
                   u.displayname AS username, a.curr_date_time AS created_at
            FROM tran_audit a
            LEFT JOIN tran_user u ON u.id = a.userid
            WHERE a.eid = @sid ORDER BY a.curr_date_time DESC");
        cmd.Parameters.AddWithValue("@sid", studyId);
        return cmd;
    }

    public static MySqlCommand InsertAudit(MySqlConnection c, int studyId, int userId, string action, string? description)
    {
        var cmd = Cmd(c, @"
            INSERT INTO tran_audit (eid, userid, status_insert, status, curr_date_time, curr_date)
            VALUES (@sid, @uid, @action, @desc, NOW(), NOW())");
        cmd.Parameters.AddWithValue("@sid", studyId);
        cmd.Parameters.AddWithValue("@uid", userId);
        cmd.Parameters.AddWithValue("@action", action);
        cmd.Parameters.AddWithValue("@desc", description ?? "");
        return cmd;
    }

    public static MySqlCommand AuditAlreadyExists(MySqlConnection c, int studyId, string action)
    {
        var cmd = Cmd(c, @"
            SELECT COUNT(*) FROM tran_audit
            WHERE eid = @sid AND status_insert = @action");
        cmd.Parameters.AddWithValue("@sid", studyId);
        cmd.Parameters.AddWithValue("@action", action);
        return cmd;
    }

    public static MySqlCommand UpdateStudyReport(MySqlConnection c, int studyId,
        string? name, string? dob, string? dos, string? modality, string? desc)
    {
        var sets = new List<string>();
        var cmd = new MySqlCommand { Connection = c };
        if (name != null)     { sets.Add("pname = @pn");        cmd.Parameters.AddWithValue("@pn", name); }
        if (dob != null)      { sets.Add("dob = @dob");         cmd.Parameters.AddWithValue("@dob", dob); }
        if (dos != null)      { sets.Add("dos = @dos");         cmd.Parameters.AddWithValue("@dos", dos); }
        if (modality != null) { sets.Add("modality = @mod");    cmd.Parameters.AddWithValue("@mod", modality); }
        if (desc != null)     { sets.Add("description = @desc");cmd.Parameters.AddWithValue("@desc", desc); }

        if (sets.Count == 0) { cmd.CommandText = "SELECT 1"; return cmd; }
        cmd.CommandText = $"UPDATE tran_typewordlist SET {string.Join(", ", sets)} WHERE id = @sid";
        cmd.Parameters.AddWithValue("@sid", studyId);
        return cmd;
    }

    public static MySqlCommand GetStudyAttachmentCount(MySqlConnection c, int studyId, string type)
    {
        var cmd = Cmd(c, "SELECT COUNT(*) FROM tran_attachment WHERE exam_id = @sid AND attachment_type = @type");
        cmd.Parameters.AddWithValue("@sid", studyId);
        cmd.Parameters.AddWithValue("@type", type);
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // NOTES  (tran_notes)
    // Column map: typewordlist_id=exam_id, user_id=userid, created_at=curr_date
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetNotes(MySqlConnection c, int studyId)
    {
        var cmd = Cmd(c, @"
            SELECT n.id, u.displayname AS username, n.notes, n.curr_date AS created_at
            FROM tran_notes n
            LEFT JOIN tran_user u ON u.id = n.userid
            WHERE n.exam_id = @sid ORDER BY n.curr_date DESC");
        cmd.Parameters.AddWithValue("@sid", studyId);
        return cmd;
    }

    public static MySqlCommand InsertNote(MySqlConnection c, int studyId, int userId, string notes)
    {
        var cmd = Cmd(c, @"
            INSERT INTO tran_notes (exam_id, userid, notes, curr_date)
            VALUES (@sid, @uid, @notes, NOW())");
        cmd.Parameters.AddWithValue("@sid", studyId);
        cmd.Parameters.AddWithValue("@uid", userId);
        cmd.Parameters.AddWithValue("@notes", notes);
        return cmd;
    }

    public static MySqlCommand UpdateNote(MySqlConnection c, int noteId, string notes)
    {
        var cmd = Cmd(c, "UPDATE tran_notes SET notes = @notes, curr_date = NOW() WHERE id = @id");
        cmd.Parameters.AddWithValue("@notes", notes);
        cmd.Parameters.AddWithValue("@id", noteId);
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // TEMPLATES  (tran_template)
    // Column map: name=temp_name, body_text=heading_text, user_id=userid (varchar)
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetTemplates(MySqlConnection c, string? search, int? userId)
    {
        var sql = @"
            SELECT t.id, t.temp_name AS name, t.heading_text AS body_text,
                   '' AS modality,
                   t.userid AS user_id, u.displayname AS user_name,
                   '' AS created_at
            FROM tran_template t
            LEFT JOIN tran_user u ON u.id = CAST(t.userid AS UNSIGNED)
            WHERE 1=1";
        var cmd = new MySqlCommand { Connection = c };
        if (!string.IsNullOrEmpty(search))
        {
            sql += " AND t.temp_name LIKE @search";
            cmd.Parameters.AddWithValue("@search", $"%{search}%");
        }
        if (userId.HasValue)
        {
            sql += " AND t.userid = @uid";
            cmd.Parameters.AddWithValue("@uid", userId.Value.ToString());
        }
        sql += " ORDER BY t.temp_name ASC";
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand InsertTemplate(MySqlConnection c, string name, string bodyText, int? userId, string? modality)
    {
        var cmd = Cmd(c, @"
            INSERT INTO tran_template
                (temp_name, heading_text, userid, radio_id, header_image, board_certify, footer_image, signature_text)
            VALUES (@name, @body, @uid, '', '', '', '', '')");
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@body", bodyText);
        cmd.Parameters.AddWithValue("@uid", userId?.ToString() ?? "0");
        return cmd;
    }

    public static MySqlCommand UpdateTemplate(MySqlConnection c, int id, string? name, string? body, string? modality)
    {
        var sets = new List<string>();
        var cmd = new MySqlCommand { Connection = c };
        if (name != null) { sets.Add("temp_name = @name");      cmd.Parameters.AddWithValue("@name", name); }
        if (body != null) { sets.Add("heading_text = @body");   cmd.Parameters.AddWithValue("@body", body); }
        if (sets.Count == 0) { cmd.CommandText = "SELECT 1"; return cmd; }
        cmd.CommandText = $"UPDATE tran_template SET {string.Join(", ", sets)} WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    public static MySqlCommand DeleteTemplate(MySqlConnection c, int id)
    {
        var cmd = Cmd(c, "DELETE FROM tran_template WHERE id = @id");
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // USERS  (tran_user)
    // Column map: name=displayname, status=block (block=0 active, block=1 blocked),
    //             country_id=country, state_id=state, city_id=city,
    //             is_billable=billable, address=office, medical_fee=medical_director_fee
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetUsers(MySqlConnection c, int? userType, string? search)
    {
        var sql = @"
            SELECT u.id, u.displayname AS name, u.email, u.username, u.usertype,
                   u.block AS status, u.phone, u.office AS address
            FROM tran_user u WHERE u.block = 0";
        var cmd = new MySqlCommand { Connection = c };
        if (userType.HasValue)
        {
            sql += " AND u.usertype = @ut";
            cmd.Parameters.AddWithValue("@ut", userType.Value);
        }
        if (!string.IsNullOrEmpty(search))
        {
            sql += " AND (u.displayname LIKE @s OR u.username LIKE @s OR u.email LIKE @s)";
            cmd.Parameters.AddWithValue("@s", $"%{search}%");
        }
        sql += " ORDER BY u.displayname ASC";
        cmd.CommandText = sql;
        return cmd;
    }

    // ── Client Lookup (paginated, referrer users with firstname/lastname/fax) ────
    public static MySqlCommand GetClientLookup(MySqlConnection c, string? search, int offset, int perPage)
    {
        var sql = @"
            SELECT u.id, u.username, u.email, u.firstname, u.lastname, u.displayname AS name,
                   u.usertype, u.block AS status, u.phone, u.fax, u.office AS address
            FROM tran_user u
            WHERE u.usertype = 4";
        var cmd = new MySqlCommand { Connection = c };
        if (!string.IsNullOrEmpty(search))
        {
            sql += @" AND (u.username LIKE @s OR u.firstname LIKE @s OR u.lastname LIKE @s
                          OR u.displayname LIKE @s OR u.email LIKE @s)";
            cmd.Parameters.AddWithValue("@s", $"%{search}%");
        }
        sql += " LIMIT @lim OFFSET @off";
        cmd.Parameters.AddWithValue("@lim", perPage);
        cmd.Parameters.AddWithValue("@off", offset);
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand CountClientLookup(MySqlConnection c, string? search)
    {
        var sql = "SELECT COUNT(*) FROM tran_user u WHERE u.usertype = 4 AND u.block = 0";
        var cmd = new MySqlCommand { Connection = c };
        if (!string.IsNullOrEmpty(search))
        {
            sql += @" AND (u.username LIKE @s OR u.firstname LIKE @s OR u.lastname LIKE @s
                          OR u.displayname LIKE @s OR u.email LIKE @s)";
            cmd.Parameters.AddWithValue("@s", $"%{search}%");
        }
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand GetUserById(MySqlConnection c, int id)
    {
        var cmd = Cmd(c, @"
            SELECT u.id, u.username, u.email, u.firstname, u.lastname, u.displayname,
                   u.phone, u.fax, u.office, u.notes,
                   u.country, u.state, u.city,
                   u.default_transcriber, u.auto_sign,
                   u.usertype, u.block,
                   co.name  AS j_country_name,
                   st.name  AS j_state_name,
                   ci.name  AS j_city_name,
                   dt.displayname AS j_transcriber_name
            FROM tran_user u
            LEFT JOIN tran_country co ON co.id = u.country
            LEFT JOIN tran_state   st ON st.id  = u.state
            LEFT JOIN tran_city    ci ON ci.id   = u.city
            LEFT JOIN tran_user    dt ON dt.id   = u.default_transcriber
            WHERE u.id = @id LIMIT 1");
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    public static MySqlCommand UpdateClientInfo(MySqlConnection c, int id,
        string? firstName, string? lastName, string? displayName, string? email,
        string? phone, string? fax, string? address, string? notes,
        int? countryId, int? stateId, int? cityId, int? defaultTranscriberId, string? username = null)
    {
        var sets = new List<string>();
        var cmd  = new MySqlCommand { Connection = c };
        void Add(string col, string param, object? val) { sets.Add($"{col} = @{param}"); cmd.Parameters.AddWithValue($"@{param}", val ?? (object)DBNull.Value); }
        if (firstName != null)            Add("firstname",           "fn",  firstName);
        if (lastName != null)             Add("lastname",            "ln",  lastName);
        if (displayName != null)          Add("displayname",         "dn",  displayName);
        if (email != null)                Add("email",               "em",  email);
        if (phone != null)                Add("phone",               "ph",  phone);
        if (fax != null)                  Add("fax",                 "fx",  fax);
        if (address != null)              Add("office",              "adr", address);
        if (notes != null)                Add("notes",               "nt",  notes);
        if (countryId != null)            Add("country",             "cid", countryId);
        if (stateId != null)              Add("state",               "sid", stateId);
        if (cityId != null)               Add("city",                "ciid",cityId);
        if (defaultTranscriberId != null) Add("default_transcriber", "dtid",defaultTranscriberId);
        if (username != null)             Add("username",            "un",  username);
        if (sets.Count == 0) { cmd.CommandText = "SELECT 1"; return cmd; }
        cmd.CommandText = $"UPDATE tran_user SET {string.Join(", ", sets)} WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    public static MySqlCommand InsertUser(MySqlConnection c, string name, string email,
        string username, string passwordHash, int userType, string? phone, string? address,
        int? countryId, int? stateId, int? cityId)
    {
        var cmd = Cmd(c, @"
            INSERT INTO tran_user
                (firstname, middlename, lastname, displayname, email, username, password, usertype,
                 phone, country, state, city, block, office, signature, npi, taxid, group_practice,
                 auto_sign, default_transcriber, altphone, fax, pin, company_name, notes, curr_date)
            VALUES ('', '', '', @name, @email, @un, @pwd, @ut,
                    COALESCE(@phone,''), COALESCE(@cid,0), COALESCE(@sid,0), COALESCE(@ciid,0),
                    0, '', '', '', '', '', '', 0, '', '', '', '', '', NOW())");
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@email", email);
        cmd.Parameters.AddWithValue("@un", username);
        cmd.Parameters.AddWithValue("@pwd", passwordHash);
        cmd.Parameters.AddWithValue("@ut", userType);
        cmd.Parameters.AddWithValue("@phone", (object?)phone ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@cid", (object?)countryId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@sid", (object?)stateId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ciid", (object?)cityId ?? DBNull.Value);
        return cmd;
    }

    public static MySqlCommand UpdateUser(MySqlConnection c, int id, string? name, string? email,
        string? username, string? phone, string? address, int? countryId, int? stateId, int? cityId)
    {
        var sets = new List<string>();
        var cmd = new MySqlCommand { Connection = c };
        if (name != null)      { sets.Add("displayname = @name"); cmd.Parameters.AddWithValue("@name", name); }
        if (email != null)     { sets.Add("email = @email");      cmd.Parameters.AddWithValue("@email", email); }
        if (username != null)  { sets.Add("username = @un");      cmd.Parameters.AddWithValue("@un", username); }
        if (phone != null)     { sets.Add("phone = @phone");      cmd.Parameters.AddWithValue("@phone", phone); }
        if (countryId != null) { sets.Add("country = @cid");      cmd.Parameters.AddWithValue("@cid", countryId); }
        if (stateId != null)   { sets.Add("state = @sid");        cmd.Parameters.AddWithValue("@sid", stateId); }
        if (cityId != null)    { sets.Add("city = @ciid");        cmd.Parameters.AddWithValue("@ciid", cityId); }
        if (sets.Count == 0)   { cmd.CommandText = "SELECT 1"; return cmd; }
        cmd.CommandText = $"UPDATE tran_user SET {string.Join(", ", sets)} WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    public static MySqlCommand UpdateUserPassword(MySqlConnection c, int id, string passwordHash)
    {
        var cmd = Cmd(c, "UPDATE tran_user SET password = @pwd, is_password_reset = '0' WHERE id = @id");
        cmd.Parameters.AddWithValue("@pwd", passwordHash);
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    public static MySqlCommand GetUserPassword(MySqlConnection c, int id)
    {
        var cmd = Cmd(c, "SELECT password FROM tran_user WHERE id = @id LIMIT 1");
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    public static MySqlCommand ChangeUserStatus(MySqlConnection c, int id, int status)
    {
        // status 1 = active → block = 0; status 0 = disabled → block = 1
        var cmd = Cmd(c, "UPDATE tran_user SET block = @block WHERE id = @id");
        cmd.Parameters.AddWithValue("@block", status == 1 ? 0 : 1);
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    public static MySqlCommand ChangeBillingStatus(MySqlConnection c, int id, int billable)
    {
        var cmd = Cmd(c, "UPDATE tran_user SET billable = @billable WHERE id = @id");
        cmd.Parameters.AddWithValue("@billable", billable);
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    public static MySqlCommand GetUserRolePermissions(MySqlConnection c, int userId)
    {
        var cmd = Cmd(c, @"
            SELECT p.name FROM permissions p
            JOIN role_has_permissions rp ON rp.permission_id = p.id
            JOIN model_has_roles mr ON mr.role_id = rp.role_id
            WHERE mr.model_id = @uid AND mr.model_type = 'App\\User'");
        cmd.Parameters.AddWithValue("@uid", userId);
        return cmd;
    }

    public static MySqlCommand DeleteUserPermissions(MySqlConnection c, int userId)
    {
        var cmd = Cmd(c, "DELETE FROM model_has_permissions WHERE model_id = @uid AND model_type = 'App\\\\User'");
        cmd.Parameters.AddWithValue("@uid", userId);
        return cmd;
    }

    public static MySqlCommand InsertUserPermission(MySqlConnection c, int userId, int permissionId)
    {
        var cmd = Cmd(c, @"
            INSERT IGNORE INTO model_has_permissions (permission_id, model_type, model_id)
            VALUES (@pid, 'App\\\\User', @uid)");
        cmd.Parameters.AddWithValue("@pid", permissionId);
        cmd.Parameters.AddWithValue("@uid", userId);
        return cmd;
    }

    public static MySqlCommand GetPermissionByName(MySqlConnection c, string name)
    {
        var cmd = Cmd(c, "SELECT id FROM permissions WHERE name = @name LIMIT 1");
        cmd.Parameters.AddWithValue("@name", name);
        return cmd;
    }

    public static MySqlCommand UpdateMedicalFee(MySqlConnection c, int userId, string? director, decimal? fee)
    {
        // medical_director is tinyint (0/1 flag), medical_director_fee is the amount
        var cmd = Cmd(c, "UPDATE tran_user SET medical_director = @dir, medical_director_fee = @fee WHERE id = @id");
        cmd.Parameters.AddWithValue("@dir", string.IsNullOrEmpty(director) ? 0 : 1);
        cmd.Parameters.AddWithValue("@fee", (object?)fee ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@id", userId);
        return cmd;
    }

    public static MySqlCommand GetCountries(MySqlConnection c, string? search)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = "SELECT id, name FROM tran_country WHERE 1=1";
        if (!string.IsNullOrEmpty(search)) { sql += " AND name LIKE @s"; cmd.Parameters.AddWithValue("@s", $"%{search}%"); }
        sql += " ORDER BY name ASC LIMIT 100";
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand GetStates(MySqlConnection c, int? countryId, string? search)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = "SELECT id, name FROM tran_state WHERE 1=1";
        if (countryId.HasValue) { sql += " AND countryid = @cid"; cmd.Parameters.AddWithValue("@cid", countryId.Value); }
        if (!string.IsNullOrEmpty(search)) { sql += " AND name LIKE @s"; cmd.Parameters.AddWithValue("@s", $"%{search}%"); }
        sql += " ORDER BY name ASC LIMIT 100";
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand GetCities(MySqlConnection c, int? stateId, string? search)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = "SELECT id, name FROM tran_city WHERE 1=1";
        if (stateId.HasValue) { sql += " AND stateid = @sid"; cmd.Parameters.AddWithValue("@sid", stateId.Value); }
        if (!string.IsNullOrEmpty(search)) { sql += " AND name LIKE @s"; cmd.Parameters.AddWithValue("@s", $"%{search}%"); }
        sql += " ORDER BY name ASC LIMIT 100";
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand GetUserTypes(MySqlConnection c, string? search)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = "SELECT id, name FROM tran_user_type WHERE 1=1";
        if (!string.IsNullOrEmpty(search)) { sql += " AND name LIKE @s"; cmd.Parameters.AddWithValue("@s", $"%{search}%"); }
        sql += " ORDER BY name ASC";
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand GetUserEmployees(MySqlConnection c, int adminId)
    {
        var cmd = Cmd(c, "SELECT transcriber_id AS employee_id FROM transadmin_employees WHERE admin_id = @aid");
        cmd.Parameters.AddWithValue("@aid", adminId);
        return cmd;
    }

    public static MySqlCommand DeleteUserEmployees(MySqlConnection c, int adminId)
    {
        var cmd = Cmd(c, "DELETE FROM transadmin_employees WHERE admin_id = @aid");
        cmd.Parameters.AddWithValue("@aid", adminId);
        return cmd;
    }

    public static MySqlCommand InsertUserEmployee(MySqlConnection c, int adminId, int employeeId)
    {
        var cmd = Cmd(c, @"
            INSERT INTO transadmin_employees (admin_id, transcriber_id, created_at, updated_at)
            VALUES (@aid, @eid, NOW(), NOW())");
        cmd.Parameters.AddWithValue("@aid", adminId);
        cmd.Parameters.AddWithValue("@eid", employeeId);
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // ATTACHMENTS / AUDIO  (tran_attachment)
    // Column map: typewordlist_id=exam_id, file_name=attachment, file_path=attachment,
    //             type=attachment_type, created_at=curr_date
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand InsertAttachment(MySqlConnection c, int studyId, string fileName, string filePath, string fileType)
    {
        var cmd = Cmd(c, @"
            INSERT INTO tran_attachment (exam_id, attachment, attachment_type, curr_date, del)
            VALUES (@sid, @fp, @ft, NOW(), '0')");
        cmd.Parameters.AddWithValue("@sid", studyId);
        cmd.Parameters.AddWithValue("@fp", filePath);
        cmd.Parameters.AddWithValue("@ft", fileType);
        return cmd;
    }

    public static MySqlCommand GetAttachedFiles(MySqlConnection c, int studyId, string? fileType)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = @"
            SELECT id, exam_id AS typewordlist_id, attachment AS file_name,
                   attachment AS file_path, attachment_type AS type, curr_date AS created_at
            FROM tran_attachment WHERE exam_id = @sid AND del = '0'";
        if (!string.IsNullOrEmpty(fileType)) { sql += " AND attachment_type = @ft"; cmd.Parameters.AddWithValue("@ft", fileType); }
        sql += " ORDER BY curr_date DESC";
        cmd.Parameters.AddWithValue("@sid", studyId);
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand GetSentAudioFiles(MySqlConnection c, string? dos, int? clientId, int? radId)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = @"
            SELECT s.id, s.firstname, s.lastname, s.dob,
                   s.incoming_order_exam_id,
                   ref_u.displayname AS client_name,
                   t.displayname AS transcriber_name,
                   tat.curr_date AS curr_date
            FROM tran_typewordlist s
            LEFT JOIN tran_user ref_u ON ref_u.id = s.ref_id
            LEFT JOIN tran_user t     ON t.id = s.trans_id
            LEFT JOIN tran_attachment tat
                   ON tat.exam_id = s.incoming_order_exam_id
                  AND tat.patient_id = s.incoming_order_patient_id
            WHERE s.type_rad = 'send audio to transcription' AND s.`delete` = 0";
        if (radId.HasValue)    { sql += " AND s.rad_id = @rid"; cmd.Parameters.AddWithValue("@rid", radId.Value); }
        if (clientId.HasValue) { sql += " AND s.ref_id = @cid"; cmd.Parameters.AddWithValue("@cid", clientId.Value); }
        sql += " ORDER BY s.id DESC LIMIT 200";
        cmd.CommandText = sql;
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // AUDIT LOG  (tran_audit)
    // Column map: typewordlist_id=eid, user_id=userid, action=status_insert,
    //             description=status, created_at=curr_date_time
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetAuditAll(MySqlConnection c, string? search, string? dateFrom, string? dateTo, int offset, int perPage)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = @"
            SELECT a.id, a.eid AS typewordlist_id, a.status_insert AS action,
                   a.status AS description, u.displayname AS username, a.curr_date_time AS created_at
            FROM tran_audit a
            LEFT JOIN tran_user u ON u.id = a.userid
            WHERE 1=1";
        if (!string.IsNullOrEmpty(search))   { sql += " AND (a.status_insert LIKE @s OR a.status LIKE @s OR u.displayname LIKE @s)"; cmd.Parameters.AddWithValue("@s", $"%{search}%"); }
        if (!string.IsNullOrEmpty(dateFrom)) { sql += " AND DATE(a.curr_date_time) >= @df"; cmd.Parameters.AddWithValue("@df", dateFrom); }
        if (!string.IsNullOrEmpty(dateTo))   { sql += " AND DATE(a.curr_date_time) <= @dt"; cmd.Parameters.AddWithValue("@dt", dateTo); }
        sql += " ORDER BY a.curr_date_time DESC LIMIT @limit OFFSET @offset";
        cmd.Parameters.AddWithValue("@limit", perPage);
        cmd.Parameters.AddWithValue("@offset", offset);
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand CountAuditAll(MySqlConnection c, string? search, string? dateFrom, string? dateTo)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = "SELECT COUNT(*) FROM tran_audit a LEFT JOIN tran_user u ON u.id = a.userid WHERE 1=1";
        if (!string.IsNullOrEmpty(search))   { sql += " AND (a.status_insert LIKE @s OR u.displayname LIKE @s)"; cmd.Parameters.AddWithValue("@s", $"%{search}%"); }
        if (!string.IsNullOrEmpty(dateFrom)) { sql += " AND DATE(a.curr_date_time) >= @df"; cmd.Parameters.AddWithValue("@df", dateFrom); }
        if (!string.IsNullOrEmpty(dateTo))   { sql += " AND DATE(a.curr_date_time) <= @dt"; cmd.Parameters.AddWithValue("@dt", dateTo); }
        cmd.CommandText = sql;
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // FAX  (fax_sent, fax_receiveds — schema not in dump, kept as-is)
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand InsertFaxSent(MySqlConnection c, string faxNumber, string fileName, int? studyId)
    {
        var cmd = Cmd(c, @"
            INSERT INTO tran_faxed_doc (fax, attachment, sid)
            VALUES (@fax, @fn, @sid)");
        cmd.Parameters.AddWithValue("@fax", faxNumber);
        cmd.Parameters.AddWithValue("@fn", fileName);
        cmd.Parameters.AddWithValue("@sid", (object?)studyId ?? DBNull.Value);
        return cmd;
    }

    public static MySqlCommand GetSentFaxes(MySqlConnection c, int offset, int perPage)
    {
        var cmd = Cmd(c, @"
            SELECT d.id, d.fax AS fax_number, d.attachment AS file_name,
                   f.status, f.created_at, f.delevered_at
            FROM tran_faxed_doc d
            JOIN fax_sent f ON f.fax_sid = d.sid
            ORDER BY d.id DESC LIMIT @limit OFFSET @offset");
        cmd.Parameters.AddWithValue("@limit", perPage);
        cmd.Parameters.AddWithValue("@offset", offset);
        return cmd;
    }

    public static MySqlCommand GetInboundFaxes(MySqlConnection c, int offset, int perPage)
    {
        var cmd = Cmd(c, @"
            SELECT fr.id, fr.`from`, fr.media_file AS file_name,
                   fr.created_at, fr.modified_at,
                   u.username AS client_username
            FROM fax_receiveds fr
            LEFT JOIN tran_user u ON u.id = fr.ref_id
            ORDER BY fr.id DESC LIMIT @limit OFFSET @offset");
        cmd.Parameters.AddWithValue("@limit", perPage);
        cmd.Parameters.AddWithValue("@offset", offset);
        return cmd;
    }

    public static MySqlCommand GetIncomingFaxesByClient(MySqlConnection c, string clientUsername)
    {
        var cmd = Cmd(c, @"
            SELECT fr.id, fr.`from`, fr.media_file AS file_name, fr.created_at
            FROM fax_receiveds fr
            JOIN tran_user u ON u.id = fr.ref_id
            WHERE u.username = @cu
            ORDER BY fr.id DESC");
        cmd.Parameters.AddWithValue("@cu", clientUsername);
        return cmd;
    }

    public static MySqlCommand RenameFax(MySqlConnection c, int faxId, string newName)
    {
        var cmd = Cmd(c, "UPDATE fax_receiveds SET media_file = @name, modified_at = NOW() WHERE id = @id");
        cmd.Parameters.AddWithValue("@name", newName);
        cmd.Parameters.AddWithValue("@id", faxId);
        return cmd;
    }

    public static MySqlCommand MoveFax(MySqlConnection c, int faxId, int clientId)
    {
        var cmd = Cmd(c, "UPDATE fax_receiveds SET ref_id = @cid, modified_at = NOW() WHERE id = @id");
        cmd.Parameters.AddWithValue("@cid", clientId);
        cmd.Parameters.AddWithValue("@id", faxId);
        return cmd;
    }

    public static MySqlCommand GetUrgentNotifications(MySqlConnection c, int offset, int perPage)
    {
        var cmd = Cmd(c, @"
            SELECT fsd.id, fsd.file_path, fsd.is_read, fsd.created_at,
                   s.pname AS patient_name, s.access_number AS accession_number
            FROM tran_fax_status_detail fsd
            LEFT JOIN tran_typewordlist s ON s.id = fsd.typewordlist_id
            ORDER BY fsd.created_at DESC LIMIT @limit OFFSET @offset");
        cmd.Parameters.AddWithValue("@limit", perPage);
        cmd.Parameters.AddWithValue("@offset", offset);
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // FINAL REPORTS
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetFinalReports(MySqlConnection c, string? dateFrom, string? dateTo, int? radId, int offset, int perPage)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = @"
            SELECT s.id, s.pname AS patient_name, s.modality, s.dos,
                   s.report_text, s.impression_text AS impression,
                   s.type AS status, ref_u.displayname AS client_name,
                   s.pdf_report AS pdf_path, s.report_date AS updated_at,
                   r.displayname AS rad_name
            FROM tran_typewordlist s
            LEFT JOIN tran_user r     ON r.id = s.rad_id
            LEFT JOIN tran_user ref_u ON ref_u.id = s.ref_id
            WHERE s.type = 'rad final report' AND s.`delete` = 0";
        if (radId.HasValue)                  { sql += " AND s.rad_id = @rid";            cmd.Parameters.AddWithValue("@rid", radId.Value); }
        if (!string.IsNullOrEmpty(dateFrom)) { sql += " AND DATE(s.report_date) >= @df"; cmd.Parameters.AddWithValue("@df", dateFrom); }
        if (!string.IsNullOrEmpty(dateTo))   { sql += " AND DATE(s.report_date) <= @dt"; cmd.Parameters.AddWithValue("@dt", dateTo); }
        sql += " ORDER BY s.report_date DESC LIMIT @limit OFFSET @offset";
        cmd.Parameters.AddWithValue("@limit", perPage);
        cmd.Parameters.AddWithValue("@offset", offset);
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand UpdateFinalReport(MySqlConnection c, int studyId, string reportText, string? impression, bool isAddendum)
    {
        var cmd = Cmd(c, @"
            UPDATE tran_typewordlist
            SET report_text = @rt, impression_text = @imp,
                is_addendum = @add, type = 'rad final report', report_date = CURDATE()
            WHERE id = @id");
        cmd.Parameters.AddWithValue("@rt", reportText);
        cmd.Parameters.AddWithValue("@imp", impression ?? "");
        cmd.Parameters.AddWithValue("@add", isAddendum ? 1 : 0);
        cmd.Parameters.AddWithValue("@id", studyId);
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // CHARTS / DASHBOARD
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetChartPendingReports(MySqlConnection c, int? radId)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = "SELECT s.`type` AS label, COUNT(*) AS total FROM tran_typewordlist s WHERE s.`type` IN ('hold for comparison','missing paperwork','speak to tech','missing images','client reports on hold','new study')";
        if (radId.HasValue) { sql += " AND s.rad_id = @rid"; cmd.Parameters.AddWithValue("@rid", radId.Value); }
        sql += " GROUP BY s.`type` ORDER BY total DESC";
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand GetChartPendingByTranscriber(MySqlConnection c, int? radId = null)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = @"
            SELECT u.displayname AS label, COUNT(*) AS total
            FROM tran_typewordlist s JOIN tran_user u ON u.id = s.trans_id
            WHERE s.`type` = 'trans new messages'";
        if (radId.HasValue) { sql += " AND s.rad_id = @rid"; cmd.Parameters.AddWithValue("@rid", radId.Value); }
        sql += " GROUP BY s.trans_id, u.displayname ORDER BY total DESC";
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand GetChartOnHoldByTranscriber(MySqlConnection c, int? radId = null)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = @"
            SELECT u.displayname AS label, COUNT(*) AS total
            FROM tran_typewordlist s JOIN tran_user u ON u.id = s.trans_id
            WHERE s.`type` = 'trans reports on hold'";
        if (radId.HasValue) { sql += " AND s.rad_id = @rid"; cmd.Parameters.AddWithValue("@rid", radId.Value); }
        sql += " GROUP BY s.trans_id, u.displayname ORDER BY total DESC";
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand GetChartOnHoldByRadiologist(MySqlConnection c, int? radId = null)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = @"
            SELECT COALESCE(u.displayname, '') AS label, COUNT(*) AS total
            FROM tran_typewordlist s LEFT JOIN tran_user u ON u.id = s.ref_id
            WHERE s.`type` = 'rad reports on hold' AND s.`delete` = 0";
        if (radId.HasValue) { sql += " AND s.rad_id = @rid"; cmd.Parameters.AddWithValue("@rid", radId.Value); }
        sql += " GROUP BY s.ref_id ORDER BY total DESC";
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand GetChartPendingSignatureByRadiologist(MySqlConnection c, int? radId = null)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = @"
            SELECT COALESCE(u.displayname, '') AS label, COUNT(*) AS total
            FROM tran_typewordlist s LEFT JOIN tran_user u ON u.id = s.ref_id
            WHERE s.`type` = 'rad reports pending signature' AND s.`delete` = 0";
        if (radId.HasValue) { sql += " AND s.rad_id = @rid"; cmd.Parameters.AddWithValue("@rid", radId.Value); }
        sql += " GROUP BY s.ref_id ORDER BY total DESC";
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand GetChartFinalizedPerRadiologist(MySqlConnection c, int? radId, string? df, string? dt)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = @"
            SELECT COALESCE(u.displayname, '') AS label, COUNT(*) AS total
            FROM tran_typewordlist s LEFT JOIN tran_user u ON u.id = s.ref_id
            WHERE s.`type` = 'rad final report'";
        if (radId.HasValue) { sql += " AND s.rad_id = @rid"; cmd.Parameters.AddWithValue("@rid", radId.Value); }
        if (!string.IsNullOrEmpty(df)) { sql += " AND s.report_date >= @df"; cmd.Parameters.AddWithValue("@df", df); }
        if (!string.IsNullOrEmpty(dt)) { sql += " AND s.report_date <= @dt"; cmd.Parameters.AddWithValue("@dt", dt); }
        sql += " GROUP BY s.ref_id ORDER BY total DESC";
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand GetChartNoAudioFile(MySqlConnection c, int? radId = null)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = "SELECT s.modality AS label, COUNT(*) AS total FROM tran_typewordlist s WHERE s.`type` = 'no audio' AND s.`delete` = 0";
        if (radId.HasValue) { sql += " AND s.rad_id = @rid"; cmd.Parameters.AddWithValue("@rid", radId.Value); }
        sql += " GROUP BY s.modality ORDER BY total DESC";
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand GetChartEarningByModality(MySqlConnection c, int? radId, string? df, string? dt)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = @"SELECT s.modality AS label,
            SUM(COALESCE(p.price, 0)) AS total
            FROM tran_typewordlist s
            LEFT JOIN tran_price_referrer p ON p.userid = s.ref_id AND p.modality = s.modality
            WHERE s.`type` = 'rad final report' AND s.`delete` = 0";
        if (radId.HasValue) { sql += " AND s.rad_id = @rid"; cmd.Parameters.AddWithValue("@rid", radId.Value); }
        if (!string.IsNullOrEmpty(df)) { sql += " AND DATE(s.report_date) >= @df"; cmd.Parameters.AddWithValue("@df", df); }
        if (!string.IsNullOrEmpty(dt)) { sql += " AND DATE(s.report_date) <= @dt"; cmd.Parameters.AddWithValue("@dt", dt); }
        sql += " GROUP BY s.modality ORDER BY total DESC";
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand GetStudiesByStatus(MySqlConnection c, string status, int? scopeUserId, int? userType, int offset, int perPage)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = @"
            SELECT s.id, s.pname AS patient_name, s.access_number AS accession_number,
                   s.modality, s.type AS status, s.dos,
                   ref_u.displayname AS client_name, s.stat AS is_stat, s.report_date AS updated_at
            FROM tran_typewordlist s
            LEFT JOIN tran_user ref_u ON ref_u.id = s.ref_id
            WHERE s.type = @status AND s.`delete` = 0";
        if (userType == 2 && scopeUserId.HasValue) { sql += " AND s.rad_id = @sid";    cmd.Parameters.AddWithValue("@sid", scopeUserId.Value); }
        if (userType == 3 && scopeUserId.HasValue) { sql += " AND s.trans_id = @sid";  cmd.Parameters.AddWithValue("@sid", scopeUserId.Value); }
        if (userType == 4 && scopeUserId.HasValue) { sql += " AND s.ref_id = @sid";    cmd.Parameters.AddWithValue("@sid", scopeUserId.Value); }
        sql += " ORDER BY s.report_date DESC LIMIT @limit OFFSET @offset";
        cmd.Parameters.AddWithValue("@status", status);
        cmd.Parameters.AddWithValue("@limit", perPage);
        cmd.Parameters.AddWithValue("@offset", offset);
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand GetFinalizedData(MySqlConnection c, int? radId, int? transcriberId, string? df, string? dt)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = @"
            SELECT s.id, s.pname AS patient_name, s.access_number AS accession_number,
                   s.modality, s.dos, ref_u.displayname AS client_name,
                   s.report_date AS updated_at, r.displayname AS rad_name
            FROM tran_typewordlist s
            LEFT JOIN tran_user r     ON r.id = s.rad_id
            LEFT JOIN tran_user ref_u ON ref_u.id = s.ref_id
            WHERE s.type = 'rad final report' AND s.`delete` = 0";
        if (radId.HasValue)         { sql += " AND s.rad_id = @rid";          cmd.Parameters.AddWithValue("@rid", radId.Value); }
        if (transcriberId.HasValue) { sql += " AND s.trans_id = @tid";        cmd.Parameters.AddWithValue("@tid", transcriberId.Value); }
        if (!string.IsNullOrEmpty(df)) { sql += " AND DATE(s.report_date) >= @df"; cmd.Parameters.AddWithValue("@df", df); }
        if (!string.IsNullOrEmpty(dt)) { sql += " AND DATE(s.report_date) <= @dt"; cmd.Parameters.AddWithValue("@dt", dt); }
        sql += " ORDER BY s.report_date DESC LIMIT 500";
        cmd.CommandText = sql;
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // BILLING
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetStudiesForInvoice(MySqlConnection c, int clientId, int? transcriberId, string dateFrom, string dateTo)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = @"
            SELECT s.id, s.modality, s.type AS status, s.dos, s.pname AS patient_name
            FROM tran_typewordlist s
            WHERE s.ref_id = @cid AND s.type = 'rad final report' AND s.`delete` = 0
              AND DATE(s.report_date) BETWEEN @df AND @dt";
        if (transcriberId.HasValue) { sql += " AND s.trans_id = @tid"; cmd.Parameters.AddWithValue("@tid", transcriberId.Value); }
        sql += " ORDER BY s.report_date DESC, s.trans_id ASC";
        cmd.Parameters.AddWithValue("@cid", clientId);
        cmd.Parameters.AddWithValue("@df", dateFrom);
        cmd.Parameters.AddWithValue("@dt", dateTo);
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand GetModalityPriceForUser(MySqlConnection c, int userId, string category, string modality)
    {
        var table = category == "client" ? "tran_price_referrer" : "tran_price_transcriber";
        var cmd = Cmd(c, $"SELECT price FROM {table} WHERE userid = @uid AND modality = @mod LIMIT 1");
        cmd.Parameters.AddWithValue("@uid", userId);
        cmd.Parameters.AddWithValue("@mod", modality);
        return cmd;
    }

    public static MySqlCommand InsertTranscriberInvoice(MySqlConnection c, int transcriberId, int clientId,
        string dateFrom, string dateTo, decimal total, string invoiceNum)
    {
        var cmd = Cmd(c, @"
            INSERT INTO tran_transcriber_modality_invoice
                (user_id, start_date, end_date, total_amount, invoice_no,
                 payment_status, payment_due, amount_paid, payment_mode, check_no,
                 invoice_generate_curr_date, billing_pdf, description)
            VALUES (@tid, @df, @dt, @amt, @inv,
                    'Not Paid', 0, 0, '', '',
                    NOW(), '', '')");
        cmd.Parameters.AddWithValue("@tid", transcriberId);
        cmd.Parameters.AddWithValue("@df", dateFrom);
        cmd.Parameters.AddWithValue("@dt", dateTo);
        cmd.Parameters.AddWithValue("@amt", total);
        cmd.Parameters.AddWithValue("@inv", invoiceNum);
        return cmd;
    }

    public static MySqlCommand InsertClientInvoice(MySqlConnection c, int clientId,
        string dateFrom, string dateTo, decimal total, string invoiceNum)
    {
        var cmd = Cmd(c, @"
            INSERT INTO tran_referrer_invoice
                (user_id, start_date, end_date, total_amount, invoice_no,
                 payment_status, payment_due, amount_paid, payment_mode, check_no,
                 invoice_generate_curr_date, billing_pdf, description)
            VALUES (@cid, @df, @dt, @amt, @inv,
                    'Not Paid', 0, 0, '', '',
                    NOW(), '', '')");
        cmd.Parameters.AddWithValue("@cid", clientId);
        cmd.Parameters.AddWithValue("@df", dateFrom);
        cmd.Parameters.AddWithValue("@dt", dateTo);
        cmd.Parameters.AddWithValue("@amt", total);
        cmd.Parameters.AddWithValue("@inv", invoiceNum);
        return cmd;
    }

    public static MySqlCommand GetInvoicePayments(MySqlConnection c, int? clientId, int offset, int perPage)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = @"
            SELECT i.id, i.invoice_no AS invoice_number, i.start_date AS date_from,
                   i.end_date AS date_to, i.total_amount, i.payment_status AS status,
                   i.invoice_generate_curr_date AS created_at
            FROM tran_referrer_invoice i WHERE 1=1";
        if (clientId.HasValue) { sql += " AND i.user_id = @cid"; cmd.Parameters.AddWithValue("@cid", clientId.Value); }
        sql += " ORDER BY i.invoice_generate_curr_date DESC LIMIT @limit OFFSET @offset";
        cmd.Parameters.AddWithValue("@limit", perPage);
        cmd.Parameters.AddWithValue("@offset", offset);
        cmd.CommandText = sql;
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // PRICE MANAGEMENT
    // Column map: user_id=userid; price tables have no unique key → DELETE+INSERT
    // tran_price_transcriber_char: price_per_char=price, chart col unused
    // tran_price_transcriber_page: price_per_page=price, modality stored via modalityid lookup
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetAllModalityPricesForUser(MySqlConnection c, int userId, string category)
    {
        var table = category == "client" ? "tran_price_referrer" : "tran_price_transcriber";
        var cmd = Cmd(c, $"SELECT modality, price FROM {table} WHERE userid = @uid ORDER BY modality");
        cmd.Parameters.AddWithValue("@uid", userId);
        return cmd;
    }

    public static MySqlCommand UpsertModalityPrice(MySqlConnection c, int userId, string category, string modality, decimal price)
    {
        var table = category == "client" ? "tran_price_referrer" : "tran_price_transcriber";
        var cmd = Cmd(c, $@"
            DELETE FROM {table} WHERE userid = @uid AND modality = @mod;
            INSERT INTO {table} (userid, modality, price) VALUES (@uid, @mod, @price)");
        cmd.Parameters.AddWithValue("@uid", userId);
        cmd.Parameters.AddWithValue("@mod", modality);
        cmd.Parameters.AddWithValue("@price", price);
        return cmd;
    }

    public static MySqlCommand UpsertPerPagePrice(MySqlConnection c, int userId, decimal pricePerPage, string? modality)
    {
        var cmd = Cmd(c, @"
            DELETE FROM tran_price_transcriber_page WHERE userid = @uid;
            INSERT INTO tran_price_transcriber_page (userid, price) VALUES (@uid, @price)");
        cmd.Parameters.AddWithValue("@uid", userId);
        cmd.Parameters.AddWithValue("@price", pricePerPage);
        return cmd;
    }

    public static MySqlCommand UpsertPerCharPrice(MySqlConnection c, int userId, decimal pricePerChar)
    {
        var cmd = Cmd(c, @"
            DELETE FROM tran_price_transcriber_char WHERE userid = @uid;
            INSERT INTO tran_price_transcriber_char (userid, price) VALUES (@uid, @price)");
        cmd.Parameters.AddWithValue("@uid", userId);
        cmd.Parameters.AddWithValue("@price", pricePerChar);
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // ORDER STATUS
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetOrderStatus(MySqlConnection c, string? search, string? df, string? dt, int offset, int perPage)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = @"
            SELECT s.id, s.pname AS patient_name, s.access_number AS accession_number,
                   s.modality, s.type AS status, s.dos, s.dob, s.idnumber, s.sid,
                   s.report_date, s.pdf_report, s.incoming_order_exam_id, s.incoming_order_patient_id,
                   s.`delete` AS is_deleted,
                   ref_u.displayname AS client_name,
                   t.displayname AS transcriber_name, r.displayname AS rad_name,
                   DATE_FORMAT(ta1.curr_date_time, '%m-%d-%Y') AS dot_time,
                   DATE_FORMAT(ta2.curr_date_time, '%m-%d-%Y') AS dod_time
            FROM tran_typewordlist s
            LEFT JOIN tran_user t     ON t.id = s.trans_id
            LEFT JOIN tran_user r     ON r.id = s.rad_id
            LEFT JOIN tran_user ref_u ON ref_u.id = s.ref_id
            LEFT JOIN tran_audit ta1  ON ta1.eid = s.id AND ta1.status_insert = 'DOT Time'
            LEFT JOIN tran_audit ta2  ON ta2.eid = s.id AND ta2.status_insert = 'DOD Time'
            WHERE 1=1";
        if (!string.IsNullOrEmpty(search)) { sql += " AND (s.pname LIKE @s OR s.access_number LIKE @s OR s.idnumber LIKE @s)"; cmd.Parameters.AddWithValue("@s", $"%{search}%"); }
        if (!string.IsNullOrEmpty(df))     { sql += " AND DATE(s.dos) >= @df"; cmd.Parameters.AddWithValue("@df", df); }
        if (!string.IsNullOrEmpty(dt))     { sql += " AND DATE(s.dos) <= @dt"; cmd.Parameters.AddWithValue("@dt", dt); }
        sql += " ORDER BY s.id DESC LIMIT @limit OFFSET @offset";
        cmd.Parameters.AddWithValue("@limit", perPage);
        cmd.Parameters.AddWithValue("@offset", offset);
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand ReassignTranscriber(MySqlConnection c, int studyId, int transcriberId)
    {
        // Laravel only updates trans_id, does NOT change type
        var cmd = Cmd(c, "UPDATE tran_typewordlist SET trans_id = @tid WHERE id = @sid");
        cmd.Parameters.AddWithValue("@tid", transcriberId);
        cmd.Parameters.AddWithValue("@sid", studyId);
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MODALITIES  (tran_modality)
    // Column map: name=modality (varchar), status='on'/'off' (varchar not 0/1)
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetModalities(MySqlConnection c)
    {
        return Cmd(c, "SELECT id, modality AS name, status FROM tran_modality ORDER BY modality ASC");
    }

    public static MySqlCommand ToggleModalityStatus(MySqlConnection c, int id)
    {
        return Cmd(c, "UPDATE tran_modality SET status = IF(status='on','off','on') WHERE id = @id");
    }

    public static MySqlCommand ToggleModalityStatusCmd(MySqlConnection c, int id)
    {
        var cmd = Cmd(c, "UPDATE tran_modality SET status = IF(status='on','off','on') WHERE id = @id");
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // NON-DICOM  (manually entered studies — identified by empty sid)
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetNonDicomAccounts(MySqlConnection c, string? search, int offset, int perPage, int? radId, int? userType)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = @"
            SELECT tw.id, tw.pname AS patient_name, tw.firstname, tw.lastname,
                   tw.idnumber, tw.dos, tw.type AS status, tw.modality,
                   tw.description AS exam, u.displayname AS client_name,
                   t.displayname AS transcriber_name
            FROM tran_typewordlist tw
            LEFT JOIN tran_user u ON u.id = tw.ref_id
            LEFT JOIN tran_user t ON t.id = tw.trans_id
            WHERE tw.`delete` = 0";
        if (userType == 2 && radId.HasValue)
        {
            sql += " AND tw.rad_id = @rid AND tw.type = 'rad non dicom accunts'";
            cmd.Parameters.AddWithValue("@rid", radId.Value);
        }
        else
        {
            sql += " AND tw.type_rad = 'non dicom accounts from admin'";
        }
        if (!string.IsNullOrEmpty(search)) { sql += " AND (tw.pname LIKE @s OR tw.idnumber LIKE @s OR u.displayname LIKE @s)"; cmd.Parameters.AddWithValue("@s", $"%{search}%"); }
        sql += " ORDER BY tw.id DESC LIMIT @limit OFFSET @offset";
        cmd.Parameters.AddWithValue("@limit", perPage);
        cmd.Parameters.AddWithValue("@offset", offset);
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand InsertNonDicomEntry(MySqlConnection c,
        string patientName, string? dob, string? dos, string? modality, string? examType,
        string? description, int? clientId, int? physicianId)
    {
        var cmd = Cmd(c, @"
            INSERT INTO tran_typewordlist
                (pname, dob, dos, modality, type, description, ref_id,
                 ordering_physician, pom_id, rad_id, trans_id, gender, idnumber,
                 access_number, dos_time_from, dos_time_to, modalityid, examid, cpt,
                 covered, client_location, templateid, document_type, pay_status, report_date,
                 dictation_status, dictating_trans, dictating_time, audio_name, report_date_time,
                 report_text, impression_text, report_text_addendum, report_text_signature,
                 addendum_text_signature, addendum_text_temp, pdf_report, pdf_page, type_rad,
                 transfer_id, incoming_order_patient_id, incoming_order_exam_id,
                 trans_new_message_time, lock, ip, archive, delete, sid, report_key_image,
                 firstname, middlename, lastname, location)
            VALUES (@pn, @dob, @dos, @mod, 'new study', @desc, @cid,
                    '', 0, 0, 0, '', '', '', '', '', 0, 0, '', '', 0, 0, '', '', CURDATE(),
                    '', 0, '', '', '', '', '', '', '', '', '', '', 0, '', 0, 0, 0,
                    NOW(), 0, '', '', 0, '', '', '', '', '', '')");
        cmd.Parameters.AddWithValue("@pn",  patientName);
        cmd.Parameters.AddWithValue("@dob", (object?)dob ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@dos", (object?)dos ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@mod", (object?)modality ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@desc",(object?)description ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@cid", (object?)clientId ?? 0);
        return cmd;
    }

    public static MySqlCommand DeleteNonDicomEntry(MySqlConnection c, int studyId)
    {
        // Laravel hard-deletes the record (findOrFail + delete())
        var cmd = Cmd(c, "DELETE FROM tran_typewordlist WHERE id = @id");
        cmd.Parameters.AddWithValue("@id", studyId);
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // STANDARD REPORTS  (tran_standard_report)
    // Column map: modality_id=modality (varchar), no created_at
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetStandardReports(MySqlConnection c, int? radId, int offset, int perPage)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = @"
            SELECT sr.id, sr.label, sr.report_text, sr.rad_id,
                   u.displayname AS rad_name,
                   sr.modality AS modality_name, sr.examid AS modality_id
            FROM tran_standard_report sr
            LEFT JOIN tran_user u ON u.id = sr.rad_id
            WHERE 1=1";
        if (radId.HasValue) { sql += " AND sr.rad_id = @rid"; cmd.Parameters.AddWithValue("@rid", radId.Value); }
        sql += " ORDER BY sr.id DESC LIMIT @limit OFFSET @offset";
        cmd.Parameters.AddWithValue("@limit", perPage);
        cmd.Parameters.AddWithValue("@offset", offset);
        cmd.CommandText = sql;
        return cmd;
    }

    public static MySqlCommand InsertStandardReport(MySqlConnection c, string label, string reportText, int radId, int modalityId)
    {
        var cmd = Cmd(c, @"
            INSERT INTO tran_standard_report (label, report_text, rad_id, examid, modality)
            SELECT @label, @rt, @rid, @mid, modality
            FROM tran_modality WHERE id = @mid LIMIT 1");
        cmd.Parameters.AddWithValue("@label", label);
        cmd.Parameters.AddWithValue("@rt", reportText);
        cmd.Parameters.AddWithValue("@rid", radId);
        cmd.Parameters.AddWithValue("@mid", modalityId);
        return cmd;
    }

    public static MySqlCommand UpdateStandardReport(MySqlConnection c, int id, string? label, string? reportText, int? radId, int? modalityId)
    {
        var sets = new List<string>();
        var cmd = new MySqlCommand { Connection = c };
        if (label != null)      { sets.Add("label = @label");     cmd.Parameters.AddWithValue("@label", label); }
        if (reportText != null) { sets.Add("report_text = @rt");  cmd.Parameters.AddWithValue("@rt", reportText); }
        if (radId != null)      { sets.Add("rad_id = @rid");      cmd.Parameters.AddWithValue("@rid", radId); }
        if (modalityId != null) { sets.Add("examid = @mid");      cmd.Parameters.AddWithValue("@mid", modalityId); }
        if (sets.Count == 0)    { cmd.CommandText = "SELECT 1"; return cmd; }
        cmd.CommandText = $"UPDATE tran_standard_report SET {string.Join(", ", sets)} WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    public static MySqlCommand DeleteStandardReport(MySqlConnection c, int id)
    {
        var cmd = Cmd(c, "DELETE FROM tran_standard_report WHERE id = @id");
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // ROLES
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetRoles(MySqlConnection c)
    {
        return Cmd(c, "SELECT id, name FROM tran_user_type ORDER BY name ASC");
    }

    public static MySqlCommand InsertRole(MySqlConnection c, string name)
    {
        var cmd = Cmd(c, "INSERT INTO tran_user_type (name) VALUES (@name)");
        cmd.Parameters.AddWithValue("@name", name);
        return cmd;
    }

    public static MySqlCommand UpdateRole(MySqlConnection c, int id, string name)
    {
        var cmd = Cmd(c, "UPDATE tran_user_type SET name = @name WHERE id = @id");
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    public static MySqlCommand DeleteRole(MySqlConnection c, int id)
    {
        var cmd = Cmd(c, "DELETE FROM tran_user_type WHERE id = @id");
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    public static MySqlCommand GetRolePermissions(MySqlConnection c, int roleId)
    {
        var cmd = Cmd(c, @"
            SELECT p.name FROM permissions p
            JOIN role_has_permissions rp ON rp.permission_id = p.id
            WHERE rp.role_id = @rid");
        cmd.Parameters.AddWithValue("@rid", roleId);
        return cmd;
    }

    public static MySqlCommand DeleteRolePermissions(MySqlConnection c, int roleId)
    {
        var cmd = Cmd(c, "DELETE FROM role_has_permissions WHERE role_id = @rid");
        cmd.Parameters.AddWithValue("@rid", roleId);
        return cmd;
    }

    public static MySqlCommand InsertRolePermission(MySqlConnection c, int roleId, int permissionId)
    {
        var cmd = Cmd(c, "INSERT IGNORE INTO role_has_permissions (permission_id, role_id) VALUES (@pid, @rid)");
        cmd.Parameters.AddWithValue("@pid", permissionId);
        cmd.Parameters.AddWithValue("@rid", roleId);
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // SITE MANAGEMENT  (tran_settings)
    // Column: `key` (reserved word — needs backticks)
    // No unique index on `key` → DELETE+INSERT pattern
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetSetting(MySqlConnection c, string key)
    {
        var cmd = Cmd(c, "SELECT value FROM tran_settings WHERE `key` = @key LIMIT 1");
        cmd.Parameters.AddWithValue("@key", key);
        return cmd;
    }

    public static MySqlCommand UpsertSetting(MySqlConnection c, string key, string value)
    {
        var cmd = Cmd(c, @"
            DELETE FROM tran_settings WHERE `key` = @key;
            INSERT INTO tran_settings (`key`, value, created_at, updated_at)
            VALUES (@key, @val, NOW(), NOW())");
        cmd.Parameters.AddWithValue("@key", key);
        cmd.Parameters.AddWithValue("@val", value);
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // OSRIX  (tran_osrix, tran_osrix_instution)
    // tran_osrix columns: id, rad_id, osrix_user, ref_id  (NO password column)
    // tran_osrix_instution columns: id, osrix_user, ref_id  (NO name/description)
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetOsrix(MySqlConnection c, int offset, int perPage)
    {
        var cmd = Cmd(c, @"
            SELECT o.id, o.osrix_user AS username, o.rad_id, u.displayname AS rad_name
            FROM tran_osrix o
            LEFT JOIN tran_user u ON u.id = o.rad_id
            ORDER BY o.id DESC LIMIT @limit OFFSET @offset");
        cmd.Parameters.AddWithValue("@limit", perPage);
        cmd.Parameters.AddWithValue("@offset", offset);
        return cmd;
    }

    public static MySqlCommand InsertOsrixUser(MySqlConnection c, string username, string passwordHash, int radId)
    {
        var cmd = Cmd(c, "INSERT INTO tran_osrix (osrix_user, rad_id) VALUES (@un, @rid)");
        cmd.Parameters.AddWithValue("@un", username);
        cmd.Parameters.AddWithValue("@rid", radId);
        return cmd;
    }

    public static MySqlCommand UpdateOsrixUser(MySqlConnection c, int id, string username, string? passwordHash, int radId)
    {
        var cmd = Cmd(c, "UPDATE tran_osrix SET osrix_user = @un, rad_id = @rid WHERE id = @id");
        cmd.Parameters.AddWithValue("@un", username);
        cmd.Parameters.AddWithValue("@rid", radId);
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    public static MySqlCommand DeleteOsrixUser(MySqlConnection c, int id)
    {
        var cmd = Cmd(c, "DELETE FROM tran_osrix WHERE id = @id");
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    public static MySqlCommand GetOsrixInstitutions(MySqlConnection c, int offset, int perPage)
    {
        var cmd = Cmd(c, @"
            SELECT id, osrix_user AS name, ref_id
            FROM tran_osrix_instution
            ORDER BY id DESC LIMIT @limit OFFSET @offset");
        cmd.Parameters.AddWithValue("@limit", perPage);
        cmd.Parameters.AddWithValue("@offset", offset);
        return cmd;
    }

    public static MySqlCommand InsertOsrixInstitution(MySqlConnection c, string name, string? description)
    {
        var cmd = Cmd(c, "INSERT INTO tran_osrix_instution (osrix_user, ref_id) VALUES (@name, 0)");
        cmd.Parameters.AddWithValue("@name", name);
        return cmd;
    }

    public static MySqlCommand UpdateOsrixInstitution(MySqlConnection c, int id, string name, string? description)
    {
        var cmd = Cmd(c, "UPDATE tran_osrix_instution SET osrix_user = @name WHERE id = @id");
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    public static MySqlCommand DeleteOsrixInstitution(MySqlConnection c, int id)
    {
        var cmd = Cmd(c, "DELETE FROM tran_osrix_instution WHERE id = @id");
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    public static MySqlCommand GetRadiologists(MySqlConnection c, string? search)
    {
        var cmd = new MySqlCommand { Connection = c };
        var sql = "SELECT id, displayname AS name, email FROM tran_user WHERE usertype = 2 AND block = 0";
        if (!string.IsNullOrEmpty(search)) { sql += " AND displayname LIKE @s"; cmd.Parameters.AddWithValue("@s", $"%{search}%"); }
        sql += " ORDER BY displayname ASC";
        cmd.CommandText = sql;
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // REGION — COUNTRIES
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetAllCountries(MySqlConnection c, string? search, int start, int length)
    {
        var cmd = Cmd(c, @"
            SELECT id, name FROM tran_country
            WHERE (@search IS NULL OR name LIKE @search)
            ORDER BY name LIMIT @length OFFSET @start");
        cmd.Parameters.AddWithValue("@search", search == null ? (object)DBNull.Value : $"%{search}%");
        cmd.Parameters.AddWithValue("@start",  start);
        cmd.Parameters.AddWithValue("@length", length);
        return cmd;
    }

    public static MySqlCommand CountCountries(MySqlConnection c, string? search)
    {
        var cmd = Cmd(c, "SELECT COUNT(*) FROM tran_country WHERE (@search IS NULL OR name LIKE @search)");
        cmd.Parameters.AddWithValue("@search", search == null ? (object)DBNull.Value : $"%{search}%");
        return cmd;
    }

    public static MySqlCommand InsertCountry(MySqlConnection c, string name)
    {
        var cmd = Cmd(c, "INSERT INTO tran_country (name) VALUES (@name); SELECT LAST_INSERT_ID();");
        cmd.Parameters.AddWithValue("@name", name);
        return cmd;
    }

    public static MySqlCommand UpdateCountry(MySqlConnection c, int id, string name)
    {
        var cmd = Cmd(c, "UPDATE tran_country SET name = @name WHERE id = @id");
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@id",   id);
        return cmd;
    }

    public static MySqlCommand DeleteCountry(MySqlConnection c, int id)
    {
        var cmd = Cmd(c, "DELETE FROM tran_country WHERE id = @id");
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // REGION — STATES  (tran_state: countryid not country_id)
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetAllStates(MySqlConnection c, string? search, int start, int length)
    {
        var cmd = Cmd(c, @"
            SELECT s.id, s.name, s.countryid, c.name AS country_name
            FROM tran_state s
            LEFT JOIN tran_country c ON c.id = s.countryid
            WHERE (@search IS NULL OR s.name LIKE @search)
            ORDER BY s.name LIMIT @length OFFSET @start");
        cmd.Parameters.AddWithValue("@search", search == null ? (object)DBNull.Value : $"%{search}%");
        cmd.Parameters.AddWithValue("@start",  start);
        cmd.Parameters.AddWithValue("@length", length);
        return cmd;
    }

    public static MySqlCommand CountStates(MySqlConnection c, string? search)
    {
        var cmd = Cmd(c, "SELECT COUNT(*) FROM tran_state WHERE (@search IS NULL OR name LIKE @search)");
        cmd.Parameters.AddWithValue("@search", search == null ? (object)DBNull.Value : $"%{search}%");
        return cmd;
    }

    public static MySqlCommand InsertState(MySqlConnection c, int countryId, string name)
    {
        var cmd = Cmd(c, "INSERT INTO tran_state (name, countryid) VALUES (@name, @cid); SELECT LAST_INSERT_ID();");
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@cid",  countryId);
        return cmd;
    }

    public static MySqlCommand UpdateState(MySqlConnection c, int id, int countryId, string name)
    {
        var cmd = Cmd(c, "UPDATE tran_state SET name = @name, countryid = @cid WHERE id = @id");
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@cid",  countryId);
        cmd.Parameters.AddWithValue("@id",   id);
        return cmd;
    }

    public static MySqlCommand DeleteState(MySqlConnection c, int id)
    {
        var cmd = Cmd(c, "DELETE FROM tran_state WHERE id = @id");
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // REGION — CITIES  (tran_city: stateid not state_id)
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetAllCities(MySqlConnection c, string? search, int start, int length)
    {
        var cmd = Cmd(c, @"
            SELECT ci.id, ci.name, ci.stateid, s.name AS state_name
            FROM tran_city ci
            LEFT JOIN tran_state s ON s.id = ci.stateid
            WHERE (@search IS NULL OR ci.name LIKE @search)
            ORDER BY ci.name LIMIT @length OFFSET @start");
        cmd.Parameters.AddWithValue("@search", search == null ? (object)DBNull.Value : $"%{search}%");
        cmd.Parameters.AddWithValue("@start",  start);
        cmd.Parameters.AddWithValue("@length", length);
        return cmd;
    }

    public static MySqlCommand CountCities(MySqlConnection c, string? search)
    {
        var cmd = Cmd(c, "SELECT COUNT(*) FROM tran_city WHERE (@search IS NULL OR name LIKE @search)");
        cmd.Parameters.AddWithValue("@search", search == null ? (object)DBNull.Value : $"%{search}%");
        return cmd;
    }

    public static MySqlCommand InsertCity(MySqlConnection c, int stateId, string name)
    {
        var cmd = Cmd(c, "INSERT INTO tran_city (name, stateid) VALUES (@name, @sid); SELECT LAST_INSERT_ID();");
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@sid",  stateId);
        return cmd;
    }

    public static MySqlCommand UpdateCity(MySqlConnection c, int id, int stateId, string name)
    {
        var cmd = Cmd(c, "UPDATE tran_city SET name = @name, stateid = @sid WHERE id = @id");
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@sid",  stateId);
        cmd.Parameters.AddWithValue("@id",   id);
        return cmd;
    }

    public static MySqlCommand DeleteCity(MySqlConnection c, int id)
    {
        var cmd = Cmd(c, "DELETE FROM tran_city WHERE id = @id");
        cmd.Parameters.AddWithValue("@id", id);
        return cmd;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // QUICKBOOKS
    // ══════════════════════════════════════════════════════════════════════════

    public static MySqlCommand GetQbConnection(MySqlConnection c)
        => Cmd(c, @"
            SELECT id, realm_id, access_token_value, refresh_token_value,
                   access_token_expires_at, country, company_name, check_account
            FROM quickbooks LIMIT 1");

    public static MySqlCommand UpdateQbToken(MySqlConnection c,
        string accessToken, string refreshToken, string expiresAt)
    {
        var cmd = Cmd(c, @"
            UPDATE quickbooks
            SET access_token_value      = @at,
                refresh_token_value     = @rt,
                access_token_expires_at = @exp
            WHERE id = 1");
        cmd.Parameters.AddWithValue("@at",  accessToken);
        cmd.Parameters.AddWithValue("@rt",  refreshToken);
        cmd.Parameters.AddWithValue("@exp", expiresAt);
        return cmd;
    }

    public static MySqlCommand UpdateQbSettings(MySqlConnection c, string checkAccount)
    {
        var cmd = Cmd(c, "UPDATE quickbooks SET check_account = @ca WHERE id = 1");
        cmd.Parameters.AddWithValue("@ca", checkAccount);
        return cmd;
    }

    public static MySqlCommand GetQbMappings(MySqlConnection c, string systemEntity)
    {
        var cmd = Cmd(c, @"
            SELECT system_id, qbo_id
            FROM quickbook_mappings
            WHERE system_entity = @entity");
        cmd.Parameters.AddWithValue("@entity", systemEntity);
        return cmd;
    }

    public static MySqlCommand UpsertQbMapping(MySqlConnection c,
        string systemId, string systemEntity, string qbEntity, string qboId)
    {
        var cmd = Cmd(c, @"
            INSERT INTO quickbook_mappings (system_id, system_entity, qb_entity, qbo_id)
            VALUES (@sid, @se, @qbe, @qid)
            ON DUPLICATE KEY UPDATE qbo_id = @qid");
        cmd.Parameters.AddWithValue("@sid", systemId);
        cmd.Parameters.AddWithValue("@se",  systemEntity);
        cmd.Parameters.AddWithValue("@qbe", qbEntity);
        cmd.Parameters.AddWithValue("@qid", qboId);
        return cmd;
    }

    public static MySqlCommand DeleteQbMapping(MySqlConnection c,
        string systemId, string systemEntity, string qbEntity)
    {
        var cmd = Cmd(c, @"
            DELETE FROM quickbook_mappings
            WHERE system_id = @sid AND system_entity = @se AND qb_entity = @qbe");
        cmd.Parameters.AddWithValue("@sid", systemId);
        cmd.Parameters.AddWithValue("@se",  systemEntity);
        cmd.Parameters.AddWithValue("@qbe", qbEntity);
        return cmd;
    }

    public static MySqlCommand GetDistinctModalities(MySqlConnection c)
        => Cmd(c, @"
            SELECT DISTINCT modality FROM tran_typewordlist
            WHERE modality IS NOT NULL AND modality != '' AND `delete` = 0
            ORDER BY modality");

    public static MySqlCommand GetUsersForQbPush(MySqlConnection c, IEnumerable<int> ids)
    {
        var cmd   = new MySqlCommand { Connection = c };
        var pList = ids.Select((id, i) => { var p = $"@id{i}"; cmd.Parameters.AddWithValue(p, id); return p; });
        cmd.CommandText = $@"
            SELECT u.id, u.firstname, u.middlename, u.lastname, u.displayname,
                   u.email, u.phone, u.office,
                   ci.name AS city_name, s.name AS state_name, co.name AS country_name
            FROM tran_user u
            LEFT JOIN tran_city    ci ON ci.id = u.city
            LEFT JOIN tran_state   s  ON s.id  = u.state
            LEFT JOIN tran_country co ON co.id = u.country
            WHERE u.id IN ({string.Join(",", pList)})";
        return cmd;
    }

    public static MySqlCommand InsertModality(MySqlConnection c, string name)
    {
        var cmd = Cmd(c, @"
            INSERT INTO tran_modality (modality, locationid, status, mwl_display_name)
            VALUES (@name, 1, 'on', @name)");
        cmd.Parameters.AddWithValue("@name", name);
        return cmd;
    }

    public static MySqlCommand InsertUserFromQb(MySqlConnection c,
        string firstname, string middlename, string lastname, string displayname,
        string phone, string email, int userType,
        int countryId, int stateId, int cityId)
    {
        var cmd = Cmd(c, @"
            INSERT INTO tran_user
                (firstname, middlename, lastname, displayname, phone, email,
                 usertype, country, state, city, block, office, signature, npi, taxid,
                 group_practice, auto_sign, default_transcriber, altphone, fax, pin,
                 company_name, notes, curr_date)
            VALUES
                (@fn, @mn, @ln, @dn, @ph, @em,
                 @ut, @co, @st, @ci, 0, '', '', '', '',
                 '', '', 0, '', '', '',
                 '', '', NOW());
            SELECT LAST_INSERT_ID();");
        cmd.Parameters.AddWithValue("@fn", firstname);
        cmd.Parameters.AddWithValue("@mn", middlename);
        cmd.Parameters.AddWithValue("@ln", lastname);
        cmd.Parameters.AddWithValue("@dn", displayname);
        cmd.Parameters.AddWithValue("@ph", phone);
        cmd.Parameters.AddWithValue("@em", email);
        cmd.Parameters.AddWithValue("@ut", userType);
        cmd.Parameters.AddWithValue("@co", countryId);
        cmd.Parameters.AddWithValue("@st", stateId);
        cmd.Parameters.AddWithValue("@ci", cityId);
        return cmd;
    }

    public static MySqlCommand FindCountryByName(MySqlConnection c, string name)
    {
        var cmd = Cmd(c, "SELECT id FROM tran_country WHERE name LIKE @name LIMIT 1");
        cmd.Parameters.AddWithValue("@name", name);
        return cmd;
    }

    public static MySqlCommand FindStateByName(MySqlConnection c, string name, int countryId)
    {
        var cmd = Cmd(c, "SELECT id FROM tran_state WHERE name LIKE @name AND countryid = @cid LIMIT 1");
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@cid",  countryId);
        return cmd;
    }

    public static MySqlCommand FindCityByName(MySqlConnection c, string name, int stateId)
    {
        var cmd = Cmd(c, "SELECT id FROM tran_city WHERE name LIKE @name AND stateid = @sid LIMIT 1");
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@sid",  stateId);
        return cmd;
    }

    // ─────────────────────────────────────────────────────────────────────────

    private static MySqlCommand Cmd(MySqlConnection c, string sql)
        => new MySqlCommand(sql, c);

    private static string AddList(MySqlCommand cmd, IEnumerable<string> values, string prefix)
    {
        var pnames = values.Select((v, i) =>
        {
            var p = $"@{prefix}{i}";
            cmd.Parameters.AddWithValue(p, v.Trim());
            return p;
        });
        return string.Join(",", pnames);
    }
}
