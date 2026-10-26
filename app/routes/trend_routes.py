from quart import Blueprint, jsonify, request, Response
import json
import io
import csv
import os
import base64
from datetime import datetime
from runtime_paths import DataPaths
from uuid import uuid4
from app.utils.db_utils import DB

trend = Blueprint("trend", __name__, url_prefix="/api/trend")

report = Blueprint("report", __name__, url_prefix="/Report")

def convert_time(ts):
    try:
        return datetime.strptime(ts, "%Y/%b/%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
    except:
        return ts

CONFIG_PATH = "config/device_config.json"
device_config = DataPaths.resolve(CONFIG_PATH)
EXPORT_FOLDER = "exports"
os.makedirs(EXPORT_FOLDER, exist_ok=True)

EXPORT_FOLDER = "exports"
os.makedirs(EXPORT_FOLDER, exist_ok=True)

# ?? LoadDevices()
@trend.route("/devices", methods=["GET"])
async def get_devices():
    try:
        with open(device_config, "r") as f:
            config = json.load(f)
    except:
        return jsonify({"devices": []})

    return jsonify({"devices": config.get("devices", [])})


# ?? LoadEquipments (Params)
@trend.route("/params/<device_id>", methods=["GET"])
async def get_params(device_id):

    query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{device_id}'
    """

    async with DB() as db:
        rows = await db.fetch(query)

    columns = [r["column_name"] for r in rows]

    ignore = ["id", "time_stamp"]
    params = [c for c in columns if c not in ignore]

    return jsonify({"params": params})


# ?? Trend Data
@trend.route("/data", methods=["POST"])
async def get_trend_data():

    body = await request.get_json()

    device_ids = body.get("deviceIds", [])
    params = body.get("params", [])
    startTs = body.get("startTs")
    endTs = body.get("endTs")

    if not device_ids or not params:
        return jsonify([])

    result_map = {}

    for device_id in device_ids:

        query = f"""
            SELECT time_stamp AS timestamp, {", ".join(params)}
            FROM {device_id}
            WHERE time_stamp BETWEEN '{startTs}' AND '{endTs}'
            ORDER BY time_stamp ASC
        """

        async with DB() as db:
            rows = await db.fetch(query)

        for param in params:

            # ?? create tag structure if not exist
            key = f"{param}_{device_id}"   # multi-device support

            if key not in result_map:
                result_map[key] = {
                    "Tag": {
                        "ID": key,
                        "Name": f"{param} ({device_id})",
                        "Unit": "",
                        "Identifier": "",
                        "EquipmentName": device_id,
                        "Visible": True,
                        "Locked": False,
                        "MinValue": 0,
                        "MaxValue": 100
                    },
                    "TagData": []
                }

            # ?? fill data
            for r in rows:
                value = r.get(param)

                if value is not None:
                    result_map[key]["TagData"].append({
                        "Timestamps": str(r["timestamp"]),
                        "Value": float(value)
                    })

    return jsonify(list(result_map.values()))

@report.route("/LoadTableData", methods=["POST"])
async def load_table_data():

    form = await request.form

    request_details = {}

    # ?? RequestDetails parse
    for key in form.keys():
        if key.startswith("RequestDetails"):
            parts = key.replace("]", "").split("[")
            idx = parts[1]
            field = parts[2]

            if idx not in request_details:
                request_details[idx] = {}

            request_details[idx][field] = form.get(key)

    print("REQUEST DETAILS:", request_details)

    if not request_details:
        return jsonify({
            "Data": [],
            "TotalRows": 0,
            "page": 1,
            "total": 1
        })

    rows_map = {}

    for item in request_details.values():

        device = item.get("EquipmentName")
        param = item.get("ID")
        start = convert_time(item.get("StartTime"))
        end = convert_time(item.get("EndTime"))

        print("DEVICE:", device)
        print("PARAM:", param)

        if not param:
            continue

        # ?? fallback fix for empty/invalid device values
        if not device or str(device).lower() == "none":
            print("?? Skipping invalid device:", device)
            continue

        query = f"""
            SELECT time_stamp, {param}
            FROM {device}
            WHERE time_stamp BETWEEN '{start}' AND '{end}'
            ORDER BY time_stamp ASC
        """

        async with DB() as db:
            rows = await db.fetch(query)

        for r in rows:
            ts = str(r["time_stamp"])

            if ts not in rows_map:
                rows_map[ts] = {"Time_Stamp": ts}

            key = f"{param}_{device}"   # ?? unique column per device

            rows_map[ts][key] = r[param]

    final_rows = list(rows_map.values())

    return jsonify({
    "Data": final_rows,
    "TotalRows": len(final_rows),
    "page": 1,
    "total": 1
})

# ?? ExportToExcel
@report.route("/ExportToExcel", methods=["POST"])
async def export_to_excel():

    form = await request.form

    request_details = {}

    # ?? parse RequestDetails
    for key in form.keys():
        if key.startswith("RequestDetails"):
            parts = key.replace("]", "").split("[")
            idx = parts[1]
            field = parts[2]

            if idx not in request_details:
                request_details[idx] = {}

            request_details[idx][field] = form.get(key)

    rows_map = {}

    for item in request_details.values():

        device = item.get("EquipmentName")
        param = item.get("ID")

        start = convert_time(item.get("StartTime"))
        end = convert_time(item.get("EndTime"))

        if not device or not param:
            continue

        query = f"""
            SELECT time_stamp, {param}
            FROM {device}
            WHERE time_stamp BETWEEN '{start}' AND '{end}'
            ORDER BY time_stamp ASC
        """

        async with DB() as db:
            rows = await db.fetch(query)

        for r in rows:
            ts = str(r["time_stamp"])

            if ts not in rows_map:
                rows_map[ts] = {"Time_Stamp": ts}

            key = f"{param}_{device}"
            rows_map[ts][key] = r[param]

    final_rows = list(rows_map.values())

    # ?? CSV ?????
    output = io.StringIO()
    writer = csv.writer(output)

    if final_rows:
        headers = final_rows[0].keys()
        writer.writerow(headers)

        for row in final_rows:
            writer.writerow(row.values())

    csv_data = output.getvalue()
    output.close()

    return Response(
        csv_data,
        mimetype="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=data_export.csv"
        }
    )


# ?? ExportConfig
@report.route("/ExportConfig", methods=["GET"])
async def export_config():
    return """
    <div style="padding:20px">
        <h3>Export Config</h3>
        <p>Export feature working...</p>
    </div>
    """


# ?? ExportList
@report.route("/ExportList", methods=["GET"])
async def export_list():

    html = """
    <div style="padding:20px">
        <h3>Export List</h3>
        <table border="1" style="width:100%">
            <tr>
                <th>Name</th>
                <th>Status</th>
            </tr>
            <tr>
                <td>Sample Export</td>
                <td>Completed</td>
            </tr>
        </table>
    </div>
    """

    return html


# ?? InsertExportRequest
@report.route("/InsertExportRequest", methods=["POST"])
async def insert_export_request():

    data = await request.get_json()

    print("EXPORT REQUEST:", data)

    # ??? DB ??? save ???? ?? ??? (dummy success)
    return jsonify({
        "Result": 1,
        "Message": "Export request saved successfully"
    })


# ?? Index
@report.route("/Index", methods=["GET"])
async def export_index():

    html = """
    <div style="padding:20px">
        <h2>Export Dashboard</h2>
        <p>Your export is being processed...</p>
    </div>
    """

    return html


# ?? Save File API
@report.route("/SaveFileStringAsLocalFile", methods=["POST"])
async def save_file_string():

    form = await request.form

    file_content = form.get("FileContent")
    file_name = form.get("FileName", "chart.png")

    if not file_content:
        return jsonify({"Result": 0, "Message": "No file content"})

    try:
        # base64 clean
        if "base64," in file_content:
            file_content = file_content.split("base64,")[1]

        file_bytes = base64.b64decode(file_content)

        file_path = os.path.join(EXPORT_FOLDER, file_name)

        with open(file_path, "wb") as f:
            f.write(file_bytes)

        return jsonify({
            "Result": 1,
            "Message": "File saved successfully"
        })

    except Exception as e:
        return jsonify({
            "Result": 0,
            "Message": str(e)
        })

# ?? Download API
@report.route("/Download", methods=["GET"])
async def download_file():

    filename = request.args.get("filename")

    if not filename:
        return "File name missing", 400

    file_path = os.path.join(EXPORT_FOLDER, filename)

    if not os.path.exists(file_path):
        return "File not found", 404

    with open(file_path, "rb") as f:
        data = f.read()

    return Response(
        data,
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "Content-Type": "application/octet-stream"
        }
    )


@report.route("/LoadDashboardTree", methods=["GET"])
async def load_dashboard_tree():
    try:
        user_id = request.args.get("userId")

        async with DB() as db:

            dashboards = await db.fetch(f"""
                SELECT id, dashboard_id, dashboard_name
                FROM user_dashboards
                WHERE user_id = '{user_id}'
                ORDER BY id ASC
            """)

            result = []

            for d in dashboards:
                dashboard_id = d["dashboard_id"]

                widgets = await db.fetch(f"""
                    SELECT id, widget_name
                    FROM user_widgets_new
                    WHERE dashboard_id = '{dashboard_id}'
                    ORDER BY id ASC
                """)

                widget_list = []
                for w in widgets:
                    widget_list.append({
                        "ID": str(w["id"]),
                        "Name": w["widget_name"],
                        "IsEditable": True
                    })

                result.append({
                    "ID": dashboard_id,
                    "Name": d["dashboard_name"],
                    "IsEditable": True,
                    "DashboardWidgets": widget_list
                })

        return jsonify({
            "Result": 1,
            "Data": result,
            "Message": ""
        })

    except Exception as e:
        print("ERROR LoadDashboardTree:", e)
        return jsonify({
            "Result": 0,
            "Data": [],
            "Message": str(e)
        })



@report.route("/SaveUserDefineDashboard", methods=["POST"])
async def save_user_define_dashboard():

    data = await request.get_json()
    print("SAVE DASHBOARD:", data)

    return jsonify({
        "Result": 1,
        "Message": "Dashboard saved"
    })

# /SaveDashBoardWidget endpoint removed — use /SaveWidget instead

# /SaveUserDefineWidget endpoint removed — use /SaveWidget instead


@report.route("/SaveWidget", methods=["POST"])
async def save_widget_new():
    try:
        #  handle both JSON and FORM
        data = await request.get_json(silent=True)

        if data:
            dashboard_id = data.get("ParentID")
            widget_name = data.get("Name")
            user_id = data.get("CreatedBy")

            config = {
    "deviceIds": data.get("DeviceIDs"),
    "tags": data.get("ParameterID"),
    "widgetType": data.get("WidgetType"),     #  ADD
    "datetimeFilter": data.get("DatetimeFilter"),
    "startTime": data.get("StartTime"),
    "endTime": data.get("EndTime"),
    "dataFilter": data.get("DataFilter"),
    "srNo": data.get("SrNo"),                 #  ADD
    "name": data.get("Name")                  #  ADD
}

        else:
            form = await request.form

            dashboard_id = form.get("ParentID")
            widget_name = form.get("Name")
            user_id = form.get("CreatedBy")

            config = {
    "deviceIds": form.get("DeviceIDs"),
    "tags": form.get("ParameterID"),
    "widgetType": form.get("WidgetType"),     #  ADD
    "datetimeFilter": form.get("DatetimeFilter"),
    "startTime": form.get("StartTime"),
    "endTime": form.get("EndTime"),
    "dataFilter": form.get("DataFilter"),
    "srNo": form.get("SrNo"),                 #  ADD
    "name": form.get("Name")                  #  ADD
}

        #  DB insert
        async with DB() as db:
            await db.execute(f"""
                INSERT INTO user_widgets_new (user_id, dashboard_id, widget_name, config)
                VALUES ('{user_id}', '{dashboard_id}', '{widget_name}', '{json.dumps(config)}')
            """)

        return jsonify({"Result": 1, "Message": "Widget saved"})

    except Exception as e:
        print("SAVE WIDGET ERROR:", e)
        return jsonify({"Result": 0, "Message": str(e)})

@report.route("/SaveDashBoard", methods=["GET"])
async def save_dashboard():
    try:
        dashboard_name = request.args.get("dashboardName")
        user_id = request.args.get("userId")

        dashboard_id = str(uuid4())
        #  FIXED QUERY (NO %s)
        query = f"""
            INSERT INTO user_dashboards (user_id, dashboard_id, dashboard_name)
            VALUES ('{user_id}', '{dashboard_id}', '{dashboard_name}')
        """

        async with DB() as db:
            await db.execute(query)

        return jsonify({
            "Result": 1,
            "Message": "Dashboard saved"
        })

    except Exception as e:
        print("SAVE DASHBOARD ERROR:", e)
        return jsonify({
            "Result": 0,
            "Message": str(e)
        })
        
@trend.route("/getWidgetConfig", methods=["GET"])
async def get_widget_config():
    widget_id = request.args.get("widgetId")

    query = f"""
        SELECT config
        FROM user_widgets_new
        WHERE id = '{widget_id}'
        LIMIT 1
    """

    async with DB() as db:
        row = await db.fetchrow(query)

    if not row:
        return jsonify({})

    config = row["config"]

    if isinstance(config, str):
        config = json.loads(config)

    return jsonify(config)