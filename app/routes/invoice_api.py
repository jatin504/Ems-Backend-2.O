from quart import Blueprint, jsonify, request, send_file
import os
from datetime import datetime

invoice_bp = Blueprint("invoice", __name__)

BASE_FOLDER = r"D:\new_EMS\ems_backend_data\ems_backend_data\pass"
DAY_FOLDER = os.path.join(BASE_FOLDER, "Day")
MONTH_FOLDER = os.path.join(BASE_FOLDER, "month")


# ===========================
# GET INVOICES
# ===========================
@invoice_bp.route("/", methods=["GET"])
async def get_invoices():
    try:
        filter_type = request.args.get("filterType", "day")

        selected_date = request.args.get("date")
        selected_month = request.args.get("month")
        selected_year = request.args.get("year")

        page = int(request.args.get("page", 1))
        limit = int(request.args.get("limit", 10))

        sort_by = request.args.get("sortBy", "created_at")
        sort_order = request.args.get("sortOrder", "desc")

        # folder selection
        FOLDER_PATH = MONTH_FOLDER if filter_type == "month" else DAY_FOLDER

        if not os.path.exists(FOLDER_PATH):
            return jsonify({"success": False, "error": "Folder not found"})

        files = os.listdir(FOLDER_PATH)
        pdf_files = [f for f in files if f.endswith(".pdf")]

        data = []

        for file in pdf_files:
            full_path = os.path.join(FOLDER_PATH, file)

            # DATE FROM FILENAME
            try:
                file_date = file.split("_")[-1].replace(".pdf", "")
                file_dt = datetime.strptime(file_date, "%Y-%m-%d")
            except:
                continue

            file_month = file_dt.month
            file_year = file_dt.year

            # ===================
            # FILTERING
            # ===================
            if filter_type == "day":
                if selected_date and file_date != selected_date:
                    continue

            if filter_type == "month":
                if selected_month and int(selected_month) != file_month:
                    continue
                if selected_year and int(selected_year) != file_year:
                    continue

            data.append({
                "invoice_no": file.replace(".pdf", ""),
                "file_name": file,
                "created_at": file_dt.isoformat()
            })

        # ===================
        # SORTING
        # ===================
        reverse = sort_order.lower() == "desc"

        if sort_by == "invoice_no":
            data.sort(key=lambda x: x["invoice_no"], reverse=reverse)
        else:
            data.sort(key=lambda x: x["created_at"], reverse=reverse)

        # ===================
        # PAGINATION
        # ===================
        total = len(data)
        start = (page - 1) * limit
        end = start + limit

        paginated_data = data[start:end]

        for i, item in enumerate(paginated_data, start=start + 1):
            item["s_no"] = i

        return jsonify({
            "success": True,
            "data": paginated_data,
            "pagination": {
                "page": page,
                "limit": limit,
                "total": total,
                "total_pages": (total + limit - 1) // limit
            }
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


# ===========================
# PDF VIEW ROUTE (IMPORTANT)
# ===========================
@invoice_bp.route("/files/<filename>")
async def serve_file(filename):
    try:
        day_path = os.path.join(DAY_FOLDER, filename)
        month_path = os.path.join(MONTH_FOLDER, filename)

        if os.path.exists(day_path):
            return await send_file(day_path, mimetype="application/pdf")

        if os.path.exists(month_path):
            return await send_file(month_path, mimetype="application/pdf")

        return jsonify({"success": False, "error": "File not found"}), 404

    except Exception as e:
        return jsonify({"success": False, "error": str(e)})