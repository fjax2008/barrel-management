"""
生管部在制品桶号管理系统 - 后端服务
FastAPI + SQLite，支持批量出入库 + Excel 导出
"""
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import sqlite3
import os
import io
from datetime import datetime

app = FastAPI(title="生管部在制品桶号管理系统")

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "barrel.db")


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS barrel_inventory (
        barrel_no TEXT PRIMARY KEY,
        location TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'in',
        update_time TEXT NOT NULL
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS barrel_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        barrel_no TEXT NOT NULL,
        action TEXT NOT NULL,
        location TEXT NOT NULL,
        destination TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL
    )''')
    # 兼容旧数据库：添加 destination 列
    try:
        c.execute("ALTER TABLE barrel_log ADD COLUMN destination TEXT NOT NULL DEFAULT ''")
    except:
        pass
    conn.commit()
    conn.close()


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ─── Models ───────────────────────────────────────────────

class InboundRequest(BaseModel):
    barrel_no: str
    location: str


class OutboundRequest(BaseModel):
    barrel_no: str
    destination: str = ""  # 出库目的地，如 RG03 / FJ05


class BatchInboundRequest(BaseModel):
    barrel_nos: list[str]
    location: str


class BatchOutboundRequest(BaseModel):
    barrel_nos: list[str]
    destination: str = ""  # 出库目的地


# ─── 单桶入库 ─────────────────────────────────────────────

@app.post("/api/inbound")
def inbound(req: InboundRequest):
    conn = get_db()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    c.execute("SELECT * FROM barrel_inventory WHERE barrel_no = ?", (req.barrel_no,))
    existing = c.fetchone()
    if existing and existing["status"] == "in":
        conn.close()
        return {"success": False, "msg": f"桶号 {req.barrel_no} 已在 {existing['location']}，请先出库再入库"}

    c.execute('''INSERT OR REPLACE INTO barrel_inventory
                 (barrel_no, location, status, update_time)
                 VALUES (?, ?, 'in', ?)''',
              (req.barrel_no, req.location, now))
    c.execute("INSERT INTO barrel_log (barrel_no, action, location, created_at) VALUES (?, '入库', ?, ?)",
              (req.barrel_no, req.location, now))
    conn.commit()
    conn.close()
    return {"success": True, "msg": f"桶号 {req.barrel_no} 已入库 → {req.location}"}


# ─── 批量入库 ─────────────────────────────────────────────

@app.post("/api/inbound/batch")
def inbound_batch(req: BatchInboundRequest):
    conn = get_db()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    success_list, skip_list, fail_list = [], [], []
    for barrel_no in req.barrel_nos:
        barrel_no = barrel_no.strip()
        if not barrel_no:
            continue
        c.execute("SELECT * FROM barrel_inventory WHERE barrel_no = ?", (barrel_no,))
        existing = c.fetchone()
        if existing and existing["status"] == "in":
            skip_list.append(f"{barrel_no}(已在{existing['location']})")
            continue

        try:
            c.execute('''INSERT OR REPLACE INTO barrel_inventory
                         (barrel_no, location, status, update_time)
                         VALUES (?, ?, 'in', ?)''',
                      (barrel_no, req.location, now))
            c.execute("INSERT INTO barrel_log (barrel_no, action, location, created_at) VALUES (?, '入库', ?, ?)",
                      (barrel_no, req.location, now))
            success_list.append(barrel_no)
        except Exception:
            fail_list.append(barrel_no)

    conn.commit()
    conn.close()
    return {
        "success": True,
        "location": req.location,
        "success_count": len(success_list),
        "skip_count": len(skip_list),
        "success_list": success_list,
        "skip_list": skip_list,
    }


# ─── 单桶出库 ─────────────────────────────────────────────

@app.post("/api/outbound")
def outbound(req: OutboundRequest):
    conn = get_db()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    c.execute("SELECT * FROM barrel_inventory WHERE barrel_no = ? AND status = 'in'", (req.barrel_no,))
    existing = c.fetchone()
    if not existing:
        conn.close()
        return {"success": False, "msg": f"桶号 {req.barrel_no} 不在库中，无法出库"}

    loc = existing["location"]
    c.execute("UPDATE barrel_inventory SET status = 'out', update_time = ? WHERE barrel_no = ?",
              (now, req.barrel_no))
    dest = req.destination.strip()
    c.execute("INSERT INTO barrel_log (barrel_no, action, location, destination, created_at) VALUES (?, '出库', ?, ?, ?)",
              (req.barrel_no, loc, dest, now))
    conn.commit()
    conn.close()
    dest_info = f" → {dest}" if dest else ""
    return {"success": True, "msg": f"桶号 {req.barrel_no} 已从 {loc} 出库{dest_info}", "from_location": loc}


# ─── 批量出库 ─────────────────────────────────────────────

@app.post("/api/outbound/batch")
def outbound_batch(req: BatchOutboundRequest):
    conn = get_db()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    success_list, not_found_list = [], []
    dest = req.destination.strip()
    for barrel_no in req.barrel_nos:
        barrel_no = barrel_no.strip()
        if not barrel_no:
            continue
        c.execute("SELECT * FROM barrel_inventory WHERE barrel_no = ? AND status = 'in'", (barrel_no,))
        existing = c.fetchone()
        if not existing:
            not_found_list.append(barrel_no)
            continue

        loc = existing["location"]
        c.execute("UPDATE barrel_inventory SET status = 'out', update_time = ? WHERE barrel_no = ?",
                  (now, barrel_no))
        c.execute("INSERT INTO barrel_log (barrel_no, action, location, destination, created_at) VALUES (?, '出库', ?, ?, ?)",
                  (barrel_no, loc, dest, now))
        success_list.append({"barrel_no": barrel_no, "from_location": loc})

    conn.commit()
    conn.close()
    return {
        "success": True,
        "success_count": len(success_list),
        "not_found_count": len(not_found_list),
        "success_list": success_list,
        "not_found_list": not_found_list,
    }


class DeleteRequest(BaseModel):
    barrel_nos: list[str]


# ─── 删除桶号 ─────────────────────────────────────────────

@app.delete("/api/barrel/{barrel_no}")
def delete_barrel(barrel_no: str):
    conn = get_db()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("SELECT * FROM barrel_inventory WHERE barrel_no = ?", (barrel_no,))
    row = c.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"桶号 {barrel_no} 不存在")
    loc = row["location"]
    status = row["status"]
    c.execute("DELETE FROM barrel_inventory WHERE barrel_no = ?", (barrel_no,))
    # 保留历史日志，新增一条删除记录
    c.execute("INSERT INTO barrel_log (barrel_no, action, location, destination, created_at) VALUES (?, '删除', ?, '', ?)",
              (barrel_no, loc, now))
    conn.commit()
    conn.close()
    return {"success": True, "msg": f"桶号 {barrel_no}（{status} / {loc}）已删除"}


@app.post("/api/barrel/delete/batch")
def delete_barrel_batch(req: DeleteRequest):
    conn = get_db()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    deleted, not_found = [], []
    for barrel_no in req.barrel_nos:
        barrel_no = barrel_no.strip()
        if not barrel_no: continue
        c.execute("SELECT * FROM barrel_inventory WHERE barrel_no = ?", (barrel_no,))
        row = c.fetchone()
        if not row:
            not_found.append(barrel_no)
            continue
        loc = row["location"]
        c.execute("DELETE FROM barrel_inventory WHERE barrel_no = ?", (barrel_no,))
        c.execute("INSERT INTO barrel_log (barrel_no, action, location, destination, created_at) VALUES (?, '删除', ?, '', ?)",
                  (barrel_no, loc, now))
        deleted.append(barrel_no)
    conn.commit()
    conn.close()
    return {"success": True, "deleted": deleted, "not_found": not_found,
            "msg": f"已删除 {len(deleted)} 桶" + (f"，{len(not_found)} 桶不存在" if not_found else "")}


# ─── 数据恢复 ─────────────────────────────────────────────

@app.post("/api/import")
def import_data(data: list[dict]):
    """批量导入备份数据"""
    conn = get_db()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    imported = 0
    for item in data:
        bn = item.get("barrel_no", "").strip()
        loc = item.get("location", "").strip()
        st = item.get("status", "in")
        if not bn or not loc: continue
        c.execute("INSERT OR REPLACE INTO barrel_inventory (barrel_no, location, status, update_time) VALUES (?, ?, ?, ?)",
                  (bn, loc, st, now))
        c.execute("INSERT INTO barrel_log (barrel_no, action, location, destination, created_at) VALUES (?, '入库', ?, '', ?)",
                  (bn, loc, now))
        imported += 1
    conn.commit()
    conn.close()
    return {"success": True, "msg": f"已恢复 {imported} 条记录"}


# ─── 查询 ─────────────────────────────────────────────────

@app.get("/api/inventory")
def get_inventory():
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM barrel_inventory WHERE status = 'in' ORDER BY location")
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


@app.get("/api/inventory/{keyword}")
def query_barrel(keyword: str):
    """模糊查询: 按桶号或储位搜索"""
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM barrel_inventory WHERE barrel_no LIKE ? ORDER BY status='in' DESC, update_time DESC", (f"%{keyword}%",))
    rows = [dict(r) for r in c.fetchall()]
    if not rows:
        c.execute("SELECT * FROM barrel_inventory WHERE location LIKE ? AND status = 'in' ORDER BY location", (f"%{keyword}%",))
        rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


@app.get("/api/inventory/location/{location}")
def query_location(location: str):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM barrel_inventory WHERE location = ? AND status = 'in'", (location,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


@app.get("/api/logs")
def get_logs(limit: int = 100):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM barrel_log ORDER BY id DESC LIMIT ?", (limit,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


# ─── Excel 导出 ───────────────────────────────────────────

@app.get("/api/export")
def export_inventory():
    try:
        from urllib.parse import quote
        from openpyxl import Workbook
        from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
    except ImportError:
        raise HTTPException(500, "openpyxl 未安装，请执行 pip install openpyxl")

    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM barrel_inventory WHERE status = 'in' ORDER BY location")
    rows = c.fetchall()
    conn.close()

    wb = Workbook()
    ws = wb.active
    ws.title = "在库清单"

    # 表头样式
    header_font = Font(name="微软雅黑", bold=True, size=12, color="FFFFFF")
    header_fill = PatternFill(start_color="1677FF", end_color="1677FF", fill_type="solid")
    header_align = Alignment(horizontal="center", vertical="center")
    thin_border = Border(
        left=Side(style="thin"), right=Side(style="thin"),
        top=Side(style="thin"), bottom=Side(style="thin")
    )
    cell_font = Font(name="微软雅黑", size=11)
    cell_align = Alignment(horizontal="center", vertical="center")

    # 写标题行
    ws.merge_cells("A1:D1")
    ws["A1"] = "生管部在制品桶号管理 - 库存清单"
    ws["A1"].font = Font(name="微软雅黑", bold=True, size=14, color="1677FF")
    ws["A1"].alignment = Alignment(horizontal="center", vertical="center")

    ws.merge_cells("A2:D2")
    ws["A2"] = f"导出时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}　　共 {len(rows)} 桶在库"
    ws["A2"].font = Font(name="微软雅黑", size=10, color="888888")
    ws["A2"].alignment = Alignment(horizontal="center", vertical="center")

    # 表头
    headers = ["序号", "桶号", "储位", "入库时间"]
    for col_idx, header in enumerate(headers, 1):
        cell = ws.cell(row=4, column=col_idx, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_align
        cell.border = thin_border

    # 数据行
    for row_idx, row in enumerate(rows, 1):
        values = [row_idx, row["barrel_no"], row["location"], row["update_time"]]
        for col_idx, val in enumerate(values, 1):
            cell = ws.cell(row=row_idx + 4, column=col_idx, value=val)
            cell.font = cell_font
            cell.alignment = cell_align
            cell.border = thin_border

    # 列宽
    ws.column_dimensions["A"].width = 8
    ws.column_dimensions["B"].width = 22
    ws.column_dimensions["C"].width = 14
    ws.column_dimensions["D"].width = 22

    # 行高
    ws.row_dimensions[1].height = 30
    ws.row_dimensions[2].height = 20
    ws.row_dimensions[4].height = 24

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    filename = f"桶号库存_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    encoded_filename = quote(filename)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"}
    )


# ─── 静态文件 (在 API 路由之后挂载) ─────────────────────────

app.mount("/", StaticFiles(directory="static", html=True), name="static")

init_db()
print("✅ 数据库初始化完成")
