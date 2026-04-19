# # viz_blocks.py
# import pyvista as pv
# import numpy as np
# import psycopg2
# from shapely.wkt import loads as wkt_loads

# import utils_z


# # ── Database connection (directly written here) ──────────────────────────────
# conn = psycopg2.connect(
#     host="localhost",
#     database="Test20260413",
#     user="postgres",
#     password="we6666",
#     port=5432,
# )

# block_table_name       = "vienna_blocks"
# lod2_table_name        = "vienna_buildings_lod2"
# lod2_surface_table_name = "vienna_building_surfaces_lod2"


# # ── 1. Randomly fetch 30 blocks with LOD2 buildings from the database ────────
# sql_blocks = f"""
#     SELECT bl.block_id, ST_AsText(bl.geom) AS geom_wkt
#     FROM {block_table_name} bl
#     WHERE EXISTS (
#         SELECT 1 FROM {lod2_table_name} b
#         WHERE b.block_id = bl.block_id
#     )
#     ORDER BY RANDOM()
#     LIMIT 30;
# """
# rows = utils_z.run_sql(sql_blocks, conn=conn, fetch=True)
# block_ids  = [r[0] for r in rows]
# block_wkts = [r[1] for r in rows]
# print(f"Selected {len(block_ids)} blocks")


# # ── 2. Query function ─────────────────────────────────────────────────────────
# def fetch_surfaces(block_id):
#     sql = f"""
#         SELECT s.surface_type, ST_AsText(s.geom_3d) AS geom_wkt
#         FROM {lod2_surface_table_name} s
#         JOIN {lod2_table_name} b ON s.building_id = b.building_id
#         WHERE b.block_id = '{block_id}';
#     """
#     return utils_z.run_sql(sql, conn=conn, fetch=True)


# # ── 3. Geometry construction tools ───────────────────────────────────────────
# def polygon_to_pyvista(coords_3d, cx, cy, base_z):
#     """Convert a set of 3D coordinates to a PyVista face array (remove closed duplicate points)"""
#     pts = np.array([[c[0]-cx, c[1]-cy, (c[2] if len(c)>2 else base_z)-base_z]
#                     for c in coords_3d[:-1]])   # Remove the last duplicate point
#     if len(pts) < 3:
#         return None, None
#     faces = np.array([len(pts)] + list(range(len(pts))))
#     return pts, faces


# def build_block_scene(idx):
#     """Build the complete PyVista scene for the idx-th block, return plotter"""
#     block_id  = block_ids[idx]
#     block_wkt = block_wkts[idx]
#     block_geom = wkt_loads(block_wkt)

#     surfaces = fetch_surfaces(block_id)

#     # Center point and base Z
#     minx, miny, maxx, maxy = block_geom.bounds
#     cx = (minx + maxx) / 2
#     cy = (miny + maxy) / 2

#     base_z = float('inf')
#     for stype, wkt in surfaces:
#         try:
#             poly = wkt_loads(wkt)
#             for c in poly.exterior.coords:
#                 if len(c) > 2:
#                     base_z = min(base_z, c[2])
#         except:
#             continue
#     if base_z == float('inf'):
#         base_z = 0.0

#     # ── Create plotter ───────────────────────────────────────────────────────
#     pl = pv.Plotter(window_size=[1100, 850], title=f"Block {block_id}  [{idx+1}/30]")
#     pl.set_background("#F0EDE8")

#     # ── Draw block ground outline (extrude a thin slice) ─────────────────────
#     block_coords = list(block_geom.exterior.coords)
#     pts_2d = np.array([[c[0]-cx, c[1]-cy, -0.2] for c in block_coords[:-1]])
#     faces_2d = np.array([len(pts_2d)] + list(range(len(pts_2d))))
#     if len(pts_2d) >= 3:
#         mesh_block = pv.PolyData(pts_2d, faces_2d)
#         pl.add_mesh(mesh_block, color="#C8C4B0", show_edges=True,
#                     edge_color="#888880", line_width=1.5, label="Block")

#     # ── Color mapping ────────────────────────────────────────────────────────
#     color_map = {
#         "RoofSurface":   "#B85050",
#         "WallSurface":   "#D4A878",
#         "GroundSurface": "#909088",
#     }

#     # Batch collect vertices by type to reduce add_mesh calls
#     type_points = {"RoofSurface": [], "WallSurface": [], "GroundSurface": [], "Other": []}
#     type_faces  = {"RoofSurface": [], "WallSurface": [], "GroundSurface": [], "Other": []}
#     type_offset = {"RoofSurface": 0,  "WallSurface": 0,  "GroundSurface": 0,  "Other": 0}

#     for stype, wkt in surfaces:
#         try:
#             poly = wkt_loads(wkt)
#             coords_3d = list(poly.exterior.coords)
#             pts, faces = polygon_to_pyvista(coords_3d, cx, cy, base_z)
#             if pts is None:
#                 continue
#             key = stype if stype in type_points else "Other"
#             offset = type_offset[key]
#             type_points[key].append(pts)
#             # Vertex indices in the faces array need to be offset by existing vertices
#             faces[1:] += offset
#             type_faces[key].append(faces)
#             type_offset[key] += len(pts)
#         except:
#             continue

#     for key, color in {**color_map, "Other": "#AAAAAA"}.items():
#         if not type_points[key]:
#             continue
#         all_pts   = np.vstack(type_points[key])
#         all_faces = np.concatenate(type_faces[key])
#         mesh = pv.PolyData(all_pts, all_faces)
#         pl.add_mesh(mesh, color=color, show_edges=True,
#                     edge_color="#00000033", line_width=0.5)

#     # ── Camera: Axonometric bird's-eye view ──────────────────────────────────
#     extent = max(maxx-minx, maxy-miny)
#     pl.camera_position = [
#         (extent*1.2,  -extent*1.2, extent*1.5),   # Camera position
#         (0, 0, 0),                                  # Look at center
#         (0, 0, 1),                                  # Up direction
#     ]
#     pl.enable_parallel_projection()   # Axonometric projection (remove perspective)

#     # ── Annotation ───────────────────────────────────────────────────────────
#     pl.add_text(
#         f"Block: {block_id}    [{idx+1}/30]\n"
#         f"← J / K → Switch      Q Exit",
#         position="upper_left", font_size=11, color="black"
#     )

#     return pl

# # ── 4. Main loop ─────────────────────────────────────────────────────────────
# current_idx = [0]
# direction   = [0]

# def show_block(idx):
#     direction[0] = 0
#     pl = build_block_scene(idx)

#     def on_key_j():
#         direction[0] = -1

#     def on_key_k():
#         direction[0] = 1

#     pl.add_key_event("j", on_key_j)
#     pl.add_key_event("k", on_key_k)

#     pl.add_timer_event(max_steps=10000, duration=100, callback=lambda: pl.close() if direction[0] != 0 else None)
#     pl.show()

# while True:
#     show_block(current_idx[0])
#     if direction[0] == 0:
#         break
#     current_idx[0] = (current_idx[0] + direction[0]) % len(block_ids)
#     print(f"切换到 {current_idx[0]+1}/30: {block_ids[current_idx[0]]}")

# print("退出")

# block_unit_viz_final.py

import pyvista as pv
import numpy as np
import psycopg2
from shapely.wkt import loads as wkt_loads
import utils_z


# ==============================
# 数据库连接
# ==============================
conn = psycopg2.connect(
    host="localhost",
    database="Test20260413",
    user="postgres",
    password="we6666",
    port=5432,
)

block_table_name        = "vienna_blocks"
lod2_table_name         = "vienna_buildings_lod2"
lod2_surface_table_name = "vienna_building_surfaces_lod2"


# ==============================
# 获取 block 列表
# ==============================
sql_blocks = f"""
    SELECT bl.block_id, ST_AsText(bl.geom) AS geom_wkt
    FROM {block_table_name} bl
    WHERE EXISTS (
        SELECT 1 FROM {lod2_table_name} b
        WHERE b.block_id = bl.block_id
    )
    ORDER BY RANDOM()
    LIMIT 30;
"""
rows = utils_z.run_sql(sql_blocks, conn=conn, fetch=True)

block_ids  = [str(r[0]) for r in rows]
block_wkts = [r[1] for r in rows]

print(f"Selected {len(block_ids)} blocks")


# ==============================
# 查询 surfaces
# ==============================
def fetch_surfaces(block_id):
    sql = f"""
        SELECT s.surface_type, ST_AsText(s.geom_3d)
        FROM {lod2_surface_table_name} s
        JOIN {lod2_table_name} b ON s.building_id = b.building_id
        WHERE b.block_id = '{block_id}';
    """
    return utils_z.run_sql(sql, conn=conn, fetch=True)


# ==============================
# polygon → triangulated mesh
# ==============================
def polygon_to_mesh(coords, cx, cy, base_z):
    pts = np.array([
        [c[0]-cx, c[1]-cy, (c[2] if len(c)>2 else base_z)-base_z]
        for c in coords[:-1]
    ])

    if len(pts) < 3:
        return None

    faces = []
    for i in range(1, len(pts)-1):
        faces.append([3, 0, i, i+1])

    return pv.PolyData(pts, np.array(faces).flatten())


# ==============================
# 构建 mesh（核心）
# ==============================
def build_block_mesh(idx):
    block_id  = block_ids[idx]
    block_geom = wkt_loads(block_wkts[idx])

    surfaces = fetch_surfaces(block_id)

    # ---- center ----
    minx, miny, maxx, maxy = block_geom.bounds
    cx = (minx + maxx) / 2
    cy = (miny + maxy) / 2

    # ---- base Z ----
    base_z = float('inf')
    for stype, wkt in surfaces:
        try:
            poly = wkt_loads(wkt)
            for c in poly.exterior.coords:
                if len(c) > 2:
                    base_z = min(base_z, c[2])
        except Exception as e:
            print("Z parse error:", e)

    if base_z == float('inf'):
        base_z = 0.0

    # =====================
    # block mesh
    # =====================
    block_mesh = None
    try:
        block_coords = list(block_geom.exterior.coords)
        block_mesh = polygon_to_mesh(block_coords, cx, cy, base_z)
    except Exception as e:
        print("Block mesh error:", e)

    # =====================
    # building mesh（合并）
    # =====================
    meshes = []

    for stype, wkt in surfaces:
        try:
            poly = wkt_loads(wkt)
            mesh = polygon_to_mesh(poly.exterior.coords, cx, cy, base_z)

            if mesh is None:
                continue

            # 颜色
            if stype == "RoofSurface":
                color = [180, 80, 80]
            elif stype == "WallSurface":
                color = [200, 160, 120]
            else:
                color = [150, 150, 140]

            mesh["color"] = np.tile(color, (mesh.n_cells, 1))
            meshes.append(mesh)

        except Exception as e:
            print("Surface error:", e)

    building_mesh = None
    if meshes:
        building_mesh = meshes[0].merge(meshes[1:]) if len(meshes) > 1 else meshes[0]

    return block_mesh, building_mesh


# ==============================
# 全局 plotter（只创建一次）
# ==============================
plotter = pv.Plotter(window_size=[1100, 850])
current_idx = [0]


# ==============================
# 更新场景
# ==============================
def update_scene(idx):
    block_id = block_ids[idx]
    print(f"显示 {idx+1}/30: {block_id}")

    plotter.clear()

    block_mesh, building_mesh = build_block_mesh(idx)

    # block
    if block_mesh is not None:
        # ---- 填充面（无边）----
        plotter.add_mesh(
            block_mesh,
            color="#C8C4B0",
            opacity=0.5,
            show_edges=False
        )

        # ---- 单独画轮廓线 ----
        block_edges = block_mesh.extract_feature_edges(
            boundary_edges=True,
            feature_edges=False,
            manifold_edges=False
        )

        plotter.add_mesh(
            block_edges,
            color="#888880",
            line_width=2
        )

    # building
    if building_mesh is not None:
        plotter.add_mesh(
            building_mesh,
            scalars="color",
            rgb=True,
            show_edges=False
        )

    plotter.add_text(
        f"Block: {block_id}   [{idx+1}/30]\nJ/K 切换   Q 退出",
        position="upper_left",
        font_size=11
    )

    plotter.reset_camera()


# ==============================
# 键盘事件（核心稳定逻辑）
# ==============================
def key_callback(obj, event):
    key = obj.GetKeySym().lower()

    if key == "j":
        current_idx[0] = (current_idx[0] - 1) % len(block_ids)
        update_scene(current_idx[0])

    elif key == "k":
        current_idx[0] = (current_idx[0] + 1) % len(block_ids)
        update_scene(current_idx[0])

    # elif key == "q":
    #     print("退出")
    #     plotter.close()


# 绑定键盘事件
plotter.iren.add_observer("KeyPressEvent", key_callback)


# ==============================
# 启动
# ==============================
update_scene(current_idx[0])
plotter.show()