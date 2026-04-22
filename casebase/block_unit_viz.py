import pyvista as pv
import numpy as np
import psycopg2
import math
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

city_name = "zurich"

block_table_name        = f"{city_name}_blocks"
lod2_table_name         = f"{city_name}_buildings_lod2"
lod2_surface_table_name = f"{city_name}_building_surfaces_lod2"


# ==============================
# 获取 block 列表
# ==============================
sql_blocks = f"""
    SELECT bl.block_id, ST_AsText(bl.geom) AS geom_wkt, 
           ST_X(bl.centroid::geometry) AS lon, 
           ST_Y(bl.centroid::geometry) AS lat
    FROM {block_table_name} bl
    WHERE EXISTS (
        SELECT 1 FROM {lod2_table_name} b
        WHERE b.block_id = bl.block_id
    )
    ORDER BY RANDOM()
    LIMIT 30;
"""
rows = utils_z.run_sql(sql_blocks, conn=conn, fetch=True)

block_ids           = [str(r[0]) for r in rows]
block_wkts          = [r[1] for r in rows]
block_centroids_lon = [r[2] for r in rows]  # 经度
block_centroids_lat = [r[3] for r in rows]  # 纬度

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
def polygon_to_mesh(coords, cx, cy, base_z, meters_per_deg_lon, meters_per_deg_lat):
    pts = np.array([
        [
            (c[0] - cx) * meters_per_deg_lon,
            (c[1] - cy) * meters_per_deg_lat,
            (c[2] if len(c) > 2 else base_z) - base_z
        ]
        for c in coords[:-1]
    ])
    if len(pts) < 3:
        return None
    faces = np.array([len(pts)] + list(range(len(pts))))
    return pv.PolyData(pts, faces).triangulate()


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

    # ---- 米每度（在该纬度）----
    meters_per_deg_lat = 111320
    meters_per_deg_lon = math.cos(math.radians(cy)) * 111320

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

    # ---- block mesh ----
    block_mesh = None
    try:
        block_coords = list(block_geom.exterior.coords)
        block_mesh = polygon_to_mesh(
            block_coords, cx, cy, base_z,
            meters_per_deg_lon, meters_per_deg_lat
        )
    except Exception as e:
        print("Block mesh error:", e)

    # ---- building surfaces ----
    meshes = []
    for stype, wkt in surfaces:
        try:
            poly = wkt_loads(wkt)
            mesh = polygon_to_mesh(
                list(poly.exterior.coords), cx, cy, base_z,
                meters_per_deg_lon, meters_per_deg_lat
            )
            if mesh is None:
                continue

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
    lon = block_centroids_lon[idx]
    lat = block_centroids_lat[idx]
    print(f"显示 {idx+1}/30: {block_id}")

    plotter.clear()

    block_mesh, building_mesh = build_block_mesh(idx)

    if building_mesh is not None:
        plotter.add_mesh(
            building_mesh,
            scalars="color",
            rgb=True,
            show_edges=False
        )
        # 新增：提取并绘制建筑边框
        building_edges = building_mesh.extract_feature_edges(
            boundary_edges=True,
            feature_edges=True,
            feature_angle=30,
            manifold_edges=False
        )
        plotter.add_mesh(
            building_edges,
            color="#383838",
            line_width=1
        )

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


    # 显示 Block ID 和中心点坐标
    plotter.add_text(
        f"Block: {block_id}   [{idx+1}/30]\nCentroid: ({lon:.6f}, {lat:.6f})\nJ/K 切换   Q 退出",
        position="upper_left",
        font_size=11
    )

    plotter.reset_camera()


# ==============================
# 键盘事件
# ==============================
def key_callback(obj, event):
    key = obj.GetKeySym().lower()

    if key == "j":
        current_idx[0] = (current_idx[0] - 1) % len(block_ids)
        update_scene(current_idx[0])
    elif key == "k":
        current_idx[0] = (current_idx[0] + 1) % len(block_ids)
        update_scene(current_idx[0])


plotter.iren.add_observer("KeyPressEvent", key_callback)


# ==============================
# 启动
# ==============================
update_scene(current_idx[0])
plotter.show()
