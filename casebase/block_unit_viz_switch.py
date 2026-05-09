import os
import pyvista as pv
import numpy as np
import psycopg2
import math
from shapely.wkt import loads as wkt_loads
import utils_z
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

db_config = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT")) # 可以设置默认值并转换类型
}

# ==============================
# 数据库连接
# ==============================
conn = psycopg2.connect(**db_config)

city_name = "lyon"
z_scale   = 1  # 米（纽约用0.3048）

block_table_name = f"block.{city_name}_blocks"

lod1_table_name         = f"lod1.{city_name}_buildings_lod1"
lod1_surface_table_name = f"lod1.{city_name}_building_surfaces_lod1"

lod2_table_name         = f"lod2.{city_name}_buildings_lod2"
lod2_surface_table_name = f"lod2.{city_name}_building_surfaces_lod2"


# ==============================
# 获取 block 列表
# ==============================
sql_blocks = f"""
    SELECT bl.block_id, ST_AsText(bl.geom) AS geom_wkt,
           ST_X(bl.centroid::geometry) AS lon,
           ST_Y(bl.centroid::geometry) AS lat,
           bl.area_m2
    FROM {block_table_name} bl
    WHERE EXISTS (
        SELECT 1 FROM {lod1_table_name} b WHERE b.block_id = bl.block_id
    )
    AND EXISTS (
        SELECT 1 FROM {lod2_table_name} b WHERE b.block_id = bl.block_id
    )
    ORDER BY RANDOM()
    LIMIT 30;
"""
rows = utils_z.run_sql(sql_blocks, conn=conn, fetch=True)

block_ids           = [str(r[0]) for r in rows]
block_wkts          = [r[1] for r in rows]
block_centroids_lon = [r[2] for r in rows]
block_centroids_lat = [r[3] for r in rows]
block_areas         = [float(r[4]) for r in rows]

print(f"Selected {len(block_ids)} blocks (有LOD1和LOD2数据)")


# ==============================
# 查询 surfaces
# ==============================
def fetch_surfaces(block_id, lod):
    if lod == 1:
        building_table = lod1_table_name
        surface_table  = lod1_surface_table_name
    else:
        building_table = lod2_table_name
        surface_table  = lod2_surface_table_name

    sql = f"""
        SELECT s.surface_type, ST_AsText(s.geom_3d)
        FROM {surface_table} s
        JOIN {building_table} b ON s.building_id = b.building_id
        WHERE b.block_id = '{block_id}';
    """
    return utils_z.run_sql(sql, conn=conn, fetch=True)


# ==============================
# polygon → triangulated mesh
# ==============================
def polygon_to_mesh(coords, cx, cy, base_z, meters_per_deg_lon, meters_per_deg_lat, z_scale=1.0):
    pts = np.array([
        [
            (c[0] - cx) * meters_per_deg_lon,
            (c[1] - cy) * meters_per_deg_lat,
            ((c[2] if len(c) > 2 else base_z) - base_z) * z_scale
        ]
        for c in coords[:-1]
    ])
    if len(pts) < 3:
        return None
    faces = np.array([len(pts)] + list(range(len(pts))))
    return pv.PolyData(pts, faces).triangulate()


# ==============================
# 构建建筑mesh（不含block地面）
# ==============================
def build_building_mesh(block_id, cx, cy, base_z,
                         meters_per_deg_lon, meters_per_deg_lat, lod):
    surfaces = fetch_surfaces(block_id, lod)

    # 重新计算base_z（当前lod的）
    local_base_z = float('inf')
    for stype, wkt in surfaces:
        try:
            poly = wkt_loads(wkt)
            for c in poly.exterior.coords:
                if len(c) > 2:
                    local_base_z = min(local_base_z, c[2])
        except Exception as e:
            print("Z parse error:", e)
    if local_base_z == float('inf'):
        local_base_z = base_z

    meshes = []
    for stype, wkt in surfaces:
        try:
            poly   = wkt_loads(wkt)
            coords = list(poly.exterior.coords)
            if len(coords) < 4:
                continue
            mesh = polygon_to_mesh(coords, cx, cy, local_base_z,
                                   meters_per_deg_lon, meters_per_deg_lat, z_scale)
            if mesh is None or mesh.n_points == 0 or mesh.n_cells == 0:
                continue

            if stype == "RoofSurface":
                hex_color = "#f1a493"  
            elif stype == "WallSurface":
                hex_color = "#f8efea"
            else:
                hex_color = "#c1a4a0"
            color = pv.Color(hex_color).int_rgb

            mesh["color"] = np.tile(color, (mesh.n_cells, 1))
            meshes.append(mesh)

        except Exception as e:
            print("Surface error:", e)

    if not meshes:
        return None
    return meshes[0].merge(meshes[1:]) if len(meshes) > 1 else meshes[0]


# ==============================
# 构建block地面mesh（只算一次）
# ==============================
def build_block_mesh(idx):
    block_geom = wkt_loads(block_wkts[idx])
    minx, miny, maxx, maxy = block_geom.bounds
    cx = (minx + maxx) / 2
    cy = (miny + maxy) / 2

    meters_per_deg_lat = 111320
    meters_per_deg_lon = math.cos(math.radians(cy)) * 111320

    # base_z用lod1数据计算
    surfaces_lod1 = fetch_surfaces(block_ids[idx], lod=1)
    base_z = float('inf')
    for stype, wkt in surfaces_lod1:
        try:
            poly = wkt_loads(wkt)
            for c in poly.exterior.coords:
                if len(c) > 2:
                    base_z = min(base_z, c[2])
        except Exception:
            pass
    if base_z == float('inf'):
        base_z = 0.0

    block_mesh = None
    try:
        block_coords = list(block_geom.exterior.coords)
        block_mesh   = polygon_to_mesh(
            block_coords, cx, cy, base_z,
            meters_per_deg_lon, meters_per_deg_lat
        )
    except Exception as e:
        print("Block mesh error:", e)

    return block_mesh, cx, cy, base_z, meters_per_deg_lon, meters_per_deg_lat


# ==============================
# 全局状态
# ==============================
plotter     = pv.Plotter(window_size=[1100, 850])
plotter.set_background("#FFFFFF") 
current_idx = [0]
current_lod = [1]  # 初始显示LOD1

# 缓存当前block的地面信息，避免切换lod时重复查询
cached_block = {
    "idx":                None,
    "block_mesh":         None,
    "cx":                 None,
    "cy":                 None,
    "base_z":             None,
    "meters_per_deg_lon": None,
    "meters_per_deg_lat": None,
}


# ==============================
# 只更新建筑mesh（切换lod时调用）
# ==============================
def update_buildings_only():
    idx      = current_idx[0]
    lod      = current_lod[0]
    block_id = block_ids[idx]

    # 移除旧建筑actor（保留block地面）
    # pyvista没有按名称移除的简单API，所以clear后重绘所有
    plotter.clear()

    # 重绘block地面
    block_mesh = cached_block["block_mesh"]
    if block_mesh is not None:
        plotter.add_mesh(
            block_mesh,
            color="#e0d0c8",
            opacity=0.5,
            show_edges=False
        )
        block_edges = block_mesh.extract_feature_edges(
            boundary_edges=True,
            feature_edges=False,
            manifold_edges=False
        )
        plotter.add_mesh(block_edges, color="#1F1F1F", line_width=4)

    # 重绘建筑
    building_mesh = build_building_mesh(
        block_id,
        cached_block["cx"],
        cached_block["cy"],
        cached_block["base_z"],
        cached_block["meters_per_deg_lon"],
        cached_block["meters_per_deg_lat"],
        lod
    )
    if building_mesh is not None:
        plotter.add_mesh(building_mesh, scalars="color", rgb=True, show_edges=False)
        building_edges = building_mesh.extract_feature_edges(
            boundary_edges=True,
            feature_edges=True,
            feature_angle=30,
            manifold_edges=False
        )
        plotter.add_mesh(building_edges, color="#383838", line_width=1.2)

    _add_text()
    # 不reset_camera，保持当前视角


# ==============================
# 更新整个场景（切换block时调用）
# ==============================
def update_scene(idx):
    block_id = block_ids[idx]
    print(f"显示 {idx+1}/30: {block_id}  LOD{current_lod[0]}")

    plotter.clear()

    # 构建block地面并缓存
    block_mesh, cx, cy, base_z, mpd_lon, mpd_lat = build_block_mesh(idx)
    cached_block["idx"]                = idx
    cached_block["block_mesh"]         = block_mesh
    cached_block["cx"]                 = cx
    cached_block["cy"]                 = cy
    cached_block["base_z"]             = base_z
    cached_block["meters_per_deg_lon"] = mpd_lon
    cached_block["meters_per_deg_lat"] = mpd_lat

    # 绘制block地面
    if block_mesh is not None:
        plotter.add_mesh(
            block_mesh,
            color="#C8C4B0",
            opacity=0.5,
            show_edges=False
        )
        block_edges = block_mesh.extract_feature_edges(
            boundary_edges=True,
            feature_edges=False,
            manifold_edges=False
        )
        plotter.add_mesh(block_edges, color="#888880", line_width=2)

    # 绘制建筑
    building_mesh = build_building_mesh(
        block_id, cx, cy, base_z, mpd_lon, mpd_lat, current_lod[0]
    )
    if building_mesh is not None:
        plotter.add_mesh(building_mesh, scalars="color", rgb=True, show_edges=False)
        building_edges = building_mesh.extract_feature_edges(
            boundary_edges=True,
            feature_edges=True,
            feature_angle=30,
            manifold_edges=False
        )
        plotter.add_mesh(building_edges, color="#383838", line_width=1)

    _add_text()
    plotter.reset_camera()


# ==============================
# 显示文字
# ==============================
def _add_text():
    idx   = current_idx[0]
    lod   = current_lod[0]
    lon   = block_centroids_lon[idx]
    lat   = block_centroids_lat[idx]
    area  = block_areas[idx]
    block_id = block_ids[idx]

    lod_str = f"LOD{lod}"
    plotter.add_text(
        f"Block: {block_id}   [{idx+1}/30]   {lod_str}\n"
        f"Centroid: ({lon:.6f}, {lat:.6f})\n"
        f"Area: {area:.2f} m²\n"
        f"J/K Switch Block   S Switch LOD   Q Quit",
        position="upper_left",
        font_size=11
    )


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

    elif key == "s":
        # 切换LOD，保持视角
        current_lod[0] = 2 if current_lod[0] == 1 else 1
        print(f"切换到 LOD{current_lod[0]}")
        update_buildings_only()


plotter.iren.add_observer("KeyPressEvent", key_callback)


# ==============================
# 启动
# ==============================
update_scene(current_idx[0])
plotter.show()
