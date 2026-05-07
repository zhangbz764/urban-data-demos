from shapely.wkt import dumps as wkt_dumps
from shapely.wkt import loads as wkt_loads
from shapely.geometry import Polygon
import pandas as pd
import numpy as np


 # TODO: 筛除了multipolygon，筛除了没有高度的，目前处理的是3857

# def insert_buildings_shp(gdf, conn, lod1_table, city_prefix,
#                           building_counter,
#                           col_height=None,
#                           col_height_top=None,
#                           col_height_bottom=None,
#                           col_ground_z=None,
#                           col_citygml_id=None,
#                           col_floor_count=None,
#                           col_function=None,
#                           set_crs=None):
#     """
#     通用SHP建筑入库函数
#     高度支持两种模式：
#     - 直接提供：col_height（相对高度字段）
#     - 计算得到：col_height_top - col_height_bottom（绝对高程相减）
#     """
#     gdf = gdf.copy()

#     # 高度处理
#     if col_height is not None:
#         gdf['_height'] = gdf[col_height]
#     elif col_height_top is not None and col_height_bottom is not None:
#         gdf['_height'] = gdf[col_height_top] - gdf[col_height_bottom]
#     else:
#         raise ValueError("必须提供col_height，或者同时提供col_height_top和col_height_bottom")

#     #print(f"原始建筑数：{len(gdf)}")
    
#     # 过滤无效数据
#     gdf = gdf[gdf['_height'].notna() & (gdf['_height'] > 0)]
#     if col_ground_z:
#         gdf = gdf[gdf[col_ground_z].notna()]
#     #print(f"过滤后建筑数：{len(gdf)}")

#     # 转4326
#     if set_crs:
#         gdf = gdf.set_crs(set_crs, allow_override=True).to_crs(4326)
#     else:
#         gdf = gdf.to_crs(4326)

#     cur = conn.cursor()
#     sql = f"""
#         INSERT INTO {lod1_table}
#             (building_id, citygml_id, geom_2d, height, ground_z, floor_count, function)
#         VALUES (%s, %s, ST_GeomFromText(%s, 4326), %s, %s, %s, %s)
#         ON CONFLICT (building_id) DO NOTHING;
#     """

#     rows = []
#     for _, row in gdf.iterrows():
#         building_id = f"{city_prefix}_B_{str(building_counter).zfill(7)}"
#         building_counter += 1

#         geom = row.geometry
#         if geom is None or geom.is_empty:
#             continue
#         if geom.geom_type == 'MultiPolygon':
#             geom = max(geom.geoms, key=lambda g: g.area)

#         geom_2d     = Polygon([(c[0], c[1]) for c in geom.exterior.coords])
#         citygml_id  = str(row[col_citygml_id]) if col_citygml_id else None
#         ground_z    = float(row[col_ground_z]) if col_ground_z else 0.0
#         height      = float(row['_height'])
#         floor_count = int(row[col_floor_count]) if col_floor_count and pd.notna(row[col_floor_count]) else None
#         function    = str(row[col_function]) if col_function and pd.notna(row[col_function]) else None
#         rows.append((
#             building_id,
#             citygml_id,
#             wkt_dumps(geom_2d),
#             height,
#             ground_z,
#             floor_count,
#             function
#         ))

#     cur.executemany(sql, rows)
#     conn.commit()
#     cur.close()

#     return len(rows), building_counter

def insert_buildings_gdf_lod1(gdf, conn, lod1_table, city_prefix,
                          building_counter,
                          col_height=None,
                          col_area=None,
                          col_perimeter=None,
                          col_height_top=None,
                          col_height_bottom=None,
                          col_ground_z=None,
                          col_citygml_id=None,
                          col_floor_count=None,
                          col_function=None,
                          set_crs=None):
    """
    高度支持两种模式：
    - 直接提供：col_height（相对高度字段）
    - 计算得到：col_height_top - col_height_bottom（绝对高程相减）
    """
    gdf = gdf.copy()

    # 高度处理
    if col_height is not None:
        gdf['_height'] = gdf[col_height]
    elif col_height_top is not None and col_height_bottom is not None:
        gdf['_height'] = gdf[col_height_top] - gdf[col_height_bottom]
    else:
        raise ValueError("必须提供col_height，或者同时提供col_height_top和col_height_bottom")

    #print(f"原始建筑数：{len(gdf)}")
    
    # 过滤无效数据
    gdf = gdf[gdf['_height'].notna() & (gdf['_height'] > 0)]
    if col_ground_z:
        gdf = gdf[gdf[col_ground_z].notna()]
    print(f"过滤后建筑数：{len(gdf)}")

    # 转4326
    if set_crs:
        gdf = gdf.set_crs(set_crs, allow_override=True).to_crs(4326)
    else:
        gdf = gdf.to_crs(4326)

    cur = conn.cursor()
    sql = f"""
        INSERT INTO {lod1_table}
            (building_id, citygml_id, geom_2d, height, ground_z, floor_count, function, area, perimeter)
        VALUES (%s, %s, ST_GeomFromText(%s, 4326), %s, %s, %s, %s, %s, %s)
        ON CONFLICT (building_id) DO NOTHING;
    """

    rows = []
    for _, row in gdf.iterrows():
        building_id = f"{city_prefix}_B_{str(building_counter).zfill(7)}"
        building_counter += 1

        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        if geom.geom_type == 'MultiPolygon':
            geom = max(geom.geoms, key=lambda g: g.area)

        geom_2d     = Polygon([(c[0], c[1]) for c in geom.exterior.coords])
        citygml_id  = str(row[col_citygml_id]) if col_citygml_id else None
        ground_z    = float(row[col_ground_z]) if col_ground_z else 0.0
        height      = float(row['_height'])
        area        = float(row[col_area]) if col_area else 0.0
        perimeter   = float(row[col_perimeter]) if col_perimeter else 0.0
        floor_count = int(row[col_floor_count]) if col_floor_count and pd.notna(row[col_floor_count]) else None
        function    = str(row[col_function]) if col_function and pd.notna(row[col_function]) else None
        rows.append((
            building_id,
            citygml_id,
            wkt_dumps(geom_2d),
            height,
            ground_z,
            floor_count,
            function,
            area,
            perimeter
        ))

    cur.executemany(sql, rows)
    conn.commit()
    cur.close()

    return len(rows), building_counter


def insert_buildings_gdf_lod2(gdf, conn, lod2_table, surface_table, city_prefix,
                                    building_counter, surface_counter,
                                    source_srid=None,
                                    col_grd_z="GRD_ELEV_MIN_Z",
                                    col_roof_z="ROOFTOP_ELEV_Z",
                                    col_citygml_id=None,
                                    col_floor_count=None,
                                    col_function=None,
                                    col_roof_type=None,
                                    col_year_built=None,
                                    invalid_z=-32767):
    """
    通用3D MultiPolygon LOD2入库函数
    适用于geometry已经是3D面集合的gpkg/geojson数据
    """
    gdf = gdf.copy()

    # 坐标系处理
    if source_srid:
        gdf = gdf.set_crs(source_srid, allow_override=True)

    # 高度过滤
    gdf = gdf[gdf[col_grd_z] > invalid_z]
    gdf = gdf[gdf[col_roof_z] > invalid_z]
    gdf['_height'] = gdf[col_roof_z] - gdf[col_grd_z]
    gdf = gdf[gdf['_height'] > 0]
    print(f"原始行数：{len(gdf)}")

    cur = conn.cursor()

    if source_srid:
        geom_expr = f"ST_Transform(ST_GeomFromText(%s, {source_srid}), 4326)"
    else:
        geom_expr = "ST_GeomFromText(%s, 4326)"

    sql_building = f"""
        INSERT INTO {lod2_table}
            (building_id, citygml_id, geom_2d, height, floor_count, function, roof_type, year_built)
        VALUES (%s, %s, {geom_expr}, %s, %s, %s, %s, %s)
        ON CONFLICT (building_id) DO NOTHING;
    """

    sql_surface = f"""
        INSERT INTO {surface_table}
            (surface_id, building_id, surface_type, geom_3d)
        VALUES (%s, %s, %s, {geom_expr})
    """

    building_rows = []
    surface_rows  = []
    skipped = 0

    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            skipped += 1
            print("geom is None or geom.is_empty")
            continue

        # 收集所有polygon作为faces
        faces = list(geom.geoms) if geom.geom_type == 'MultiPolygon' else [geom]
        faces = [Polygon(list(f.exterior.coords)) for f in faces if len(f.exterior.coords) >= 3]

        if not faces:
            skipped += 1
            print("not faces")
            continue

        # infer surface type
        all_z  = [c[2] for f in faces for c in f.exterior.coords if len(c) > 2]
        if not all_z:
            skipped += 1
            print("not all_z")
            continue
        base_z = min(all_z)
        max_z  = max(all_z)

        # surfaces = {"RoofSurface": [], "WallSurface": [], "GroundSurface": []}
        # for face in faces:
        #     stype = classify_surfaces_flat_single(face, base_z, max_z)
        #     if stype in surfaces:
        #         surfaces[stype].append(face)

        # if not surfaces["GroundSurface"]:
        #     skipped += 1
        #     print("not surfaces[GroundSurface]")
        #     continue

        surfaces = {"RoofSurface": [], "WallSurface": [], "GroundSurface": []}
        for face in faces:
            stype = classify_surfaces_flat_single(face, base_z, max_z)
            surfaces[stype].append(face)

        # 补救：没有底面则取z_mean最低的面
        if not surfaces["GroundSurface"]:
            all_faces = [(np.mean([c[2] for c in f.exterior.coords if len(c) > 2]), f) 
                        for stype_list in surfaces.values() for f in stype_list]
            lowest = min(all_faces, key=lambda x: x[0])
            # 从原来的分类里移除，加入GroundSurface
            for stype in ["WallSurface", "RoofSurface"]:
                if lowest[1] in surfaces[stype]:
                    surfaces[stype].remove(lowest[1])
            surfaces["GroundSurface"].append(lowest[1])
        
        if not surfaces["GroundSurface"]:
            skipped += 1
            continue

        ground_face = surfaces["GroundSurface"][0]
        geom_2d     = Polygon([(c[0], c[1]) for c in ground_face.exterior.coords])

        building_id = f"{city_prefix}_B_{str(building_counter).zfill(7)}"
        building_counter += 1

        citygml_id  = str(row[col_citygml_id]) if col_citygml_id and pd.notna(row[col_citygml_id]) else None
        floor_count = int(row[col_floor_count]) if col_floor_count and pd.notna(row[col_floor_count]) else None
        function    = str(row[col_function]) if col_function and pd.notna(row[col_function]) else None
        roof_type   = str(row[col_roof_type]) if col_roof_type and pd.notna(row[col_roof_type]) else None
        year_built  = int(row[col_year_built]) if col_year_built and pd.notna(row[col_year_built]) else None
        height      = float(row['_height'])

        building_rows.append((
            building_id,
            citygml_id,
            wkt_dumps(geom_2d),
            height,
            floor_count,
            function,
            roof_type,
            year_built
        ))

        for stype, face_list in surfaces.items():
            for face in face_list:
                surface_id = f"{city_prefix}_S_{str(surface_counter).zfill(8)}"
                surface_counter += 1
                surface_rows.append((
                    surface_id,
                    building_id,
                    stype,
                    wkt_dumps(face)
                ))

    cur.executemany(sql_building, building_rows)
    cur.executemany(sql_surface, surface_rows)
    conn.commit()
    cur.close()

    print(f"入库建筑：{len(building_rows)}，跳过：{skipped}")
    print(f"入库surface：{len(surface_rows)}")

    return len(building_rows), building_counter, surface_counter


def infer_surface_type(poly, base_z):
    coords = list(poly.exterior.coords)
    z_values = [c[2] for c in coords if len(c) > 2]
    if not z_values:
        return "WallSurface"

    pts = np.array(coords)
    v0 = pts[0]
    v1 = None
    for p in pts[1:]:
        if np.linalg.norm(p - v0) > 1e-6:
            v1 = p
            break
    if v1 is not None:
        for p in pts[1:]:
            candidate = np.cross(v1 - v0, p - v0)
            if np.linalg.norm(candidate) > 1e-6:
                normal_z = abs(candidate[2] / np.linalg.norm(candidate))
                if normal_z > 0.7:
                    return "GroundSurface" if min(z_values) <= base_z + 0.5 else "RoofSurface"
                break

    return "WallSurface"

def classify_surfaces_flat_single(face, base_z, max_z):
    z_mean = np.mean([c[2] for c in face.exterior.coords if len(c) > 2])
    total_range = max_z - base_z
    # 动态阈值：取范围的30%和固定2米中的较大值
    threshold = max((total_range) * 0.3, 2.0)
    if z_mean <= base_z + threshold:
        return "GroundSurface"
    elif z_mean >= max_z - threshold:
        return "RoofSurface"
    else:
        return "WallSurface"




def generate_surfaces_from_buildings(conn, lod1_table, surface_table, city_prefix):
    cur = conn.cursor()
    
    cur.execute(f"""
        SELECT building_id, ST_AsText(geom_2d), height, ground_z 
        FROM {lod1_table}
        WHERE height > 0 AND geom_2d IS NOT NULL
    """)
    buildings = cur.fetchall()
    print(f"共{len(buildings)}栋建筑待生成surface")

    # 获取当前最大surface编号
    cur.execute(f"SELECT MAX(surface_id) FROM {surface_table}")
    max_sid = cur.fetchone()[0]
    surface_counter = int(max_sid.split("_S_")[1]) + 1 if max_sid else 1

    sql = f"""
        INSERT INTO {surface_table} (surface_id, building_id, surface_type, geom_3d)
        VALUES (%s, %s, %s, ST_GeomFromText(%s, 4326))
    """

    batch = []
    batch_size = 10000

    for building_id, geom_wkt, height, ground_z in buildings:
        poly = wkt_loads(geom_wkt)
        coords = list(poly.exterior.coords)

        # GroundSurface
        ground_coords = [(c[0], c[1], ground_z) for c in coords]
        ground_poly = Polygon(ground_coords)
        batch.append((
            f"{city_prefix}_S_{str(surface_counter).zfill(8)}",
            building_id, "GroundSurface", wkt_dumps(ground_poly)
        ))
        surface_counter += 1

        # RoofSurface
        roof_z = ground_z + height
        roof_coords = [(c[0], c[1], roof_z) for c in coords]
        roof_poly = Polygon(roof_coords)
        batch.append((
            f"{city_prefix}_S_{str(surface_counter).zfill(8)}",
            building_id, "RoofSurface", wkt_dumps(roof_poly)
        ))
        surface_counter += 1

        # WallSurface：每条边拉伸
        for i in range(len(coords) - 1):
            p1, p2 = coords[i], coords[i+1]
            wall_coords = [
                (p1[0], p1[1], ground_z),
                (p2[0], p2[1], ground_z),
                (p2[0], p2[1], roof_z),
                (p1[0], p1[1], roof_z),
                (p1[0], p1[1], ground_z)
            ]
            wall_poly = Polygon(wall_coords)
            batch.append((
                f"{city_prefix}_S_{str(surface_counter).zfill(8)}",
                building_id, "WallSurface", wkt_dumps(wall_poly)
            ))
            surface_counter += 1

        # 分批插入
        if len(batch) >= batch_size:
            cur.executemany(sql, batch)
            conn.commit()
            batch = []
            print(f"\r已插入surface：{surface_counter - 1}", end='', flush=True)

    # 插入剩余
    if batch:
        cur.executemany(sql, batch)
        conn.commit()

    cur.close()

    # 统计
    cur = conn.cursor()
    cur.execute(f"""
        SELECT surface_type, COUNT(*) 
        FROM {surface_table} 
        GROUP BY surface_type ORDER BY surface_type
    """)
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]}")
    cur.close()