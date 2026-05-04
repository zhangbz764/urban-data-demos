from shapely.wkt import dumps as wkt_dumps
from shapely.wkt import loads as wkt_loads
from shapely.geometry import Polygon
import pandas as pd

 # TODO: 筛除了multipolygon，筛除了没有高度的，目前处理的是3857
def insert_buildings_shp(gdf, conn, lod1_table, city_prefix,
                          building_counter,
                          col_height=None,
                          col_height_top=None,
                          col_height_bottom=None,
                          col_ground_z=None,
                          col_citygml_id=None,
                          col_floor_count=None,
                          set_crs=None):
    """
    通用SHP建筑入库函数
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
    #print(f"过滤后建筑数：{len(gdf)}")

    # 转4326
    if set_crs:
        gdf = gdf.set_crs(set_crs, allow_override=True).to_crs(4326)
    else:
        gdf = gdf.to_crs(4326)

    cur = conn.cursor()
    sql = f"""
        INSERT INTO {lod1_table}
            (building_id, citygml_id, geom_2d, height, ground_z, floor_count, function)
        VALUES (%s, %s, ST_GeomFromText(%s, 4326), %s, %s, %s, %s)
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
        floor_count = int(row[col_floor_count]) if col_floor_count and pd.notna(row[col_floor_count]) else None

        rows.append((
            building_id,
            citygml_id,
            wkt_dumps(geom_2d),
            height,
            ground_z,
            floor_count,
            None  # function
        ))

    cur.executemany(sql, rows)
    conn.commit()
    cur.close()

    return len(rows), building_counter


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