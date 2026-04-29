from shapely.wkt import dumps as wkt_dumps
from shapely.wkt import loads as wkt_loads
from shapely.geometry import Polygon

def insert_buildings_shp_toronto(gdf, conn, lod1_table, city_prefix,
                                  building_counter):
    # TODO: 筛除了multipolygon，筛除了没有高度的，目前处理的是3857
    # 过滤无效数据
    gdf_valid = gdf[gdf['MAX_HEIGHT'] > 0].copy()
    print(f"过滤后建筑数：{len(gdf_valid)}")

    # 转4326
    gdf_valid = gdf_valid.to_crs(4326)

    cur = conn.cursor()
    sql = f"""
        INSERT INTO {lod1_table}
            (building_id, citygml_id, geom_2d, height, ground_z, function)
        VALUES (%s, %s, ST_GeomFromText(%s, 4326), %s, %s, %s)
        ON CONFLICT (building_id) DO NOTHING;
    """

    rows = []
    for _, row in gdf_valid.iterrows():
        building_id = f"{city_prefix}_B_{str(building_counter).zfill(7)}"
        building_counter += 1

        # geometry转2D（去掉Z）
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        if geom.geom_type == 'MultiPolygon':
            # 取面积最大的polygon
            geom = max(geom.geoms, key=lambda g: g.area)

        geom_2d = Polygon([(c[0], c[1]) for c in geom.exterior.coords])


        rows.append((
            building_id,
            None,                        # 无citygml_id
            wkt_dumps(geom_2d),
            float(row['MAX_HEIGHT']),
            float(row['SURF_ELEV']),
            None                         # 无function字段
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
            print(f"已插入surface：{surface_counter - 1}", flush=True)

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