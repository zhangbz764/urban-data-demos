import utils_z

def create_lod1_tables(
        city_prefix, 
        lod1_table_name, 
        lod1_table_name_full, 
        lod1_surface_table_name, 
        lod1_surface_table_name_full, 
        target_srid, 
        conn
    ):
    # LOD1建筑主表
    utils_z.run_sql(f"""
        CREATE TABLE IF NOT EXISTS {lod1_table_name_full} (
            building_id     VARCHAR PRIMARY KEY,
            block_id        VARCHAR,
            citygml_id      VARCHAR,
            geom_2d         GEOMETRY(Polygon, {target_srid}),
            height          FLOAT,
            area            FLOAT,
            perimeter       FLOAT,
            floor_count     INTEGER,
            function        VARCHAR,
            ground_z        FLOAT
        );
    """, conn=conn)

    utils_z.run_sql(f"""
        CREATE INDEX IF NOT EXISTS {lod1_table_name}_geom_idx
        ON {lod1_table_name_full} USING GIST (geom_2d);
    """, conn=conn)

    # LOD1 surface子表（无citygml_id，面由计算生成）
    utils_z.run_sql(f"""
        CREATE TABLE IF NOT EXISTS {lod1_surface_table_name_full} (
            surface_id      VARCHAR PRIMARY KEY,
            building_id     VARCHAR REFERENCES {lod1_table_name_full}(building_id),
            surface_type    VARCHAR,
            geom_3d         GEOMETRY(PolygonZ, {target_srid})
        );
    """, conn=conn)

    utils_z.run_sql(f"""
        CREATE INDEX IF NOT EXISTS {lod1_surface_table_name}_building_idx
        ON {lod1_surface_table_name_full} (building_id);
    """, conn=conn)

    print(city_prefix + " LOD1表创建完成")


def create_lod2_tables(
        city_prefix, 
        lod2_table_name, 
        lod2_table_name_full, 
        lod2_surface_table_name, 
        lod2_surface_table_name_full, 
        target_srid, 
        conn
    ):
    # LOD2建筑主表
    utils_z.run_sql(f"""
        CREATE TABLE IF NOT EXISTS {lod2_table_name_full} (
            building_id     VARCHAR PRIMARY KEY,
            block_id        VARCHAR,
            citygml_id      VARCHAR,
            geom_2d         GEOMETRY(Polygon, {target_srid}),
            height          FLOAT,
            area            FLOAT,
            perimeter       FLOAT,
            floor_count     INTEGER,
            function        VARCHAR,
            roof_type       VARCHAR,
            year_built      INTEGER
        );
    """, conn=conn)

    utils_z.run_sql(f"""
        CREATE INDEX IF NOT EXISTS {lod2_table_name}_geom_idx
        ON {lod2_table_name_full} USING GIST (geom_2d);
    """, conn=conn)

    # LOD2 surface子表
    utils_z.run_sql(f"""
        CREATE TABLE IF NOT EXISTS {lod2_surface_table_name_full} (
            surface_id      VARCHAR PRIMARY KEY,
            citygml_id      VARCHAR,
            building_id     VARCHAR REFERENCES {lod2_table_name_full}(building_id),
            surface_type    VARCHAR,
            geom_3d         GEOMETRY(PolygonZ, {target_srid})
        );
    """, conn=conn)

    utils_z.run_sql(f"""
        CREATE INDEX IF NOT EXISTS {lod2_surface_table_name}_building_idx
        ON {lod2_surface_table_name_full} (building_id);
    """, conn=conn)

    print(city_prefix+" LOD2表创建完成")

def map_buildings_to_blocks(block_table_name, building_table_name, building_table_name_full, conn):
    lod = building_table_name_full.split(".")[0]

    # 确认空间索引
    print(block_table_name, building_table_name_full)
    utils_z.run_sql(f"""
        CREATE INDEX IF NOT EXISTS {building_table_name}_geom_idx
        ON {building_table_name_full} USING GIST (geom_2d);
    """, conn=conn)
    print("准备开始空间叠合...")

    utils_z.run_sql(f"""
        UPDATE {building_table_name_full} b
        SET block_id = (
            SELECT bl.block_id
            FROM {block_table_name} bl
            WHERE ST_Within(ST_Centroid(b.geom_2d), bl.geom)
            LIMIT 1
        );
    """, conn=conn)
    print("空间叠合完成")

    # 检查匹配情况
    result = utils_z.run_sql(f"""
        SELECT
            COUNT(*) AS total,
            COUNT(block_id) AS matched,
            COUNT(*) FILTER (WHERE block_id IS NULL) AS unmatched
        FROM {building_table_name_full};
    """, conn=conn, fetch=True)

    print(f"总建筑数：{result[0][0]}")
    print(f"成功匹配block：{result[0][1]}")
    print(f"未匹配block：{result[0][2]}")

    # 检查包含建筑的街区数量
    sql_counts = f"SELECT COUNT(DISTINCT block_id) FROM {building_table_name_full} WHERE block_id IS NOT NULL;"
    (count,) = utils_z.run_sql(sql_counts, conn=conn, fetch=True)[0]

    print(f"包含 {lod} 建筑的街区数量: {count}")
