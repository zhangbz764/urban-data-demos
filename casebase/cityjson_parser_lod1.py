"""
AUTHOR: zhangbz
PROJECT: UrbanPlayground
DATE: 2026/4/26
TIME: 17:52
DESCRIPTION: This module provides functions to parse CityJSON files, specifically for LOD1 building data.
"""

import json
import numpy as np
from shapely.geometry import Polygon
from shapely.wkt import dumps as wkt_dumps

def parse_cityjson_lod1(filepath, target_lod="1"):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    scale = np.array(data["transform"]["scale"])
    translate = np.array(data["transform"]["translate"])
    vertices = np.array(data["vertices"])
    real_vertices = vertices * scale + translate

    buildings = []
    for obj_id, obj in data["CityObjects"].items():
        if obj["type"] not in ("Building", "BuildingPart"):
            continue

        attrs = obj.get("attributes", {})

        height = attrs.get("measuredHeight")
        floor_count = attrs.get("storeysAboveGround")
        function = attrs.get("function")

        geom_entry = next((g for g in obj.get("geometry", []) if str(g.get("lod")) == target_lod), None)
        if geom_entry is None:
            print(f"No LOD1 geometry for building: {obj_id}")
            continue

        faces = []
        for shell in geom_entry["boundaries"]:
            for face in shell:
                ring = face[0]
                coords = [tuple(real_vertices[i]) for i in ring]
                if len(coords) >= 3:
                    faces.append(Polygon(coords))

        if not faces:
            print(f"No valid faces for building: {obj_id}")
            continue

        # 底面：Z均值最低的面
        bottom_face = min(faces, key=lambda p: np.mean([c[2] for c in p.exterior.coords]))
        # 顶面：Z均值最高的面
        top_face = max(faces, key=lambda p: np.mean([c[2] for c in p.exterior.coords]))

        ground_z = float(np.mean([c[2] for c in bottom_face.exterior.coords]))

        # 没有属性高度时从几何计算
        if height is None:
            top_z = float(np.mean([c[2] for c in top_face.exterior.coords]))
            height = top_z - ground_z
        else:
            height = float(height)

        geom_2d = Polygon([(c[0], c[1]) for c in bottom_face.exterior.coords])

        # 判断surface_type
        surfaces = []
        for face in faces:
            z_mean = np.mean([c[2] for c in face.exterior.coords])
            if abs(z_mean - ground_z) < 0.1:
                stype = "GroundSurface"
            elif abs(z_mean - (ground_z + height)) < 0.5:
                stype = "RoofSurface"
            else:
                stype = "WallSurface"
            surfaces.append((stype, face))

        buildings.append({
            "citygml_id": obj_id,
            "height": height,
            "ground_z": ground_z,
            "floor_count": floor_count,
            "function": function,
            "geom_2d": geom_2d,
            "surfaces": surfaces
        })

    return buildings


def insert_buildings_lod1(buildings, conn, lod1_table, surface_table,
                           city_prefix, target_srid, source_srid,
                           building_counter, surface_counter):
    """Batch insert LOD1 buildings and surfaces into the database."""
    cur = conn.cursor()

    if source_srid == target_srid:
        geom_expr = f"ST_GeomFromText(%s, {target_srid})"
    else:
        geom_expr = f"ST_Transform(ST_GeomFromText(%s, {source_srid}), {target_srid})"

    sql_building = f"""
        INSERT INTO {lod1_table}
            (building_id, citygml_id, geom_2d, height, ground_z, floor_count, function)
        VALUES (%s, %s, {geom_expr}, %s, %s, %s, %s)
        ON CONFLICT (building_id) DO NOTHING;
    """

    sql_surface = f"""
        INSERT INTO {surface_table}
            (surface_id, building_id, surface_type, geom_3d)
        VALUES (%s, %s, %s, {geom_expr})
    """

    building_rows = []
    surface_rows = []

    for b in buildings:
        building_id = f"{city_prefix}_B_{str(building_counter).zfill(7)}"
        building_counter += 1

        building_rows.append((
            building_id,
            b["citygml_id"],
            wkt_dumps(b["geom_2d"]),
            b["height"],
            b["ground_z"],
            b["floor_count"],
            b["function"]
        ))

        for stype, poly in b["surfaces"]:
            surface_id = f"{city_prefix}_S_{str(surface_counter).zfill(8)}"
            surface_counter += 1
            surface_rows.append((
                surface_id,
                building_id,
                stype,
                wkt_dumps(poly)
            ))

    cur.executemany(sql_building, building_rows)
    cur.executemany(sql_surface, surface_rows)
    conn.commit()
    cur.close()

    return len(building_rows), building_counter, surface_counter

# --------------------- Exception handling for data standardization ---------------------

# Handle inconsistent boundary hierarchy issues
# Determine if the first-level element is a list of lists (normal shell) or a list of ints (missing shell layer)
def normalize_boundaries(boundaries):
    """Normalize boundaries into 3 levels of nesting: shell > face > ring"""
    if not boundaries:
        return []
    first = boundaries[0]
    # Normal case: first is a shell, i.e., list of faces, where each face is a list of rings
    # Exception case: first is directly a face, i.e., list of rings, where each ring is a list of ints
    if isinstance(first[0][0], int):
        # Missing shell layer, wrap it in one layer
        return [boundaries]
    return boundaries

# Handle semantics values in the same way
def normalize_values(values):
    """Normalize values into 2 levels of nesting: [[...]]"""
    if not values:
        return [[]]
    if isinstance(values[0], int) or values[0] is None:
        # Missing outer list
        return [values]
    return values