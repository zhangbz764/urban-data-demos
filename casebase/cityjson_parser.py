"""
AUTHOR: zhangbz
PROJECT: UrbanPlayground
DATE: 2026/4/15
TIME: 10:25
DESCRIPTION: This module provides functions to parse CityJSON files, specifically for LOD1 building data.
"""

import json
import numpy as np
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import unary_union
from shapely.wkt import dumps as wkt_dumps


def parse_cityjson_lod1(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    scale = np.array(data["transform"]["scale"])
    translate = np.array(data["transform"]["translate"])
    vertices = np.array(data["vertices"])

    # real coords
    real_vertices = vertices * scale + translate

    buildings = []
    for obj_id, obj in data["CityObjects"].items():
        if obj["type"] not in ("Building", "BuildingPart"):
            # print(f"Skipping non-building object: {obj_id}")
            continue

        attrs = obj.get("attributes", {})

        # attributes
        height = attrs.get("measuredHeight")
        floor_count = attrs.get("storeysAboveGround")
        function = attrs.get("function")
        roof_type = attrs.get("roofType")
        year_built = attrs.get("yearOfConstruction")

        # LOD1 geos
        geom_entry = next((g for g in obj.get("geometry", []) if str(g.get("lod")) == "1"), None)
        if geom_entry is None:
            print(f"No LOD1 geometry for building: {obj_id}")
            continue

        # rebuild 3D faces from boundaries
        faces = []
        for shell in geom_entry["boundaries"]:
            for face in shell:
                ring = face[0]  # exterior ring indices
                coords = [tuple(real_vertices[i]) for i in ring]
                if len(coords) >= 3:
                    faces.append(Polygon(coords))

        if not faces:
            print(f"No valid faces for building: {obj_id}")
            continue

        # 2D footprint: Z min (bottom face) as footprint
        bottom_face = min(faces, key=lambda p: np.mean([c[2] for c in p.exterior.coords]))
        geom_2d = Polygon([(c[0], c[1]) for c in bottom_face.exterior.coords])

        buildings.append({
            "building_id": obj_id,
            "height": height,
            "floor_count": floor_count,
            "function": function,
            "roof_type": roof_type,
            "year_built": year_built,
            "geom_2d": geom_2d,
            "faces": faces  # all 3d surfaces to construct geom_3d
        })

    return buildings


def insert_buildings_lod1(buildings, conn, lod1_table):
    """Batch insert the parsed building list into the database"""
    cur = conn.cursor()
    sql = f"""
        INSERT INTO {lod1_table} 
            (building_id, geom_2d, height, floor_count, function, roof_type, year_built)
        VALUES (%s, ST_GeomFromText(%s, 25832), %s, %s, %s, %s, %s)
        ON CONFLICT (building_id) DO NOTHING;
    """
    rows = [
        (
            b["building_id"],
            wkt_dumps(b["geom_2d"]),
            b["height"],
            b["floor_count"],
            b["function"],
            b["roof_type"],
            b["year_built"]
        )
        for b in buildings
    ]
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    return len(rows)

def parse_cityjson_lod2(filepath):
    """Parse a single LOD2 CityJSON file, return buildings with 2D footprint and classified 3D surfaces"""
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
        roof_type = attrs.get("roofType")
        year_built = attrs.get("yearOfConstruction")

        geom_entry = next((g for g in obj.get("geometry", []) if str(g.get("lod")) == "2"), None)
        if geom_entry is None:
            continue

        boundaries = geom_entry["boundaries"]
        boundaries = normalize_boundaries(boundaries)
        semantics = geom_entry.get("semantics", {})
        surface_types = semantics.get("surfaces", [])

        surfaces = {"RoofSurface": [], "WallSurface": [], "GroundSurface": []}

        flat_values = []
        values_raw = normalize_values(semantics.get("values", []))
        for shell_values in values_raw:
            if isinstance(shell_values, list):
                flat_values.extend(shell_values)
            else:
                flat_values.append(shell_values)

        face_idx = 0
        for shell in boundaries:
            for face in shell:
                stype = "Unknown"
                if face_idx < len(flat_values):
                    type_idx = flat_values[face_idx]
                    if type_idx is not None and type_idx < len(surface_types):
                        stype = surface_types[type_idx]["type"]
                        scitygml_id = surface_types[type_idx].get("id")  

                ring = face[0]
                coords = [tuple(real_vertices[i]) for i in ring]
                if len(coords) >= 3:
                    poly = Polygon(coords)
                    if stype in surfaces:
                        surfaces[stype].append((poly, scitygml_id))

                face_idx += 1

        if surfaces["GroundSurface"]:
            ground = surfaces["GroundSurface"][0][0]
            geom_2d = Polygon([(c[0], c[1]) for c in ground.exterior.coords])
        else:
            continue

        buildings.append({
            "citygml_id": obj_id, 
            "height": height,
            "floor_count": floor_count,
            "function": function,
            "roof_type": roof_type,
            "year_built": year_built,
            "geom_2d": geom_2d,
            "surfaces": surfaces
        })

    return buildings


def insert_buildings_lod2(buildings, conn, lod2_table, surface_table,
                           city_prefix, target_srid, source_srid,
                           building_counter, surface_counter):
    """
    Batch insert LOD2 buildings into the database.
    """
    cur = conn.cursor()

    if source_srid == target_srid:
        geom_2d_expr = f"ST_GeomFromText(%s, {target_srid})"
        geom_3d_expr = f"ST_GeomFromText(%s, {target_srid})"
    else:
        geom_2d_expr = f"ST_Transform(ST_GeomFromText(%s, {source_srid}), {target_srid})"
        geom_3d_expr = f"ST_Transform(ST_GeomFromText(%s, {source_srid}), {target_srid})"


    sql_building = f"""
        INSERT INTO {lod2_table}
            (building_id, citygml_id, geom_2d, height, floor_count, function, roof_type, year_built)
        VALUES (%s, %s, {geom_2d_expr}, %s, %s, %s, %s, %s)
        ON CONFLICT (building_id) DO NOTHING;
    """

    sql_surface = f"""
        INSERT INTO {surface_table}
            (surface_id, building_id, citygml_id, surface_type, geom_3d)
        VALUES (%s, %s, %s, %s, {geom_3d_expr})
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
            b["floor_count"],
            b["function"],
            b["roof_type"],
            b["year_built"]
        ))

        for stype, polys in b["surfaces"].items():
            for poly, scitygml_id in polys:  
                surface_id = f"{city_prefix}_S_{str(surface_counter).zfill(8)}"
                surface_counter += 1

                surface_rows.append((
                    surface_id,
                    building_id,
                    scitygml_id,  
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