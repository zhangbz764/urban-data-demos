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
        if obj["type"] != "Building":
            print(f"Skipping non-building object: {obj_id}")
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
