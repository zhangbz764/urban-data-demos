"""
AUTHOR: zhangbz
PROJECT: UrbanPlayground
DATE: 2026/4/15
TIME: 10:25
DESCRIPTION: This module provides functions to parse CityJSON files, specifically for LOD2 building data.
"""

import json
import numpy as np
from shapely.geometry import Polygon
from shapely.wkt import dumps as wkt_dumps
from lxml import etree

def parse_cityjson_lod2(filepath, target_lod="2"):
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

        geom_entry = next((g for g in obj.get("geometry", []) if str(g.get("lod")) == target_lod), None)
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

        # If no height specified, use RoofSurface maximum Z minus GroundSurface minimum Z
        if height is None:
            if surfaces["RoofSurface"] and surfaces["GroundSurface"]:
                roof_z = max(c[2] for poly, _ in surfaces["RoofSurface"] for c in poly.exterior.coords)
                ground_z = min(c[2] for poly, _ in surfaces["GroundSurface"] for c in poly.exterior.coords)
                height = roof_z - ground_z
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

# --------------------- Special parser & insert: Amsterdam ---------------------
def parse_cityjson_lod2_NL_AM(filepath, target_lod="2.2"):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    scale         = np.array(data["transform"]["scale"])
    translate     = np.array(data["transform"]["translate"])
    vertices      = np.array(data["vertices"])
    real_vertices = vertices * scale + translate

    # ── 新增：先建立Building属性查找表 ──────────────────────────────────────
    building_attrs = {}
    for obj_id, obj in data["CityObjects"].items():
        if obj["type"] == "Building":
            building_attrs[obj_id] = obj.get("attributes", {})

    buildings = []
    for obj_id, obj in data["CityObjects"].items():
        if obj["type"] != "BuildingPart":
            continue

        # ── 新增：从父级Building取属性 ──────────────────────────────────────
        parent_id = obj_id.rsplit("-", 1)[0]
        attrs     = building_attrs.get(parent_id, {})

        height      = attrs.get("b3_h_dak_50p")
        floor_count = attrs.get("b3_bouwlagen")
        function    = attrs.get("status")
        roof_type   = attrs.get("b3_dak_type")
        year_built  = attrs.get("oorspronkelijkbouwjaar")

        geom_entry = next(
            (g for g in obj.get("geometry", []) if str(g.get("lod")) == target_lod),
            None
        )
        if geom_entry is None:
            continue

        boundaries    = normalize_boundaries(geom_entry["boundaries"])
        semantics     = geom_entry.get("semantics", {})
        surface_types = semantics.get("surfaces", [])
        values_raw    = normalize_values(semantics.get("values", []))

        flat_values = []
        for shell_values in values_raw:
            if isinstance(shell_values, list):
                flat_values.extend(shell_values)
            else:
                flat_values.append(shell_values)

        surfaces = {"RoofSurface": [], "WallSurface": [], "GroundSurface": []}

        face_idx = 0
        for shell in boundaries:
            for face in shell:
                stype       = "Unknown"
                scitygml_id = None
                if face_idx < len(flat_values):
                    type_idx = flat_values[face_idx]
                    if type_idx is not None and type_idx < len(surface_types):
                        stype       = surface_types[type_idx].get("type", "Unknown")
                        scitygml_id = surface_types[type_idx].get("id")

                ring   = face[0]
                coords = [tuple(real_vertices[i]) for i in ring]
                if len(coords) >= 3:
                    poly = Polygon(coords)
                    if stype in surfaces:
                        surfaces[stype].append((poly, scitygml_id))

                face_idx += 1

        if not surfaces["GroundSurface"]:
            continue

        ground  = surfaces["GroundSurface"][0][0]
        geom_2d = Polygon([(c[0], c[1]) for c in ground.exterior.coords])

        buildings.append({
            "citygml_id":  obj_id,
            "height":      height,
            "floor_count": floor_count,
            "function":    function,
            "roof_type":   roof_type,
            "year_built":  year_built,
            "geom_2d":     geom_2d,
            "surfaces":    surfaces
        })

    return buildings

def parse_cityjson_lod2_CH_ZU(filepath, target_lod="2"):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    scale         = np.array(data["transform"]["scale"])
    translate     = np.array(data["transform"]["translate"])
    vertices      = np.array(data["vertices"])
    real_vertices = vertices * scale + translate

    def infer_surface_type(poly, base_z):
        """通过几何法向量推断surface类型"""
        coords = list(poly.exterior.coords)
        z_values = [c[2] for c in coords if len(c) > 2]
        if not z_values:
            return "WallSurface"
        try:
            pts = np.array(coords[:3])
            v1  = pts[1] - pts[0]
            v2  = pts[2] - pts[0]
            normal   = np.cross(v1, v2)
            norm_len = np.linalg.norm(normal)
            if norm_len > 0:
                normal = normal / norm_len
                if abs(normal[2]) > 0.7:
                    return "GroundSurface" if min(z_values) <= base_z + 0.5 else "RoofSurface"
        except:
            pass
        return "WallSurface"

    buildings = []

    for obj_id, obj in data["CityObjects"].items():
        if obj["type"] not in ("Building", "BuildingPart"):
            continue

        attrs = obj.get("attributes", {})

        # ── 属性提取（德语字段映射）────────────────────────────────────────
        height      = attrs.get("measuredHeight") or (
                        (attrs.get("DACH_MAX", 0) - attrs.get("GELAENDEPUNKT", 0))
                        if "DACH_MAX" in attrs and "GELAENDEPUNKT" in attrs
                        else None
                      )
        floor_count = round(height / 3.0) if height else None
        function    = attrs.get("OBJEKTART")          # 建筑类型（德语）
        roof_type   = None                             # 数据中无屋顶类型字段
        year_built  = attrs.get("HERKUNFT_JAHR") or attrs.get("ERSTELLUNG_JAHR")
        citygml_id  = obj_id

        # ── 几何：优先取LOD2，同时兼容Solid和MultiSurface ────────────────
        geom_entry = next(
            (g for g in obj.get("geometry", []) if str(g.get("lod")) == target_lod),
            None
        )
        if geom_entry is None:
            continue

        boundaries    = normalize_boundaries(geom_entry["boundaries"])
        semantics     = geom_entry.get("semantics", {})
        surface_types = semantics.get("surfaces", [])
        values_raw    = normalize_values(semantics.get("values", []))

        flat_values = []
        for shell_values in values_raw:
            if isinstance(shell_values, list):
                flat_values.extend(shell_values)
            else:
                flat_values.append(shell_values)

        # semantics可信判断：有>=3种定义且values包含>=3个不同值
        unique_vals        = set(v for v in flat_values if v is not None)
        semantics_reliable = (len(surface_types) >= 3 and len(unique_vals) >= 3)

        surfaces = {"RoofSurface": [], "WallSurface": [], "GroundSurface": []}

        # 先算base_z（用于几何推断）
        base_z = float('inf')
        for shell in boundaries:
            for face in shell:
                ring = face[0]
                for vi in ring:
                    z = real_vertices[vi][2]
                    if z < base_z:
                        base_z = z
        if base_z == float('inf'):
            base_z = 0.0

        face_idx = 0
        for shell in boundaries:
            for face in shell:
                stype       = None
                scitygml_id = None

                if semantics_reliable and face_idx < len(flat_values):
                    type_idx = flat_values[face_idx]
                    if type_idx is not None and type_idx < len(surface_types):
                        stype       = surface_types[type_idx].get("type")
                        scitygml_id = surface_types[type_idx].get("id")

                ring   = face[0]
                coords = [tuple(real_vertices[i]) for i in ring]
                if len(coords) >= 3:
                    poly  = Polygon(coords)
                    stype = infer_surface_type(poly, base_z) \
                            if stype not in ("RoofSurface", "WallSurface", "GroundSurface") \
                            else stype
                    surfaces[stype].append((poly, scitygml_id))

                face_idx += 1

        if not surfaces["GroundSurface"]:
            continue

        ground  = surfaces["GroundSurface"][0][0]
        geom_2d = Polygon([(c[0], c[1]) for c in ground.exterior.coords])

        buildings.append({
            "citygml_id":  citygml_id,
            "height":      height,
            "floor_count": floor_count,
            "function":    function,
            "roof_type":   roof_type,
            "year_built":  year_built,
            "geom_2d":     geom_2d,
            "surfaces":    surfaces
        })

    return buildings

def parse_cityjson_lod2_LU_LU(filepath, target_lod="2"):
    """Parse LOD2 CityJSON for Luxembourg City"""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    scale         = np.array(data["transform"]["scale"])
    translate     = np.array(data["transform"]["translate"])
    vertices      = np.array(data["vertices"])
    real_vertices = vertices * scale + translate
    real_vertices = real_vertices[:, [1, 0, 2]]  # 左手坐标系, 把列顺序从[X,Y,Z]改为[Y,X,Z]

    buildings = []

    for obj_id, obj in data["CityObjects"].items():
        if obj["type"] != "Building":
            continue

        attrs = obj.get("attributes", {})

        height      = attrs.get("measuredHeight")
        floor_count = round(height / 3.0) if height else None
        function    = None   # name字段是行政区名，不是建筑功能，不录入
        roof_type   = None
        year_built  = None   # creationDate是数据生产日期，不是建造年份

        geom_entry = next(
            (g for g in obj.get("geometry", []) if str(g.get("lod")) == target_lod),
            None
        )
        if geom_entry is None:
            continue

        boundaries    = normalize_boundaries(geom_entry["boundaries"])
        semantics     = geom_entry.get("semantics", {})
        surface_types = semantics.get("surfaces", [])
        values_raw    = normalize_values(semantics.get("values", []))

        flat_values = []
        for shell_values in values_raw:
            if isinstance(shell_values, list):
                flat_values.extend(shell_values)
            else:
                flat_values.append(shell_values)

        surfaces = {"RoofSurface": [], "WallSurface": [], "GroundSurface": []}

        face_idx = 0
        for shell in boundaries:
            for face in shell:
                stype       = "Unknown"
                scitygml_id = None
                if face_idx < len(flat_values):
                    type_idx = flat_values[face_idx]
                    if type_idx is not None and type_idx < len(surface_types):
                        stype       = surface_types[type_idx].get("type", "Unknown")
                        scitygml_id = surface_types[type_idx].get("id")

                ring   = face[0]
                coords = [tuple(real_vertices[i]) for i in ring]
                if len(coords) >= 3:
                    poly = Polygon(coords)
                    if stype in surfaces:
                        surfaces[stype].append((poly, scitygml_id))

                face_idx += 1

        if not surfaces["GroundSurface"]:
            continue

        ground  = surfaces["GroundSurface"][0][0]
        geom_2d = Polygon([(c[0], c[1]) for c in ground.exterior.coords])

        buildings.append({
            "citygml_id":  obj_id,
            "height":      height,
            "floor_count": floor_count,
            "function":    function,
            "roof_type":   roof_type,
            "year_built":  year_built,
            "geom_2d":     geom_2d,
            "surfaces":    surfaces
        })

    return buildings

def parse_cityjson_lod2_AT_LZ(filepath, target_lod="3"):
    """Parse LOD2 CityJSON for Linz (Austria), LOD is labelled as 3"""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    scale         = np.array(data["transform"]["scale"])
    translate     = np.array(data["transform"]["translate"])
    vertices      = np.array(data["vertices"])
    real_vertices = vertices * scale + translate

    buildings = []

    for obj_id, obj in data["CityObjects"].items():
        if obj["type"] not in ("Building", "BuildingPart"):
            continue

        attrs = obj.get("attributes", {})

        height      = attrs.get("LoD1 Hoehe")
        floor_count = round(height / 3.0) if height else None
        function    = None
        roof_type   = None
        year_built  = None

        geom_entry = next(
            (g for g in obj.get("geometry", []) if str(g.get("lod")) == target_lod),
            None
        )
        if geom_entry is None:
            continue

        boundaries    = normalize_boundaries(geom_entry["boundaries"])
        semantics     = geom_entry.get("semantics", {})
        surface_types = semantics.get("surfaces", [])
        values_raw    = normalize_values(semantics.get("values", []))

        flat_values = []
        for shell_values in values_raw:
            if isinstance(shell_values, list):
                flat_values.extend(shell_values)
            else:
                flat_values.append(shell_values)

        surfaces = {"RoofSurface": [], "WallSurface": [], "GroundSurface": []}

        face_idx = 0
        for shell in boundaries:
            for face in shell:
                stype       = "Unknown"
                scitygml_id = None
                if face_idx < len(flat_values):
                    type_idx = flat_values[face_idx]
                    if type_idx is not None and type_idx < len(surface_types):
                        stype       = surface_types[type_idx].get("type", "Unknown")
                        scitygml_id = surface_types[type_idx].get("id")

                ring   = face[0]
                coords = [tuple(real_vertices[i]) for i in ring]
                if len(coords) >= 3:
                    poly = Polygon(coords)
                    if stype in surfaces:
                        surfaces[stype].append((poly, scitygml_id))

                face_idx += 1

        # 跳过没有GroundSurface的建筑（占比3.3%，直接忽略）
        if not surfaces["GroundSurface"]:
            continue

        ground  = surfaces["GroundSurface"][0][0]
        geom_2d = Polygon([(c[0], c[1]) for c in ground.exterior.coords])

        buildings.append({
            "citygml_id":  obj_id,
            "height":      height,
            "floor_count": floor_count,
            "function":    function,
            "roof_type":   roof_type,
            "year_built":  year_built,
            "geom_2d":     geom_2d,
            "surfaces":    surfaces
        })

    return buildings

def parse_cityjson_lod2_BE_NA(filepath, target_lod="2"):
    """Parse LOD2 CityJSON for Namur (Belgium)
    无semantics，surface类型通过法向量推断
    """
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    scale         = np.array(data["transform"]["scale"])
    translate     = np.array(data["transform"]["translate"])
    vertices      = np.array(data["vertices"])
    real_vertices = vertices * scale + translate

    def infer_surface_type(poly, base_z, building_height):
        coords   = list(poly.exterior.coords)
        z_values = [c[2] for c in coords if len(c) > 2]
        if not z_values:
            return "WallSurface"

        z_min   = min(z_values)
        z_max   = max(z_values)
        z_range = z_max - z_min

        ground_threshold = max(0.3, building_height * 0.05) if building_height else 0.3

        # 计算法向量（遍历顶点找非共线的三点）
        normal_z = None
        for i in range(len(coords) - 2):
            try:
                pts = np.array(coords[i:i+3])
                v1  = pts[1] - pts[0]
                v2  = pts[2] - pts[0]
                n   = np.cross(v1, v2)
                norm_len = np.linalg.norm(n)
                if norm_len > 1e-6:
                    normal_z = abs(n[2] / norm_len)
                    break
            except:
                continue

        # ── GroundSurface判断（同时满足两个条件）────────────────────────────
        # 条件1：Z值接近base_z
        # 条件2：面几乎水平（z_range极小 或 法向量Z分量很大）
        is_near_ground = z_min <= base_z + ground_threshold
        is_horizontal  = z_range < 0.15 or (normal_z is not None and normal_z > 0.95)
        if is_near_ground and is_horizontal:
            return "GroundSurface"

        # ── RoofSurface判断────────────────────────────────────────────────
        # 条件1：Z值整体高于地面（不与base_z接近）
        # 条件2：面有一定水平分量（法向量Z分量 > 0.3，覆盖大坡度屋顶）
        #     或者：面几乎水平（z_range小）
        is_above_ground = z_min > base_z + ground_threshold
        is_roof_like    = (normal_z is not None and normal_z > 0.3) or z_range < 0.3
        if is_above_ground and is_roof_like:
            return "RoofSurface"

        # ── 其余为WallSurface─────────────────────────────────────────────
        return "WallSurface"

    buildings = []

    for obj_id, obj in data["CityObjects"].items():
        if obj["type"] != "Building":
            continue

        attrs = obj.get("attributes", {})

        height      = attrs.get("measuredHeight")
        floor_count = round(height / 3.0) if height else None
        function    = None
        roof_type   = None
        year_built  = None

        geom_entry = next(
            (g for g in obj.get("geometry", []) if str(g.get("lod")) == target_lod),
            None
        )
        if geom_entry is None:
            continue

        boundaries = normalize_boundaries(geom_entry["boundaries"])

        # 先算base_z，用于法向量推断时判断地面/屋顶
        base_z = float('inf')
        for shell in boundaries:
            for face in shell:
                ring = face[0]
                for vi in ring:
                    z = real_vertices[vi][2]
                    if z < base_z:
                        base_z = z
        if base_z == float('inf'):
            base_z = 0.0

        surfaces = {"RoofSurface": [], "WallSurface": [], "GroundSurface": []}

        for shell in boundaries:
            for face in shell:
                ring   = face[0]
                coords = [tuple(real_vertices[i]) for i in ring]
                if len(coords) >= 3:
                    poly  = Polygon(coords)
                    stype = infer_surface_type(poly, base_z, height)
                    surfaces[stype].append((poly, None))  # 无citygml_id

        if not surfaces["GroundSurface"]:
            continue

        ground  = surfaces["GroundSurface"][0][0]
        geom_2d = Polygon([(c[0], c[1]) for c in ground.exterior.coords])

        buildings.append({
            "citygml_id":  obj_id,
            "height":      height,
            "floor_count": floor_count,
            "function":    function,
            "roof_type":   roof_type,
            "year_built":  year_built,
            "geom_2d":     geom_2d,
            "surfaces":    surfaces
        })

    return buildings

def parse_cityjson_lod2_CZ_PR(filepath):
    """Parse LOD2 CityJSON for Prague (Czech Republic)
    - LOD标注为3，实为LOD2
    - 几何在BuildingPart里，属性在Building和BuildingPart里
    - EPSG:5514，X,Y顺序正常，无需轴互换
    """
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    scale         = np.array(data["transform"]["scale"])
    translate     = np.array(data["transform"]["translate"])
    vertices      = np.array(data["vertices"])
    real_vertices = vertices * scale + translate

    # ── 建立Building属性查找表 ────────────────────────────────────────────
    building_attrs = {}
    for obj_id, obj in data["CityObjects"].items():
        if obj["type"] == "Building":
            building_attrs[obj_id] = obj.get("attributes", {})

    buildings = []

    for obj_id, obj in data["CityObjects"].items():
        if obj["type"] not in ("Building", "BuildingPart"):
            continue

        # ── 属性：优先用自身，fallback到父级Building ─────────────────────
        own_attrs   = obj.get("attributes", {})
        parent_id   = obj_id.rsplit("-", 1)[0]
        parent_attrs = building_attrs.get(parent_id, {})
        attrs       = {**parent_attrs, **own_attrs}  # 自身属性覆盖父级

        roof_type   = attrs.get("usage")   # 捷克语屋顶类型描述
        height      = None                 # 从几何计算
        floor_count = None
        function    = None
        year_built  = None

        geom_entry = next(
            (g for g in obj.get("geometry", []) if str(g.get("lod")) == "3"),
            None
        )
        if geom_entry is None:
            continue

        boundaries    = normalize_boundaries(geom_entry["boundaries"])
        semantics     = geom_entry.get("semantics", {})
        surface_types = semantics.get("surfaces", [])
        values_raw    = normalize_values(semantics.get("values", []))

        flat_values = []
        for shell_values in values_raw:
            if isinstance(shell_values, list):
                flat_values.extend(shell_values)
            else:
                flat_values.append(shell_values)

        surfaces = {"RoofSurface": [], "WallSurface": [], "GroundSurface": []}

        face_idx = 0
        for shell in boundaries:
            for face in shell:
                stype       = "Unknown"
                scitygml_id = None
                if face_idx < len(flat_values):
                    type_idx = flat_values[face_idx]
                    if type_idx is not None and type_idx < len(surface_types):
                        stype       = surface_types[type_idx].get("type", "Unknown")
                        scitygml_id = surface_types[type_idx].get("id")

                ring   = face[0]
                coords = [tuple(real_vertices[i]) for i in ring]
                if len(coords) >= 3:
                    poly = Polygon(coords)
                    if stype in surfaces:
                        surfaces[stype].append((poly, scitygml_id))

                face_idx += 1

        if not surfaces["GroundSurface"]:
            continue

        # ── 从几何计算height ──────────────────────────────────────────────
        try:
            roof_z_values   = [c[2] for poly, _ in surfaces["RoofSurface"]
                                     for c in poly.exterior.coords]
            ground_z_values = [c[2] for poly, _ in surfaces["GroundSurface"]
                                     for c in poly.exterior.coords]
            if roof_z_values and ground_z_values:
                height      = max(roof_z_values) - min(ground_z_values)
                floor_count = round(height / 3.0) if height > 0 else None
        except:
            pass

        ground  = surfaces["GroundSurface"][0][0]
        geom_2d = Polygon([(c[0], c[1]) for c in ground.exterior.coords])

        buildings.append({
            "citygml_id":  obj_id,
            "height":      height,
            "floor_count": floor_count,
            "function":    function,
            "roof_type":   roof_type,
            "year_built":  year_built,
            "geom_2d":     geom_2d,
            "surfaces":    surfaces
        })

    return buildings

def parse_gml_lod2(filepath,
                    swap_xy=False,
                    col_height="measuredHeight",
                    col_floor_count=None,
                    col_function=None,
                    col_roof_type=None,
                    col_year_built=None,
                    col_citygml_id=None):
    """
    通用LOD2 GML解析函数
    swap_xy: 坐标轴顺序是否需要交换（如塔林3301需要交换）
    col_*:   各属性字段名，None表示该字段不存在
    """
    tree = etree.parse(filepath)
    root = tree.getroot()

    NS = {
        "bldg": "http://www.opengis.net/citygml/building/2.0",
        "gml":  "http://www.opengis.net/gml",
        "core": "http://www.opengis.net/citygml/2.0",
    }

    def parse_poslist(poslist_el):
        vals = list(map(float, poslist_el.text.strip().split()))
        coords = []
        for i in range(0, len(vals) - 2, 3):
            a, b, z = vals[i], vals[i+1], vals[i+2]
            if swap_xy:
                coords.append((b, a, z))
            else:
                coords.append((a, b, z))
        return coords

    def infer_surface_type(poly, base_z): # TODO: 提取为通用函数？
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

    def parse_building(bldg_el):
        obj_id = bldg_el.get("{http://www.opengis.net/gml}id")

        def get_attr(tag):
            el = bldg_el.find(f".//{{{NS['bldg']}}}{tag}")
            if el is not None and el.text:
                return el.text.strip()
            # 尝试通用属性标签
            for child in bldg_el.iter():
                local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                if local == tag and child.text:
                    return child.text.strip()
            return None

        height      = float(get_attr(col_height)) if col_height and get_attr(col_height) else None
        floor_count = get_attr(col_floor_count) if col_floor_count else None
        floor_count = int(float(floor_count)) if floor_count else None
        function    = get_attr(col_function) if col_function else None
        roof_type   = get_attr(col_roof_type) if col_roof_type else None
        year_built  = get_attr(col_year_built) if col_year_built else None
        year_built  = int(year_built) if year_built else None
        citygml_id  = get_attr(col_citygml_id) if col_citygml_id else obj_id

        # LOD2几何：优先lod2Solid，其次lod2MultiSurface
        lod2_el = bldg_el.find(".//bldg:lod2Solid", NS)
        if lod2_el is None:
            lod2_el = bldg_el.find(".//bldg:lod2MultiSurface", NS)
        if lod2_el is None:
            return None

        surfaces = {"RoofSurface": [], "WallSurface": [], "GroundSurface": []}

        # 尝试读取语义面
        semantic_tags = {
            f"{{{NS['bldg']}}}RoofSurface":    "RoofSurface",
            f"{{{NS['bldg']}}}WallSurface":    "WallSurface",
            f"{{{NS['bldg']}}}GroundSurface":  "GroundSurface",
            f"{{{NS['bldg']}}}ClosureSurface": "ClosureSurface",
        }

        has_semantics = any(bldg_el.find(f".//{tag}", {}) is not None
                           for tag in semantic_tags)

        if has_semantics:
            for sem_tag, stype in semantic_tags.items():
                if stype == "ClosureSurface":
                    continue
                for sem_el in bldg_el.iter(sem_tag):
                    sem_id = sem_el.get("{http://www.opengis.net/gml}id")
                    for poslist_el in sem_el.iter("{http://www.opengis.net/gml}posList"):
                        coords = parse_poslist(poslist_el)
                        if len(coords) >= 3:
                            surfaces[stype].append((Polygon(coords), sem_id))
        else:
            # 无语义则用infer
            faces = []
            for poslist_el in lod2_el.iter("{http://www.opengis.net/gml}posList"):
                coords = parse_poslist(poslist_el)
                if len(coords) >= 3:
                    faces.append(Polygon(coords))

            if not faces:
                return None

            all_z = [c[2] for face in faces for c in face.exterior.coords]
            base_z = min(all_z) if all_z else 0

            for face in faces:
                stype = infer_surface_type(face, base_z)
                if stype in surfaces:
                    surfaces[stype].append((face, None))

        if not surfaces["GroundSurface"]:
            print(f"No ground face: {obj_id}")
            return None

        ground  = surfaces["GroundSurface"][0][0]
        geom_2d = Polygon([(c[0], c[1]) for c in ground.exterior.coords])

        all_z = [c[2] for stype_list in surfaces.values()
                 for poly, _ in stype_list
                 for c in poly.exterior.coords]
        base_z = min(all_z) if all_z else 0

        if height is None:
            if surfaces["RoofSurface"]:
                roof   = surfaces["RoofSurface"][0][0]
                top_z  = float(np.mean([c[2] for c in roof.exterior.coords]))
                height = top_z - base_z
            else:
                print(f"No height: {obj_id}")
                return None

        return {
            "citygml_id":  str(citygml_id) if citygml_id else obj_id,
            "height":      float(height),
            "floor_count": floor_count,
            "function":    function,
            "roof_type":   roof_type,
            "year_built":  year_built,
            "geom_2d":     geom_2d,
            "surfaces":    surfaces
        }

    buildings = []
    for bldg_el in root.iter("{http://www.opengis.net/citygml/building/2.0}Building"):
        result = parse_building(bldg_el)
        if result:
            buildings.append(result)

    for bldg_el in root.iter("{http://www.opengis.net/citygml/building/2.0}BuildingPart"):
        result = parse_building(bldg_el)
        if result:
            buildings.append(result)

    return buildings

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