"""
Geofence Geometry Module
Handles geodesic distance calculations for circles and Shapely-based polygon validation
and point-in-polygon containment testing.
"""

import math
from typing import List, Tuple, Optional, Any, Dict

try:
    from shapely.geometry import Point, Polygon
    from shapely.validation import explain_validity
    SHAPELY_AVAILABLE = True
except ImportError:
    SHAPELY_AVAILABLE = False


def calculate_haversine_distance_meters(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> float:
    """
    Calculate the great-circle distance between two points on the Earth's surface in meters
    using the Haversine formula.
    """
    R = 6371000.0  # Earth radius in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * (math.sin(delta_lambda / 2.0) ** 2)
    )
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))
    return R * c


def is_point_in_circle(
    latitude: float,
    longitude: float,
    center_latitude: float,
    center_longitude: float,
    radius_meters: float,
    tolerance_meters: float = 0.0,
) -> bool:
    """
    Determine whether a geographic coordinate is inside a circular geofence.
    """
    dist = calculate_haversine_distance_meters(
        latitude, longitude, center_latitude, center_longitude
    )
    return dist <= (radius_meters + tolerance_meters)


def normalize_coordinate_pair(item: Any) -> Optional[Tuple[float, float]]:
    """Convert various coordinate representations to (latitude, longitude) float tuple."""
    try:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            lat = float(item[0])
            lon = float(item[1])
        elif isinstance(item, dict):
            lat = float(item.get("latitude") if item.get("latitude") is not None else item.get("lat"))
            lon = float(item.get("longitude") if item.get("longitude") is not None else (item.get("lng") if item.get("lng") is not None else item.get("lon")))
        else:
            return None

        if math.isfinite(lat) and math.isfinite(lon) and -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0:
            return (round(lat, 7), round(lon, 7))
        return None
    except (ValueError, TypeError):
        return None


def validate_polygon_coordinates(
    coordinates: List[Any],
) -> Tuple[bool, Optional[str], List[List[float]]]:
    """
    Validate polygon coordinates:
    - Minimum 3 distinct geographic coordinates.
    - Closed boundary (first vertex == last vertex).
    - Checks for self-intersections and validity using Shapely.
    Returns: (is_valid, error_message, normalized_coordinates_as_list_of_lat_lon)
    """
    if not coordinates or not isinstance(coordinates, list):
        return False, "Polygon coordinates must be a non-empty list of [latitude, longitude] points", []

    cleaned: List[Tuple[float, float]] = []
    for idx, item in enumerate(coordinates):
        norm = normalize_coordinate_pair(item)
        if norm is None:
            return False, f"Invalid coordinate at index {idx}: {item}", []
        cleaned.append(norm)

    # Check distinct points count
    distinct_points = set(cleaned)
    if len(distinct_points) < 3:
        return False, f"Polygon requires at least 3 distinct vertices, got {len(distinct_points)}", []

    # Ensure closed boundary
    if cleaned[0] != cleaned[-1]:
        cleaned.append(cleaned[0])

    if len(cleaned) < 4:
        return False, "Closed polygon boundary requires at least 4 coordinate entries (3 distinct + closing vertex)", []

    # Validate with Shapely if available
    if SHAPELY_AVAILABLE:
        try:
            # Shapely Polygon expects (x, y) coordinates, so (longitude, latitude)
            xy_points = [(p[1], p[0]) for p in cleaned]
            poly = Polygon(xy_points)

            if not poly.is_valid:
                reason = explain_validity(poly)
                return False, f"Polygon geometry is invalid (self-intersecting or degenerate): {reason}", []

            if poly.area <= 0:
                return False, "Polygon area must be greater than zero", []
        except Exception as exc:
            return False, f"Polygon validation error: {str(exc)}", []
    else:
        # Fallback basic checks if Shapely is not loaded
        if len(cleaned) < 4:
            return False, "Polygon must have at least 3 distinct vertices", []

    result_list = [[lat, lon] for (lat, lon) in cleaned]
    return True, None, result_list


def _point_in_polygon_ray_casting(lat: float, lon: float, polygon: List[List[float]]) -> bool:
    """Ray casting point-in-polygon algorithm fallback."""
    inside = False
    n = len(polygon)
    if n < 3:
        return False

    j = n - 1
    for i in range(n):
        lat_i, lon_i = polygon[i][0], polygon[i][1]
        lat_j, lon_j = polygon[j][0], polygon[j][1]

        # Check edge intersection with horizontal ray cast to the right from (lon, lat)
        if ((lon_i > lon) != (lon_j > lon)):
            slope = (lat_j - lat_i) / (lon_j - lon_i + 1e-15)
            if lat < lat_i + slope * (lon - lon_i):
                inside = not inside
        j = i

    return inside


def is_point_in_polygon(
    latitude: float,
    longitude: float,
    polygon_coordinates: List[List[float]],
    tolerance_meters: float = 0.0,
) -> bool:
    """
    Determine whether a point (latitude, longitude) is inside a polygon boundary.
    Uses Shapely if available, with a reliable ray-casting fallback.
    """
    if not polygon_coordinates or len(polygon_coordinates) < 3:
        return False

    # Ensure closed boundary
    closed_coords = list(polygon_coordinates)
    if closed_coords[0] != closed_coords[-1]:
        closed_coords.append(closed_coords[0])

    if SHAPELY_AVAILABLE:
        try:
            # Shapely Point(x=lon, y=lat) and Polygon with (x=lon, y=lat)
            pt = Point(longitude, latitude)
            xy_points = [(p[1], p[0]) for p in closed_coords]
            poly = Polygon(xy_points)

            if poly.contains(pt) or poly.touches(pt):
                return True

            if tolerance_meters > 0.0:
                # Convert meters tolerance to approximate degrees buffer (~111,320 meters per degree)
                deg_buf = tolerance_meters / 111320.0
                return poly.buffer(deg_buf).contains(pt)

            return False
        except Exception:
            pass

    return _point_in_polygon_ray_casting(latitude, longitude, closed_coords)
