import io
import zipfile
import mgrs
from simplekml import Kml
from typing import List, Dict, Any

def generate_kmz_from_mgrs(
    rows: List[Dict[str, Any]],
    mgrs_col: str = "MGRS"
) -> bytes:
    """
    Build a KMZ (zip of doc.kml) from rows that contain an MGRS string.
    """
    converter = mgrs.MGRS()
    kml = Kml()

    for row in rows:
        m = row.get(mgrs_col)
        if not m:
            continue
        try:
            lat, lon = converter.toLatLon(m)
        except Exception:
            continue
        # Use an ID or the MGRS string itself for the placemark name
        name = str(row.get("id") or m)
        kml.newpoint(name=name, coords=[(lon, lat)])

    # Convert to raw KML bytes
    kml_bytes = kml.kml().encode("utf-8")

    # Wrap in a KMZ (zip) with a single file doc.kml
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("doc.kml", kml_bytes)

    return buffer.getvalue()
