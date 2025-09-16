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
    Build a properly formatted KML file from rows that contain an MGRS string.
    Includes full report data for each point.
    """
    converter = mgrs.MGRS()
    kml = Kml()
    
    # Set document properties
    kml.document.name = "CORE Scout Export"
    kml.document.description = "Exported data from CORE Scout database"
    
    # Create a folder for the placemarks
    folder = kml.newfolder(name="Reports")
    folder.description = f"Database records with MGRS coordinates ({len(rows)} records)"

    for row in rows:
        m = row.get(mgrs_col)
        if not m:
            continue
        try:
            lat, lon = converter.toLatLon(m)
        except Exception:
            continue
        
        # Create placemark with full data
        placemark = folder.newpoint(
            name=str(row.get("id") or m),
            coords=[(lon, lat)]
        )
        
        # Add full report data as description with proper HTML formatting
        description_parts = []
        for key, value in row.items():
            if key != mgrs_col and value is not None:
                # Format field names nicely
                field_name = key.replace('_', ' ').title()
                # Handle different data types
                if isinstance(value, (dict, list)):
                    value_str = str(value)
                elif isinstance(value, str) and len(value) > 200:
                    value_str = value[:200] + "..."
                else:
                    value_str = str(value)
                
                # Escape HTML characters in values
                value_str = value_str.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                description_parts.append(f"<b>{field_name}:</b> {value_str}")
        
        # Create properly formatted HTML description
        description = "<![CDATA[<div style='font-family: Arial, sans-serif;'>" + "<br/>".join(description_parts) + "</div>]]>"
        placemark.description = description
        
        # Add MGRS as extended data
        placemark.extendeddata.newdata(name="MGRS", value=m)
        
        # Add other key fields as extended data for better filtering
        for key in ['id', 'name', 'title', 'description']:
            if key in row and row[key] is not None:
                placemark.extendeddata.newdata(name=key.upper(), value=str(row[key]))

    # Generate properly formatted KML
    kml_string = kml.kml()
    
    # Return as KML bytes (not KMZ)
    return kml_string.encode("utf-8")
