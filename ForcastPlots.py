import snowflake.connector as sf
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import shape
from shapely.wkt import dumps
import json
conn = sf.connect(
    user='DNEXR-JBENSALEM',
    password='jjbKW6mGwJLkr@zpCMLdWPAZEFA.YwgYnp6zqaxesKLX.r_w',
    account='gq42038.eu-west-1',
    role='TABLECREATOR_DNDEV',
    warehouse='DNDEV'
)
cursor = conn.cursor()
# Execute a SQL query to select the GEOGRAPHY data from your table
query = 'SELECT GEOM FROM DNDEV.LAB.COUNTRY_SHAPEFILES'
cursor.execute(query)
geojson_data = cursor.fetchone()[0]

cursor.close()
conn.close()

# Convert the data to a JSON-like structure
data = json.loads(geojson_data)

# Create a GeoDataFrame using the extracted data
gdf = gpd.GeoDataFrame.from_features(data["features"])

# Plot the GeoDataFrame
gdf.plot()
plt.title("Snowflake Data Plot")
plt.axis('off')  # Turn off the axis
plt.show()