from datetime import datetime
import geopandas as gpd
import psycopg2
import psycopg2.extras


def get_bike_lanes(muni_name):
    con = psycopg2.connect(database='osm')
    psycopg2.extras.register_hstore(con)

    HSTORE_TAGS = [
        'highway', 'foot', 'surface', 'lit', 'segregated', 'snowplowing',
        'footway', 'name', 'bicycle', 'cycleway', 'oneway', 'motor_vehicle', 'tunnel',
        'start_date', 'osm_timestamp'
    ]
    cols = ["tags -> '%s' AS %s" % (tag, tag) for tag in HSTORE_TAGS]
    col_sql = ', '.join(cols)

    muni_sql = """SELECT way FROM planet_osm_polygon
        WHERE boundary = 'administrative' AND name ILIKE '%s'""" % muni_name
    sql = """WITH munigeom AS (%s)
        SELECT %s,
          ST_Length(ST_Intersection(way, (SELECT way FROM munigeom))) AS length,
          ST_Transform(ST_SetSRID(way, 3067), 4326) AS way
        FROM planet_osm_line
        WHERE (highway='cycleway' OR tags ? 'cycleway') AND
          ST_Intersects(way, (SELECT way FROM munigeom))""" % (muni_sql, col_sql)

    df = gpd.GeoDataFrame.from_postgis(sql, con, geom_col='way')
    return df


if __name__ == '__main__':
    import sys
    import quilt
    from quilt.data.jyrjola import osm

    data_date = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    print("Executing SQL...")
    df = get_bike_lanes('helsinki')

    df['date'] = data_date
    # Quilt is unable to store geometry data, so drop the geometry
    # column for now.
    df.drop('way', inplace=True, axis=1)
    print("%d rows received, total length %d km" % (len(df), df['length'].sum() / 1000))

    old_df = osm.helsinki_bike_lanes()
    quilt.build('jyrjola/osm/helsinki_bike_lanes', old_df.append(df))
    #quilt.push('jyrjola/osm', is_public=True)
