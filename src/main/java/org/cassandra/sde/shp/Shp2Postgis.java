package org.cassandra.sde.shp;

import java.io.File;
import java.nio.charset.Charset;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.postgis.PGgeometry;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.WKTWriter;

public class Shp2Postgis {

	public void ingest() {
		
		String[] dates={"161001","161101","161117","161201","161227","170101","170201"};
		
		try {
			
			java.sql.Connection conn;
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://localhost:5432/japan";
			conn = DriverManager.getConnection(url, "postgres", "869222");
			Statement s = conn.createStatement();

			for(int i=0;i<dates.length;i++){
				String d=dates[i];
				String datetime="20"+d+"00";
				String shpFile="E:\\Data\\OSM\\USA\\california\\california-"+d+"-free.shp\\gis.osm_buildings_a_free_1.shp";
				
				SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
				Date date = formatter.parse(datetime);		
				ShapefileDataStoreFactory datasoreFactory = new ShapefileDataStoreFactory();
				ShapefileDataStore sds = (ShapefileDataStore) datasoreFactory.createDataStore(new File(shpFile).toURI().toURL());
				sds.setCharset(Charset.forName("GBK"));
				SimpleFeatureSource featureSource = sds.getFeatureSource();
				SimpleFeatureType featureType = featureSource.getFeatures().getSchema();
				SimpleFeatureCollection featureCollection = featureSource.getFeatures();
				FeatureIterator<SimpleFeature> features = featureCollection.features();
				Geometry geom;
				WKTWriter writer = new WKTWriter();

				StringBuilder builder;
			
				int count=0;
				System.out.println("Begin to ingest: "+shpFile);
				while (features.hasNext()) {
					builder=new StringBuilder();
					SimpleFeature feature = features.next();
					if (featureType.getGeometryDescriptor().getType().getName().toString().equals("MultiPolygon")) {
						geom = (MultiPolygon) feature.getDefaultGeometry();
					} else if (featureType.getGeometryDescriptor().getType().getName().toString()
							.equals("MultiLineString")) {
						geom = (MultiLineString) feature.getDefaultGeometry();
					} else {
						geom = (Point) feature.getDefaultGeometry();
					}
					builder.append("INSERT INTO public.\"gis.osm_buildings_a_free_2\"(osm_id, code, fclass, name, type, \"timestamp\", geom) VALUES (");
					builder.append("'"+feature.getAttribute("osm_id").toString()+"',");
					builder.append(""+feature.getAttribute("code").toString()+",");
					builder.append("'"+feature.getAttribute("fclass").toString()+"',");
					if(feature.getAttribute("name").toString().equals("")){
						builder.append("null,");
					}
					else{
						builder.append("'"+feature.getAttribute("name").toString().replace("'", "\''")+"',");
					}
					if(feature.getAttribute("type").toString().equals("")){
						builder.append("null,");
					}
					else{
						builder.append("'"+feature.getAttribute("type").toString().replace("'", "\''")+"',");
					}
					
					builder.append(""+date.getTime()+",");
					builder.append("ST_GeomFromText('"+writer.write(geom)+"'));");
					//System.out.println(builder.toString());
					s.addBatch(builder.toString());
					builder=null;
					count++;
					if(count==1000){
						try {
							s.executeBatch();
						} catch (Exception e) {
							// TODO: handle exception
						}
						s.clearBatch();
						count=0;
					}
				}
				System.out.println("Finish ingest: "+shpFile);
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void insert() {
		java.sql.Connection conn;

		try {
			/*
			 * Load the JDBC driver and establish a connection.
			 */
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://localhost:5432/japan";
			conn = DriverManager.getConnection(url, "postgres", "869222");
			/*
			 * Add the geometry types to the connection. Note that you must cast
			 * the connection to the pgsql-specific connection implementation
			 * before calling the addDataType() method.
			 */

			/*
			 * Create a statement and execute a select query.
			 */
			Statement s = conn.createStatement();
			s.execute(
					"INSERT INTO public.\"gis.osm_pois_free_2\"(osm_id, code, fclass, name, geom) VALUES ('1', 1, 'dddd', 'ssss', ST_GeomFromText('POINT(-126.4 45.32)', 4326));");
			s.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void select() {
		java.sql.Connection conn;

		try {
			/*
			 * Load the JDBC driver and establish a connection.
			 */
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://localhost:5432/japan";
			conn = DriverManager.getConnection(url, "postgres", "869222");
			/*
			 * Add the geometry types to the connection. Note that you must cast
			 * the connection to the pgsql-specific connection implementation
			 * before calling the addDataType() method.
			 */

			/*
			 * Create a statement and execute a select query.
			 */
			Statement s = conn.createStatement();
			ResultSet r = s.executeQuery("select geom,osm_id from public.\"gis.osm_pois_free_1\"");
			while (r.next()) {
				/*
				 * Retrieve the geometry as an object then cast it to the
				 * geometry type. Print things out.
				 */
				PGgeometry geom = (PGgeometry) r.getObject(1);
				int id = r.getInt(2);
				System.out.println("Row " + id + ":");
				System.out.println((geom.getGeometry()));

			}
			s.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

		new Shp2Postgis().ingest();

		
	}
}
