package org.cassandra.sde.ingest;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.WKBWriter;

public class CassandraIngest {

	public final static Map<Class, DataType> TYPE_TO_CA_MAP = new HashMap<Class, DataType>() {
		{
			put(Integer.class, DataType.cint());
			put(String.class, DataType.text());
			put(java.lang.Long.class, DataType.bigint());
			put(java.lang.Float.class, DataType.cfloat());
			put(java.lang.Double.class, DataType.cdouble());
			put(Date.class, DataType.timestamp());
			put(UUID.class, DataType.uuid());
			put(com.vividsolutions.jts.geom.Geometry.class, DataType.blob());
			put(Point.class, DataType.blob());
			put(MultiPolygon.class, DataType.blob());
			put(MultiLineString.class, DataType.blob());
		}
	};
	private Cluster cluster = null;
	private S2IndexStrategy index;
	ExecutorService executorService = Executors.newFixedThreadPool(20);
	
	public CassandraIngest() {
		PoolingOptions poolingOptions = new PoolingOptions();
		poolingOptions.setConnectionsPerHost(HostDistance.LOCAL, 4, 10).setConnectionsPerHost(HostDistance.REMOTE, 2,
				4);
		poolingOptions.setMaxRequestsPerConnection(HostDistance.LOCAL, 32768)
				.setMaxRequestsPerConnection(HostDistance.REMOTE, 2000);
		poolingOptions.setHeartbeatIntervalSeconds(60);
		cluster = Cluster.builder().addContactPoint("192.168.210.110").withCompression(ProtocolOptions.Compression.LZ4).build();
		index = new S2IndexStrategy();

	}

	public void monitor(Session session) {
		final LoadBalancingPolicy loadBalancingPolicy = cluster.getConfiguration().getPolicies()
				.getLoadBalancingPolicy();
		final PoolingOptions poolingOptions = cluster.getConfiguration().getPoolingOptions();

		ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);
		scheduled.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				Session.State state = session.getState();
				for (Host host : state.getConnectedHosts()) {
					HostDistance distance = loadBalancingPolicy.distance(host);
					int connections = state.getOpenConnections(host);
					int inFlightQueries = state.getInFlightQueries(host);
					System.out.printf("%s connections=%d, current load=%d, maxload=%d%n", host, connections,
							inFlightQueries, connections * poolingOptions.getMaxRequestsPerConnection(distance));
				}
			}
		}, 5, 5, TimeUnit.SECONDS);
	}

	public void ingest() throws Exception {
		
		String time[]={"161201"};
		
		List<IngestProcess> tasks = new ArrayList<>();
		for(String s:time){
			Session session = cluster.connect();
			monitor(session);
			IngestProcess process=new IngestProcess(session, s);
			tasks.add(process);
			
		}
		executorService.invokeAll(tasks);
		cluster.close();

	}

	class IngestProcess implements Callable<Integer>{
		Session session;
		String time="";
		public IngestProcess(Session session,String datetime) {
			this.session=session;
			this.time=datetime;
		}
		
		@Override
		public Integer call() throws Exception {
			//monitor(session);
			String datetime = "20"+time+"00";
			ShapefileDataStoreFactory datasoreFactory = new ShapefileDataStoreFactory();
			ShapefileDataStore sds = (ShapefileDataStore) datasoreFactory.createDataStore(
					new File("E:\\Data\\OSM\\USA\\california\\california-"+time+"-free.shp\\gis.osm_buildings_a_free_1.shp")
							.toURI().toURL());
			sds.setCharset(Charset.forName("GBK"));
			SimpleFeatureSource featureSource = sds.getFeatureSource();
			SimpleFeatureType featureType = featureSource.getFeatures().getSchema();
			session.execute("use usa2;");
			createSchema(session, featureType);
			SimpleFeatureCollection featureCollection = featureSource.getFeatures();
			FeatureIterator<SimpleFeature> features = featureCollection.features();
			WKBWriter writer = new WKBWriter();
			int count = 0;
			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
			Date date = formatter.parse(datetime);
			String year_month = new SimpleDateFormat("yyyyMM").format(date);

			BatchStatement bs = new BatchStatement();
			String table_name = featureType.getName().toString().replace(".", "_");

			Geometry geom;

			while (features.hasNext()) {
				SimpleFeature feature = features.next();
				if (featureType.getGeometryDescriptor().getType().getName().toString().equals("MultiPolygon")) {
					geom = (MultiPolygon) feature.getDefaultGeometry();
				} else if (featureType.getGeometryDescriptor().getType().getName().toString().equals("MultiLineString")) {
					geom = (MultiLineString) feature.getDefaultGeometry();
				} else {
					geom = (Point) feature.getDefaultGeometry();
				}

				ByteBuffer buf_geom = ByteBuffer.wrap(writer.write(geom));
				Map<String, Object> primaryKey = index.index(geom, date.getTime());
				List<AttributeDescriptor> attrDes = featureType.getAttributeDescriptors();
				List<String> col_items = new ArrayList<>();
				List<Object> values = new ArrayList<>();

				values.add(primaryKey.get("quad_id"));
				values.add(primaryKey.get("epoch"));
				values.add(primaryKey.get("cell_id"));
				values.add(primaryKey.get("timestamp"));
				values.add(primaryKey.get("fid"));

				for (AttributeDescriptor attr : attrDes) {
					if (attr instanceof GeometryDescriptor) {
						String col_name = attr.getLocalName();
						Class type = attr.getType().getBinding();
						col_items.add(col_name);
						values.add(buf_geom);
					} else {
						String col_name = attr.getLocalName();
						Class type = attr.getType().getBinding();
						col_items.add(col_name);
						values.add(feature.getAttribute(col_name));
					}

				}
				String cols = "";
				String params = "";
				for (int i = 0; i < col_items.size() - 1; i++) {
					cols += col_items.get(i) + ",";
					params += "?,";
				}
				cols += col_items.get(col_items.size() - 1);
				params += "?";
				SimpleStatement s = new SimpleStatement("INSERT INTO " + table_name
						+ " (quad_id, epoch, cell_id,timestamp,fid, " + cols + ") values (?,?,?,?,?," + params + ");",
						values.toArray());
				// System.out.println(s);
				bs.add(s);
				count++;
				if (count == 50) {
					try {
						session.execute(bs);
						bs.clear();
						count = 0;
					} catch (Exception e) {
						for (Statement stat : bs.getStatements()) {
							while (true) {
								try {
									session.execute(stat);
									break;
								} catch (Exception e2) {
									System.out.print("-");
								}
							}
							System.out.print(".");
						}
						System.out.print("\n");
						bs.clear();
						count = 0;
					}

				}

			}
			long t0 = System.currentTimeMillis();
			System.out.println("Finish!");
			return null;
		}
	}
	public void createSchema(Session session, SimpleFeatureType featureType) throws IOException {

		List<AttributeDescriptor> attrDes = featureType.getAttributeDescriptors();
		List<String> col_items = new ArrayList<>();
		for (AttributeDescriptor attr : attrDes) {
			String col_name = attr.getLocalName();
			Class type = attr.getType().getBinding();
			col_items.add(col_name + " " + CassandraIngest.TYPE_TO_CA_MAP.get(type).getName().toString());
		}
		String cols = "";
		for (int i = 0; i < col_items.size() - 1; i++) {
			cols += col_items.get(i) + ",";
		}
		cols += col_items.get(col_items.size() - 1);
		String colCreate = "(quad_id text, epoch text, cell_id text,timestamp bigint,fid text," + cols
				+ ", PRIMARY KEY ((quad_id,epoch),cell_id,timestamp,fid));";
		String stmt = "CREATE TABLE IF NOT EXISTS " + featureType.getName().toString().replace(".", "_") + " "
				+ colCreate;
		session.execute(stmt);
	}

	public static void main(String[] args) throws Exception {
		new CassandraIngest().ingest();
	}
}
