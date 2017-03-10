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
import java.util.concurrent.TimeUnit;

import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;
import com.google.common.geometry.S2RegionCoverer;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

/**
 * 分表存储 数据表与索引表分离,分为三个表：
 * 
 * 1. Layers 2. <layername>_DAT 3. <layername>_INX
 * 
 */
public class CassandraIngest2 {

	public final static Map<DataType, Class> CA_MAP_TO_TYPE = new HashMap<DataType, Class>() {
		{
			put(DataType.cint(), Integer.class);
			put(DataType.text(), String.class);
			put(DataType.bigint(), java.lang.Long.class);
			put(DataType.cfloat(), java.lang.Float.class);
			put(DataType.cdouble(), java.lang.Double.class);
			put(DataType.timestamp(), Date.class);
			put(DataType.uuid(), UUID.class);
			put(DataType.blob(), Geometry.class);
		}
	};
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
	LoadingCache<String, SimpleFeature> featureCache = null;
	LoadingCache<String, String> idCache = null;
	LoadingCache<String, Integer> countCache = null;

	public CassandraIngest2() {
		PoolingOptions poolingOptions = new PoolingOptions();
		poolingOptions.setConnectionsPerHost(HostDistance.LOCAL, 4, 10).setConnectionsPerHost(HostDistance.REMOTE, 2,
				4);
		poolingOptions.setMaxRequestsPerConnection(HostDistance.LOCAL, 32768)
				.setMaxRequestsPerConnection(HostDistance.REMOTE, 2000);
		poolingOptions.setHeartbeatIntervalSeconds(60);
		cluster = Cluster.builder().addContactPoint("192.168.210.110").withCompression(ProtocolOptions.Compression.LZ4)
				.build();
		index = new S2IndexStrategy();

		featureCache = CacheBuilder.newBuilder().expireAfterWrite(5L, TimeUnit.MINUTES).maximumSize(5000000L)
				.build(new CacheLoader<String, SimpleFeature>() {
					@Override
					public SimpleFeature load(String key) throws Exception {
						// TODO Auto-generated method stub
						return null;
					}
				});
		idCache = CacheBuilder.newBuilder().expireAfterWrite(5L, TimeUnit.MINUTES).maximumSize(5000000L)
				.build(new CacheLoader<String, String>() {
					@Override
					public String load(String key) throws Exception {
						// TODO Auto-generated method stub
						return null;
					}
				});
		countCache = CacheBuilder.newBuilder().expireAfterWrite(5L, TimeUnit.MINUTES).maximumSize(5000000L)
				.build(new CacheLoader<String, Integer>() {
					@Override
					public Integer load(String key) throws Exception {
						// TODO Auto-generated method stub
						return null;
					}
				});
		init();

	}

	protected void init() {
		StringBuilder cql = new StringBuilder();
		cql.append("CREATE TABLE IF NOT EXISTS meta.layers(");
		cql.append("workspace text,");
		cql.append("layer_name text,");
		cql.append("cdate bigint,");
		cql.append("geometry_type text,");
		cql.append("owner text,");
		cql.append("srid int,");
		cql.append("maxx double,");
		cql.append("minx double,");
		cql.append("maxy double,");
		cql.append("miny double,");
		cql.append("count bigint,");
		cql.append("PRIMARY KEY (workspace, layer_name, cdate));");
		Session session = cluster.connect();
		System.out.println(cql);
		session.execute(cql.toString());

		session.close();
	}

	public void ingest() throws Exception {
		Session session = cluster.connect();
		String time = "170101";
		String datetime = "20" + time + "00";
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
		Date date = formatter.parse(datetime);
		String year_month = new SimpleDateFormat("yyyyMM").format(date);

		ShapefileDataStoreFactory datasoreFactory = new ShapefileDataStoreFactory();
		// gis.osm_pois_free_1.shp
		// gis.osm_buildings_a_free_1
		// gis.osm_roads_free_1
		ShapefileDataStore sds = (ShapefileDataStore) datasoreFactory.createDataStore(new File(
				"E:\\Data\\OSM\\USA\\california\\california-" + time + "-free.shp\\gis.osm_buildings_a_free_1.shp")
						.toURI().toURL());
		sds.setCharset(Charset.forName("GBK"));
		SimpleFeatureSource featureSource = sds.getFeatureSource();
		SimpleFeatureType featureType = featureSource.getFeatures().getSchema();
		session.execute("use usa;");
		createSchema("usa", session, featureType, date);
		SimpleFeatureCollection featureCollection = featureSource.getFeatures();
		FeatureIterator<SimpleFeature> features = featureCollection.features();
		WKBWriter writer = new WKBWriter();
		int count = 0;
		BatchStatement bs = new BatchStatement();
		String table_name = featureType.getName().toString().replace(".", "_");
		Geometry geom;
		long t0 = System.currentTimeMillis();
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

			Object cell_id = primaryKey.get("quad_id");
			Object pos = primaryKey.get("cell_id");

			Object osm_id = "";
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
					Object value = feature.getAttribute(col_name);
					values.add(value);
					if (col_name.equals("osm_id")) {
						osm_id = value;
					}
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
			values.add(pos);
			SimpleStatement s = new SimpleStatement(
					"INSERT INTO " + table_name + "_DAT" + " (" + cols + ",pos) values (" + params + ",?);",
					values.toArray());
			bs.add(s);
			s = new SimpleStatement("INSERT INTO " + table_name + "_INX" + " (cell_id,pos,osm_id) values ( ?,?,?);",
					cell_id, pos, osm_id);
			bs.add(s);
			count++;
			if (count == 25) {
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

		System.out.println(System.currentTimeMillis() - t0);
		System.out.println("Finish!");
	}

	public void ingest2() throws Exception {
		Session session = cluster.connect();
		String time = "170101";
		String datetime = "20" + time + "00";
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
		Date date = formatter.parse(datetime);
		String year_month = new SimpleDateFormat("yyyyMM").format(date);

		ShapefileDataStoreFactory datasoreFactory = new ShapefileDataStoreFactory();
		// gis.osm_pois_free_1.shp
		// gis.osm_buildings_a_free_1
		// gis.osm_roads_free_1
		ShapefileDataStore sds = (ShapefileDataStore) datasoreFactory.createDataStore(new File(
				"E:\\Data\\OSM\\USA\\california\\california-" + time + "-free.shp\\gis.osm_buildings_a_free_1.shp")
						.toURI().toURL());
		sds.setCharset(Charset.forName("GBK"));
		SimpleFeatureSource featureSource = sds.getFeatureSource();
		SimpleFeatureType featureType = featureSource.getFeatures().getSchema();
		session.execute("use usa;");
		createSchema("usa", session, featureType, date);
		SimpleFeatureCollection featureCollection = featureSource.getFeatures();
		FeatureIterator<SimpleFeature> features = featureCollection.features();
		WKBWriter writer = new WKBWriter();
		int count = 0;
		BatchStatement bs = new BatchStatement();
		String table_name = featureType.getName().toString().replace(".", "_");
		Geometry geom;
		long t0 = System.currentTimeMillis();
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
			values.add(primaryKey.get("cell_id"));

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
					+ " (cell_id, pos, " + cols + ") values (?,?," + params + ");",
					values.toArray());
			
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

		System.out.println(System.currentTimeMillis() - t0);
		System.out.println("Finish!");
	}
	
	public void createSchema(String workspace, Session session, SimpleFeatureType featureType, Date date)
			throws IOException {

		String stmt = "";
		String layer_name = featureType.getTypeName();
		int srid = getSRID(featureType);
		double minx = 0;
		double miny = 0;
		double maxx = 0;
		double maxy = 0;
		String geometry_type = featureType.getGeometryDescriptor().getType().getName().getLocalPart();
		SimpleStatement statement = new SimpleStatement(
				"INSERT INTO meta.layers (workspace,layer_name,cdate,owner,geometry_type,srid,minx,miny,maxx,maxy,count)"
						+ "VALUES (?,?,?,?,?,?,?,?,?,?,?);",
				workspace, layer_name.toString().replace(".", "_"), date.getTime(), "xiaofei", geometry_type, srid,
				minx, miny, maxx, maxy, 0L);
		session.execute(statement);
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
		
		String colCreate = "(cell_id text, pos text," + cols
				+ ", PRIMARY KEY (cell_id,pos,osm_id));";
		stmt = "CREATE TABLE IF NOT EXISTS " + featureType.getName().toString().replace(".", "_") + " "
				+ colCreate;
		System.out.println(stmt);
		session.execute(stmt);
		
	    colCreate = "(pos text," + cols + ", PRIMARY KEY (osm_id));";
		stmt = "CREATE TABLE IF NOT EXISTS " + featureType.getName().toString().replace(".", "_") + "_DAT " + colCreate;
		System.out.println(stmt);
		session.execute(stmt);

		stmt = "CREATE TABLE IF NOT EXISTS " + featureType.getName().toString().replace(".", "_")
				+ "_INX (cell_id text,pos text,osm_id text,PRIMARY KEY (cell_id, pos, osm_id));";
		session.execute(stmt);
		System.out.println(stmt);
	}

	/**
	 * Looks up the geometry srs by trying a number of heuristics. Returns -1 if
	 * all attempts at guessing the srid failed.
	 */
	protected int getSRID(SimpleFeatureType featureType) {
		int srid = -1;
		CoordinateReferenceSystem flatCRS = CRS.getHorizontalCRS(featureType.getCoordinateReferenceSystem());
		try {
			Integer candidate = CRS.lookupEpsgCode(flatCRS, false);
			if (candidate != null)
				srid = candidate;
			else
				srid = 4326;
		} catch (Exception e) {
			// ok, we tried...
		}
		return srid;
	}

	// 多线程同步查询(5线程，目前效果最好)
	public void query1() throws Exception {
		Session session = cluster.connect();
		// monitor(session);
		session.execute("use usa;");
		long t0 = System.currentTimeMillis();
		// String polygon = "32:-125,32:-114,42.44:-114,42.44:-125;";
		String polygon = "36.84:-123.1,36.84:-120.54,38.88:-120.54,38.88:-123.1;";
		// String polygon
		// ="36.24:-123.42,36.24:-120.0,39.13:-120.0,39.13:-123.42;";
		// String polygon =
		// "32.58:-119.26,32.58:-115.64,34.7:-115.64,34.7:-119.26;";
		// String polygon =
		// "32.49:-121.32,32.49:-115.34,40.8:-115.40,37.8:-121.32;";
		// String polygon ="-122.824 48.100, -122.824 46.902, -121.869 46.902,
		// -121.869 48.100, -122.824 48.100";
		// String polygon
		// ="36.84:-123.1,36.84:-121.869,38.88:-121.869,38.88:-123.1;";
		// String polygon =
		// "32.90:-125.04,32.9:-113.34,42.36:-113.34,42.36:-125.04;";
		List<String> quad_ids = new ArrayList<>();
		S2Polygon a = makePolygon(polygon);
		ArrayList<S2CellId> covering = new ArrayList<>();
		S2RegionCoverer coverer = new S2RegionCoverer();
		coverer.setMaxCells(1);
		coverer.getCovering(a, covering);
		S2CellId cell = covering.get(0);
		int level = cell.level();
		for (int i = 1; i <= level; i++) {
			S2CellId parent = cell.parent(i);
			quad_ids.add(parent.toToken());
		}
		for (int i = level + 1; i <= 10; i++) {
			covering.clear();
			coverer.setMaxLevel(i);
			coverer.setMinLevel(i);
			coverer.getCovering(a, covering);
			for (S2CellId id : covering) {
				quad_ids.add(id.toToken());
			}
		}
		System.out.println(quad_ids.size());
		PreparedStatement statement = session
				.prepare("select cell_id,pos,osm_id from gis_osm_buildings_a_free_1_INX where cell_id = ?;");
		List<ResultSetFuture> futures = new ArrayList<>();
		// Envelope bbox = new ReferencedEnvelope(-121.32, -115.34, 32.49,
		// 35.8,DefaultGeographicCRS.WGS84);
		//Envelope bbox = new ReferencedEnvelope(-125.04, -113.33, 32.9, 42.36, DefaultGeographicCRS.WGS84);
		Envelope bbox = new ReferencedEnvelope(-123.1,-120.54,36.84, 38.88, DefaultGeographicCRS.WGS84);
		SimpleFeatureType sft = getSchema(new NameImpl("gis_osm_buildings_a_free_1_DAT"),
				cluster.getMetadata().getKeyspace("usa").getTable("gis_osm_buildings_a_free_1_DAT"));
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
		System.out.println((System.currentTimeMillis() - t0) + " ms");
		List<QueryProcess> tasks = new ArrayList<>();
		int index = 0;
		for (String quad_id : quad_ids) {
			QueryProcess process = new QueryProcess(quad_id, session, countCache, statement.bind(quad_id), builder,
					bbox);
			tasks.add(process);
			index++;
		}
		executorService.invokeAll(tasks);

		int sum = 0;
		for (Integer count : countCache.asMap().values()) {
			sum += count;
		}
		System.out.println(sum + " : " + (System.currentTimeMillis() - t0) + " ms");

		session.close();
		//cluster.close();
	}

	public void query2() throws Exception {
		Session session = cluster.connect();
		session.execute("use usa;");
		long t0 = System.currentTimeMillis();
		String polygon = "36.84:-123.1,36.84:-120.54,38.88:-120.54,38.88:-123.1;";
		List<String> quad_ids = new ArrayList<>();
		S2Polygon a = makePolygon(polygon);
		ArrayList<S2CellId> covering = new ArrayList<>();
		S2RegionCoverer coverer = new S2RegionCoverer();
		coverer.setMaxCells(1);
		coverer.getCovering(a, covering);

		S2CellId cell = covering.get(0);
		int level = cell.level();
		for (int i = 1; i <= level; i++) {
			S2CellId parent = cell.parent(i);
			quad_ids.add(parent.toToken());
		}
		for (int i = level + 1; i <= 10; i++) {
			covering.clear();
			coverer.setMaxLevel(i);
			coverer.setMinLevel(i);
			coverer.getCovering(a, covering);
			for (S2CellId id : covering) {
				quad_ids.add(id.toToken());
			}
		}
		System.out.println(quad_ids);

		//PreparedStatement statement = session.prepare(
		//		"select quad_id,epoch,the_geom,fid,timestamp from gis_osm_buildings_a_free_1 where quad_id = ? and epoch=? and timestamp>=? and timestamp<=? allow filtering;");
		
		PreparedStatement statement = session.prepare("select osm_id,the_geom from gis_osm_buildings_a_free_1 where cell_id = ?;");	
		List<ResultSetFuture> futures = new ArrayList<>();
		ByteBuffer buffer;
		Geometry geometry = null;
		WKBReader reader = new WKBReader();
		//Envelope bbox = new ReferencedEnvelope(-121.32, -115.34, 32.49, 35.8, DefaultGeographicCRS.WGS84);
		Envelope bbox = new ReferencedEnvelope(-123.1,-120.54,36.84, 38.88, DefaultGeographicCRS.WGS84);
		SimpleFeatureType sft = getSchema(new NameImpl("gis_osm_buildings_a_free_1"),
				cluster.getMetadata().getKeyspace("usa").getTable("gis_osm_buildings_a_free_1"));
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
		System.out.println((System.currentTimeMillis() - t0) + " ms");
		List<QueryProcess> tasks = new ArrayList<>();
		int index=0;
		for (String quad_id : quad_ids) {
			QueryProcess process = new QueryProcess(quad_id,session, countCache, statement.bind(quad_id),builder, bbox);
			tasks.add(process);
			index++;
		}
		executorService.invokeAll(tasks);

		int sum=0;
		for(Integer count:countCache.asMap().values()){
			sum+=count;
		}
		System.out.println(sum + " : " + (System.currentTimeMillis() - t0) + " ms");

		session.close();
	}
	
	class QueryProcess implements Callable<Integer> {

		LoadingCache<String, Integer> queue;
		Session session;
		String datetime;
		Geometry geometry;
		Statement statement;
		ByteBuffer buffer;
		SimpleFeatureBuilder builder;
		WKBReader reader = new WKBReader();
		Envelope bbox;
		String index;

		public QueryProcess(String index, Session session, LoadingCache<String, Integer> queue, Statement statement,
				SimpleFeatureBuilder builder, Envelope bbox) {
			this.index = index;
			this.queue = queue;
			this.session = session;
			this.statement = statement;
			this.builder = builder;
			this.bbox = bbox;
		}

		@Override
		public Integer call() throws Exception {
			ResultSet rs = session.execute(statement);
			int count=0;
			for (Row row : rs) {
				String id = row.getString("osm_id");
				buffer = row.getBytes("the_geom");
				geometry = reader.read(buffer.array());
				if (!bbox.intersects(geometry.getEnvelopeInternal())) {
					continue;
				}
				builder.set("the_geom", geometry);
				SimpleFeature feature = builder.buildFeature(id);
				count++;
			}
			queue.put(index, count);
			return null;
		}

	}

	public void queryRange() {
		long t0 = System.currentTimeMillis();
		Session session = cluster.connect();
		String polygon = "36.84:-123.1,36.84:-120.54,38.88:-120.54,38.88:-123.1;";
		List<String> quad_ids = new ArrayList<>();
		S2Polygon a = makePolygon(polygon);
		ArrayList<S2CellId> covering = new ArrayList<>();
		S2RegionCoverer coverer = new S2RegionCoverer();
		coverer.setMaxCells(1);
		coverer.getCovering(a, covering);
		S2CellId cell = covering.get(0);

		SimpleStatement stmt = new SimpleStatement(
				"select * from usa.gis_osm_buildings_a_free_1_DAT where pos>=? and pos<=? allow filtering",
				cell.rangeMin().toToken(), cell.rangeMax().toToken());
		Envelope bbox = new ReferencedEnvelope(-123.1,-120.54,36.84, 38.88, DefaultGeographicCRS.WGS84);
		ResultSet rs = session.execute(stmt);
		int count = 0;
		ByteBuffer buffer;
		Geometry geometry;
		Statement statement;
		SimpleFeatureBuilder builder;
		WKBReader reader = new WKBReader();
		for (Row row : rs) {
			buffer = row.getBytes("the_geom");
			try {
				geometry = reader.read(buffer.array());
				if (!bbox.intersects(geometry.getEnvelopeInternal())) {
					continue;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			count++;
		}
		session.close();
		System.out.println(count + " : " + (System.currentTimeMillis() - t0) + " ms");
	}

	private S2Polygon makePolygon(String str) {
		List<S2Loop> loops = Lists.newArrayList();

		for (String token : Splitter.on(';').omitEmptyStrings().split(str)) {
			S2Loop loop = makeLoop(token);
			loop.normalize();
			loops.add(loop);
		}

		return new S2Polygon(loops);
	}

	private S2Loop makeLoop(String str) {
		List<S2Point> vertices = Lists.newArrayList();
		parseVertices(str, vertices);
		return new S2Loop(vertices);
	}

	private void parseVertices(String str, List<S2Point> vertices) {
		if (str == null) {
			return;
		}

		for (String token : Splitter.on(',').split(str)) {
			int colon = token.indexOf(':');
			if (colon == -1) {
				throw new IllegalArgumentException("Illegal string:" + token + ". Should look like '35:20'");
			}
			double lat = Double.parseDouble(token.substring(0, colon));
			double lng = Double.parseDouble(token.substring(colon + 1));
			vertices.add(S2LatLng.fromDegrees(lat, lng).toPoint());
		}
	}

	public SimpleFeatureType getSchema(Name name, TableMetadata table) {
		List<ColumnMetadata> columns = table.getColumns();
		SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		builder.setName(name);
		AttributeTypeBuilder attrTypeBuilder = new AttributeTypeBuilder();
		for (ColumnMetadata cm : columns) {
			String cname = cm.getName();
			Class binding = CA_MAP_TO_TYPE.get(cm.getType());
			if (!cm.getName().equals("quad_id") && !cm.getName().equals("epoch") && !cm.getName().equals("cell_id")
					&& !cm.getName().equals("timestamp") && !cm.getName().equals("fid")) {
				if (Geometry.class.isAssignableFrom(binding)) {
					attrTypeBuilder.binding(binding);
					CoordinateReferenceSystem wsg84 = null;
					try {
						wsg84 = CRS.decode("EPSG:4326");
					} catch (Exception e) {
						e.printStackTrace();
					}
					attrTypeBuilder.setCRS(wsg84);
					builder.add(attrTypeBuilder.buildDescriptor(cname, attrTypeBuilder.buildGeometryType()));
				} else {
					builder.add(attrTypeBuilder.binding(binding).nillable(false).buildDescriptor(cname));
				}
			}
		}
		return builder.buildFeatureType();
	}

	public static void main(String[] args) throws Exception {
		CassandraIngest2 ingest = new CassandraIngest2();
		//ingest.ingest2();
		//ingest.query2();
		ingest.query2();
		//ingest.queryRange();
	}

}
