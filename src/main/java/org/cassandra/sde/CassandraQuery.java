package org.cassandra.sde;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.cassandra.sde.ingest.S2IndexStrategy;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
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
import com.datastax.driver.core.policies.LoadBalancingPolicy;
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
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKTReader;

public class CassandraQuery {

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

	private Cluster cluster = null;
	Executor executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
	LoadingCache<String, SimpleFeature> featureCache = null;
	LoadingCache<String, Integer> featureCache2= null;
	ExecutorService executorService = Executors.newFixedThreadPool(20);

	public CassandraQuery() {
		// cluster = Cluster.builder().addContactPoint("localhost").build();
		PoolingOptions poolingOptions = new PoolingOptions();
		poolingOptions.setConnectionsPerHost(HostDistance.LOCAL, 4, 10).setConnectionsPerHost(HostDistance.REMOTE, 2,
				4);
		poolingOptions.setMaxRequestsPerConnection(HostDistance.LOCAL, 32768)
				.setMaxRequestsPerConnection(HostDistance.REMOTE, 2000);
		poolingOptions.setHeartbeatIntervalSeconds(60);
		cluster = Cluster.builder().addContactPoint("192.168.210.110").withCompression(ProtocolOptions.Compression.LZ4).build();
		//cluster = Cluster.builder().addContactPoint("192.168.210.110").build();
		featureCache = CacheBuilder.newBuilder().expireAfterWrite(5L, TimeUnit.MINUTES).maximumSize(5000000L)
				.build(new CacheLoader<String, SimpleFeature>() {
					@Override
					public SimpleFeature load(String key) throws Exception {
						// TODO Auto-generated method stub
						return null;
					}
				});
		
		
		featureCache2 = CacheBuilder.newBuilder().expireAfterWrite(5L, TimeUnit.MINUTES).maximumSize(5000000L)
				.build(new CacheLoader<String, Integer>() {
					@Override
					public Integer load(String key) throws Exception {
						// TODO Auto-generated method stub
						return null;
					}
				});
	}

	public void query(Geometry geom, long timestamp) {
		Map<String, Object> index = new HashMap<>();
		Envelope envelope = geom.getEnvelopeInternal();
		StringBuilder sb = new StringBuilder();
		sb.append(envelope.getMinY() + ":" + envelope.getMinX() + ",");
		sb.append(envelope.getMinY() + ":" + envelope.getMaxX() + ",");
		sb.append(envelope.getMaxY() + ":" + envelope.getMaxX() + ",");
		sb.append(envelope.getMaxY() + ":" + envelope.getMinX() + ";");
		S2Polygon a = makePolygon(sb.toString());

	}

	// 使用1-10层进行查询
	public void query() throws Exception {
		Session session = cluster.connect();
		session.execute("use usa;");
		long t0 = System.currentTimeMillis();
		// String polygon = "32:-125,32:-114,42.44:-114,42.44:-125;";
		// String polygon =
		// "36.84:-123.1,36.84:-120.54,38.88:-120.54,38.88:-123.1;";
		// String polygon =
		// "36.24:-122.42,36.24:-120.69,39.13:-120.69,39.13:-122.42;";
		String polygon = "36.24:-122.42,36.24:-120.69,39.13:-120.69,39.13:-122.42;";
		String datetime = "2016120100";
		String start = "2016121500";
		String end = "2016121600";
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

		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
		Date date = formatter.parse(datetime);
		String year_month = new SimpleDateFormat("yyyyMM").format(date);
		long timestamp = date.getTime();

		PreparedStatement statement = session.prepare(
				"select quad_id,epoch,the_geom,fid,timestamp from gis_osm_buildings_a_free_1 where quad_id = ? and epoch=? and timestamp>=? and timestamp<=? allow filtering;");
		List<ResultSetFuture> futures = new ArrayList<>();

		long s = formatter.parse(start).getTime();
		long e = formatter.parse(end).getTime();
		for (String quad_id : quad_ids) {
			ResultSetFuture resultSetFuture = session.executeAsync(statement.bind(quad_id, year_month, s, e));
			futures.add(resultSetFuture);
		}

		ByteBuffer buffer;
		Geometry geometry = null;
		WKBReader reader = new WKBReader();
		Envelope bbox = new ReferencedEnvelope(-123.1, -120.54, 36.84, 38.88, DefaultGeographicCRS.WGS84);
		SimpleFeatureType sft = getSchema(new NameImpl("gis_osm_buildings_a_free_1"),
				cluster.getMetadata().getKeyspace("usa").getTable("gis_osm_buildings_a_free_1"));
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);

		System.out.println((System.currentTimeMillis() - t0) + " ms");
		for (ResultSetFuture future : futures) {
			ResultSet rows = future.getUninterruptibly();
			for (Row row : rows) {
				// System.out.println(row);
				buffer = row.getBytes("the_geom");

				String fid = row.getString("fid");
				long time = row.getLong("timestamp");
				try {
					if (time < s || time > e) {
						continue;
					}
					geometry = reader.read(buffer.array());
					if (!bbox.intersects(geometry.getEnvelopeInternal())) {
						continue;
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				}

				builder.set("the_geom", geometry);
				SimpleFeature feature = builder.buildFeature(fid);

				featureCache.put(fid, feature);
			}

		}
		System.out.println(featureCache.size() + " : " + (System.currentTimeMillis() - t0) + " ms");

		session.close();
		cluster.close();
	}

	// 分页查询
	public void asyncPagingCache() throws Exception {
		Session session = cluster.connect();
		session.execute("use usa;");
		long t0 = System.currentTimeMillis();
		// String polygon = "32:-125,32:-114,42.44:-114,42.44:-125;";
		String polygon = "36.84:-123.1,36.84:-120.54,38.88:-120.54,38.88:-123.1;";
		String datetime = "2016120100";
		String start = "2016121500";
		String end = "2016121600";
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

		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
		Date date = formatter.parse(datetime);
		String year_month = new SimpleDateFormat("yyyyMM").format(date);
		long timestamp = date.getTime();
		long s = formatter.parse(start).getTime();
		long e = formatter.parse(end).getTime();
		SimpleFeatureType sft = getSchema(new NameImpl("gis_osm_buildings_a_free_1"),
				cluster.getMetadata().getKeyspace("usa").getTable("gis_osm_buildings_a_free_1"));
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
		Statement statement = new SimpleStatement(
				"select * from gis_osm_buildings_a_free_1 where timestamp>=? and timestamp<=? allow filtering;", s, e)
						.setFetchSize(1000);

		ListenableFuture<ResultSet> future1 = Futures.transformAsync(session.executeAsync(statement),
				iterate(1, t0, builder, featureCache), executor);

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
		}, 2, 2, TimeUnit.SECONDS);
	}
	// 单线程同步查询
	public void query2() throws Exception {
		Session session = cluster.connect();
		session.execute("use usa;");
		long t0 = System.currentTimeMillis();
		// String polygon = "32:-125,32:-114,42.44:-114,42.44:-125;";
		// String polygon =
		// "36.84:-123.1,36.84:-120.54,38.88:-120.54,38.88:-123.1;";
		// String polygon =
		// "36.24:-123.42,36.24:-120.0,39.13:-120.0,39.13:-123.42;";
		// String polygon =
		// "32.58:-119.26,32.58:-115.64,34.7:-115.64,34.7:-119.26;";
		String polygon = "32.49:-120.32,32.49:-116.34,34.8:-116.34,34.8:-120.32;";

		String datetime = "2016120100";
		String start = "2016120100";
		String end = "2016123100";
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

		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
		Date date = formatter.parse(datetime);
		String year_month = new SimpleDateFormat("yyyyMM").format(date);
		long timestamp = date.getTime();

		PreparedStatement statement = session.prepare(
				"select quad_id,epoch,the_geom,fid,timestamp from gis_osm_buildings_a_free_1 where quad_id = ? and epoch=? and timestamp>=? and timestamp<=? allow filtering;");
		List<ResultSetFuture> futures = new ArrayList<>();

		long s = formatter.parse(start).getTime();
		long e = formatter.parse(end).getTime();
		ByteBuffer buffer;
		Geometry geometry = null;
		WKBReader reader = new WKBReader();
		Envelope bbox = new ReferencedEnvelope(-120.32, -116.34, 32.49, 34.8, DefaultGeographicCRS.WGS84);
		SimpleFeatureType sft = getSchema(new NameImpl("gis_osm_buildings_a_free_1"),
				cluster.getMetadata().getKeyspace("usa").getTable("gis_osm_buildings_a_free_1"));
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);

		System.out.println((System.currentTimeMillis() - t0) + " ms");
		int count=0;
		for (String quad_id : quad_ids) {
			ResultSet rs = session.execute(statement.bind(quad_id, year_month, s, e));
			for (Row row : rs) {
				// System.out.println(row);
				buffer = row.getBytes("the_geom");

				String fid = row.getString("fid");
				long time = row.getLong("timestamp");
				try {
					if (time < s || time > e) {
						continue;
					}
					geometry = reader.read(buffer.array());
					if (!bbox.intersects(geometry.getEnvelopeInternal())) {
						continue;
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				}

				builder.set("the_geom", geometry);
				SimpleFeature feature = builder.buildFeature(fid);
				count++;
				//featureCache.put(fid, feature);
				//System.out.println(featureCache.size());
			}
		}

		System.out.println(count+ " : " + (System.currentTimeMillis() - t0) + " ms");

		session.close();
		cluster.close();
	}

	// 多线程同步查询(5线程，目前效果最好)
	public void query3() throws Exception {
		Session session = cluster.connect();
		//monitor(session);
		session.execute("use usa;");
		long t0 = System.currentTimeMillis();
		// String polygon = "32:-125,32:-114,42.44:-114,42.44:-125;";
		// String polygon ="36.84:-123.1,36.84:-120.54,38.88:-120.54,38.88:-123.1;";
		//String polygon ="36.24:-123.42,36.24:-120.0,39.13:-120.0,39.13:-123.42;";
		// String polygon =
		// "32.58:-119.26,32.58:-115.64,34.7:-115.64,34.7:-119.26;";
		//String polygon = "32.49:-121.32,32.49:-115.34,40.8:-115.40,37.8:-121.32;";
		//String polygon ="-122.824 48.100, -122.824 46.902, -121.869 46.902, -121.869 48.100, -122.824 48.100";
		//String polygon ="36.84:-123.1,36.84:-121.869,38.88:-121.869,38.88:-123.1;";
		String polygon = "32.90:-125.04,32.9:-113.34,42.36:-113.34,42.36:-125.04;";
		String datetime = "2017010100";
		String start = "2016120100";
		String end = "2016120200";
		
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

		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
		Date date = formatter.parse(datetime);
		String year_month = new SimpleDateFormat("yyyyMM").format(date);
		long timestamp = date.getTime();

		//PreparedStatement statement = session.prepare(
		//		"select quad_id,epoch,the_geom,fid,timestamp from gis_osm_buildings_a_free_1 where quad_id = ? and epoch=? and timestamp>=? and timestamp<=? allow filtering;");
		
		PreparedStatement statement = session.prepare("select cell_id,epoch,the_geom,fid,timestamp from gis_osm_buildings_a_free_1 where cell_id = ? and epoch=?;");
				
		List<ResultSetFuture> futures = new ArrayList<>();

		long s = formatter.parse(start).getTime();
		long e = formatter.parse(end).getTime();

		ByteBuffer buffer;
		Geometry geometry = null;
		WKBReader reader = new WKBReader();
		//Envelope bbox = new ReferencedEnvelope(-121.32, -115.34, 32.49, 35.8, DefaultGeographicCRS.WGS84);

		Envelope bbox = new ReferencedEnvelope(-125.04, -113.33,32.9,42.36, DefaultGeographicCRS.WGS84);

		SimpleFeatureType sft = getSchema(new NameImpl("gis_osm_buildings_a_free_1"),
				cluster.getMetadata().getKeyspace("usa").getTable("gis_osm_buildings_a_free_1"));
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);

		System.out.println((System.currentTimeMillis() - t0) + " ms");
		List<QueryProcess> tasks = new ArrayList<>();
		int index=0;
		for (String quad_id : quad_ids) {
			
			QueryProcess process = new QueryProcess(quad_id,session, featureCache2, statement.bind("808d91", year_month),builder, bbox);
			tasks.add(process);
			index++;
		}
		executorService.invokeAll(tasks);

		int sum=0;
		for(Integer count:featureCache2.asMap().values()){
			sum+=count;
		}
		System.out.println(sum + " : " + (System.currentTimeMillis() - t0) + " ms");

		session.close();
		cluster.close();
	}

	// 对pos和体mestamp分别进行扫描
	public void query4() throws Exception {
		Session session = cluster.connect();
		session.execute("use usa;");
		long t0 = System.currentTimeMillis();
		// String polygon = "32:-125,32:-114,42.44:-114,42.44:-125;";
		// String polygon =
		// "36.84:-123.1,36.84:-120.54,38.88:-120.54,38.88:-123.1;";
		// String polygon =
		// "36.24:-123.42,36.24:-120.0,39.13:-120.0,39.13:-123.42;";
		// String polygon =
		// "32.58:-119.26,32.58:-115.64,34.7:-115.64,34.7:-119.26;";
		String polygon = "32.49:-121.32,32.49:-115.34,35.8:-115.34,35.8:-121.32;";
		String datetime = "2017020100";
		String start = "2017020100";
		String end = "2017020200";
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

		for (int i = level + 1; i <=10; i++) {
			covering.clear();
			coverer.setMaxLevel(i);
			coverer.setMinLevel(i);
			coverer.getCovering(a, covering);
			for (S2CellId id : covering) {
				quad_ids.add(id.toToken());
			}
		}

		coverer = new S2RegionCoverer();
		coverer.setMinLevel(9);
		covering.clear();
		coverer.getCovering(a, covering);
		for (S2CellId id : covering) {
			if (id.level() == 9)
				quad_ids.add(id.toToken());
			else
				quad_ids.add(id.parent(9).toToken());
		}
		

		System.out.println(quad_ids.size());

		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
		Date date = formatter.parse(datetime);
		String year_month = new SimpleDateFormat("yyyyMM").format(date);

		PreparedStatement statement = session.prepare(
				"select quad_id,epoch,the_geom,fid,timestamp from gis_osm_buildings_a_free_1 where quad_id = ? and epoch=? and timestamp>=? and timestamp<=? allow filtering;");
		List<ResultSetFuture> futures = new ArrayList<>();

		long s = formatter.parse(start).getTime();
		long e = formatter.parse(end).getTime();
		ByteBuffer buffer;
		Geometry geometry = null;
		WKBReader reader = new WKBReader();
		Envelope bbox = new ReferencedEnvelope(-121.32, -115.34, 32.49, 35.8, DefaultGeographicCRS.WGS84);
		SimpleFeatureType sft = getSchema(new NameImpl("gis_osm_buildings_a_free_1"),
				cluster.getMetadata().getKeyspace("usa").getTable("gis_osm_buildings_a_free_1"));
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);

		System.out.println((System.currentTimeMillis() - t0) + " ms");
		List<QueryProcess> tasks = new ArrayList<>();
		for (String quad_id : quad_ids) {
			//QueryProcess process = new QueryProcess(1,session, featureCache, statement.bind(quad_id, year_month, s, e),
			//		builder, bbox);
			//tasks.add(process);
		}
		executorService.invokeAll(tasks);

		System.out.println(featureCache.size() + " : " + (System.currentTimeMillis() - t0) + " ms");

		session.close();
		cluster.close();

	}
	
	public void sizeCompare(){
		long t0 = System.currentTimeMillis();
		// String polygon = "32:-125,32:-114,42.44:-114,42.44:-125;";
		// String polygon =
		// "36.84:-123.1,36.84:-120.54,38.88:-120.54,38.88:-123.1;";
		// String polygon =
		// "36.24:-123.42,36.24:-120.0,39.13:-120.0,39.13:-123.42;";
		// String polygon =
		// "32.58:-119.26,32.58:-115.64,34.7:-115.64,34.7:-119.26;";
		String polygon = "32.49:-121.32,32.49:-115.34,35.8:-115.34,35.8:-121.32;";
		String datetime = "2017020100";
		String start = "2017020100";
		String end = "2017020200";
		List<String> quad_ids = new ArrayList<>();
		S2Polygon a = makePolygon(polygon);

		ArrayList<S2CellId> covering = new ArrayList<>();
		S2RegionCoverer coverer = new S2RegionCoverer();
		coverer.setMaxCells(1);
		coverer.getCovering(a, covering);

		coverer.setMaxCells(1);
		coverer.getCovering(a, covering);

		S2CellId cell = covering.get(0);
		int level = cell.level();
		for (int i = 1; i <= level; i++) {
			S2CellId parent = cell.parent(i);
			quad_ids.add(parent.toToken());
		}
		for (int i = level + 1; i <= 9; i++) {
			covering.clear();
			coverer.setMaxLevel(i);
			coverer.setMinLevel(i);
			coverer.getCovering(a, covering);
			System.out.println(i+":"+covering.size());
			for (S2CellId id : covering) {
				quad_ids.add(id.toToken());
			}
		}
		System.out.println(quad_ids.size());
		quad_ids.clear();
		
		for (int i = 1; i <= level; i++) {
			S2CellId parent = cell.parent(i);
			quad_ids.add(parent.toToken());
		}

		for (int i = level + 1; i < 9; i++) {
			covering.clear();
			coverer.setMaxLevel(i);
			coverer.setMinLevel(i);
			coverer.getCovering(a, covering);
			for (S2CellId id : covering) {
				quad_ids.add(id.toToken());
			}
		}

		coverer = new S2RegionCoverer();
		coverer.setMinLevel(9);
		covering.clear();
		coverer.getCovering(a, covering);

		for (S2CellId id : covering) {
			System.out.println(id);
			if (id.level() == 9)
				quad_ids.add(id.toToken());
			else
				quad_ids.add(id.parent(9).toToken());
		}
		

		System.out.println(quad_ids.size());
	}

	class QueryProcess implements Callable<Integer> {

		LoadingCache<String,Integer> queue;
		Session session;
		String datetime;
		Geometry geometry;
		Statement statement;
		ByteBuffer buffer;
		SimpleFeatureBuilder builder;
		WKBReader reader = new WKBReader();
		Envelope bbox;
		String index;
		public QueryProcess(String index,Session session, LoadingCache<String,Integer> queue, Statement statement,
				SimpleFeatureBuilder builder, Envelope bbox) {
			this.index=index;
			this.queue = queue;
			this.session = session;
			this.statement = statement;
			this.builder = builder;
			this.bbox = bbox;
		}

		@Override
		public Integer call() throws Exception {
			//System.out.println(statement);
			ResultSet rs = session.execute(statement);
			int count=0;
			for (Row row : rs) {
				buffer = row.getBytes("the_geom");
				UUID fid = row.getUUID("fid");
				long time = row.getLong("timestamp");
				
				try {
					geometry = reader.read(buffer.array());
					
					
					if (!bbox.intersects(geometry.getEnvelopeInternal())) {
						continue;
					}
				} catch (Exception ex) {
					ex.printStackTrace();
				}

				builder.set("the_geom", geometry);
				SimpleFeature feature = builder.buildFeature(fid.toString());
				count++;

			}
			featureCache2.put(index,count);
			//System.out.println(index+":"+count);
			return null;
		}

	}

	private static AsyncFunction<ResultSet, ResultSet> iterate(final int page, final long t0,
			final SimpleFeatureBuilder builder, LoadingCache<String, SimpleFeature> featureCache) {
		return new AsyncFunction<ResultSet, ResultSet>() {
			@Override
			public ListenableFuture<ResultSet> apply(ResultSet rs) throws Exception {
				// How far we can go without triggering the blocking fetch:
				int remainingInPage = rs.getAvailableWithoutFetching();
				// System.out.printf("Starting page %d (%d rows)%n", page,
				// remainingInPage);
				ByteBuffer buffer;
				Geometry geometry;
				WKBReader reader = new WKBReader();
				SimpleFeature feature;
				String osm_id;
				for (Row row : rs) {
					buffer = row.getBytes("the_geom");
					osm_id = row.getString("osm_id");
					geometry = reader.read(buffer.array());
					builder.set("the_geom", geometry);
					feature = builder.buildFeature(osm_id);
					featureCache.put(osm_id, feature);
					if (--remainingInPage == 0)
						break;
				}

				// System.out.printf("Done page %d%n", page);

				boolean wasLastPage = rs.getExecutionInfo().getPagingState() == null;
				if (wasLastPage) {
					System.out.println(" Done iterating");
					System.out.println("[" + featureCache.size() + "]");
					System.out.println(System.currentTimeMillis() - t0);
					return Futures.immediateFuture(rs);
				} else {
					ListenableFuture<ResultSet> future = rs.fetchMoreResults();
					return Futures.transformAsync(future, iterate(page + 1, t0, builder, featureCache));
				}
			}
		};
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
	
	public void testEnvelope(){
		Envelope bbox = new ReferencedEnvelope(-121.32, -121.33, 32.49, 32.50, DefaultGeographicCRS.WGS84);
		StringBuilder sb = new StringBuilder();
		sb.append(bbox.getMinY() + ":" + bbox.getMinX() + ",");
		sb.append(bbox.getMinY() + ":" + bbox.getMaxX() + ",");
		sb.append(bbox.getMaxY() + ":" + bbox.getMaxX() + ",");
		sb.append(bbox.getMaxY() + ":" + bbox.getMinX() + ";");
		S2Polygon a = makePolygon(sb.toString());
		
		
		
	}

	public static void main(String[] args) throws Exception {
		// new CassandraQuery().query3();
		// SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");
		// long s = formatter.parse("2016122600").getTime();
		// long e = formatter.parse("2016122800").getTime();
		// System.out.println(s + "," + e);
		WKTReader reader = new WKTReader();
		Polygon polygon = (Polygon) reader
				.read("POLYGON((-122.824 48.100, -122.824 46.902, -121.869 46.902, -121.869 48.100, -122.824 48.100))");
		CassandraQuery query=new CassandraQuery();
		//query.query3();
		query.query3();
		
	}

}
