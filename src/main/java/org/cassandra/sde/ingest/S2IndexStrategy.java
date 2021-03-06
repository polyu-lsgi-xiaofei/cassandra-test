package org.cassandra.sde.ingest;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.geometry.S2Cell;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Loop;
import com.google.common.geometry.S2Point;
import com.google.common.geometry.S2Polygon;
import com.google.common.geometry.S2RegionCoverer;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

public class S2IndexStrategy {

	public final static int QUAD_LEVEL = 9;
	String tableName;
	S2RegionCoverer coverer;
	SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH");

	public S2IndexStrategy(String tableName) {
		this.tableName = tableName;
		this.coverer = new S2RegionCoverer();
	}

	public S2IndexStrategy() {
		this.coverer = new S2RegionCoverer();
	}

	// Level 1-10
	public Map<String, Object> index(Geometry geom, long timestamp) {
		Map<String, Object> index = new HashMap<>();
		S2CellId id = null;
		if (geom instanceof Point) {
			Point point = (Point) geom;
			double x = point.getX();
			double y = point.getY();
			id = new S2Cell(S2LatLng.fromDegrees(y, x)).id();
		} else {
			Envelope envelope = geom.getEnvelopeInternal();
			StringBuilder sb = new StringBuilder();
			sb.append(envelope.getMinY() + ":" + envelope.getMinX() + ",");
			sb.append(envelope.getMinY() + ":" + envelope.getMaxX() + ",");
			sb.append(envelope.getMaxY() + ":" + envelope.getMaxX() + ",");
			sb.append(envelope.getMaxY() + ":" + envelope.getMinX() + ";");
			S2Polygon a = makePolygon(sb.toString());
			ArrayList<S2CellId> covering = new ArrayList<>();
			coverer.setMaxCells(1);
			coverer.getCovering(a, covering);
			id = covering.get(0);
		}

		Date date = new Date(timestamp);
		String year_month = new SimpleDateFormat("yyyyMM").format(date);
		index.put("epoch", year_month);
		if (id.level() <= QUAD_LEVEL) {
			index.put("quad_id", id.toToken());
		} else {
			index.put("quad_id", id.parent(QUAD_LEVEL).toToken());
		}
		index.put("cell_id", id.toToken());
		index.put("timestamp", date.getTime());
		index.put("fid", UUID.randomUUID().toString());
		return index;
	}

	public Map<String, Object> index(double x, double y, long timestamp) {

		return null;
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

	private S2Loop makeLoop(String str) {
		List<S2Point> vertices = Lists.newArrayList();
		parseVertices(str, vertices);
		return new S2Loop(vertices);
	}

}
