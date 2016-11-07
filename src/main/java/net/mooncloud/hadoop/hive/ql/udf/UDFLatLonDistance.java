package net.mooncloud.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

/**
 * UDFLatLonDistinct.
 *
 */
@Description(name = "latlon_distance", value = "_FUNC_(lat1, lon1, lat2, lon2[, r]) - returns the distinct of (lat1, lon1) and (lat2 lon2), "
		+ "r is radius of the earth, default 6378137 metres", extended = "")
public class UDFLatLonDistance extends UDF {

	private final static DoubleWritable r = new DoubleWritable(6378137.0);

	public DoubleWritable evaluate(DoubleWritable lat1, DoubleWritable lon1,
			DoubleWritable lat2, DoubleWritable lon2) {
		return evaluate(lat1, lon1, lat2, lon2, r);
	}

	public DoubleWritable evaluate(DoubleWritable lat1, DoubleWritable lon1,
			DoubleWritable lat2, DoubleWritable lon2, DoubleWritable r) {
		return new DoubleWritable(
				2
						* r.get()
						* Math.asin(Math.sqrt(Math.pow(
								Math.sin(Math.PI / 180
										* (lat1.get() - lat2.get()) / 2), 2)
								+ Math.cos(Math.PI
										/ 180
										* lat1.get()
										* Math.cos(Math.PI / 180 * lat2.get())
										* Math.pow(
												Math.sin(Math.PI
														/ 180
														* (lon1.get() - lon2
																.get()) / 2), 2)))));
	}
}
