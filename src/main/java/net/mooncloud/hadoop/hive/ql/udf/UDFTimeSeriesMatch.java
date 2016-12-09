package net.mooncloud.hadoop.hive.ql.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.mooncloud.hadoop.hive.ql.util.TimeSeriesMatcher;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFTimeSeriesMatch.
 *
 */
@Description(name = "time_series_match", value = "_FUNC_(time_series_str_array, time_series_str_array[, threshold[, threshold]]) - Returns the first occurrence "
		+ " of str in str_array where str_array is a comma-delimited string."
		+ " Returns null if either argument is null."
		+ " Returns 0 if the first argument has any commas.", extended = "Example:\n"
		+ "  > SELECT _FUNC_('ab','abc,b,ab,c,def') FROM src LIMIT 1;\n"
		+ "  3\n"
		+ "  > SELECT * FROM src1 WHERE NOT _FUNC_(key,'311,128,345,956')=0;\n"
		+ "  311  val_311\n" + "  128"

)
public class UDFTimeSeriesMatch extends UDF {
	private final IntWritable result = new IntWritable();

	public IntWritable evaluate(Text timearray1, Text timearray2) {
		if (timearray1 == null || timearray2 == null) {
			return null;
		}
		List<Long> a = new ArrayList<Long>();
		List<Long> b = new ArrayList<Long>();
		if (timearray1.toString().trim().length() == 0)
			return null;
		String[] timearrays1 = timearray1.toString().split(",");
		if (timearrays1.length == 0)
			return null;
		if (timearray2.toString().trim().length() == 0)
			return null;
		String[] timearrays2 = timearray2.toString().split(",");
		if (timearrays2.length == 0)
			return null;
		for (String ts : timearrays1) {
			try {
				a.add(Long.valueOf(ts));
			} catch (Exception e) {
				return null;
			}
		}
		for (String ts : timearrays2) {
			try {
				b.add(Long.valueOf(ts));
			} catch (Exception e) {
				return null;
			}
		}
		result.set(TimeSeriesMatcher.match(a, b, 0));
		return result;
	}

	public IntWritable evaluate(Text timearray1, Text timearray2,
			LongWritable threshold) {
		return evaluate(timearray1, timearray2, threshold, threshold);
	}

	public IntWritable evaluate(Text timearray1, Text timearray2,
			LongWritable threshold1, LongWritable threshold2) {
		if (timearray1 == null || timearray2 == null) {
			return null;
		}
		List<Long> a = new ArrayList<Long>();
		List<Long> b = new ArrayList<Long>();
		if (timearray1.toString().trim().length() == 0)
			return null;
		String[] timearrays1 = timearray1.toString().split(",");
		if (timearrays1.length == 0)
			return null;
		if (timearray2.toString().trim().length() == 0)
			return null;
		String[] timearrays2 = timearray2.toString().split(",");
		if (timearrays2.length == 0)
			return null;
		for (String ts : timearrays1) {
			try {
				a.add(Long.valueOf(ts));
			} catch (Exception e) {
				return null;
			}
		}
		for (String ts : timearrays2) {
			try {
				b.add(Long.valueOf(ts));
			} catch (Exception e) {
				return null;
			}
		}
		result.set(TimeSeriesMatcher.match(a, b, threshold1.get(),
				threshold2.get()));
		return result;
	}

	public static void main(String[] args) throws IOException {
		System.out.println(new UDFTimeSeriesMatch().evaluate(new Text("1,2,3"), new Text("1,2,3")));
	}
}
