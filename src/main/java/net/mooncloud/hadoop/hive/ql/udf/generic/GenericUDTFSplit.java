package net.mooncloud.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * GenericUDTFSplit.
 *
 */
@Description(name = "split_cols", value = "_FUNC_(str, regex, limit) - Splits str around occurances that match "
		+ "regex", extended = "Example:\n"
		+ "  > SELECT _FUNC_('oneAtwoBthreeC', '[ABC]') FROM src LIMIT 1;\n"
		+ "  [\"one\", \"two\", \"three\"]")
public class GenericUDTFSplit extends GenericUDTF {
	private transient ObjectInspectorConverters.Converter[] converters;
	private Object[] forwardObjs;
	private int limit;

	@Override
	public StructObjectInspector initialize(ObjectInspector[] argOIs)
			throws UDFArgumentException {
		int size = argOIs.length;
		if (size != 3) {
			throw new UDFArgumentException("split_cols takes at 3 arguments: "
					+ size);
		}

		converters = new ObjectInspectorConverters.Converter[size - 1];
		converters[0] = ObjectInspectorConverters.getConverter(argOIs[0],
				PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		converters[1] = ObjectInspectorConverters.getConverter(argOIs[1],
				PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		ConstantObjectInspector constOI = (ConstantObjectInspector) argOIs[2];
		limit = ((IntWritable) constOI.getWritableConstantValue()).get();

		this.forwardObjs = new Object[limit];
		final ArrayList<String> fieldNames = new ArrayList<String>(limit);
		final ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(
				limit);

		for (int i = 0; i < limit; i++) {
			fieldNames.add("c" + i);
			ObjectInspector argOI = argOIs[0];
			fieldOIs.add(argOI);
		}

		return ObjectInspectorFactory.getStandardStructObjectInspector(
				fieldNames, fieldOIs);
	}

	@Override
	public void process(Object[] arguments) throws HiveException {
		assert (arguments.length == 3);

		if (arguments[0] == null || arguments[1] == null) {
			return;
		}

		final Object[] forwardObjs = this.forwardObjs;

		Text s = (Text) converters[0].convert(arguments[0]);
		Text regex = (Text) converters[1].convert(arguments[1]);

		int i = 0;
		for (String str : s.toString().split(regex.toString(), limit)) {
			forwardObjs[i++] = new Text(str);
		}
		for (; i < limit; i++) {
			forwardObjs[i++] = null;
		}
		forward(forwardObjs);
	}

	@Override
	public void close() throws HiveException {
		this.forwardObjs = null;
	}

	public static void main(String[] args) throws Exception {
		GenericUDTFSplit udtf = new GenericUDTFSplit();
		ObjectInspector[] argOIs = new ObjectInspector[] {
				PrimitiveObjectInspectorFactory.javaStringObjectInspector,
				PrimitiveObjectInspectorFactory.javaStringObjectInspector,
				ObjectInspectorUtils.getConstantObjectInspector(
						PrimitiveObjectInspectorFactory.javaIntObjectInspector,
						5) };

		udtf.initialize(argOIs);
		udtf.process(new Object[] {"1,2,3,",",",5});
	}

}
