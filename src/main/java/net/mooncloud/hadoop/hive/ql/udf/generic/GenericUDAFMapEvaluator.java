package net.mooncloud.hadoop.hive.ql.udf.generic;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class GenericUDAFMapEvaluator extends GenericUDAFEvaluator implements
		Serializable {

	private static final long serialVersionUID = 1l;

	// For PARTIAL1 and COMPLETE
	private transient ObjectInspector xInputOI;
	private transient ObjectInspector yInputOI;
	private transient StandardMapObjectInspector moi;

	public GenericUDAFMapEvaluator() {
	}

	@Override
	public ObjectInspector init(Mode m, ObjectInspector[] parameters)
			throws HiveException {
		super.init(m, parameters);
		// init output object inspectors

		// init input
		if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
			assert (parameters.length == 2);
			xInputOI = parameters[0];
			yInputOI = parameters[1];
		} else {
			assert (parameters.length == 1);
			moi = (StandardMapObjectInspector) parameters[0];
		}

		GenericUDFUtils.ReturnObjectInspectorResolver keyOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(
				true);
		GenericUDFUtils.ReturnObjectInspectorResolver valueOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(
				true);

		ObjectInspector keyOI = keyOIResolver
				.get(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		ObjectInspector valueOI = valueOIResolver
				.get(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		return ObjectInspectorFactory.getStandardMapObjectInspector(keyOI,
				valueOI);
	}

	class MapAggregationBuffer extends AbstractAggregationBuffer {

		private Map<Object, Object> container;

		public MapAggregationBuffer() {
			container = new LinkedHashMap<Object, Object>();
		}
	}

	@Override
	public void reset(AggregationBuffer agg) throws HiveException {
		((MapAggregationBuffer) agg).container.clear();
	}

	@Override
	public AggregationBuffer getNewAggregationBuffer() throws HiveException {
		MapAggregationBuffer ret = new MapAggregationBuffer();
		return ret;
	}

	// mapside
	@Override
	public void iterate(AggregationBuffer agg, Object[] parameters)
			throws HiveException {
		assert (parameters.length == 2);
		Object p1 = parameters[0];
		Object p2 = parameters[1];

		if (p1 != null && p2 != null) {
			MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
			myagg.container.put(
					ObjectInspectorUtils.copyToStandardObject(p1,  this.xInputOI),
					ObjectInspectorUtils.copyToStandardObject(p2,  this.yInputOI));
		}
	}

	// mapside
	@Override
	public Object terminatePartial(AggregationBuffer agg) throws HiveException {
		MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
		Map<Object, Object> ret = new LinkedHashMap<Object, Object>(
				myagg.container.size());
		ret.putAll(myagg.container);
		return ret;
	}

	@Override
	public void merge(AggregationBuffer agg, Object partial)
			throws HiveException {
		MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
		Map<Object, Object> partialResult = (Map<Object, Object>) moi
				.getMap(partial);
		if (partialResult != null) {
			myagg.container.putAll(partialResult);
		}
	}

	@Override
	public Object terminate(AggregationBuffer agg) throws HiveException {
		MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
		Map<Object, Object> ret = new LinkedHashMap<Object, Object>(
				myagg.container.size());
		ret.putAll(myagg.container);
		return ret;
	}
}
