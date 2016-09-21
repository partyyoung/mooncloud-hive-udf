package net.mooncloud.hadoop.hive.ql.udf.generic;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import net.mooncloud.hadoop.hive.ql.util.MapAggregation;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * GenericUDAFMapAggregation.
 * 
 * @author yangjd
 *
 */
@Description(name = "aggregate_map", value = "_FUNC_(x) - x的格式必须为map {key:[value]}, x的数据结构存储的是一个从key到value的有向关系结构. 此方法将把一棵树内的所有key进行合并, 推选一个key作为代表, 其他的key将放在value的list中. 并返回合并后的map {key:[value]}")
public class GenericUDAFMapAggregation extends AbstractGenericUDAFResolver {

	public GenericUDAFMapAggregation() {
	}

	@Override
	public GenericUDAFMapAggregationEvaluator getEvaluator(TypeInfo[] parameters)
			throws SemanticException {

		if (parameters.length != 1) {
			throw new UDFArgumentTypeException(parameters.length - 1,
					"Exactly one arguments are expected.");
		}

		switch (parameters[0].getCategory()) {
		case MAP:
			return new GenericUDAFMapAggregationEvaluator();
		default:
			throw new UDFArgumentTypeException(0,
					"Only map type arguments are accepted but "
							+ parameters[0].getTypeName()
							+ " was passed as parameter 1.");
		}
	}

	static class GenericUDAFMapAggregationEvaluator extends
			GenericUDAFEvaluator implements Serializable {

		private static final long serialVersionUID = 1l;

		// For PARTIAL1 and COMPLETE
		private transient StandardMapObjectInspector inputOI;

		public GenericUDAFMapAggregationEvaluator() {
		}

		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			super.init(m, parameters);
			// init output object inspectors

			// init input
			assert (parameters.length == 1);
			// if (m == Mode.PARTIAL1 || m == Mode.COMPLETE)
			// inputOI = parameters[0];
			// else
			inputOI = (StandardMapObjectInspector) parameters[0];

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
			assert (parameters.length == 1);
			Object p = parameters[0];

			if (p != null) {
				MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
				aggregateMap(p, myagg);
			}
		}

		// mapside
		@Override
		public Object terminatePartial(AggregationBuffer agg)
				throws HiveException {
			MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
			Map<Object, Object> ret = new LinkedHashMap<Object, Object>(
					myagg.container.size());
			ret.putAll(myagg.container);
			return ret;
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial)
				throws HiveException {
			// throw new UDFArgumentTypeException(0, partial.getClass() + "="
			// + partial);
			MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
			aggregateMap(partial, myagg);
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
			Map<Object, Object> ret = new LinkedHashMap<Object, Object>(
					myagg.container.size());
			ret.putAll(myagg.container);
			return ret;
		}

		private void aggregateMap(Object p, MapAggregationBuffer myagg)
				throws UDFArgumentTypeException {
			Map<Object, Object> partialResult;
			// if (p instanceof Map)
			// partialResult = (Map<Object, Object>) p;
			// else
			partialResult = (Map<Object, Object>) (inputOI).getMap(p);
			if (myagg.container.size() == 0) {
				myagg.container.putAll(partialResult);
			} else {
				try {
					myagg.container = MapAggregation.aggregateMap(
							myagg.container, partialResult);
				} catch (UDFArgumentTypeException e) {
					e.printStackTrace();
				}
			}
		}
	}
}