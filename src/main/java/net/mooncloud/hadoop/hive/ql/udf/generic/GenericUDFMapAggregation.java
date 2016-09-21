/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.mooncloud.hadoop.hive.ql.udf.generic;

import java.util.Map;

import net.mooncloud.hadoop.hive.ql.util.CommonSubsequence;
import net.mooncloud.hadoop.hive.ql.util.MapAggregation;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * GenericUDFMapAggregation.
 * 
 * @author yangjd
 *
 */
@Description(name = "aggregate_map", value = "_FUNC_(x) - x的格式必须为map {key:[value]}, x的数据结构存储的是一个从key到value的有向关系结构. 此方法将把一棵树内的所有key进行合并, 推选一个key作为代表, 其他的key将放在value的list中. 并返回合并后的map {key:[value]}")
public class GenericUDFMapAggregation extends GenericUDF {
	private transient StandardMapObjectInspector inputOI;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {

		if (arguments.length != 1) {
			throw new UDFArgumentTypeException(arguments.length - 1,
					"Exactly one arguments are expected.");
		}

		switch (arguments[0].getCategory()) {
		case MAP:
			break;
		default:
			throw new UDFArgumentTypeException(0,
					"Only map type arguments are accepted but "
							+ arguments[0].getTypeName()
							+ " was passed as parameter 1.");
		}

		inputOI = (StandardMapObjectInspector) arguments[0];

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
		// return inputOI;
		// ObjectInspectorFactory.getStandardMapObjectInspector(
		// inputOI.getMapKeyObjectInspector(),
		// inputOI.getMapValueObjectInspector());
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		assert (arguments.length == 1);

		if (arguments[0].get() == null) {
			return null;
		}

		Map<Object, Object> partialResult = (Map<Object, Object>) inputOI
				.getMap(arguments[0].get());

		return MapAggregation.aggregateMap(partialResult);
	}

	@Override
	public String getDisplayString(String[] children) {
		assert (children.length == 1);
		return getStandardDisplayString("aggregate_map", children);
	}

}