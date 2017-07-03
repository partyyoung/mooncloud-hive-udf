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

import net.mooncloud.hadoop.hive.ql.util.CommonSubsequence;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * GenericUDFLongestCommonSubsequence.
 * 
 * @author yangjd
 *
 */
@Description(name = "lcs_subsequence", value = "_FUNC_(str1, str2) - Calculates the Longest Common Subsequence for the two strings.", extended = "The value is returned as a string, or NULL if the argument was NULL.\n"
		+ "Example:\n" + "  > SELECT _FUNC_('BAAABABC', 'BABACACC');\n" + "  ['ABA', 'B', 'ABA', 'C'], the first element is the Longest Common Substring, and the remaining elements is the Longest Common Subsequence")
public class GenericUDFLongestCommonSubsequence extends GenericUDF {
	private transient ObjectInspectorConverters.Converter[] converters;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		if (arguments.length != 2) {
			throw new UDFArgumentLengthException(
					"The function mostCommonSubsequence(str1, str2) takes exactly 2 arguments.");
		}

		converters = new ObjectInspectorConverters.Converter[arguments.length];
		for (int i = 0; i < arguments.length; i++) {
			converters[i] = ObjectInspectorConverters
					.getConverter(
							arguments[i],
							PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		}

		return ObjectInspectorFactory
				.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		assert (arguments.length == 2);

		if (arguments[0].get() == null || arguments[1].get() == null) {
			return null;
		}

		Text str1 = (Text) converters[0].convert(arguments[0].get());
		Text str2 = (Text) converters[1].convert(arguments[1].get());

		return CommonSubsequence.LongestCommonSubsequence(str1.toString(),
				str2.toString());
	}

	@Override
	public String getDisplayString(String[] children) {
		assert (children.length == 2);
		return getStandardDisplayString("mostCommonSubsequence", children);
	}

}