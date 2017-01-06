package net.mooncloud.hadoop.hive.ql.udf;

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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

@Description(name = "mc_multiply", value = "_FUNC_('a,b[, ...]') - multiply the value of list")
public class UDFMultiply extends UDF {
	private final transient Text result = new Text();

	public Text evaluate(Text b) {
		if (b == null) {
			return null;
		}
		double mul = 1.0;
		String ss[] = b.toString().split(",");
		for (String s : ss) {
			mul = mul * Double.parseDouble(s.trim());
		}
		result.set(String.valueOf(mul));
		return result;
	}
}
