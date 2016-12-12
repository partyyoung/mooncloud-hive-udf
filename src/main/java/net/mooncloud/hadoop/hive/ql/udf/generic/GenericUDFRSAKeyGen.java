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

import java.util.ArrayList;
import java.util.Map;

import net.mooncloud.hadoop.hive.ql.util.RSAUtils;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;

/**
 * UDFRSAKeyGen.
 *
 */
@Description(name = "rsa_keygen", value = "_FUNC_() - RSAKeyGen(0-PUBLIC_KEY/1-PRIVATE_KEY)")
public class GenericUDFRSAKeyGen extends GenericUDF {
	BytesWritable PRIVATE_KEY;
	BytesWritable PUBLIC_KEY;

	ArrayList<BytesWritable> keyArrayList = new ArrayList<BytesWritable>(2);

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
		return ObjectInspectorFactory
				.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		Map<String, Object> keyMap;
		try {
			keyMap = RSAUtils.genKeyPair(512);
			PRIVATE_KEY = new BytesWritable(RSAUtils.getPrivateKey(keyMap));
			PUBLIC_KEY = new BytesWritable(RSAUtils.getPublicKey(keyMap));
			keyArrayList.add(PUBLIC_KEY);
			keyArrayList.add(PRIVATE_KEY);
			return keyArrayList;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public String getDisplayString(String[] children) {
		return getStandardDisplayString("rsa_keygen", children);
	}
}
