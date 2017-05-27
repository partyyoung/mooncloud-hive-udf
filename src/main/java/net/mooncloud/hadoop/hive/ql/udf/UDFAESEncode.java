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

package net.mooncloud.hadoop.hive.ql.udf;

import net.mooncloud.hadoop.hive.ql.util.AES;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * UDFAESEncode.
 *
 */
@Description(name = "aes_encode", value = "_FUNC_(text[, key]) - 加密.", extended = ".\n"
		+ "Example:\n"
		+ "  > SELECT _FUNC_('Tom', 'f8uNVRaoVMxhTC7d0TdTVlIvWXDX4xAsVSK0OqHzhqN1kNN7+paRNR/q2dC0OZN6BPl1WeVrPLoEZLT8jo9s3Q==');")
public class UDFAESEncode extends UDF {

	private Text result = new Text();

	public Text evaluate(Text text, Text key) {
		if (text == null || key == null) {
			return null;
		}

		try {
			result.set(AES.encodeUTF8(text.toString(), key.toString()));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public Text evaluate(Text text) {
		if (text == null) {
			return null;
		}

		try {
			result.set(AES.encodeUTF8(text.toString()));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
}
