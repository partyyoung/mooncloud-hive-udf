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

import net.mooncloud.hadoop.hive.ql.util.CommonSubsequence;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * UDFLongestCommonSubstring.
 * 
 * @author yangjd
 *
 */
@Description(name = "lcs_substring", value = "_FUNC_(str1, str2) - Calculates the Longest Common Substring for the two strings.", extended = "The value is returned as a string, or NULL if the argument was NULL.\n"
		+ "Example:\n" + "  > SELECT _FUNC_('BANANA', 'ATANA');\n" + "  'ANA'")
public class UDFLongestCommonSubstring extends UDF {

	/**
	 * Longest Common Substring
	 */
	public Text evaluate(Text str1, Text str2) {
		if (str1 == null || str2 == null) {
			return null;
		}

		String szStr1 = str1.toString();
		String szStr2 = str2.toString();

		return new Text(
				CommonSubsequence.LongestCommonSubstring(szStr1, szStr2));
	}
}
