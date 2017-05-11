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

import net.mooncloud.hadoop.hive.ql.util.RSA;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFRSAVerify.
 *
 */
@Description(name = "rsa_verify", value = "_FUNC_(text, sign, publicKey) - 校验数字签名.", extended = ".\n"
		+ "Example:\n"
		+ "  > SELECT _FUNC_('Tom', 'f8uNVRaoVMxhTC7d0TdTVlIvWXDX4xAsVSK0OqHzhqN1kNN7+paRNR/q2dC0OZN6BPl1WeVrPLoEZLT8jo9s3Q==', 'MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAIUD7Cy30esym5Tn4TIUyJZ4bJ3NoqKADWN+Mtdlb1JEDsjGUPx5tsUvQsiyeaW1FZqj8KJGMUgs0ZfnKLZ7fLECAwEAAQ==');\n"
		+ "  'true'\n"
		+ "  > SELECT _FUNC_(binary('Tom'), unbase64('f8uNVRaoVMxhTC7d0TdTVlIvWXDX4xAsVSK0OqHzhqN1kNN7+paRNR/q2dC0OZN6BPl1WeVrPLoEZLT8jo9s3Q=='), unbase64('MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAIUD7Cy30esym5Tn4TIUyJZ4bJ3NoqKADWN+Mtdlb1JEDsjGUPx5tsUvQsiyeaW1FZqj8KJGMUgs0ZfnKLZ7fLECAwEAAQ=='));\n"
		+ "  'true'")
public class UDFRSAVerify extends UDF {

	private BooleanWritable result = new BooleanWritable(false);

	public BooleanWritable evaluate(Text n, Text sign, Text publicKey) {
		if (n == null || sign == null || publicKey == null) {
			return null;
		}

		try {
			byte[] publicKeybytes = new byte[publicKey.getLength()];
			System.arraycopy(publicKey.getBytes(), 0, publicKeybytes, 0,
					publicKey.getLength());
			byte[] publicKeydecoded = Base64.decodeBase64(publicKeybytes);

			byte[] signbytes = new byte[sign.getLength()];
			System.arraycopy(sign.getBytes(), 0, signbytes, 0, sign.getLength());
			byte[] signdecoded = Base64.decodeBase64(signbytes);

			result = new BooleanWritable(RSA.verify(n.getBytes(),
					publicKeydecoded, signdecoded));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public BooleanWritable evaluate(BytesWritable b, BytesWritable sign,
			BytesWritable publicKey) {
		if (b == null || sign == null || publicKey == null) {
			return null;
		}

		try {
			result = new BooleanWritable(RSA.verify(b.getBytes(),
					publicKey.getBytes(), sign.getBytes()));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
}
