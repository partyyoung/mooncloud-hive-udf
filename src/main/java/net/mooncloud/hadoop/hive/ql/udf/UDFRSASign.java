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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * UDFRSASign.
 *
 */
@Description(name = "rsa_sign", value = "_FUNC_(text, privateKey) - 用私钥对信息生成数字签名.", extended = ".\n"
		+ "Example:\n"
		+ "  > SELECT base64(_FUNC_('Tom', 'MIIBVQIBADANBgkqhkiG9w0BAQEFAASCAT8wggE7AgEAAkEAhQPsLLfR6zKblOfhMhTIlnhsnc2iooANY34y12VvUkQOyMZQ/Hm2xS9CyLJ5pbUVmqPwokYxSCzRl+cotnt8sQIDAQABAkBqfod6DfndKnlxsGyV4hnujp+3f8mz/H27qHAgD7Ae1f8Hs60ZB5ajE7CpDvWCdLcVyq73R98htg3Co565bfp5AiEA2L00M/yHh5AQac637MpTeqPfWMnCEYRWYXp6+CuUkjMCIQCdHDbh2nZeCF5xAvU4OCOp/5wai0SeBnWNxBdvY3J5iwIgLBPOWgQxS9BwhhQUM4OyFm7dLSFa5lUTfB98gpvaSyECIQCD5ef9fNba4tPGtOECLb9jPQDlF/6nXGzcc7/o9+hnOQIhAK0duA+LK7tz+toPDxGZ1s/MiYLcwOTWBlPZV8CeDQlC'));\n"
		+ "  'f8uNVRaoVMxhTC7d0TdTVlIvWXDX4xAsVSK0OqHzhqN1kNN7+paRNR/q2dC0OZN6BPl1WeVrPLoEZLT8jo9s3Q=='\n"
		+ "  > SELECT base64(_FUNC_(binary('Tom'), unbase64('MIIBVQIBADANBgkqhkiG9w0BAQEFAASCAT8wggE7AgEAAkEAhQPsLLfR6zKblOfhMhTIlnhsnc2iooANY34y12VvUkQOyMZQ/Hm2xS9CyLJ5pbUVmqPwokYxSCzRl+cotnt8sQIDAQABAkBqfod6DfndKnlxsGyV4hnujp+3f8mz/H27qHAgD7Ae1f8Hs60ZB5ajE7CpDvWCdLcVyq73R98htg3Co565bfp5AiEA2L00M/yHh5AQac637MpTeqPfWMnCEYRWYXp6+CuUkjMCIQCdHDbh2nZeCF5xAvU4OCOp/5wai0SeBnWNxBdvY3J5iwIgLBPOWgQxS9BwhhQUM4OyFm7dLSFa5lUTfB98gpvaSyECIQCD5ef9fNba4tPGtOECLb9jPQDlF/6nXGzcc7/o9+hnOQIhAK0duA+LK7tz+toPDxGZ1s/MiYLcwOTWBlPZV8CeDQlC')));\n"
		+ "  'f8uNVRaoVMxhTC7d0TdTVlIvWXDX4xAsVSK0OqHzhqN1kNN7+paRNR/q2dC0OZN6BPl1WeVrPLoEZLT8jo9s3Q=='")
public class UDFRSASign extends UDF {

	private BytesWritable result = null;

	public BytesWritable evaluate(Text n, Text privateKey) {
		if (n == null || privateKey == null) {
			return null;
		}

		try {
			byte[] bytes = new byte[privateKey.getLength()];
			System.arraycopy(privateKey.getBytes(), 0, bytes, 0,
					privateKey.getLength());
			byte[] decoded = Base64.decodeBase64(bytes);

			result = new BytesWritable(RSA.sign(n.getBytes(), decoded));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public BytesWritable evaluate(BytesWritable b, BytesWritable privateKey) {
		if (b == null || privateKey == null) {
			return null;
		}

		try {
			result = new BytesWritable(RSA.sign(b.getBytes(),
					privateKey.getBytes()));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
}
