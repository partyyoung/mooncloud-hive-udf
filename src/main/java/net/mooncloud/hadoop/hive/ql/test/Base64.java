package net.mooncloud.hadoop.hive.ql.test;

import net.mooncloud.hadoop.hive.ql.udf.UDFBase64;
import net.mooncloud.hadoop.hive.ql.udf.UDFUnbase64;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class Base64 {

	public static void main(String[] args) throws Exception {
		Text alphabet = new Text(
				"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz01234#6789+/");
		UDFBase64 base64 = new UDFBase64();
		BytesWritable b = new BytesWritable("杨建党".getBytes());
		Text base64Text = base64.evaluate(b, alphabet);
		System.out.println(base64Text);
		UDFUnbase64 unbase64 = new UDFUnbase64();
		System.out.println(new String(unbase64.evaluate(base64Text, alphabet).getBytes()));
	}
}
