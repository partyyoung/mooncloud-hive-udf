package net.mooncloud.hadoop.hive.ql.util;

import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.utils.Charsets;

public class AES {
	static final String algorithmStr = "AES/ECB/PKCS5Padding";
	private static KeyGenerator keyGen;
	private static Cipher cipher;
	static boolean isInited = false;

	static {
		init();
	}

	private static void init() {
		try {
			keyGen = KeyGenerator.getInstance("AES");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		keyGen.init(128);
		try {
			cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			e.printStackTrace();
		}
		isInited = true;
	}

	public static byte[] GenKey() {
		if (!isInited) {
			init();
		}
		return keyGen.generateKey().getEncoded();
	}

	public static byte[] Encrypt(byte[] content, byte[] keyBytes) {
		byte[] encryptedText = null;
		if (!isInited) {
			init();
		}
		Key key = new SecretKeySpec(keyBytes, "AES");
		try {
			cipher.init(1, key);
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		}
		try {
			encryptedText = cipher.doFinal(content);
		} catch (IllegalBlockSizeException e) {
			e.printStackTrace();
		} catch (BadPaddingException e) {
			e.printStackTrace();
		}
		return encryptedText;
	}

	public static byte[] Encrypt(byte[] content) {
		byte[] encryptedText = null;
		if (!isInited) {
			init();
		}
		Key key = new SecretKeySpec(theKeyBytes, "AES");
		try {
			cipher.init(1, key);
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		}
		try {
			encryptedText = cipher.doFinal(content);
		} catch (IllegalBlockSizeException e) {
			e.printStackTrace();
		} catch (BadPaddingException e) {
			e.printStackTrace();
		}
		return encryptedText;
	}

	public static byte[] DecryptToBytes(byte[] content, byte[] keyBytes) {
		byte[] originBytes = null;
		if (!isInited) {
			init();
		}
		Key key = new SecretKeySpec(keyBytes, "AES");
		try {
			cipher.init(2, key);
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		}
		try {
			originBytes = cipher.doFinal(content);
		} catch (IllegalBlockSizeException e) {
			e.printStackTrace();
		} catch (BadPaddingException e) {
			e.printStackTrace();
		}
		return originBytes;
	}

	public static byte[] DecryptToBytes(byte[] content) {
		byte[] originBytes = null;
		if (!isInited) {
			init();
		}
		Key key = new SecretKeySpec(theKeyBytes, "AES");
		try {
			cipher.init(2, key);
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		}
		try {
			originBytes = cipher.doFinal(content);
		} catch (IllegalBlockSizeException e) {
			e.printStackTrace();
		} catch (BadPaddingException e) {
			e.printStackTrace();
		}
		return originBytes;
	}

	public static String byte2hex(byte[] b) {
		String hs = "";
		String stmp = "";
		for (int n = 0; n < b.length; n++) {
			stmp = Integer.toHexString(b[n] & 0xFF);
			if (stmp.length() == 1) {
				hs = hs + "0" + stmp;
			} else {
				hs = hs + stmp;
			}
		}
		return hs.toUpperCase();
	}

	public static byte[] hex2byte(byte[] b) {
		if (b.length % 2 != 0) {
			throw new IllegalArgumentException("������������������");
		}
		byte[] b2 = new byte[b.length / 2];
		for (int n = 0; n < b.length; n += 2) {
			String item = new String(b, n, 2);
			b2[(n / 2)] = ((byte) Integer.parseInt(item, 16));
		}
		return b2;
	}

	public static String encodeHex(byte[] in_b) {
		char[] encodeHex = Hex.encodeHex(in_b);
		return new String(encodeHex);
	}

	public static byte[] decodeHex(String str) {
		try {
			return Hex.decodeHex(str.toCharArray());
		} catch (DecoderException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String encodeUTF8(String data, String key) {
		byte[] encrypt = Encrypt(
				new String(data.getBytes(), Charsets.UTF_8).getBytes(),
				key.getBytes());
		char[] encodeHex = Hex.encodeHex(encrypt);
		return new String(encodeHex);
	}

	public static String encodeUTF8(String data) {
		byte[] encrypt = Encrypt(new String(data.getBytes(), Charsets.UTF_8)
				.getBytes());
		char[] encodeHex = Hex.encodeHex(encrypt);
		return new String(encodeHex);
	}

	public static String decode(String data) {
		try {
			byte[] decodeHex = Hex.decodeHex(data.toCharArray());
			byte[] decryptToBytes = DecryptToBytes(decodeHex);
			return new String(decryptToBytes);
		} catch (DecoderException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String decode(String data, String key) {
		try {
			byte[] decodeHex = Hex.decodeHex(data.toCharArray());
			byte[] decryptToBytes = DecryptToBytes(decodeHex, key.getBytes());
			return new String(decryptToBytes);
		} catch (DecoderException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String encodeThis(String data, String key) {
		byte[] encrypt = Encrypt(data.getBytes(), key.getBytes());
		return byte2hex(encrypt);
	}

	public static String decodeThis(String data, String key) {
		byte[] decryptToBytes = DecryptToBytes(hex2byte(data.getBytes()),
				key.getBytes());
		return new String(decryptToBytes);
	}

	static byte[] theKeyBytes = DigestUtils
			.md5Hex(new String("AES/ECB/PKCS5Padding".getBytes(),
					Charsets.UTF_8)).substring(5, 21).getBytes();

	// .md5("AES/ECB/PKCS5Padding").substring(5, 21).getBytes();

	public static void main(String[] args) {
		String content = "0571-61091999";
		String key = "qwsazxcderfuh783";
		String encodeThis = encodeThis(content, key);
		System.out.println("���������������:" + content
				+ "=====���������������:" + key);
		System.out.println("���������������" + encodeThis);
		System.out.println("���������������" + decode(encodeThis, key));
		System.out
				.println("########################################################################");
		String encodeUTF8 = encodeUTF8(content, key);
		System.out.println("������utf-8:" + encodeUTF8);
		System.out.println(decode("cb3f066d36011008a252a97019796842", key)
				+ "======222");

		System.out.println("encode=====" + encodeUTF8("0571-61091999"));
		System.out.println(decode("57bbf8b9357a6859f49a425555508edd", key));

		String ad = "057163750571";
		String p1 = new String(Hex.encodeHex(ad.getBytes()));
		System.out.println(p1);
		try {
			String p2 = new String(Hex.decodeHex(p1.toCharArray()));
			System.out.println(p2);
		} catch (DecoderException e) {
			e.printStackTrace();
		}
		String encodeUTF82 = "123456";
		System.out.println(encodeUTF82);
	}
}
