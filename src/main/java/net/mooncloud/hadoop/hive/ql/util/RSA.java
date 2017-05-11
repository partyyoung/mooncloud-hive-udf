package net.mooncloud.hadoop.hive.ql.util;

import java.io.ByteArrayOutputStream;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;

import org.apache.commons.codec.binary.StringUtils;

/** */
/**
 * <p>
 * RSA公钥/私钥/签名工具包
 * </p>
 * <p>
 * 罗纳德·李维斯特（Ron [R]ivest）、阿迪·萨莫尔（Adi [S]hamir）和伦纳德·阿德曼（Leonard [A]dleman）
 * </p>
 * <p>
 * 字符串格式的密钥在未在特殊说明情况下都为BASE64编码格式<br/>
 * 由于非对称加密速度极其缓慢，一般文件不使用它来加密而是使用对称加密，<br/>
 * 非对称加密算法可以用来对对称加密的密钥加密，这样保证密钥的安全也就保证了数据的安全
 * </p>
 * 
 * @author IceWee
 * @date 2012-4-26
 * @version 1.0
 */
public class RSA {

	/** */
	/**
	 * 加密算法RSA
	 */
	public static final String KEY_ALGORITHM = "RSA";

	/** */
	/**
	 * 签名算法
	 */
	public static final String SIGNATURE_ALGORITHM = "MD5withRSA";

	/** */
	/**
	 * 获取公钥的key
	 */
	private static final String PUBLIC_KEY = "RSAPublicKey";

	/** */
	/**
	 * 获取私钥的key
	 */
	private static final String PRIVATE_KEY = "RSAPrivateKey";

	/** */
	/**
	 * RSA最大加密明文大小
	 */
	private static final int MAX_ENCRYPT_BLOCK = 53;// 117;

	/** */
	/**
	 * RSA最大解密密文大小
	 */
	private static final int MAX_DECRYPT_BLOCK = 64;// 128;

	/** */
	/**
	 * <p>
	 * 生成密钥对(公钥和私钥)
	 * </p>
	 * 
	 * @return
	 * @throws Exception
	 */
	public static Map<String, Object> genKeyPair(int keysize) throws Exception {
		/** RSA算法要求有一个可信任的随机数源 */
		SecureRandom sr = new SecureRandom();
		/** 为RSA算法创建一个KeyPairGenerator对象 */
		KeyPairGenerator keyPairGen = KeyPairGenerator
				.getInstance(KEY_ALGORITHM);
		/** 利用上面的随机数据源初始化这个KeyPairGenerator对象 */
		keyPairGen.initialize(keysize, sr);// 1024
		/** 生成密匙对 */
		KeyPair keyPair = keyPairGen.generateKeyPair();
		/** 得到公钥 */
		RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
		/** 得到私钥 */
		RSAPrivateKey privateKey = (RSAPrivateKey) keyPair.getPrivate();
		Map<String, Object> keyMap = new HashMap<String, Object>(2);
		keyMap.put(PUBLIC_KEY, publicKey);
		keyMap.put(PRIVATE_KEY, privateKey);
		return keyMap;
	}

	/** */
	/**
	 * <p>
	 * 用私钥对信息生成数字签名
	 * </p>
	 * 
	 * @param data
	 *            已加密数据
	 * @param privateKey
	 *            私钥(BASE64编码)
	 * 
	 * @return
	 * @throws Exception
	 */
	public static byte[] sign(byte[] data, byte[] privateKey) throws Exception {
		byte[] keyBytes = privateKey;
		PKCS8EncodedKeySpec pkcs8KeySpec = new PKCS8EncodedKeySpec(keyBytes);
		KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
		PrivateKey privateK = keyFactory.generatePrivate(pkcs8KeySpec);
		Signature signature = Signature.getInstance(SIGNATURE_ALGORITHM);
		signature.initSign(privateK);
		signature.update(data);
		return signature.sign();
	}

	/** */
	/**
	 * <p>
	 * 校验数字签名
	 * </p>
	 * 
	 * @param data
	 *            已加密数据
	 * @param publicKey
	 *            公钥(BASE64编码)
	 * @param sign
	 *            数字签名
	 * 
	 * @return
	 * @throws Exception
	 * 
	 */
	public static boolean verify(byte[] data, byte[] publicKey, byte[] sign)
			throws Exception {
		byte[] keyBytes = publicKey;
		X509EncodedKeySpec keySpec = new X509EncodedKeySpec(keyBytes);
		KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
		PublicKey publicK = keyFactory.generatePublic(keySpec);
		Signature signature = Signature.getInstance(SIGNATURE_ALGORITHM);
		signature.initVerify(publicK);
		signature.update(data);
		return signature.verify(sign);
	}

	/** */
	/**
	 * <P>
	 * 私钥解密
	 * </p>
	 * 
	 * @param encryptedData
	 *            已加密数据
	 * @param privateKey
	 *            私钥(BASE64编码)
	 * @return
	 * @throws Exception
	 */
	public static byte[] decryptByPrivateKey(byte[] encryptedData,
			byte[] privateKey) throws Exception {
		byte[] keyBytes = privateKey;
		PKCS8EncodedKeySpec pkcs8KeySpec = new PKCS8EncodedKeySpec(keyBytes);
		KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
		Key privateK = keyFactory.generatePrivate(pkcs8KeySpec);
		Cipher cipher = Cipher.getInstance(keyFactory.getAlgorithm());
		cipher.init(Cipher.DECRYPT_MODE, privateK);
		int inputLen = encryptedData.length;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		int offSet = 0;
		byte[] cache;
		int i = 0;
		// 对数据分段解密
		while (inputLen - offSet > 0) {
			if (inputLen - offSet > MAX_DECRYPT_BLOCK) {
				cache = cipher
						.doFinal(encryptedData, offSet, MAX_DECRYPT_BLOCK);
			} else {
				cache = cipher
						.doFinal(encryptedData, offSet, inputLen - offSet);
			}
			out.write(cache, 0, cache.length);
			i++;
			offSet = i * MAX_DECRYPT_BLOCK;
		}
		byte[] decryptedData = out.toByteArray();
		out.close();
		return decryptedData;
	}

	/** */
	/**
	 * <p>
	 * 公钥解密
	 * </p>
	 * 
	 * @param encryptedData
	 *            已加密数据
	 * @param publicKey
	 *            公钥(BASE64编码)
	 * @return
	 * @throws Exception
	 */
	public static byte[] decryptByPublicKey(byte[] encryptedData,
			byte[] publicKey) throws Exception {
		byte[] keyBytes = publicKey;
		X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(keyBytes);
		KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
		Key publicK = keyFactory.generatePublic(x509KeySpec);
		Cipher cipher = Cipher.getInstance(keyFactory.getAlgorithm());
		cipher.init(Cipher.DECRYPT_MODE, publicK);
		int inputLen = encryptedData.length;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		int offSet = 0;
		byte[] cache;
		int i = 0;
		// 对数据分段解密
		while (inputLen - offSet > 0) {
			if (inputLen - offSet > MAX_DECRYPT_BLOCK) {
				cache = cipher
						.doFinal(encryptedData, offSet, MAX_DECRYPT_BLOCK);
			} else {
				cache = cipher
						.doFinal(encryptedData, offSet, inputLen - offSet);
			}
			out.write(cache, 0, cache.length);
			i++;
			offSet = i * MAX_DECRYPT_BLOCK;
		}
		byte[] decryptedData = out.toByteArray();
		out.close();
		return decryptedData;
	}

	/** */
	/**
	 * <p>
	 * 公钥加密
	 * </p>
	 * 
	 * @param data
	 *            源数据
	 * @param publicKey
	 *            公钥(BASE64编码)
	 * @return
	 * @throws Exception
	 */
	public static byte[] encryptByPublicKey(byte[] data, byte[] publicKey)
			throws Exception {
		byte[] keyBytes = publicKey;
		X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(keyBytes);
		KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
		Key publicK = keyFactory.generatePublic(x509KeySpec);
		// 对数据加密
		Cipher cipher = Cipher.getInstance(keyFactory.getAlgorithm());
		cipher.init(Cipher.ENCRYPT_MODE, publicK);
		int inputLen = data.length;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		int offSet = 0;
		byte[] cache;
		int i = 0;
		// 对数据分段加密
		while (inputLen - offSet > 0) {
			if (inputLen - offSet > MAX_ENCRYPT_BLOCK) {
				cache = cipher.doFinal(data, offSet, MAX_ENCRYPT_BLOCK);
			} else {
				cache = cipher.doFinal(data, offSet, inputLen - offSet);
			}
			out.write(cache, 0, cache.length);
			i++;
			offSet = i * MAX_ENCRYPT_BLOCK;
		}
		byte[] encryptedData = out.toByteArray();
		out.close();
		return encryptedData;
	}

	/** */
	/**
	 * <p>
	 * 私钥加密
	 * </p>
	 * 
	 * @param data
	 *            源数据
	 * @param privateKey
	 *            私钥(BASE64编码)
	 * @return
	 * @throws Exception
	 */
	public static byte[] encryptByPrivateKey(byte[] data, byte[] privateKey)
			throws Exception {
		byte[] keyBytes = privateKey;
		PKCS8EncodedKeySpec pkcs8KeySpec = new PKCS8EncodedKeySpec(keyBytes);
		KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
		Key privateK = keyFactory.generatePrivate(pkcs8KeySpec);
		Cipher cipher = Cipher.getInstance(keyFactory.getAlgorithm());
		cipher.init(Cipher.ENCRYPT_MODE, privateK);
		int inputLen = data.length;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		int offSet = 0;
		byte[] cache;
		int i = 0;
		// 对数据分段加密
		while (inputLen - offSet > 0) {
			if (inputLen - offSet > MAX_ENCRYPT_BLOCK) {
				cache = cipher.doFinal(data, offSet, MAX_ENCRYPT_BLOCK);
			} else {
				cache = cipher.doFinal(data, offSet, inputLen - offSet);
			}
			out.write(cache, 0, cache.length);
			i++;
			offSet = i * MAX_ENCRYPT_BLOCK;
		}
		byte[] encryptedData = out.toByteArray();
		out.close();
		return encryptedData;
	}

	/** */
	/**
	 * <p>
	 * 获取私钥
	 * </p>
	 * 
	 * @param keyMap
	 *            密钥对
	 * @return
	 * @throws Exception
	 */
	public static byte[] getPrivateKey(Map<String, Object> keyMap)
			throws Exception {
		Key key = (Key) keyMap.get(PRIVATE_KEY);
		return key.getEncoded();
	}

	/** */
	/**
	 * <p>
	 * 获取公钥
	 * </p>
	 * 
	 * @param keyMap
	 *            密钥对
	 * @return
	 * @throws Exception
	 */
	public static byte[] getPublicKey(Map<String, Object> keyMap)
			throws Exception {
		Key key = (Key) keyMap.get(PUBLIC_KEY);
		return key.getEncoded();
	}

	public static void main(String[] args) throws Exception {
//		Map<String, Object> keyMap = RSAUtils.genKeyPair(512);
//		byte[] PublicKey = RSAUtils.getPublicKey(keyMap);
//		byte[] PrivateKey = RSAUtils.getPrivateKey(keyMap);
		Base64.alphabet(Base64.CAURL);
		byte[] PublicKey = Base64.decode("MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBALWH4UKDn59fH9r8t1rgiAY7whkmHSJizIE2gdSgx2oVJ505GTej9qxzmY0O5V4jwzRjumyfimsGMV_CXey7_skCAwEAAQ");
		byte[] PrivateKey = Base64.decode("MIIBVAIBADANBgkqhkiG9w0BAQEFAASCAT4wggE6AgEAAkEAtYfhQoOfn18f2vy3WuCIBjvCGSYdImLMgTaB1KDHahUnnTkZN6P2rHOZjQ7lXiPDNGO6bJ-KawYxX8Jd7Lv-yQIDAQABAkAfbKtJCWILg7sO_Kp899t0QMoD8n4eor3GLHpjJYX_zei5w_jm3SFwM1WggEj_U_r9ATU5-ho0s7wYdhKh3rTRAiEA7QVi26HoSIKGFAUILgGu4E_b8pPrjx0DoTUdnilo9oUCIQDEEQSNLOU3N1Yo0MFXFcLLzxydPVXnbesT2f4PK6hEdQIhAL9VIcB6qrAd7W4rwbah1TFuRfdEQVt96HgyEH6knv6hAiBQBlYiPQnNUKO1Awd2wOYCA6W9M_SlAgMp1hbRIFOMpQIgEtRRZl4UoDJu5MEM9gAEf4AuuYfvqzhiQ5EgRt08ZZw");
		System.out.println("PublicKey = " + Base64.encodeToString(PublicKey));
		System.out.println("PrivateKey = " + Base64.encodeToString(PrivateKey));
		String plainText = "13732262669";// 5p2o5bu65YWa
		System.out.println("plainText = " + plainText);
		System.out.println("base64Text = " + Base64.encodeToString(plainText));
		System.out.println("base64Text = "
				+ Base64.decodeToString(Base64.encodeToString(StringUtils
						.getBytesUtf8(plainText))));

		System.out.println("数字签名");
		byte[] sign = RSA.sign(StringUtils.getBytesUtf8(plainText),
				PrivateKey);
		String signText = Base64.encodeToString(sign);
		System.out.println("signText = " + signText);
		boolean verify = RSA.verify(StringUtils.getBytesUtf8(plainText),
				PublicKey, sign);
		System.out.println("verify = " + verify);

//		System.out.println("公钥加密");
//		byte[] encryptedData = RSAUtils.encryptByPublicKey(
//				StringUtils.getBytesUtf8(plainText),
//				PublicKey);
//		System.out.println("encryptText = "
//				+ Base64.encodeToString(encryptedData));
		byte[] encryptedData = Base64.decode("gkj-CfbGV-LIzoR09iI_Zv6lZ7g0Af2smkxtYyXkeeo2cdANcEk0YvN7SygzB6270Vo9uz8y7F941JwpNM2BRw");
		System.out.println("私钥解密");
		byte[] decryptedData = RSA.decryptByPrivateKey(encryptedData,
				PrivateKey);
		System.out.println("plainText = "
				+ StringUtils.newStringUtf8(decryptedData));

//		System.out.println("私钥加密");
//		encryptedData = RSAUtils.encryptByPrivateKey(
//				StringUtils.getBytesUtf8(plainText),
//				PrivateKey);
//		System.out.println("encryptText = "
//				+ Base64.encodeToString(encryptedData));
//		System.out.println("公钥解密");
//		decryptedData = RSAUtils.decryptByPublicKey(encryptedData,
//				PublicKey);
//		System.out.println("plainText = "
//				+ StringUtils.newStringUtf8(decryptedData));
	}
}