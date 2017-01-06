package net.mooncloud.hadoop.hive.ql.util;

import java.util.Arrays;

import org.apache.commons.codec.binary.StringUtils;

public class Base64 {

	public static final char[] CACOMMON = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
			.toCharArray();
	public static final char[] CAURL = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
			.toCharArray();
	public static char[] CA = CACOMMON;
	private static int[] IA = new int[256];
	static {
		Arrays.fill(IA, -1);
		for (int i = 0, iS = CA.length; i < iS; i++)
			IA[CA[i]] = i;
		IA['='] = 0;
	}

	public static void alphabet(char[] CA) {
		Base64.CA = CA;
		Arrays.fill(IA, -1);
		for (int i = 0, iS = CA.length; i < iS; i++)
			IA[CA[i]] = i;
		IA['='] = 0;
	}

	private final static boolean doPadding = false;

	private final static int outLength(int srclen) {
		int len = 0;
		if (doPadding) {
			len = 4 * ((srclen + 2) / 3);
		} else {
			int n = srclen % 3;
			len = 4 * (srclen / 3) + (n == 0 ? 0 : n + 1);
		}
		return len;
	}

	/**
	 * Encodes all bytes from the specified byte array into a newly-allocated
	 * byte array using the {@link Base64} encoding scheme. The returned byte
	 * array is of the length of the resulting bytes.
	 *
	 * @param src
	 *            the byte array to encode
	 * @return A newly-allocated byte array containing the resulting encoded
	 *         bytes.
	 */
	public final static byte[] encode(byte[] src) {
		int len = outLength(src.length); // dst array size
		byte[] dst = new byte[len];
		int ret = encode0(src, 0, src.length, dst);
		if (ret != dst.length)
			return Arrays.copyOf(dst, ret);
		return dst;
	}

	public final static byte[] encode(String src) {
		return encode(StringUtils.getBytesUtf8(src));
	}

	public final static String encodeToString(byte[] src) {
		return StringUtils.newStringUtf8(encode(src));
	}

	public final static String encodeToString(String src) {
		return StringUtils.newStringUtf8(encode(StringUtils.getBytesUtf8(src)));
	}

	private static int encode0(byte[] src, int off, int end, byte[] dst) {
		char[] base64 = CA;
		int sp = off;
		int slen = (end - off) / 3 * 3;
		int sl = off + slen;
		int dp = 0;
		while (sp < sl) {
			int sl0 = Math.min(sp + slen, sl);
			for (int sp0 = sp, dp0 = dp; sp0 < sl0;) {
				int bits = (src[sp0++] & 0xff) << 16 | (src[sp0++] & 0xff) << 8
						| (src[sp0++] & 0xff);
				dst[dp0++] = (byte) base64[(bits >>> 18) & 0x3f];
				dst[dp0++] = (byte) base64[(bits >>> 12) & 0x3f];
				dst[dp0++] = (byte) base64[(bits >>> 6) & 0x3f];
				dst[dp0++] = (byte) base64[bits & 0x3f];
			}
			int dlen = (sl0 - sp) / 3 * 4;
			dp += dlen;
			sp = sl0;
		}
		if (sp < end) { // 1 or 2 leftover bytes
			int b0 = src[sp++] & 0xff;
			dst[dp++] = (byte) base64[b0 >> 2];
			if (sp == end) {
				dst[dp++] = (byte) base64[(b0 << 4) & 0x3f];
				if (doPadding) {
					dst[dp++] = '=';
					dst[dp++] = '=';
				}
			} else {
				int b1 = src[sp++] & 0xff;
				dst[dp++] = (byte) base64[(b0 << 4) & 0x3f | (b1 >> 4)];
				dst[dp++] = (byte) base64[(b1 << 2) & 0x3f];
				if (doPadding) {
					dst[dp++] = '=';
				}
			}
		}
		return dp;
	}

	/**
	 * Decodes a BASE64 encoded char array that is known to be resonably well
	 * formatted. The method is about twice as fast as #decode(char[]). The
	 * preconditions are:<br>
	 * + The array must have a line length of 76 chars OR no line separators at
	 * all (one line).<br>
	 * + Line separator must be "\r\n", as specified in RFC 2045 + The array
	 * must not contain illegal characters within the encoded string<br>
	 * + The array CAN have illegal characters at the beginning and end, those
	 * will be dealt with appropriately.<br>
	 * 
	 * @param chars
	 *            The source array. Length 0 will return an empty array.
	 *            <code>null</code> will throw an exception.
	 * @return The decoded array of bytes. May be of length 0.
	 */
	private final static byte[] decodeFast(char[] chars, int offset,
			int charsLen) {
		// Check special case
		if (charsLen == 0) {
			return new byte[0];
		}

		int sIx = offset, eIx = offset + charsLen - 1; // Start and end index
														// after trimming.

		// Trim illegal chars from start
		while (sIx < eIx && IA[chars[sIx]] < 0)
			sIx++;

		// Trim illegal chars from end
		while (eIx > 0 && IA[chars[eIx]] < 0)
			eIx--;

		// get the padding count (=) (0, 1 or 2)
		int pad = chars[eIx] == '=' ? (chars[eIx - 1] == '=' ? 2 : 1) : 0; // Count
																			// '='
																			// at
																			// end.
		int cCnt = eIx - sIx + 1; // Content count including possible separators
		int sepCnt = charsLen > 76 ? (chars[76] == '\r' ? cCnt / 78 : 0) << 1
				: 0;

		int len = ((cCnt - sepCnt) * 6 >> 3) - pad; // The number of decoded
													// bytes
		byte[] bytes = new byte[len]; // Preallocate byte[] of exact length

		// Decode all but the last 0 - 2 bytes.
		int d = 0;
		for (int cc = 0, eLen = (len / 3) * 3; d < eLen;) {
			// Assemble three bytes into an int from four "valid" characters.
			int i = IA[chars[sIx++]] << 18 | IA[chars[sIx++]] << 12
					| IA[chars[sIx++]] << 6 | IA[chars[sIx++]];

			// Add the bytes
			bytes[d++] = (byte) (i >> 16);
			bytes[d++] = (byte) (i >> 8);
			bytes[d++] = (byte) i;

			// If line separator, jump over it.
			if (sepCnt > 0 && ++cc == 19) {
				sIx += 2;
				cc = 0;
			}
		}

		if (d < len) {
			// Decode last 1-3 bytes (incl '=') into 1-3 bytes
			int i = 0;
			for (int j = 0; sIx <= eIx - pad; j++)
				i |= IA[chars[sIx++]] << (18 - j * 6);

			for (int r = 16; d < len; r -= 8)
				bytes[d++] = (byte) (i >> r);
		}

		return bytes;
	}

	private final static byte[] decodeFast(String chars, int offset,
			int charsLen) {
		// Check special case
		if (charsLen == 0) {
			return new byte[0];
		}

		int sIx = offset, eIx = offset + charsLen - 1; // Start and end index
														// after trimming.

		// Trim illegal chars from start
		while (sIx < eIx && IA[chars.charAt(sIx)] < 0)
			sIx++;

		// Trim illegal chars from end
		while (eIx > 0 && IA[chars.charAt(eIx)] < 0)
			eIx--;

		// get the padding count (=) (0, 1 or 2)
		int pad = chars.charAt(eIx) == '=' ? (chars.charAt(eIx - 1) == '=' ? 2
				: 1) : 0; // Count '=' at end.
		int cCnt = eIx - sIx + 1; // Content count including possible separators
		int sepCnt = charsLen > 76 ? (chars.charAt(76) == '\r' ? cCnt / 78 : 0) << 1
				: 0;

		int len = ((cCnt - sepCnt) * 6 >> 3) - pad; // The number of decoded
													// bytes
		byte[] bytes = new byte[len]; // Preallocate byte[] of exact length

		// Decode all but the last 0 - 2 bytes.
		int d = 0;
		for (int cc = 0, eLen = (len / 3) * 3; d < eLen;) {
			// Assemble three bytes into an int from four "valid" characters.
			int i = IA[chars.charAt(sIx++)] << 18
					| IA[chars.charAt(sIx++)] << 12
					| IA[chars.charAt(sIx++)] << 6 | IA[chars.charAt(sIx++)];

			// Add the bytes
			bytes[d++] = (byte) (i >> 16);
			bytes[d++] = (byte) (i >> 8);
			bytes[d++] = (byte) i;

			// If line separator, jump over it.
			if (sepCnt > 0 && ++cc == 19) {
				sIx += 2;
				cc = 0;
			}
		}

		if (d < len) {
			// Decode last 1-3 bytes (incl '=') into 1-3 bytes
			int i = 0;
			for (int j = 0; sIx <= eIx - pad; j++)
				i |= IA[chars.charAt(sIx++)] << (18 - j * 6);

			for (int r = 16; d < len; r -= 8)
				bytes[d++] = (byte) (i >> r);
		}

		return bytes;
	}

	/**
	 * Decodes a BASE64 encoded string that is known to be resonably well
	 * formatted. The method is about twice as fast as decode(String). The
	 * preconditions are:<br>
	 * + The array must have a line length of 76 chars OR no line separators at
	 * all (one line).<br>
	 * + Line separator must be "\r\n", as specified in RFC 2045 + The array
	 * must not contain illegal characters within the encoded string<br>
	 * + The array CAN have illegal characters at the beginning and end, those
	 * will be dealt with appropriately.<br>
	 * 
	 * @param s
	 *            The source string. Length 0 will return an empty array.
	 *            <code>null</code> will throw an exception.
	 * @return The decoded array of bytes. May be of length 0.
	 */
	private final static byte[] decodeFast(String s) {
		// Check special case
		int sLen = s.length();
		if (sLen == 0) {
			return new byte[0];
		}

		int sIx = 0, eIx = sLen - 1; // Start and end index after trimming.

		// Trim illegal chars from start
		while (sIx < eIx && IA[s.charAt(sIx) & 0xff] < 0)
			sIx++;

		// Trim illegal chars from end
		while (eIx > 0 && IA[s.charAt(eIx) & 0xff] < 0)
			eIx--;

		// get the padding count (=) (0, 1 or 2)
		int pad = s.charAt(eIx) == '=' ? (s.charAt(eIx - 1) == '=' ? 2 : 1) : 0; // Count
																					// '='
																					// at
																					// end.
		int cCnt = eIx - sIx + 1; // Content count including possible separators
		int sepCnt = sLen > 76 ? (s.charAt(76) == '\r' ? cCnt / 78 : 0) << 1
				: 0;

		int len = ((cCnt - sepCnt) * 6 >> 3) - pad; // The number of decoded
													// bytes
		byte[] dArr = new byte[len]; // Preallocate byte[] of exact length

		// Decode all but the last 0 - 2 bytes.
		int d = 0;
		for (int cc = 0, eLen = (len / 3) * 3; d < eLen;) {
			// Assemble three bytes into an int from four "valid" characters.
			int i = IA[s.charAt(sIx++)] << 18 | IA[s.charAt(sIx++)] << 12
					| IA[s.charAt(sIx++)] << 6 | IA[s.charAt(sIx++)];

			// Add the bytes
			dArr[d++] = (byte) (i >> 16);
			dArr[d++] = (byte) (i >> 8);
			dArr[d++] = (byte) i;

			// If line separator, jump over it.
			if (sepCnt > 0 && ++cc == 19) {
				sIx += 2;
				cc = 0;
			}
		}

		if (d < len) {
			// Decode last 1-3 bytes (incl '=') into 1-3 bytes
			int i = 0;
			for (int j = 0; sIx <= eIx - pad; j++)
				i |= IA[s.charAt(sIx++)] << (18 - j * 6);

			for (int r = 16; d < len; r -= 8)
				dArr[d++] = (byte) (i >> r);
		}

		return dArr;
	}

	public final static byte[] decode(byte[] s) {
		return decode(StringUtils.newStringUtf8(s));
	}

	public final static byte[] decode(String s) {
		return decodeFast(s);
	}

	public final static String decodeToString(byte[] s) {
		return StringUtils.newStringUtf8(decode(StringUtils.newStringUtf8(s)));
	}

	public final static String decodeToString(String s) {
		return StringUtils.newStringUtf8(decode(s));
	}
}
