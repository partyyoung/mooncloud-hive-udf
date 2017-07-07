package net.mooncloud.hadoop.hive.ql.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;

public class CommonSubsequence {
	private static final String[] stopwords;
	static {
		stopwords = new String[] { ",", ".", "`", "-", "_", "=", "?", "'", "|",
				"\"", "(", ")", "{", "}", "[", "]", "<", ">", "*", "#", "&",
				"^", "$", "@", "!", "~", ":", ";", "+", "/", "\\", "《", "》",
				"—", "－", "，", "。", "、", "：", "；", "！", "·", "？", "“", "”",
				"）", "（", "【", "】", "［", "］", "●" };
		Arrays.sort(stopwords);
	}

	public static String LongestCommonSubstring(String str1, String str2) {
		if (str1 == null || str2 == null || str1.length() <= 0
				|| str2.length() <= 0)
			return null;
		int lcs = 0;
		int[] c = new int[str2.length()];
		int index = 0;
		for (int i = 0; i < (str1.length()); i++) {
			for (int j = (str2.length()) - 1; j >= 0; j--) {
				if ((str1.charAt(i)) == (str2.charAt(j))) {
					if (i == 0 || j == 0)
						c[j] = 1;
					else
						c[j] = c[j - 1] + 1;
					if (lcs < c[j]) {
						lcs = c[j];
						index = j + 1 - lcs;// 定位最长序列的起点
					}
				} else
					c[j] = 0;
			}
		}
		int maxlen = (lcs < str2.length() ? lcs : str2.length());
		return str2.substring(index, index + maxlen);
	}

	/**
	 * https://en.wikipedia.org/wiki/Longest_common_subsequence_problem
	 * 
	 * @param str1
	 * @param str2
	 * @return
	 */
	public static Object LongestCommonSubsequence(final String str1,
			final String str2) {
		if (str1 == null || str2 == null || str1.length() <= 0
				|| str2.length() <= 0)
			return null;

		int rowCount = str1.length();
		int colCount = str2.length();

		int lcs = 0;
		int row = 0, col = 0;

		int[] c = new int[colCount];

		int[][] R = new int[rowCount][colCount];// rowCount×colCount的矩阵

		ArrayList<Integer> lcsRowCols = new ArrayList<Integer>(4);//

		for (int i = 0; i < rowCount; i++) {
			for (int j = colCount - 1; j >= 0; j--) {
				if ((str1.charAt(i)) == (str2.charAt(j))) {
					if (i == 0 || j == 0)
						c[j] = 1;
					else
						c[j] = c[j - 1] + 1;
					if (lcs < c[j]) { // 出现了最长子串
						lcs = c[j];
						row = i - lcs + 1;// 最长公共子串在str1开始的位置
						col = j - lcs + 1;// 最长公共子串在str2开始的位置
						lcsRowCols.clear();
						lcsRowCols.add(row); // 记录最长子串的start位置
						lcsRowCols.add(col);
					} else if (lcs > 0 && lcs == c[j]) { // 出现了两个最长子串
						row = i - lcs + 1;// 最长公共子串在str1开始的位置
						col = j - lcs + 1;// 最长公共子串在str2开始的位置
						lcsRowCols.add(row); // 记录最长公共子串的start位置
						lcsRowCols.add(col);
					}
				} else
					c[j] = 0;
				R[i][j] = c[j];// 保存公共序列关系矩阵
			}
		}

		lcs = (lcs < str1.length() ? lcs : str1.length());
		lcs = (lcs < str2.length() ? lcs : str2.length());

		LinkedList<Text> longestCommonSubsequenceFinal = null;
		for (int i = 0; i < lcsRowCols.size(); i += 2) {

			row = lcsRowCols.get(i);
			col = lcsRowCols.get(i + 1);

			LinkedList<Text> longestCommonSubsequence = new LinkedList<Text>();
			Text longestCommonSubstring = new Text(str2.substring(col, col
					+ lcs));
			longestCommonSubsequence.add(longestCommonSubstring);

			// 对头部求最长公共子序列
			longestCommonSubsequence.addAll(
					0,
					LongestCommonSubsequence(str1, str2, R, 0, row - 1, 0,
							col - 1));

			// 对尾部求最长公共子序列
			longestCommonSubsequence.addAll(LongestCommonSubsequence(str1,
					str2, R, row + lcs + 1, rowCount - 1, col + lcs + 1,
					colCount - 1));

			// 最长公共子串 LongestCommonSubstring
			// longestCommonSubsequence.add(0, longestCommonSubstring);

			if (longestCommonSubsequenceFinal == null
					|| longestCommonSubsequenceFinal.size() < longestCommonSubsequence
							.size()) {
				longestCommonSubsequenceFinal = longestCommonSubsequence;
			}
		}

		return longestCommonSubsequenceFinal != null ? longestCommonSubsequenceFinal
				: new LinkedList<Text>();
	}

	private static LinkedList<Text> LongestCommonSubsequence(final String str1,
			final String str2, final int[][] R, int row1, int row2, int col1,
			int col2) {
		LinkedList<Text> longestCommonSubsequenceFinal = new LinkedList<Text>();
		if (row1 > row2 || col1 > col2)
			return longestCommonSubsequenceFinal;
		ArrayList<Integer> lcsRowCols = new ArrayList<Integer>(4);
		int lcs = 0;
		int row = 0, col = 0;
		for (int i = row2; i >= row1; i--) {
			for (int j = col2; j >= col1; j--)
				if (lcs < R[i][j]) {
					lcs = R[i][j];
					row = i - lcs + 1;
					col = j - lcs + 1;
					row = row < row1 ? row1 : row;
					col = col < col1 ? col1 : col;
					lcsRowCols.clear();
					lcsRowCols.add(row);
					lcsRowCols.add(col);
				} else if (lcs > 0 && lcs == R[i][j]) { // 出现了两个最长子串
					row = i - lcs + 1;// 最长公共子串在str1开始的位置
					col = j - lcs + 1;// 最长公共子串在str2开始的位置
					row = row < row1 ? row1 : row;
					col = col < col1 ? col1 : col;
					lcsRowCols.add(row); // 记录最长公共子串的start位置
					lcsRowCols.add(col);
				}
		}
		if (lcs == 0)
			return longestCommonSubsequenceFinal;
		for (int i = 0; i < lcsRowCols.size(); i += 2) {
			LinkedList<Text> longestCommonSubsequence = new LinkedList<Text>();
			row = lcsRowCols.get(i);
			col = lcsRowCols.get(i + 1);
			longestCommonSubsequence.add(new Text(str2
					.substring(col, col + lcs)));
			longestCommonSubsequence.addAll(
					0,
					LongestCommonSubsequence(str1, str2, R, row1, row - 1,
							col1, col - 1));
			longestCommonSubsequence.addAll(LongestCommonSubsequence(str1,
					str2, R, row + lcs + 1, row2, col + lcs + 1, col2));
			if (longestCommonSubsequenceFinal.size() < longestCommonSubsequence
					.size()) {
				longestCommonSubsequenceFinal = longestCommonSubsequence;
			}
		}
		return longestCommonSubsequenceFinal;
	}

	/**
	 * https://en.wikipedia.org/wiki/Levenshtein_distance
	 * 
	 * @param str1
	 * @param str2
	 * @return
	 */
	public static Object LCSwithLevenshteinDistance(final String str1,
			final String str2) {

		int[][] R = LevenshteinDistanceWagnerFischerMatrix(str1, str2);
		int rowCount = R.length;
		int colCount = R[0].length;

		LinkedList<Text> longestCommonSubsequence = new LinkedList<Text>();
		int lcs = 0;
		for (int i = rowCount - 1, j = colCount - 1; i != 0 || j != 0;) {
			if (i == 0 || j == 0) {
				if (lcs > 0) {
					longestCommonSubsequence.add(0,
							new Text(str1.substring(i, i + lcs)));
					lcs = 0;
				}
			}
			if (i == 0) {
				j--;
				continue;
			}
			if (j == 0) {
				i--;
				continue;
			}

			int distance = R[i][j];
			int substitution = R[i - 1][j - 1];
			int deletion = R[i - 1][j];
			int insertion = R[i][j - 1];
			if (deletion >= substitution && insertion >= substitution
					&& substitution == distance) {
				lcs++;
				i--;
				j--;
				continue;
			} else {
				if (lcs > 0) {
					longestCommonSubsequence.add(0,
							new Text(str1.substring(i, i + lcs)));
					lcs = 0;
				}
			}
			if (deletion <= insertion) {
				if (deletion <= substitution) {
					i--;
				} else {
					i--;
					j--;
				}
			} else {
				if (insertion <= substitution) {
					j--;
				} else {
					i--;
					j--;
				}
			}
		}
		return longestCommonSubsequence;
	}

	/**
	 * https://en.wikipedia.org/wiki/Levenshtein_distance
	 * 
	 * @param str1
	 * @param str2
	 * @return
	 */
	public static Object LevenshteinDistance(final String str1,
			final String str2) {

		int[][] R = LevenshteinDistanceWagnerFischerMatrix(str1, str2);
		int rowCount = R.length;
		int colCount = R[0].length;

		int[] result = new int[4];
		result[0] = R[rowCount - 1][colCount - 1];

		// LinkedList<Text> longestCommonSubsequence = new LinkedList<Text>();
		// int lcs = 0;
		for (int i = rowCount - 1, j = colCount - 1; i != 0 || j != 0;) {
			// if (i == 0 || j == 0) {
			// if (lcs > 0) {
			// longestCommonSubsequence.add(0,
			// new Text(str1.substring(i, i + lcs)));
			// lcs = 0;
			// }
			// }
			if (i == 0) {
				result[3]++;
				j--;
				continue;
			}
			if (j == 0) {
				result[2]++;
				i--;
				continue;
			}

			int distance = R[i][j];
			int substitution = R[i - 1][j - 1];
			int deletion = R[i - 1][j];
			int insertion = R[i][j - 1];
			if (deletion >= substitution && insertion >= substitution
					&& substitution == distance) {
				// lcs++;
				i--;
				j--;
				continue;
			} else {
				// if (lcs > 0) {
				// longestCommonSubsequence.add(0,
				// new Text(str1.substring(i, i + lcs)));
				// lcs = 0;
				// }
			}
			if (deletion <= insertion) {
				if (deletion <= substitution) {
					result[2]++;
					i--;
				} else {
					result[1]++;
					i--;
					j--;
				}
			} else {
				if (insertion <= substitution) {
					result[3]++;
					j--;
				} else {
					result[1]++;
					i--;
					j--;
				}
			}
		}
		// System.out.println(longestCommonSubsequence);
		return Arrays.toString(result);
	}

	/**
	 * In computer science, the Wagner–Fischer algorithm is a dynamic
	 * programming algorithm that computes the edit distance between two strings
	 * of characters.
	 * 
	 * @param str1
	 * @param str2
	 * @return
	 */
	private static int[][] LevenshteinDistanceWagnerFischerMatrix(
			final String str1, final String str2) {
		int rowCount = 1;
		int colCount = 1;

		if (StringUtils.isNotEmpty(str1))
			rowCount = str1.length() + 1;
		if (StringUtils.isNotEmpty(str2))
			colCount = str2.length() + 1;

		int[][] R = new int[rowCount][colCount];// rowCount×colCount的矩阵

		for (int i = 1; i < rowCount; i++) {
			R[i][0] = i;
		}
		for (int j = 1; j < colCount; j++) {
			R[0][j] = j;
		}

		// System.out.println(Arrays.toString(R[0]));
		for (int i = 1; i < rowCount; i++) {
			for (int j = 1; j < colCount; j++) {
				int substitutionCost = 1;
				if (str1.charAt(i - 1) == str2.charAt(j - 1)) {
					substitutionCost = 0;
				}
				int deletion = R[i - 1][j] + 1;
				int insertion = R[i][j - 1] + 1;
				int substitution = R[i - 1][j - 1] + substitutionCost;
				R[i][j] = Math.min(substitution, Math.min(deletion, insertion));
			}
			// System.out.println(Arrays.toString(R[i]));
		}

		return R;
	}

	public static void main(String[] args) throws IOException {
		String a = "XMJYAUZ";
		String b = "MZJAWXU";
		// a = "Sunday";
		// b = "Saturday";
		a = "BANANA";
		b = "ATANATA";
		a = "sitting";
		b = "kitten";
		a = "";
		b = "kitten";
		System.out.println(LongestCommonSubstring(a, b));
		System.out.println(LongestCommonSubsequence(a, b));
		System.out.println(LevenshteinDistance(a, b));
	}
}
