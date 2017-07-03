package net.mooncloud.hadoop.hive.ql.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.hadoop.io.Text;

public class CommonSubsequence {
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

	public static Object MostCommonSubsequence(String str1, String str2) {
		if (str1 == null || str2 == null || str1.length() <= 0
				|| str2.length() <= 0)
			return null;

		int rowCount = str1.length();
		int colCount = str2.length();

		int lcs = 0;
		int row = 0, col = 0;

		int[] c = new int[colCount];

		int[][] R = new int[rowCount][colCount];// rowCount×colCount的矩阵

		for (int i = 0; i < rowCount; i++) {
			for (int j = colCount - 1; j >= 0; j--) {
				if ((str1.charAt(i)) == (str2.charAt(j))) {
					if (i == 0 || j == 0)
						c[j] = 1;
					else
						c[j] = c[j - 1] + 1;
					if (lcs < c[j]) {
						lcs = c[j];
						row = i - lcs + 1;// 最长公共子序列在str1开始的位置
						col = j - lcs + 1;// 最长公共子序列在str2开始的位置
					}
				} else
					c[j] = 0;
				R[i][j] = c[j];// 保存公共序列关系矩阵
			}
		}

		lcs = (lcs < str2.length() ? lcs : str2.length());
		// String longestCommonSubsequence = str2.substring(col, col
		// + maxlen);

		int mostCommon = lcs
				+ MostCommonSubsequence(R, 0, row - 1, 0, col - 1)
				+ MostCommonSubsequence(R, row + lcs + 1, rowCount - 1, col
						+ lcs + 1, colCount - 1);// 对头部和尾部求最长公共子序列

		ArrayList<Text> result = new ArrayList<Text>();

		result.add(new Text(String.valueOf(rowCount)));
		result.add(new Text(String.valueOf(colCount)));
		result.add(new Text(String.valueOf(lcs)));
		result.add(new Text(String.valueOf(mostCommon)));

		return result;
	}

	private static int MostCommonSubsequence(int[][] R, int row1, int row2,
			int col1, int col2) {
		if (row1 > row2 || col1 > col2)
			return 0;
		int max = 0;
		int row = 0, col = 0;
		for (int i = row2; i >= row1; i--) {
			for (int j = col2; j >= col1; j--)
				if (max < R[i][j]) {
					max = R[i][j];
					row = i;
					col = j;
				}
		}
		if (max == 0)
			return 0;
		return max + MostCommonSubsequence(R, row1, row - max, col1, col - max)
				+ MostCommonSubsequence(R, row + 1, row2, col + 1, col2);
	}

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

		for (int i = 0; i < rowCount; i++) {
			for (int j = colCount - 1; j >= 0; j--) {
				if ((str1.charAt(i)) == (str2.charAt(j))) {
					if (i == 0 || j == 0)
						c[j] = 1;
					else
						c[j] = c[j - 1] + 1;
					if (lcs < c[j]) {
						lcs = c[j];
						row = i - lcs + 1;// 最长公共子序列在str1开始的位置
						col = j - lcs + 1;// 最长公共子序列在str2开始的位置
					}
				} else
					c[j] = 0;
				R[i][j] = c[j];// 保存公共序列关系矩阵
			}
		}

		lcs = (lcs < str1.length() ? lcs : str1.length());
		lcs = (lcs < str2.length() ? lcs : str2.length());

		LinkedList<Text> longestCommonSubsequence = new LinkedList<Text>();
		Text longestCommonSubstring = new Text(str2.substring(col, col + lcs));
		longestCommonSubsequence.add(longestCommonSubstring);

		// 对头部求最长公共子序列
		longestCommonSubsequence
				.addAll(0,
						LongestCommonSubsequence(str1, str2, R, 0, row - 1, 0,
								col - 1));

		// 对尾部求最长公共子序列
		longestCommonSubsequence.addAll(LongestCommonSubsequence(str1, str2, R,
				row + lcs + 1, rowCount - 1, col + lcs + 1, colCount - 1));

		// 最长公共子串 LongestCommonSubstring
		longestCommonSubsequence.add(0, longestCommonSubstring);
		return longestCommonSubsequence;
	}

	private static LinkedList<Text> LongestCommonSubsequence(final String str1,
			final String str2, final int[][] R, int row1, int row2, int col1,
			int col2) {
		LinkedList<Text> longestCommonSubsequence = new LinkedList<Text>();
		if (row1 > row2 || col1 > col2)
			return longestCommonSubsequence;
		int lcs = 0;
		int row = 0, col = 0;
		for (int i = row2; i >= row1; i--) {
			for (int j = col2; j >= col1; j--)
				if (lcs < R[i][j]) {
					lcs = R[i][j];
					row = i - lcs + 1;
					col = j - lcs + 1;
				}
		}
		if (lcs == 0)
			return longestCommonSubsequence;
		longestCommonSubsequence.add(new Text(str2.substring(col, col + lcs)));
		longestCommonSubsequence.addAll(
				0,
				LongestCommonSubsequence(str1, str2, R, row1, row - 1, col1,
						col - 1));
		longestCommonSubsequence.addAll(LongestCommonSubsequence(str1, str2, R,
				row + lcs + 1, row2, col + lcs + 1, col2));
		return longestCommonSubsequence;
	}

	public static void main(String[] args) throws IOException {
		String a = "【民生银行】您信用卡附属卡*4841于3日19:35转账转入人民币2130.00元。民生卡手机支付，7月底前周一星巴克(非江浙沪)满60减20";
		String b = "【民生银行】您信用卡附属卡*4841于3日19:35转账转入人民币12130.00元。民生卡手机支付，7月底前周一星巴克(非江浙沪)满60减20";
		a = "BAAABABC";
		b = "BABACACC";
		System.out.println(LongestCommonSubstring(a, b));
		System.out.println(LongestCommonSubsequence(a, b));
	}
}
