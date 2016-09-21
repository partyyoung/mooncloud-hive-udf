package net.mooncloud.hadoop.hive.ql.util;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;

public class CommonSubsequence {
	public static String LongestCommonSubsequence(String szStr1, String szStr2) {
		if (szStr1 == null || szStr2 == null || szStr1.length() <= 0
				|| szStr2.length() <= 0)
			return null;
		int lcs = 0;
		int[] c = new int[szStr2.length()];
		int index = 0;
		for (int i = 0; i < (szStr1.length()); i++) {
			for (int j = (szStr2.length()) - 1; j >= 0; j--) {
				if ((szStr1.charAt(i)) == (szStr2.charAt(j))) {
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
		int maxlen = (lcs < szStr2.length() ? lcs : szStr2.length());
		return szStr2.substring(index, index + maxlen);
	}

	public static Object MostCommonSubsequence(String szStr1, String szStr2) {
		if (szStr1 == null || szStr2 == null || szStr1.length() <= 0
				|| szStr2.length() <= 0)
			return null;

		int size1 = szStr1.length();
		int size2 = szStr2.length();

		int lcs = 0;
		int index1 = 0, index2 = 0;

		int[] c = new int[size2];

		int[][] R = new int[size1][size2];// size1×size2的矩阵

		for (int i = 0; i < size1; i++) {
			for (int j = size2 - 1; j >= 0; j--) {
				if ((szStr1.charAt(i)) == (szStr2.charAt(j))) {
					if (i == 0 || j == 0)
						c[j] = 1;
					else
						c[j] = c[j - 1] + 1;
					if (lcs < c[j]) {
						lcs = c[j];
						index1 = i - lcs + 1;// 最长公共子序列在szStr1开始的位置
						index2 = j - lcs + 1;// 最长公共子序列在szStr2开始的位置
					}
				} else
					c[j] = 0;
				R[i][j] = c[j];// 保存公共序列关系矩阵
			}
		}

		int maxlen = (lcs < szStr2.length() ? lcs : szStr2.length());
		// String longestCommonSubsequence = szStr2.substring(index2, index2
		// + maxlen);

		int mostCommon = lcs
				+ MostCommonSubsequence(R, 0, index1 - 1, 0, index2 - 1)
				+ MostCommonSubsequence(R, index1 + lcs + 1, size1 - 1, index2
						+ lcs + 1, size2 - 1);// 对头部和尾部求最长公共子序列

		ArrayList<Text> result = new ArrayList<Text>();

		result.add(new Text(String.valueOf(size1)));
		result.add(new Text(String.valueOf(size2)));
		result.add(new Text(String.valueOf(maxlen)));
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

	public static void main(String[] args) throws IOException {
		String a = "【民生银行】您信用卡附属卡*4841于3日19:35转账转入人民币2130.00元。民生卡手机支付，7月底前周一星巴克(非江浙沪)满60减20";
		String b = "【民生银行】您信用卡附属卡*4841于3日19:35转账转入人民币12130.00元。民生卡手机支付，7月底前周一星巴克(非江浙沪)满60减20";
		System.out.println(LongestCommonSubsequence(a, b));
		System.out.println(MostCommonSubsequence(a, b));
	}
}
