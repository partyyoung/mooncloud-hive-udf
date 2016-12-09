package net.mooncloud.hadoop.hive.ql.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TimeSeriesMatcher {

	public static int match(List<Long> a, List<Long> b, long threshold) {
		return match(a, b, threshold, threshold);
	}

	public static int match(List<Long> a, List<Long> b, long threshold1,
			long threshold2) {
		return match(a, 0, a.size(), b, 0, b.size(), threshold1, threshold2);
	}

	public static int match(List<Long> a, int a1, int a2, List<Long> b, int b1,
			int b2, long threshold1, long threshold2) {
		long min = threshold1 + threshold2 + 1;
		int aMid = (a1 + a2) / 2;
		int bMid = (b1 + b2) / 2;
		int aa = 0, bb = 0;
		for (int i = a1; i < a2; i++) {
			for (int j = b1; j < b2; j++) {
				long diff = b.get(j) - a.get(i);
				if (diff >= -threshold1 && diff <= threshold2
						&& Math.abs(diff) < Math.abs(min)) {
					min = diff;
					aa = i;
					bb = j;
				}
			}
		}
		if (min > threshold2 || min < -threshold1) {
			return 0;
		}
		return 1 + match(a, aa + 1, a2, b, bb + 1, b2, threshold1, threshold2)
				+ match(a, a1, aa, b, b1, bb, threshold1, threshold2);
	}

	// public static int match(List<Long> a, int a1, int a2, List<Long> b, int
	// b1,
	// int b2, long threshold1, long threshold2) {
	// long min = threshold1 + threshold2 + 1;
	// int aa = 0, bb = 0;
	// for (int i = a1; i < a2; i++) {
	// for (int j = b1; j < b2; j++) {
	// long diff = b.get(j) - a.get(i);
	// if (diff >= -threshold1 && diff <= threshold2
	// && Math.abs(diff) < Math.abs(min)) {
	// min = diff;
	// aa = i;
	// bb = j;
	// }
	// }
	// }
	// if (min > threshold2 || min < -threshold1) {
	// return 0;
	// }
	// return 1 + match(a, aa + 1, a2, b, bb + 1, b2, threshold1, threshold2)
	// + match(a, a1, aa, b, b1, bb, threshold1, threshold2);
	// }

	private static int find(List<Long> a, long o, int low, int high,
			long threshold1, long threshold2) {
		if (low == high)
			return low;
		int mid = (low + high) / 2;
		long l, m, r;
		l = m = r = threshold1 + threshold2 + 1;
		if (mid - 1 >= low)
			l = o - a.get(mid - 1);
		if (mid + 1 <= high)
			r = o - a.get(mid + 1);
		m = o - a.get(mid);
		if (compare(a.get(mid), o, threshold1, threshold2) == 0) {
			return mid;
		}
		if (compare(a.get(mid), o, threshold1, threshold2) > 0)
			return find(a, o, low, mid, threshold1, threshold2);
		else
			return find(a, o, mid, high, threshold1, threshold2);
	}

	private static int compare(long a, long b, long threshold1, long threshold2) {
		if (a == b)
			return 0;
		else if (a > b)
			return 1;
		else
			return -1;
	}

	public static void main(String[] args) throws IOException {
		List<Long> aList = new ArrayList<Long>();
		aList.add(0L);
		aList.add(3L);
		aList.add(8L);
		aList.add(9L);
		List<Long> bList = new ArrayList<Long>();
		bList.add(7L);
		bList.add(8L);
		bList.add(11L);
		bList.add(13L);
		System.out.println(match(aList, bList, 0, 2));
	}
}
