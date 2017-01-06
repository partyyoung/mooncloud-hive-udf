package net.mooncloud.hadoop.hive.ql.shell;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

public class ResultShell {

	public HashMap<String, Object> get(String resultPath, long n) {
		HashMap<String, Object> jsonObject = new HashMap<String, Object>();
		try {
			Process ps = Runtime.getRuntime().exec(
					"head -n " + n + " " + resultPath);
			int exitcode = ps.waitFor();
			jsonObject.put("exitcode", exitcode);

			BufferedReader br = new BufferedReader(new InputStreamReader(
					ps.getInputStream()));
			ArrayList<String> aLogList = new ArrayList<String>();
			String line;
			while ((line = br.readLine()) != null) {
				aLogList.add(line);
			}
			jsonObject.put("datalist", aLogList);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonObject;
	}

	public static void main(String[] args) {
		if (args.length == 1) {
			System.out.println(new ResultShell().get(args[0], 100));
		} else if (args.length == 2) {
			System.out.println(new ResultShell().get(args[0], Long.parseLong(args[1])));
		} else {
			return;
		}
	}
}
