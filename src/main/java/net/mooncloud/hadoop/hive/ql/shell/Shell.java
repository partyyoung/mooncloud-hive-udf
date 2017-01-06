package net.mooncloud.hadoop.hive.ql.shell;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

public class Shell {

	public HashMap<String, Object> shell(String command) {
		HashMap<String, Object> jsonObject = new HashMap<String, Object>();
		try {
			Process ps = Runtime.getRuntime().exec(command);
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
			System.out.println(new Shell().shell(args[0]));
		} else {
			return;
		}
	}
}
