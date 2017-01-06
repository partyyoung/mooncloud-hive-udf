package net.mooncloud.hadoop.hive.ql.shell;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

public class LogShell {

	public HashMap<String, Object> get(String logPath, long n) {
		HashMap<String, Object> jsonObject = new HashMap<String, Object>();
		try {
			File file = new File("shell/sed.sh");
			if (file.getParentFile() != null && !file.getParentFile().exists()) {
				file.mkdirs();
			}
			if (!file.exists()) {
				FileWriter upfw = new FileWriter(file, false);
				BufferedWriter upbw = new BufferedWriter(upfw);
				upbw.write("sed -n \"$1,$\"p $2");
				upbw.flush();
				upbw.close();
				upfw.close();
			}

			Process ps = Runtime.getRuntime().exec(
					"sh shell/sed.sh " + n + " " + logPath);
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
			System.out.println(new LogShell().get(args[0], 1));
		} else if (args.length == 2) {
			System.out.println(new LogShell().get(args[0], Long.parseLong(args[1])));
		} else {
			return;
		}
	}
}
