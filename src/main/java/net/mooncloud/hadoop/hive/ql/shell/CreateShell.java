package net.mooncloud.hadoop.hive.ql.shell;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.nullCondition_return;

public class CreateShell {

	public int create(String shellPath, String command, String resultPath,
			String logPath) {
		try {
			File file = new File(shellPath).getParentFile();
			if (file != null && !file.exists()) {
				file.mkdirs();
			}
			file = new File(resultPath).getParentFile();
			if (file != null && !file.exists()) {
				file.mkdirs();
			}
			file = new File(logPath).getParentFile();
			if (file != null && !file.exists()) {
				file.mkdirs();
			}

			FileWriter upfw = new FileWriter(shellPath, false);
			BufferedWriter upbw = new BufferedWriter(upfw);
			upbw.write(command + " >" + resultPath + " 2>" + logPath);
			upbw.flush();
			upbw.close();
			upfw.close();
			Runtime.getRuntime().exec("chmod +x " + shellPath).waitFor();
			return 0;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

	public static void main(String[] args) {
		if (args.length != 4) {
			return;
		}
		String shellPath = args[0];
		String command = args[1];
		String resultPath = args[2];
		String logPath = args[3];
		CreateShell createShell = new CreateShell();
		createShell.create(shellPath, command, resultPath, logPath);
		// System.out.println(createShell.create("sh/sh.sh", "jps", "log/log1",
		// "log/log2"));
	}

}
