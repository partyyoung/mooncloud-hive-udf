package net.mooncloud.hadoop.hive.ql.shell;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;

public class KillShell {

	public int kill(String shellName, String logPath) {
		try {
			File file = new File("shell/ps.sh");
			if (file.getParentFile() != null && !file.getParentFile().exists()) {
				file.mkdirs();
			}
			if (!file.exists()) {
				FileWriter upfw = new FileWriter(file, false);
				BufferedWriter upbw = new BufferedWriter(upfw);
				upbw.write("ps -ef | grep $1 | grep -v grep | grep -v 'ps.sh' |  grep -v 'net.mooncloud.hadoop.hive.ql.shell' | awk '{print $2}'");
				upbw.flush();
				upbw.close();
				upfw.close();
			}

			Process ps = Runtime.getRuntime().exec(
					"sh shell/ps.sh " + shellName);
			if (ps.waitFor() == 0) {
				// kill shell
				BufferedReader br = new BufferedReader(new InputStreamReader(
						ps.getInputStream()));
				String line = null;
				while ((line = br.readLine()) != null) {
					Process ps1 = Runtime.getRuntime().exec(
							"sh shell/ps.sh " + line);
					ps1.waitFor();

					// kill ppid
					Runtime.getRuntime().exec("pkill -9 -P " + line).waitFor();

					// kill hadoop job
					Process ps2 = Runtime.getRuntime().exec(
							"sh shell/sed.sh 1 " + logPath);
					ps2.waitFor();
					BufferedReader br2 = new BufferedReader(
							new InputStreamReader(ps2.getInputStream()));
					String line2 = null;
					String hadoopJob = null;
					while ((line2 = br2.readLine()) != null) {
						if (line2.startsWith("Kill Command =")) {
							String[] killCommand = line2.split(" ");
							hadoopJob = killCommand[killCommand.length - 1];
						}
					}
					if (hadoopJob != null) {
						System.out.println(hadoopJob);
						ps = Runtime.getRuntime().exec(
								"hadoop job -kill " + hadoopJob);
					}

					// kill pid
					BufferedReader br1 = new BufferedReader(
							new InputStreamReader(ps1.getInputStream()));
					String line1 = null;
					while ((line1 = br1.readLine()) != null) {
						Runtime.getRuntime().exec("pkill -9 -P " + line1)
								.waitFor();
					}
				}

				return 0;
			}
			return 1;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

	public static void main(String[] args) {
		if (args.length != 2) {
			return;
		}
		System.out.println(new KillShell().kill(args[0], args[1]));
	}
}
