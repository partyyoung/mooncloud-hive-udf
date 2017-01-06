package net.mooncloud.hadoop.hive.ql.shell;

public class RunShell {

	public int run(String shellPath) {
		try {
			Process ps = Runtime.getRuntime().exec("sh " + shellPath);
			return ps.waitFor();
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

	public static void main(String[] args) {
		if (args.length != 1) {
			return;
		}
		RunShell runShell = new RunShell();
		System.out.println(runShell.run(args[0]));
	}

}
