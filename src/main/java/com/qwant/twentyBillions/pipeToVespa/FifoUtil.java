package com.qwant.twentyBillions.pipeToVespa;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FifoUtil {
	public static File createFifoPipe(String fifoPathName) throws IOException, InterruptedException {
	    Path fifoPath = Paths.get(fifoPathName);
	    Process process = null;
	    String[] command = new String[] {"mkfifo", fifoPath.toString()};
	    process = Runtime.getRuntime().exec(command);
	    process.waitFor();
	    return new File(fifoPath.toString());
	}

	public static File getFifoPipe(String fifoPathName) {
	    Path fifoPath = Paths.get(fifoPathName);
	    return new File(fifoPath.toString());
	}

	public static void removeFifoPipe(String fifoPathName) throws IOException {
	    Files.delete(Paths.get(fifoPathName));
	}
}
