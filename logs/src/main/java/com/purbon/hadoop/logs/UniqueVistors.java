package com.purbon.hadoop.logs;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import com.purbon.hadoop.logs.jobs.LogDistinctCountJob;

public class UniqueVistors {

	public void run(String source, String target) {
		
		LogDistinctCountJob launcher = new LogDistinctCountJob();
		try {
			launcher.launch(new Path(source), new Path(target));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		UniqueVistors uv = new UniqueVistors();
		uv.run(args[0], args[1]);
	}

}
