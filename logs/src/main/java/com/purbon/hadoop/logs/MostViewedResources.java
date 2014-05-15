package com.purbon.hadoop.logs;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import com.purbon.hadoop.logs.jobs.TOPKResourcesJob;

public class MostViewedResources {

	public void run(String source, String target) {
		
		TOPKResourcesJob launcher = new TOPKResourcesJob();
		try {
			launcher.launch(new Path(source), new Path(target));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		MostViewedResources rr = new MostViewedResources();
		rr.run(args[0], args[1]);
	}

}
