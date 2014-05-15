package com.purbon.hadoop.logs;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import com.purbon.hadoop.logs.jobs.RequestedResourcesJob;

public class RequestedResources {

	public void run(String source, String target) {
		
		RequestedResourcesJob launcher = new RequestedResourcesJob();
		try {
			launcher.launch(new Path(source), new Path(target));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		RequestedResources rr = new RequestedResources();
		rr.run(args[0], args[1]);
	}

}
