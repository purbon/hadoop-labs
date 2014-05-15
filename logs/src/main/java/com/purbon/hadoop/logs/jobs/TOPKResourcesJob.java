package com.purbon.hadoop.logs.jobs;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.purbon.hadoop.tasks.EchoMapper;
import com.purbon.hadoop.tasks.TOPKCombiner;
import com.purbon.hadoop.tasks.TOPKReducer;

/**
 * Gets the TOP-K most visited resources
 * @author Pere
 *
 */
public class TOPKResourcesJob {

	public void launch(Path source, Path target) throws IOException {

		JobConf conf = new JobConf(TOPKResourcesJob.class);

		conf.setJobName("TOP-K resources");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));

		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
	
		TextInputFormat.addInputPath(conf, source);
		TextOutputFormat.setOutputPath(conf, target);
	
		conf.setMapperClass(EchoMapper.class);
		conf.setCombinerClass(TOPKCombiner.class);
		conf.setReducerClass(TOPKReducer.class);
		conf.setNumReduceTasks(1);
		
		JobClient.runJob(conf);
		
	}
}
