package com.purbon.hadoop.logs.jobs;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.purbon.hadoop.tasks.AggregateReducer;
import com.purbon.hadoop.tasks.ResourceMapper;

/**
 * Gets the number of visits per months for each resource 
 * @author Pere Urbon-Bayes
 *
 */
public class RequestedResourcesJob {

	public void launch(Path source, Path target) throws IOException {

		JobConf conf = new JobConf(RequestedResourcesJob.class);

		conf.setJobName("Requested resources");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		TextInputFormat.addInputPath(conf, source);
		TextOutputFormat.setOutputPath(conf, target);
	
		conf.setMapperClass(ResourceMapper.class);
		conf.setReducerClass(AggregateReducer.class);
		
		JobClient.runJob(conf);
		
	}
}
