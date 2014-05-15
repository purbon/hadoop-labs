package com.purbon.hadoop.logs.jobs;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.purbon.hadoop.logs.jobs.writables.CompositeKeyWritable;
import com.purbon.hadoop.tasks.AggregateReducer;
import com.purbon.hadoop.tasks.HostMapper;
import com.purbon.hadoop.tasks.DefaultReducer;
import com.purbon.hadoop.tasks.IdentityMapper;

/**
 * Count distinct visitors over a set of predefined logs.
 * @author Pere Urbon-Bayes
 */
public class LogDistinctCountJob {

	public void launch(Path source, Path target) throws IOException {

		String tmpFileName = "/tmp/fase01/"+System.currentTimeMillis();
		Path tmpPath = new Path(tmpFileName);
		
		JobConf firstFaseConf  = buildDistinctCountFase01(source, tmpPath);
		JobConf secondFaseConf = buildDistinctCountFase02(tmpPath, target);

		JobClient.runJob(firstFaseConf);
		JobClient.runJob(secondFaseConf);
		
		// Delete tmpPath
		FileSystem fs = FileSystem.get(firstFaseConf);
		fs.delete(tmpPath, true);

	}
	
	private JobConf buildDistinctCountFase02(Path source, Path target)  {
		JobConf conf = new JobConf(LogDistinctCountJob.class);

		conf.setJobName("Second fase Distinct Count");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));

		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		TextInputFormat.addInputPath(conf, source);
		TextOutputFormat.setOutputPath(conf, target);
	
		conf.setMapperClass(IdentityMapper.class);
		conf.setReducerClass(AggregateReducer.class);
		
		return conf;	
	}
	
	private JobConf buildDistinctCountFase01(Path source, Path target)  {
		JobConf conf = new JobConf(LogDistinctCountJob.class);

		conf.setJobName("First fase Distinct Count");

		conf.setMapOutputKeyClass(CompositeKeyWritable.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		TextInputFormat.addInputPath(conf, source);
		TextOutputFormat.setOutputPath(conf, target);
	
		conf.setMapperClass(HostMapper.class);
		conf.setReducerClass(DefaultReducer.class);
		
		return conf;
	}
}
