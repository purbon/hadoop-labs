package com.purbon.hadoop.tasks;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class IdentityMapper extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable> {

	public void map(Text key, Text one, OutputCollector<Text, IntWritable> collector, Reporter reporter) throws IOException {
		collector.collect(key, new IntWritable(1));
	}
	
}
