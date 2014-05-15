package com.purbon.hadoop.tasks;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class EchoMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

	public void map(Text key, Text v, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
		collector.collect(key, new Text(key+"\t"+v));
	}
	
	
}
