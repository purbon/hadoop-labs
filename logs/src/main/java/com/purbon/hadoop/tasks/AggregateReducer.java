package com.purbon.hadoop.tasks;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AggregateReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterator<IntWritable> it, OutputCollector<Text, IntWritable> collector, Reporter reporter) throws IOException {
		int count=0;
		while(it.hasNext()) {
			it.next();
			count++;
		}
		collector.collect(key, new IntWritable(count));
	}

}
