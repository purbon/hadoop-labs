package com.purbon.hadoop.tasks;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.purbon.hadoop.logs.jobs.writables.CompositeKeyWritable;

public class DefaultReducer extends MapReduceBase implements Reducer<CompositeKeyWritable, IntWritable, Text, IntWritable> {

	public void reduce(CompositeKeyWritable key, Iterator<IntWritable> it, OutputCollector<Text, IntWritable> collector, Reporter reporter) throws IOException {
		//String[] fields = key.toString().split("#");	
		collector.collect(new Text(key.getA()), new IntWritable(1));
	}

}
