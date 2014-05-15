package com.purbon.hadoop.tasks;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.purbon.hadoop.logs.jobs.writables.CompositeKeyWritable;
import com.purbon.hadoop.logs.parser.LogParser;

public class HostMapper extends MapReduceBase implements Mapper<LongWritable, Text, CompositeKeyWritable, IntWritable> {

	LogParser parser = new LogParser();

	public void map(LongWritable key, Text val, OutputCollector<CompositeKeyWritable, IntWritable> collector, Reporter reporter) throws IOException {

		Map<String, String> props = parser.parse(val.toString());
		if (!props.isEmpty()) {
			String host = props.get(LogParser.HOST);	
			//SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
			String dateString = props.get(LogParser.DATETIME).split(":")[0];
			collector.collect(new CompositeKeyWritable(dateString, host), new IntWritable(1));
		
		}
	}
}
