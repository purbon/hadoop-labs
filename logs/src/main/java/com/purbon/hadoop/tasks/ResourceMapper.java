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

import com.purbon.hadoop.logs.parser.LogParser;

public class ResourceMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	LogParser parser = new LogParser();

	public void map(LongWritable key, Text val, OutputCollector<Text, IntWritable> collector, Reporter reporter) throws IOException {

		Map<String, String> props = parser.parse(val.toString());
		if (!props.isEmpty()) {
			//SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
			String dateString = props.get(LogParser.DATETIME).split(":")[0].split("/")[1];
			collector.collect(new Text(dateString+"#"+props.get(LogParser.URL)), new IntWritable(1));
		
		}
	}
}
