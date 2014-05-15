package com.purbon.hadoop.tasks;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.purbon.hadoop.logs.utils.TOPKFilter;

public class TOPKCombiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	TOPKFilter<Integer, Text> map = new TOPKFilter<Integer, Text>(15);
	
	public void reduce(Text primaryKey, Iterator<Text> it, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
		while(it.hasNext()) {
			Text     next   = it.next();
			String[] fields = next.toString().split("\t");
			map.put(Integer.valueOf(fields[1]), new Text(fields[0]));
		}

		for(Integer key : map.descendingKeySet()) {
			Text text = map.get(key);
			collector.collect(new Text("Dummy"), new Text(text+"\t"+key));
		}
	}

}
