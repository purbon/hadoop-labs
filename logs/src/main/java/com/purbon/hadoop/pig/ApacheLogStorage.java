package com.purbon.hadoop.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.purbon.hadoop.logs.parser.LogParser;

@SuppressWarnings("rawtypes")
public class ApacheLogStorage extends LoadFunc {

	@SuppressWarnings("unused")
	private String location;

	private RecordReader reader;
	private TupleFactory tupleFactory; 
	private LogParser parser;
	
	public ApacheLogStorage() {
		super();
		this.tupleFactory = TupleFactory.getInstance();
		this.parser = new LogParser();
	}
	
	
	@Override
	public InputFormat getInputFormat() throws IOException {
		return new PigTextInputFormat();
	}

	@Override
	public Tuple getNext() throws IOException {
		List<String> list = new ArrayList<String>();
		
		try {
			if (!reader.nextKeyValue())
				return null;
			Text line = (Text)reader.getCurrentValue();
			if (line != null) {
				Map<String, String> props = parser.parse(line.toString());
				list.add(props.get(LogParser.HOST));
				list.add(props.get(LogParser.DATETIME));
				list.add(props.get(LogParser.METHOD));
				list.add(props.get(LogParser.URL));
				list.add(props.get(LogParser.PROTOCOL));
				list.add(props.get(LogParser.CODE));
				list.add(props.get(LogParser.TIME));
				return tupleFactory.newTuple(list);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		
		return null;
	}

	@Override
	public void prepareToRead(RecordReader record, PigSplit split) throws IOException {
		this.reader = record;
		this.location = "";
		InputSplit is = split.getWrappedSplit();
		String name = is.getClass().getName();
		if (name.compareTo("org.apache.hadoop.mapreduce.lib.input.FileSplit") == 0) {
			FileSplit fs = (FileSplit) is;
			location = fs.getPath().getParent().getName();
		} else 
			throw new IOException(name);
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		this.location = "";
		PigTextInputFormat.addInputPaths(job, location);

	}

}
