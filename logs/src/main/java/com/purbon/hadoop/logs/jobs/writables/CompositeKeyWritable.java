package com.purbon.hadoop.logs.jobs.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {

	private String a;
	private String b;
	
	public CompositeKeyWritable() {
		this("", "");
	}

	public CompositeKeyWritable(String a, String b) {
		this.a = a;
		this.b = b;
	}
	
	// A#B
	public void readFields(DataInput i) throws IOException {
		a = i.readUTF();
		b = i.readUTF();
		
	}

	public void write(DataOutput o) throws IOException {
		o.writeUTF(a);
		o.writeUTF(b);
		
	}

	public int compareTo(CompositeKeyWritable another) {
		int cmp = a.compareTo(another.a);
		if (cmp == 0)
			cmp = b.compareTo(another.b);
		return cmp;
	}

	public String getA() {
		return a;
	}






}
