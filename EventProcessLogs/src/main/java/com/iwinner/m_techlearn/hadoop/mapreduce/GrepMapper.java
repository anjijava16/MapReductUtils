package com.iwinner.m_techlearn.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class GrepMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		InputSplit inputSplit = context.getInputSplit();
		FileSplit fileSplit = (FileSplit)inputSplit;
		Path path = fileSplit.getPath();
		String name = path.getName();
		Path parent = path.getParent();
		System.out.println("------------------------------");
		System.out.println("path: " + path);
		System.out.println("name: " + name);
		System.out.println("parent: " + parent);
		System.out.println("------------------------------");
	}
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// read the line
		String line = value.toString();
		
		// if line contains the pattern then print it
		// other wise skip it
		String pattern = "am";
		if (line.contains(pattern)) {
			context.write(value, NullWritable.get());
		}

	}
}




































