package com.iwinner.m_techlearn.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MaxLengthWordInAFileMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {
	String maxWord;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		maxWord = "";
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// read the line
		String line = value.toString();

		// split the line into words
		String[] words = line.split(" ");

		// find the max length word in a line
		for (String word : words) {
			if (word.length() >= maxWord.length()) {
				maxWord = word;
			}
		}
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		context.write(new Text(maxWord), new LongWritable(maxWord.length()));
	}
}





























