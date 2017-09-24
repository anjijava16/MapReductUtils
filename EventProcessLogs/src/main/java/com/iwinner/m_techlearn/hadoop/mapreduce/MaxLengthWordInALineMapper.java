package com.iwinner.m_techlearn.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxLengthWordInALineMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// read the line
		String line = value.toString();

		// split the line into words
		String[] words = line.split(" ");

		// find the max length word in a line
		String maxWord = "";
		for (String word : words) {
			if (word.length() >= maxWord.length()) {
				maxWord = word;
			}
		}
		context.write(new Text(maxWord), new LongWritable(maxWord.length()));

	}
}





























