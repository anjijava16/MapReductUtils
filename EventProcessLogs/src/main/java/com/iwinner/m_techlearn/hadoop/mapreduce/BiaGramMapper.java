package com.iwinner.m_techlearn.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BiaGramMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// read the line
		String line = value.toString();

		// split the line into words
		String[] words = line.split(" ");

		// assign count(1) to each word
		for (int i = 0; i < words.length - 1; i++) {
			String word1 = words[i];
			String word2 = words[i + 1];
			String biaGram = word1 + " " + word2;
			
			context.write(new Text(biaGram), new LongWritable(1));
		}
	}
}





























