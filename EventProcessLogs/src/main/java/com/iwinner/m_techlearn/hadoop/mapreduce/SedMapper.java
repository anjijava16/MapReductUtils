package com.iwinner.m_techlearn.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SedMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// read the line
		String line = value.toString();
		
		// if line contains the pattern then replace it
		// other wise print it
		Configuration conf = context.getConfiguration();
		String pattern = conf.get("sed.pattern");
		String replace = conf.get("sed.replace");

		if (line.contains(pattern)) {
			String replaceAll = line.replaceAll(pattern, replace);
			context.write(new Text(replaceAll), NullWritable.get());
		} else {
			context.write(value, NullWritable.get());
		}

	}
}




































