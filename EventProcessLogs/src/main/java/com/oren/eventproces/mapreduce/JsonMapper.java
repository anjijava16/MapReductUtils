package com.oren.eventproces.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.oren.eventproces.utils.JSONUtils;

public class JsonMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	public void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {

		String sp[] = value.toString().split("\n");

		String jsonData = JSONUtils.formatJSON(sp[0]);

		context.write(new Text(jsonData), NullWritable.get());

	}
}
