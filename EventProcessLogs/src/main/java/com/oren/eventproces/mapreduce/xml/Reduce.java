package com.oren.eventproces.mapreduce.xml;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, NullWritable, Text, NullWritable> {

	protected void reduce(Text key, Iterable<NullWritable> arg1,
			Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {

		context.write(key, NullWritable.get());
		
		
	}
}
