package com.oren.eventproces.mapreduce;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.oren.eventproces.utils.EventProcessLog;

public class CSVReducer extends Reducer<Text, EventProcessLog, NullWritable, Text> {

	Logger LOGGER = Logger.getLogger(CSVReducer.class.getName());

	protected void reduce(Text key, Iterable<EventProcessLog> values,
			Reducer<Text, EventProcessLog, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {

		LOGGER.info("CSVReducer reduce method is stared here ");
		for (EventProcessLog value : values) {
			context.write(NullWritable.get(), new Text(value.toString()));
		}

		LOGGER.info("CSVReducer reduce method is ended here ");
	}
}
