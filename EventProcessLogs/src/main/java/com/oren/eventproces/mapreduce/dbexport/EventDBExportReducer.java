package com.oren.eventproces.mapreduce.dbexport;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class EventDBExportReducer
		extends Reducer<EventProcessWritable, NullWritable, EventProcessWritable, NullWritable> {

	public void reduce(EventProcessWritable key, Iterable<NullWritable> arg1,
			Reducer<EventProcessWritable, NullWritable, EventProcessWritable, NullWritable>.Context context)
					throws IOException, InterruptedException {

		context.write(key, NullWritable.get());

	}
}
