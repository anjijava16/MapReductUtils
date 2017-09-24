package com.oren.eventproces.mapreduce.multple;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.oren.eventproces.mapreduce.CSVReducer;
import com.oren.eventproces.utils.EventProcessLog;

public class MutlipleReducer extends Reducer<Text, EventProcessLog, Text, Text> {

	Logger LOGGER = Logger.getLogger(CSVReducer.class.getName());

	private MultipleOutputs<Text, Text> mos;

	public void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<Text, Text>(context);

	}

	public void reduce(Text key, Iterable<EventProcessLog> values,
			Reducer<Text, EventProcessLog, Text, Text>.Context context)
			throws IOException, InterruptedException {

		LOGGER.info("CSVReducer reduce method is stared here ");
		for (EventProcessLog value : values) {
			/*
			 * R 2013-11-26T19:08:21|88.108.201.57|FR|SUCCESS FR
			 * 2013-11-26T18:08:21|28.44.152.102|FR|ERROR
			 */
			// if("SUCCESS".getvalue.getStatus()

			if ("SUCCESS".equalsIgnoreCase(value.getStatus())) {
				mos.write(key, new Text(value.toString()),new Text(key+"_SUCCESS").toString());
			} else if ("ERROR".equalsIgnoreCase(value.getStatus())) {
				mos.write(key, new Text(value.toString()),new Text(key+"_ERROR").toString());
			} else {
				mos.write(key, new Text(value.toString()), new Text(key).toString());
			}
			
		}

		LOGGER.info("CSVReducer reduce method is ended here ");
	}

	public void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
}
