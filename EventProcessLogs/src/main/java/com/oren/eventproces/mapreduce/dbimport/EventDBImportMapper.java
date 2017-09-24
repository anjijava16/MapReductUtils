package com.oren.eventproces.mapreduce.dbimport;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.oren.eventproces.utils.EventConstants;

public class EventDBImportMapper
		extends
		org.apache.hadoop.mapreduce.Mapper<LongWritable, Text,  EventProcessWritable,NullWritable> {

	Logger LOGGER = Logger.getLogger(EventDBImportMapper.class.getName());

		
	public void map(LongWritable key, Text value,
			Mapper<LongWritable, Text,  EventProcessWritable,NullWritable>.Context context)
			throws IOException, InterruptedException {
		LOGGER.info("CSVMapper class stared here");
			
		String[] sps = value.toString().split(EventConstants.CSV_DELIMITER);
		


		EventProcessWritable eventProcess = new EventProcessWritable();

		eventProcess.setProcessdate(sps[0]);
		eventProcess.setIp(sps[1]);
		eventProcess.setCountry(sps[2]);
		eventProcess.setStatus(sps[3]);

		context.write(eventProcess,NullWritable.get());
		
		

		LOGGER.info("CSVMapper class ended here");
	}

}
