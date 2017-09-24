package com.oren.eventproces.mapreduce;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.oren.eventproces.utils.EventConstants;
import com.oren.eventproces.utils.EventProcessLog;

public class CSVMapper
		extends
		org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, EventProcessLog> {

	private Text country = new Text();
	Logger LOGGER = Logger.getLogger(CSVMapper.class.getName());

	public void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, EventProcessLog>.Context context)
			throws IOException, InterruptedException {
		LOGGER.info("CSVMapper class stared here");
			
		String[] sps = value.toString().split(EventConstants.CSV_DELIMITER);
		

		country.set(sps[3]);

		EventProcessLog eventProcess = new EventProcessLog();

		eventProcess.setProcesstimestamp(sps[0]);
		eventProcess.setIp(sps[1]);
		eventProcess.setCountry(sps[2]);
		eventProcess.setStatus(sps[3]);

		context.write(country, eventProcess);

		LOGGER.info("CSVMapper class ended here");
	}

}
