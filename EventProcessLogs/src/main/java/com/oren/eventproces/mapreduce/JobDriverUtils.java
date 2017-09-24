package com.oren.eventproces.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oren.eventproces.utils.EventProcessLog;
import com.oren.eventproces.utils.PartitionUtils;

public class JobDriverUtils implements Tool {
	// Initializing configuration object
	private Configuration conf;

	public Configuration getConf() {
		return conf; // getting the configuration
	}

	public void setConf(Configuration conf) {
		this.conf = conf; // setting the configuration
	}

		
	public int run(String[] args) throws Exception {

		// initializing the job configuration
		Job jobRef = new Job(getConf());

		// setting the job name
		jobRef.setJobName("Evemt Process Logs Job");

		// to call this as a jar
		jobRef.setJarByClass(this.getClass());

			
		// setting custom mapper class
		jobRef.setMapperClass(CSVMapper.class);
	
		// setting custom reducer class
		jobRef.setReducerClass(CSVReducer.class);

		// setting custom combiner class
		// jobRef.setCombinerClass(WordCountReducer.class);

		// setting no of reducers
		jobRef.setNumReduceTasks(2);

		
		// setting custom partitioner class
		jobRef.setPartitionerClass(PartitionUtils.class);

		// setting mapper output key class: K2
		jobRef.setMapOutputKeyClass(Text.class);

		// setting mapper output value class: V2
		jobRef.setMapOutputValueClass(EventProcessLog.class);

			
		// setting final output key class: K3
		jobRef.setOutputKeyClass(NullWritable.class);

		// setting final output value class: V3
		jobRef.setOutputValueClass(Text.class);

		// setting the input format class ,i.e for K1, V1
		jobRef.setInputFormatClass(TextInputFormat.class);

		// setting the output format class
		jobRef.setOutputFormatClass(TextOutputFormat.class);

		// setting the input file path
		FileInputFormat.addInputPath(jobRef, new Path(args[0]));

		// setting the output folder path
		FileOutputFormat.setOutputPath(jobRef, new Path(args[1]));

		Path outputpath = new Path(args[1]);
		// delete the output folder if exists
		outputpath.getFileSystem(conf).delete(outputpath, true);

		// to execute the job and return the status
		return jobRef.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
			
		int status = ToolRunner.run(new Configuration(), new JobDriverUtils(), args);
		System.out.println("My Status: " + status);
	}
}
