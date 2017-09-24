package com.iwinner.m_techlearn.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxLengthWordInAFileJob implements Tool {
	// Initializing configuration object
	private Configuration conf;

	@Override
	public Configuration getConf() {
		return conf; // getting the configuration
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf; // setting the configuration
	}

	@Override
	public int run(String[] args) throws Exception {

		// initializing the job configuration
		Job maxLengthWordInAFileJob = new Job(getConf());

		// setting the job name
		maxLengthWordInAFileJob.setJobName("Orien IT MaxLengthWordInAFile Job");

		// to call this as a jar
		maxLengthWordInAFileJob.setJarByClass(this.getClass());

		// setting custom mapper class
		maxLengthWordInAFileJob.setMapperClass(MaxLengthWordInAFileMapper.class);

		// setting custom reducer class
		maxLengthWordInAFileJob.setReducerClass(MaxLengthWordInAFileReducer.class);

		// setting custom combiner class
		// maxLengthWordInALineJob.setCombinerClass(WordCountReducer.class);

		// setting no of reducers
		// maxLengthWordInAFileJob.setNumReduceTasks(1);

		// setting custom partitioner class
		// maxLengthWordInALineJob.setPartitionerClass(WordCountPartitioner.class);

		// setting mapper output key class: K2
		maxLengthWordInAFileJob.setMapOutputKeyClass(Text.class);

		// setting mapper output value class: V2
		maxLengthWordInAFileJob.setMapOutputValueClass(LongWritable.class);

		// setting final output key class: K3
		maxLengthWordInAFileJob.setOutputKeyClass(Text.class);

		// setting final output value class: V3
		maxLengthWordInAFileJob.setOutputValueClass(LongWritable.class);

		// setting the input format class ,i.e for K1, V1
		maxLengthWordInAFileJob.setInputFormatClass(TextInputFormat.class);

		// setting the output format class
		maxLengthWordInAFileJob.setOutputFormatClass(TextOutputFormat.class);

		// setting the input file path
		FileInputFormat.addInputPath(maxLengthWordInAFileJob, new Path(args[0]));

		// setting the output folder path
		FileOutputFormat.setOutputPath(maxLengthWordInAFileJob, new Path(args[1]));

		Path outputpath = new Path(args[1]);
		// delete the output folder if exists
		outputpath.getFileSystem(conf).delete(outputpath, true);

		// to execute the job and return the status
		return maxLengthWordInAFileJob.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new MaxLengthWordInAFileJob(), args);

		System.out.println("My Status: " + status);
	}
}


















