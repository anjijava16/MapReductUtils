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

public class SampleJob implements Tool {
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
		Job sampleJob = new Job(getConf());

		// setting the job name
		sampleJob.setJobName("Orien IT Sample Job");

		// to call this as a jar
		sampleJob.setJarByClass(this.getClass());

		// setting the input file path
		FileInputFormat.addInputPath(sampleJob, new Path(args[0]));

		// setting the output folder path
		FileOutputFormat.setOutputPath(sampleJob, new Path(args[1]));

		Path outputpath = new Path(args[1]);
		// delete the output folder if exists
		outputpath.getFileSystem(conf).delete(outputpath, true);

		// to execute the job and return the status
		return sampleJob.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new SampleJob(), args);

		System.out.println("My Status: " + status);
	}
}
