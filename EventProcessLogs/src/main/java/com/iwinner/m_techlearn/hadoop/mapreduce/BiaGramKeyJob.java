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

public class BiaGramKeyJob implements Tool {
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
		Job biaGramKeyJob = new Job(getConf());

		// setting the job name
		biaGramKeyJob.setJobName("Orien IT BiaGramKey Job");

		// to call this as a jar
		biaGramKeyJob.setJarByClass(this.getClass());

		// setting custom mapper class
		biaGramKeyJob.setMapperClass(BiaGramKeyMapper.class);

		// setting custom reducer class
		biaGramKeyJob.setReducerClass(BiaGramKeyReducer.class);

		// setting custom combiner class
		// biaGramJob.setCombinerClass(WordCountReducer.class);

		// setting no of reducers
		// biaGramJob.setNumReduceTasks(26);

		// setting custom partitioner class
		// biaGramJob.setPartitionerClass(WordCountPartitioner.class);

		// setting mapper output key class: K2
		biaGramKeyJob.setMapOutputKeyClass(BiaGramKey.class);

		// setting mapper output value class: V2
		biaGramKeyJob.setMapOutputValueClass(LongWritable.class);

		// setting final output key class: K3
		biaGramKeyJob.setOutputKeyClass(BiaGramKey.class);

		// setting final output value class: V3
		biaGramKeyJob.setOutputValueClass(LongWritable.class);

		// setting the input format class ,i.e for K1, V1
		biaGramKeyJob.setInputFormatClass(TextInputFormat.class);

		// setting the output format class
		biaGramKeyJob.setOutputFormatClass(TextOutputFormat.class);

		// setting the input file path
		FileInputFormat.addInputPath(biaGramKeyJob, new Path(args[0]));

		// setting the output folder path
		FileOutputFormat.setOutputPath(biaGramKeyJob, new Path(args[1]));

		Path outputpath = new Path(args[1]);
		// delete the output folder if exists
		outputpath.getFileSystem(conf).delete(outputpath, true);

		// to execute the job and return the status
		return biaGramKeyJob.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner
				.run(new Configuration(), new BiaGramKeyJob(), args);

		System.out.println("My Status: " + status);
	}
}
