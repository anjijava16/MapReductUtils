package com.oren.eventproces.mapreduce.multple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oren.eventproces.utils.EventProcessLog;

public class MultipleOutputDriver extends Configured implements Tool {

	  @Override
		public int run(String[] args) throws Exception {

			if (args.length != 2) {
				System.out
						.printf("Two parameters are required for DriverFormatMultiOutput- <input dir> <output dir>\n");
				return -1;
			}

			Job job = new Job(getConf());
			job.setJobName("MultipleOutputs example");

			job.setJarByClass(MultipleOutputDriver.class);
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.setMapperClass(MutlipleMapper.class);
			job.setReducerClass(MutlipleReducer.class);
	
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(EventProcessLog.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

	//		job.setNumReduceTasks(4);

			boolean success = job.waitForCompletion(true);
			return success ? 0 : 1;
		}

		public static void main(String[] args) throws Exception {
			int exitCode = ToolRunner.run(new Configuration(),
					new MultipleOutputDriver(), args);
			System.exit(exitCode);
		}
	}