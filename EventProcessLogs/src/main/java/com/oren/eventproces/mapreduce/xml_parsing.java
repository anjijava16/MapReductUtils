/*package com.oren.eventproces.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.json.JSONException;

import org.json.JSONObject;

import org.json.XML;

public class xml_parsing {

	public static class TokenizerMapper

	extends Mapper<Object, Text, Text, NullWritable> {

		public void map(Object key, Text value, Context context

		) throws IOException, InterruptedException {

			String xml_data = value.toString();

			try {

				JSONObject xml_to_json = XML.toJSONObject(xml_data);

				String json_string = xml_to_json.toString();

				context.write(new Text(json_string.toString()),
						NullWritable.get());

			} catch (JSONException je) {

				System.out.println(je.toString());

			}

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "xml_to_json");

		job.setJarByClass(xml_parsing.class);

		job.setNumReduceTasks(0);

		job.setMapperClass(TokenizerMapper.class);

		job.setMapOutputKeyClass(Text.class);

		job.setMapOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}*/