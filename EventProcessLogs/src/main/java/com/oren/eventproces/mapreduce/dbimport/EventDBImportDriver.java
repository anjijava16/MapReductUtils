package com.oren.eventproces.mapreduce.dbimport;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oren.eventproces.mapreduce.dbexport.EmployeeWritable;




public class EventDBImportDriver extends Configured implements Tool {

	public static void main(String... args) throws Exception {
		
		int status = ToolRunner.run(new Configuration(), new EventDBImportDriver(), args);
		System.out.println("Status: " + status);
		
		
	}
	
	public int run(String[] args) throws Exception {

		DBConfiguration.configureDB(getConf(), "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/workdb"
				+ "?user=root&password=cloudera");

		Job job = new Job(getConf());
		job.addFileToClassPath(new Path("/data/lib/mysql-connector-java-5.1.14.jar"));
		
		job.setJarByClass(EventDBImportDriver.class);
		job.setMapperClass(EventDBImportMapper.class);
		job.setReducerClass(EventDBImportReducer.class);

		job.setInputFormatClass(DBInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(EventProcessWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(EventProcessWritable.class);
		job.setOutputValueClass(NullWritable.class);

		// DBInputFormat.setInput(job, EmployeeWritable.class, "select * from event_tbl", "SELECT COUNT(1) FROM event_tbl");
		
		//DBInputFormat.setInput(job, EventProcessWritable.class, inputQuery, inputCountQuery);

		// DistributedCache.addLocalFiles(getConf(), args[1]);
		
		/*
		public static void setInput(Job job, 
			      Class<? extends DBWritable> inputClass,
			      String tableName,String conditions, 
			      String orderBy, String... fieldNames)*/
		
		
		DBInputFormat.setInput(job, EmployeeWritable.class, "event_tbl", null, null, new String[]{ "processdate", "ip", "country", "status" });

		
		Path outputPath = new Path(args[0]);

		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(getConf()).delete(outputPath, true);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	

}
