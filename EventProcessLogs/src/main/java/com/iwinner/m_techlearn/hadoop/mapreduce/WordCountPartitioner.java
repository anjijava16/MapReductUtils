package com.iwinner.m_techlearn.hadoop.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, LongWritable>{

	@Override
	public int getPartition(Text key, LongWritable value, int noOfReducers) {
		// getting the word in a lowercase
		String word = key.toString().toLowerCase();
		
		if (word.length() == 0) {
			return 0;
		}
		// get the first character
		char firstChar = word.charAt(0);
		
		// get the partition number (0 to n-1) 
		int diff = Math.abs(firstChar -'a') % noOfReducers;
		
		// return the partition number
		return diff;
	}

}














