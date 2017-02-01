package com.newWordCount;

//hdfs://192.168.148.71:9000/user/avagarwal/input/input.txt

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NewWordCount extends Configured{
	
	public static void main (String args[]) throws IOException, ClassNotFoundException, InterruptedException{
	
		//Job job = Job.getInstance(new Configuration());
		//@SuppressWarnings("deprecation")
		//Job job = new Job ();
		
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setJarByClass(NewWordCount.class);
		job.setMapperClass(NewWordMapper.class);
		
		//job.setCombinerClass(NewWordReducer.class);
		
		job.setReducerClass(NewWordReducer.class);
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//If want to sort in descending order on key. Hadoop default is
		//ascending order sort
		job.setSortComparatorClass(SortComparator.class);
				
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true)?0:-1);	
	}
	
}
