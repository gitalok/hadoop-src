package com.jigsaw;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DriverSeismic {
	
	public static void main (String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		@SuppressWarnings("deprecation")
		Job job = new Job ();
		
		job.setJarByClass(DriverSeismic.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MapperSeismic.class);
		job.setReducerClass(ReducerSeismic.class);
		
		//Key will be country name and value will be earthquake magnitude
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		//Out will be country name and value will be max earthquake magnitude recorded
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		System.exit(job.waitForCompletion(true)?0:-1);		
	}


}
