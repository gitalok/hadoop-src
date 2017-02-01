package com.movieLens.RatingCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieRatingDriver {
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Movie Rating");
		
		MultipleInputs.addInputPath(job, new Path("D:\\hadoopFiles\\dataset\\movielens\\small\\movies.csv"), TextInputFormat.class, MovieMapper.class);
		MultipleInputs.addInputPath(job, new Path("D:\\hadoopFiles\\dataset\\movielens\\small\\ratings.csv"), TextInputFormat.class, RatingMapper.class);
		
		FileOutputFormat.setOutputPath(job, new Path("D:\\hadoopFiles\\dataset\\movielens\\small\\out"));

		job.setJarByClass(MovieRatingDriver.class);						
		job.setReducerClass(MovieRatingReducer.class);		

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
