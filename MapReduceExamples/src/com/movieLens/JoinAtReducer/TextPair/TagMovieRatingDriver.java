package com.movieLens.JoinAtReducer.TextPair;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TagMovieRatingDriver {
	
	public static class KeyPartitioner extends Partitioner<TextPair, Text> {
		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Movie Rating");
		
		MultipleInputs.addInputPath(job, new Path("D:\\hadoopFiles\\dataset\\movielens\\small\\movies.csv"), TextInputFormat.class, TagMovieMapper.class);
		MultipleInputs.addInputPath(job, new Path("D:\\hadoopFiles\\dataset\\movielens\\small\\ratings.csv"), TextInputFormat.class, TagRatingMapper.class);
		
		FileOutputFormat.setOutputPath(job, new Path("D:\\hadoopFiles\\dataset\\movielens\\small\\out"));

		job.setJarByClass(TagMovieRatingDriver.class);						
		job.setReducerClass(TagMovieRatingReducer.class);		

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(KeyPartitioner.class);
		job.setGroupingComparatorClass(CustomComparator.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(2);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
