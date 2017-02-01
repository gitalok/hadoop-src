package com.movieLens.RatingCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (!value.toString().contains("movieId")) {
			String[] line = value.toString().trim().split(",");
			context.write(new IntWritable(Integer.parseInt(line[0])),new Text(line[1]));
		}
	}
}
