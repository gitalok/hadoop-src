package com.movieLens.JoinAtReducer.TextPair;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TagRatingMapper extends Mapper<LongWritable, Text, TextPair, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().trim().split(",");
		if (!value.toString().contains("userId"))
			context.write(new TextPair(line[1],"1"), new Text("1"));
	}
}