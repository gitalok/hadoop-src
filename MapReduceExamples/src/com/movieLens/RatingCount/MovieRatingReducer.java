package com.movieLens.RatingCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class MovieRatingReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

	private static final String one = "1";

	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		Text key1 = null;
		for (Text value : values) {
			if (one.compareTo(value.toString().trim()) == 0) {
				count++;
			} else {
				key1 = new Text(value.toString());
				//System.out.println("key1=>" + key + " Values="+ value.toString());
			}
		}
		context.write(key1, new IntWritable(count));
	}
}
