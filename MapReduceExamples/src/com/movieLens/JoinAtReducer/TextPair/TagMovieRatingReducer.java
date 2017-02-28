package com.movieLens.JoinAtReducer.TextPair;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class TagMovieRatingReducer extends Reducer<TextPair, Text, Text, IntWritable> {

	public void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		Iterator<Text> iter = values.iterator();
		Text movName = new Text(iter.next());
		//System.out.println("Key is : " + key);
		//System.out.println("movine name is : " + movName);
		
		while(iter.hasNext()){
			//System.out.println(iter.next());
			count+=Integer.parseInt(iter.next().toString());
		}
		
		context.write(movName, new IntWritable(count));
		
		/* If want to check which movie is not rated */
		/*if(count==0)
			context.write(movName, new IntWritable(count));
			*/
	}
}
