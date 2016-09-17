package com.jigsaw;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;


public class ReducerSeismic extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
		double min = 0.0;
		double max = 0.0;
		
		System.out.println(key);
		
		while(values.iterator().hasNext()){		
			DoubleWritable i = values.iterator().next();
			if (i.get() > min ){
				max = i.get();
				min = i.get();
			}
		}
		context.write(key, new DoubleWritable(max));
	}
	
}
