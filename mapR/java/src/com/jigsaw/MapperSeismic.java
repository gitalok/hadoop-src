package com.jigsaw;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperSeismic extends Mapper <LongWritable, Text, Text, DoubleWritable>{
	

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		//Split input line based on ,
		String [] s = value.toString().split(",");
		
		//Country name is at last index in input file
		String country = s[s.length-1];
		
		//Earthquake magnitude is at 5 th index in input file
		Double magni = Double.parseDouble(s[5]);
		
		System.out.println(country);
		System.out.println(magni);
		//Write country name and earthquake magnitude to context
		context.write(new Text(country), new DoubleWritable(magni));		
	}
	
}
