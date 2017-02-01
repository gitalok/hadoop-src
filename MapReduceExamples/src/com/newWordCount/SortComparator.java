package com.newWordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator {

    protected SortComparator() {
    	super(Text.class, true);
     //super(IntWritable.class, true);
    }

    @Override
    public int compare(WritableComparable o1, WritableComparable o2) {
     Text k1 = (Text) o1;
     Text k2 = (Text) o2;
     int cmp = k1.compareTo(k2);
     return -1 * cmp;
    }

   }