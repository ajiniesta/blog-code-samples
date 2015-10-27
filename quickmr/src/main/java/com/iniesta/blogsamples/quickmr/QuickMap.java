package com.iniesta.blogsamples.quickmr;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class QuickMap extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] splits = value.toString().split(",");
		context.write(new Text(splits[0]), new DoubleWritable(Double.parseDouble(splits[2])));
	}

	
}
