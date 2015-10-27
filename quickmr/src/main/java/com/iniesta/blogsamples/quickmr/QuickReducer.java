package com.iniesta.blogsamples.quickmr;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class QuickReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		double acum = 0;
		for (DoubleWritable value : values) {
			acum += value.get();
		}
		context.write(key, new DoubleWritable(acum));
	}

	
}
