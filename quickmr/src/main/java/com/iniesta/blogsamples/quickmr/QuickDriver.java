package com.iniesta.blogsamples.quickmr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class QuickDriver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new QuickDriver(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf, "Quickstart MR");
		job.setJarByClass(QuickDriver.class);
		
		String input = getClass().getClassLoader().getResource("dataset").toString();
		FileInputFormat.addInputPath(job, new Path(input));
		String output = "output/out"+System.currentTimeMillis();
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setMapperClass(QuickMap.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setReducerClass(QuickReducer.class);
		
		job.setNumReduceTasks(1);
		
		boolean ok = job.waitForCompletion(true);
		
		return ok ? 0 : 1;
	}
}
