package com.seobi.hadoop.test02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AirlineStatCount {

	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.err.println("Usage: AirlineStatCount <input> <output>");
			System.exit(2);
		}
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "AirlineStatCount");

		job.setJarByClass(AirlineStatCount.class);
		job.setMapperClass(AirlineStatMapper.class);
		job.setReducerClass(AirlineStatReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
