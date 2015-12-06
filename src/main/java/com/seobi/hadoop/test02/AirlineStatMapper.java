package com.seobi.hadoop.test02;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AirlineStatMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	private final static LongWritable one = new LongWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		AirlineStatParser parser = new AirlineStatParser(value);

		if (parser.isArrivalDelay() && parser.getArrivalDelay() > 0) {
			word.set(parser.getYearMonth());
			context.write(word, one);
		}
	}
}