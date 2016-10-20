package com.qst.mr_frame;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class MR_Frame {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line =  value.toString();
			//2016-3-3 12：00：00   32 
			//拆分提前年份
			String [] s = line.split("-");
			String year = s[0];//2016
			
			output.collect(new Text(year), value);
		}
		
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			
			//2016  2016-3-3 12：00：00   32 
			
			List<String> list = new ArrayList<String>();
			while(values.hasNext()){
				String ss = values.next().toString();
				list.add(ss);
				
			}
		Collections.sort(list, new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				String[] s1 = o1.split("\t");
				String[] s2 = o2.split("\t");
				if( Integer.parseInt(s1[1]) == Integer.parseInt(s2[1]) )
					return 0;
				return Integer.parseInt(s1[1]) > Integer.parseInt(s2[1]) ? -1 : 1;
			}
		});
		
		for(String str : list){
			String[] s = str.split("\t");
			output.collect(new Text(s[0]), new Text(s[1]));
		}
		}
	}
	public static void main(String[] args) throws IOException {
		JobConf  conf = new JobConf(MR_Frame.class);
		conf.setJobName("MR_Frame");
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		}
}
