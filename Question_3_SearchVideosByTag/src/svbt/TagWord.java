package svbt;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;


//modify
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TagWord extends Configured implements Tool{

	public static void main(String[] args) throws Exception{
		//run Job with extra variable input
		int res = ToolRunner.run(new Configuration(),new TagWord(),args);
		System.exit(res);
	}
	
	public int run(String[] args) throws Exception{
		//check if correct number of arguments supplied
		if(args.length!=2){
			System.out.println("Usage: <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		//initialize configuration class to access -D variables if they are
		Configuration conf = getConf();
		
		//initialize Job class
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		job.setJarByClass(TagWord.class);
		job.setJobName("TagWord");
		
		//declare input paths
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//declare mapper, reducer class, reducer class uses hadoop's own reducer class
		job.setMapperClass(TagWordMapper.class);
		job.setReducerClass(IntSumReducer.class);
		
		//set output data types for key and value for both mapper and reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true)?0:1;
	}
}
