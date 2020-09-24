package tagword;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class TagWord {
	
	public static void main(String[] args) throws Exception{
		if(args.length!=2){
			System.out.println("Usage : <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		//setup Job class
		@SuppressWarnings("deprecation")
		Job job = new Job();
		
		//set up Driver class
		job.setJarByClass(TagWord.class);
		
		//setup appropriate name for the Job.
		job.setJobName("TagWord");
		
		//setup input and output paths
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//setup mapper,combiner, and reducer class
		//combiner and reducer share the same reducer class.
		job.setMapperClass(TagWordMapper.class);
		job.setCombinerClass(TagWordReducer.class);
		job.setReducerClass(TagWordReducer.class);
		
		//setup expected output key and value from mapper and reducer
		//combiner must have the same input and output data type e.g. <Text,IntWritable,Text,IntWritable>
		//essentially, combiner for both input and output data type must be the same as the output data type of the mapper
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		//check for job completion
		boolean success = job.waitForCompletion(true);
		
		System.exit(success?0:1);
	}

}
