package ccwc;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Job;

public class Driver {
	
	public static void main(String[] args) throws Exception{
		if (args.length != 2) {
		      System.out.printf(
		          "Usage: ChannelCount <input dir> <output dir>\n");
		      System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(Driver.class);
		job.setJobName("ChannelCountWithComma");
		
		//Declare the input file path
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		//Declare the output file path
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    //Initiate the Mapper to be used
	    job.setMapperClass(CCWC_Mapper.class);
	    
	    
	    
	    //Initiate the Reducer to be used
	    job.setReducerClass(CCWC_Reducer.class);
	    
	    //Declare the key datatype of the output
	    job.setOutputKeyClass(Text.class);
	    
	    //Declare the value datatype of the output
	    job.setOutputValueClass(IntWritable.class);
	    
	    //Initiate boolean to check whether MapReduce is successful
	    boolean success = job.waitForCompletion(true);
	    
	    //Execute code based on whether the job is successful
	    System.exit(success ? 0 : 1);
	}

}
