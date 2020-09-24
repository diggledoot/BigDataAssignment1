package channelcount;
import org.apache.hadoop.fs.Path;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class ChannelCount {
	public static void main(String[] args) throws Exception{
		//Check if proper arguments is inputed
				if (args.length != 2) {
				      System.out.printf(
				          "Usage: ChannelCount <input dir> <output dir>\n");
				      System.exit(-1);
				}
				
				//Initiate new MapReduce job
				Job job =  new Job();
				
				//Declare which main from the specific class to be executed
				job.setJarByClass(ChannelCount.class);
				
				//Declare the name of the MapReduce Job process
				job.setJobName("ChannelCount");
				
				//Declare the input file path
				FileInputFormat.setInputPaths(job, new Path(args[0]));
				
				//Declare the output file path
			    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			    
			    //Initiate the Mapper to be used
			    job.setMapperClass(ChannelCountMapper.class);
			    
			    
			    
			    //Initiate the Reducer to be used
			    job.setReducerClass(ChannelCountReducer.class);
			    
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
