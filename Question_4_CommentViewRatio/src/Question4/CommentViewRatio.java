package Question4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class CommentViewRatio {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception{
		if(args.length != 2){
			System.out.printf("Usage: WordCount <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Configuration conf1 = new Configuration();
		Job job = Job.getInstance(conf1);
		job.setJarByClass(CommentViewRatio.class);
		//Set the mapper class
		job.setMapperClass(commentViewMapper.class);
		//Set the partitioner class
		job.setPartitionerClass(CommentViewPartitioner.class);
		//Set the reducer class
		job.setReducerClass(CommentViewReducer.class);
		//Set the number of reducer
		//Number of reducer equals to number of partitions
		job.setNumReduceTasks(4);
		//Set the mapper output type
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		//Set the reducer output type
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		Path outputPath = new Path("FirstMapper");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf1).delete(outputPath);
		job.waitForCompletion(true);
		
		//Start of second job
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance();
		job2.setJarByClass(CommentViewRatio.class);
		//Set the mapper class
		job2.setMapperClass(SortKey.class);
		//Set the output type
		job2.setOutputKeyClass(DoubleWritable.class);
		job2.setOutputValueClass(LongWritable.class);
		//Set the sorting class 
		job2.setSortComparatorClass(descKey.class);
		job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		Path outputPath2 = new Path(args[1]);
		FileInputFormat.addInputPath(job2, outputPath);
		FileOutputFormat.setOutputPath(job2, outputPath2);
		outputPath2.getFileSystem(conf2).delete(outputPath2,true);
		
		boolean success = job2.waitForCompletion(true);
		
		System.exit(success ? 0 : 1);
	}
}
