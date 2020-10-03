package Question4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CommentViewPartitioner extends Partitioner<LongWritable,DoubleWritable>{

	// # Task 2
	// Send the correct record to the reducer accordingly
	@Override
	public int getPartition(LongWritable key, DoubleWritable value, int numPartitions) {
		int categoryID = Integer.parseInt(key.toString());
		
		if(categoryID > 0 && categoryID <= 15){
			return 0;
		}
		
		else if(categoryID > 15 && categoryID <= 30){
			return 1 % numPartitions;
		}
		
		else if(categoryID > 30 && categoryID <= 45){
			return 2 % numPartitions;
		}
		
		else{
			return 3 % numPartitions;
		}
	}
	
}
