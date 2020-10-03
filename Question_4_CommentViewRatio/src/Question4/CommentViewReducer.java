package Question4;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class CommentViewReducer extends Reducer<LongWritable,DoubleWritable,LongWritable,DoubleWritable>{
	@Override
	public void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
		//initialize the variable
		double sumRatio = 0;
		
		// Add the ratio of comment view with the same category ID
		for(DoubleWritable value : values){
			sumRatio += value.get();
		}
		
		//Emit the key and value
		context.write(key, new DoubleWritable(sumRatio));
	}
}
