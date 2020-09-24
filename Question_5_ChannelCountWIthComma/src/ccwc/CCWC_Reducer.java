package ccwc;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CCWC_Reducer extends Reducer<Text,IntWritable,Text,IntWritable>{

	@Override
	public void reduce(Text channel_title,Iterable<IntWritable> counts,Context context) throws IOException, InterruptedException{
		//initialize sum to 0
		int sum=0;
		//loop and add count to sum
		for(IntWritable count:counts) {
			sum+=count.get();
		}
		context.write(channel_title, new IntWritable(sum));
	}
}