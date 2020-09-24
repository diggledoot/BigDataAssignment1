package channelcount;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class ChannelCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

	@Override
	public void reduce(Text channel_name,Iterable<IntWritable>counts,Context context) throws IOException, InterruptedException{
		/*
		 *  We want to sum up the total likes based on channel_title
		 *  After Hadoop has shuffled similar keys and their values into key->collection, where
		 *  collection is the array of values with the same key.
		 *  
		 *  It sums it up as seen in the for loop below.
		 *  
		 * */
		int sum=0;
		for(IntWritable count: counts) {
			sum+=count.get();
		}
		context.write(channel_name, new IntWritable(sum));
	}
}
