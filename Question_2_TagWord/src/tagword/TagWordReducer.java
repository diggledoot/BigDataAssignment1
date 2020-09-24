package tagword;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class TagWordReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	
	@Override
	public void reduce(Text title,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
		//initialize sum
		int sum=0;
		
		//loop to sum up number of occurrences of videos meeting the criteria of having
		//cute inside its tags and having more than 3000 likes
		for(IntWritable val:values){
			sum+=val.get();
		}
		context.write(title,new IntWritable(sum));
	}

}
