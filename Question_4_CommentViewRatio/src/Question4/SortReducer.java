package Question4;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class SortReducer extends Reducer<DoubleWritable, LongWritable, DoubleWritable, LongWritable>{
	@Override
	public void reduce(DoubleWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
	
	long id = 0;
	
	for(LongWritable value : values){
		id = value.get();
	}
		
	context.write(key, new LongWritable(id));
	}
}
