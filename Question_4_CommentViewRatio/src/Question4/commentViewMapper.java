package Question4;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class commentViewMapper extends Mapper<LongWritable, Text, LongWritable, DoubleWritable>{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		//Skip the first line
		if(key.get() == 0){
			return;
		}
		
		
		//Split the input by tab as delimiter
		//initialize the variable accordingly
		//calculate the ratio of commentView
		//emits <category_ID, ration> to reducer
		else{
			String[] line = (value.toString()).split("\t");
			
			int catID = Integer.parseInt(line[4]);
			int comments = Integer.parseInt(line[10]);
			int views = Integer.parseInt(line[7]);
			// #Task 1
			double ratio = (double)comments / views;
			
			context.write(new LongWritable(catID),new DoubleWritable(ratio));
		}
	}
}
