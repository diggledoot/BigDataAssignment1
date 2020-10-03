package Question4;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortKey extends Mapper<LongWritable, Text, DoubleWritable, LongWritable>{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] line = value.toString().split("\t");
		
		context.write(new DoubleWritable(Double.parseDouble(line[1])),new LongWritable(Long.parseLong(line[0])));
	}
}
