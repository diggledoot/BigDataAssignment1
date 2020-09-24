package channelcount;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;


public class ChannelCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		/*
		 * When the file is inputed, the first line is read.
		 * The first line in this case, are the headers, which we do not want.
		 * Since the input is split into a key-pair structure, we only need to skip key 0.
		 * As seen below.
		 * */
		if(key.get()==0) {
			return;
		}else {
			/*
			 * After skipping the first line, we extract the necessary data to be mapped into our desired
			 * key-pair structure.
			 * 
			 * In this case, channel_title -> likes
			 * channel_title being Text data type
			 * likes being IntWritable data type
			 * 
			 * The data is split at the tab char.
			 * */
			String line = value.toString();
			Text channel_name = new Text(line.split("\t")[3]);
			IntWritable likes = new IntWritable(Integer.parseInt(line.split("\t")[8]));
			context.write(channel_name, likes);
		}
	}
}
