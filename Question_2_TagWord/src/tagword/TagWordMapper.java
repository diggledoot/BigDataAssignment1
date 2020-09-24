package tagword;

import java.io.IOException;
import java.util.regex.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class TagWordMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	
	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
		if(key.get()==0){
			return;
		}else{
			//convert Text input to string
			String line = value.toString();
			
			//split the string and extract relevant information
			Text title = new Text(line.split("\t")[2]);
			int likes = Integer.parseInt(line.split("\t")[8]);
			String tags = line.split("\t")[6];
			
			//use regex to check whether "cute" exists in side tags string
			//CASE_INSENSITIVE ensures lower/upper/mixed case characters do not affect the search
			Pattern r = Pattern.compile("\\bcute\\b",Pattern.CASE_INSENSITIVE);
			Matcher matcher = r.matcher(tags);
			boolean found = matcher.find();
			
			//check if found and likes is more than 3000
			if(found & likes>3000){
				context.write(title, new IntWritable(1));
			}
			
			
		}
	}

}
