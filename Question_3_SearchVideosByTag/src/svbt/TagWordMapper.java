package svbt;

import java.io.IOException;
import java.util.regex.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

//modify
import org.apache.hadoop.conf.Configuration;

//for logs
import org.apache.log4j.Logger;


public class TagWordMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	//to log inputs that do not have descriptions
	private static final Logger logger = Logger.getLogger(TagWordMapper.class);
	
	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
		
		if(key.get()==0){
			return;
		}else{
			//separate values into appropriate variables
			String line=value.toString();
			Text title = new Text(line.split("\t")[2]);
			String tags = line.split("\t")[6];
			int likes = Integer.parseInt(line.split("\t")[8]);
			
			//for logging
			String video_id = line.split("\t")[0];
			
			try{
				String desc = line.split("\t")[15];
			}catch(ArrayIndexOutOfBoundsException e){
				String message = "<missing video description: "+video_id+" - "+title.toString()+">";
				logger.error(message);
			}
			
			
			//grab configuration entered in CLI via -D
			Configuration conf = context.getConfiguration();
			String tag = conf.get("tag",""); //default empty string
			int min_likes = conf.getInt("likes", 0); //default is 0
			
			
			//filters
			//if no tag is supplied and no likes supplied
			if(tag.isEmpty() & min_likes==0){
				context.write(title,new IntWritable(1));
			}
			
			//if tags are supplied but likes are not supplied
			if(!tag.isEmpty() & min_likes==0){
				Pattern r = Pattern.compile("\\b"+tag+"\\b",Pattern.CASE_INSENSITIVE);
				Matcher matcher = r.matcher(tags);
				boolean found = matcher.find();
				if(found){
					context.write(title,new IntWritable(1));
				}
			}
			//if tag not supplied but min_likes is supplied
			if(tag.isEmpty() & min_likes!=0){
				if(likes>min_likes){
					context.write(title,new IntWritable(1));
				}
			}
			
			//if both tags and likes are supplied
			if(!tag.isEmpty() & min_likes!=0){
				//CASE_INSENSITIVE ensure searched tag is parsed regardless of lower/upper/mixed case
				Pattern r = Pattern.compile("\\b"+tag+"\\b",Pattern.CASE_INSENSITIVE);
				Matcher matcher = r.matcher(tags);
				boolean found = matcher.find();
				
				if(found & likes>min_likes){
					context.write(title,new IntWritable(1));
				}
			}
		}
	}
}
