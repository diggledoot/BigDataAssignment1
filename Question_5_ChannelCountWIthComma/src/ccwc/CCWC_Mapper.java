package ccwc;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
 
public class CCWC_Mapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	
	//utility function to check if value is numeric
	public static boolean isNumeric(String str){
		if(str==null){
			return false;
		}
		try{
			double d = Double.parseDouble(str);
		}catch(NumberFormatException nfe){
			return false;
		}
		return true;
	}
	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		if(key.get()==0) {
			return;
		}else {
			//initialize values to variables
			String line = value.toString();
			String[] lineArray = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
			try{
				if(lineArray.length==16 & isNumeric(lineArray[8])){
					Text channel_title=new Text(lineArray[3]);
					IntWritable likes = new IntWritable(Integer.parseInt(lineArray[8]));
					context.write(channel_title,likes);
				}
			}catch(ArrayIndexOutOfBoundsException e){

			}
			

		}
	}
}
