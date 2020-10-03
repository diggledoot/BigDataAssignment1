package Question4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//Class to sort the key in descending order
public class descKey extends WritableComparator{
	protected descKey() {
		super(DoubleWritable.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b){
		DoubleWritable key1 = (DoubleWritable) a;
		DoubleWritable key2 = (DoubleWritable) b;
		return -1 * key1.compareTo(key2);
	}

}
