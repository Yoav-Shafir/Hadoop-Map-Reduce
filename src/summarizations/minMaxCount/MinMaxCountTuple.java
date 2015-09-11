package summarizations.minMaxCount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;

// Custom Writable, this object stores 3 values.
// this class is used as the output value from the mapper.
public class MinMaxCountTuple implements Writable {
	
	private Date min = new Date();
	private Date max = new Date()	;
	private long count = 0;
	
	private final static SimpleDateFormat frmt = 
		new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

	@Override
	public void readFields(DataInput in) throws IOException {
		// Read the data out in the order it is written,
		// creating new Date objects from the UNIX timestamp
		min = new Date(in.readLong());
		max = new Date(in.readLong());
		count = in.readLong();	
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// Write the data out in the order it is read,
		// using the UNIX timestamp to represent the Date
		out.writeLong(min.getTime());
		out.writeLong(max.getTime());
		out.writeLong(count);
	}
	
	// Getters & Setters.
	
	public long getCount() {
		return count;
	}

	public Date getMax() {
		return max;
	}
	public Date getMin() {
		return min;
	}
	public void setCount(long count) {
		this.count = count;
	}

	public void setMax(Date max) {
		this.max = max;
	}

	public void setMin(Date min) {
		this.min = min;
	}
	
	public String toString() {
		return frmt.format(min) + "\t" + frmt.format(max) + "\t" + count;
	}

}
