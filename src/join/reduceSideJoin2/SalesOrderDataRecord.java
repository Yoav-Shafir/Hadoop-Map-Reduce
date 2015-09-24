package join.reduceSideJoin2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

// the data classes only need to implement Writable to allow serialization and deserialization.
// SalesOrderDataRecord will be emitted by the mapper that processes the Sales Order data, 
// and it will contain the quantity, in a field called orderQty, and line total in a field called lineTotal.
public class SalesOrderDataRecord implements Writable {

	public IntWritable orderQty = new IntWritable();
	public DoubleWritable lineTotal = new DoubleWritable();

	// no args constructor.
	public SalesOrderDataRecord() {

	}

	// args constructor.
	public SalesOrderDataRecord(int orderQty, double lineTotal) {
		this.orderQty.set(orderQty);
		this.lineTotal.set(lineTotal);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.orderQty.readFields(in);
		this.lineTotal.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.orderQty.write(out);
		this.lineTotal.write(out);
	}

}
