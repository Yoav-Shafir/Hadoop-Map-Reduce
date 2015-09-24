package join.reduceSideJoin2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class ProductIdKey implements WritableComparable<ProductIdKey> {

	public static final IntWritable PRODUCT_RECORD = new IntWritable(0);
	public static final IntWritable DATA_RECORD = new IntWritable(1);

	public IntWritable productId = new IntWritable();
	public IntWritable recordType = new IntWritable(); // cannot be null.

	public ProductIdKey() {

	}

	public ProductIdKey(int productId, IntWritable recordType) {
		this.productId.set(productId);
		this.recordType = recordType;
	}

	// allow the data to be serialized and deserialized.
	// This is required since data is sent over the network from Mapper to
	// Reducer nodes.

	// And the read methods reads it from the java.io.DataInput stream.
	@Override
	public void readFields(DataInput in) throws IOException {
		this.productId.readFields(in);
		this.recordType.readFields(in);
	}

	// The write method writes all the attributes to Java's java.io.DataOutput
	// stream.
	@Override
	public void write(DataOutput out) throws IOException {
		this.productId.write(out);
		this.recordType.write(out);
	}

	// This comparison is used by Comparators (sorting comparator used to sort
	// the key/value pairs sent by the mapper, and the grouping comparator which
	// groups them)
	// to sort and group the data when sending it to the reducer nodes.
	@Override
	public int compareTo(ProductIdKey other) {
		if (this.productId.equals(other.productId)) {
			return this.recordType.compareTo(other.recordType);
		} else {
			return this.productId.compareTo(other.productId);
		}
	}

	public boolean equals(ProductIdKey other) {
		return this.productId.equals(other.productId)
				&& this.recordType.equals(other.recordType);
	}

	// will be used by the partitioner to determine which record will be sent to
	// which reducer.
	// In this case, we want all the records with the same ProductId to arrive
	// at the same reducer.
	// Since we have defined the getHashCode function in a way that it returns
	// keys based only on productId field of our key class,
	// the default partitioner serves our purpose and there is no need to define
	// a custom partitioner.
	public int hashCode() {
		return this.productId.hashCode();
	}

}
