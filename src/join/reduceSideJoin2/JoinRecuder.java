package join.reduceSideJoin2;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinRecuder extends
		Reducer<ProductIdKey, JoinGenericWritable, NullWritable, Text> {

	protected void reduce(ProductIdKey key,
			Iterable<JoinGenericWritable> values, Context context)
			throws IOException, InterruptedException {

		StringBuilder output = new StringBuilder();
		int sumOrderQty = 0;
		double sumLineTotal = 0.0;

		for (JoinGenericWritable v : values) {
			Writable record = v.get();
			if (key.recordType.equals(ProductIdKey.PRODUCT_RECORD)) {
				ProductRecord pRecord = (ProductRecord) record;
				output.append(Integer.parseInt(key.productId.toString()))
						.append(", ");
				output.append(pRecord.productName.toString()).append(", ");
				output.append(pRecord.productNumber.toString()).append(", ");
			} else {
				SalesOrderDataRecord record2 = (SalesOrderDataRecord) record;
				sumOrderQty += Integer.parseInt(record2.orderQty.toString());
				sumLineTotal += Double
						.parseDouble(record2.lineTotal.toString());
			}
		}

		if (sumOrderQty > 0) {
			context.write(NullWritable.get(), new Text(output.toString()
					+ sumOrderQty + ", " + sumLineTotal));
		}
	};
}
