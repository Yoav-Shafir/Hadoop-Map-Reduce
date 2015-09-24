package join.reduceSideJoin2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProductMapper extends Mapper<LongWritable, Text, ProductIdKey, JoinGenericWritable> {
	
	protected void map(LongWritable key, Text value, Context context) 
		throws IOException ,InterruptedException {
		
		String[] recordFields = value.toString().split("\\t");
        int productId = Integer.parseInt(recordFields[0]);
        String productName = recordFields[1];
        String productNumber = recordFields[2];
        
        ProductIdKey recordKey = new ProductIdKey(productId, ProductIdKey.PRODUCT_RECORD);
		ProductRecord record = new ProductRecord(productName, productNumber);
        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
        context.write(recordKey, genericRecord);
	};
}
