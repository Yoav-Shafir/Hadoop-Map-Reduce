package join.reduceSideJoin2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesOrderDataMapper extends Mapper<LongWritable, Text, ProductIdKey, JoinGenericWritable> {
	
	protected void map(LongWritable key, Text value, Context context) 
		throws IOException ,InterruptedException {
		
		String[] recordFields = value.toString().split("\\t");
        int productId = Integer.parseInt(recordFields[4]);
        int orderQty = Integer.parseInt(recordFields[3]);
        double lineTotal = Double.parseDouble(recordFields[8]);
        
        ProductIdKey recordKey = new ProductIdKey(productId, ProductIdKey.DATA_RECORD);
		SalesOrderDataRecord record = new SalesOrderDataRecord(orderQty,
				lineTotal);
        
        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
        context.write(recordKey, genericRecord);
		
	};
}
