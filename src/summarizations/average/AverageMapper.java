package summarizations.average;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.MRUtils;

public class AverageMapper extends Mapper<Object, Text, IntWritable, CountAverageTuple> {
	
	private IntWritable outHour = new IntWritable();
	private CountAverageTuple outCountAverage = new CountAverageTuple();
	
	private final static SimpleDateFormat frmt = new SimpleDateFormat(
		"yyyy-MM-dd'T'HH:mm:ss.SSS");
	
	@SuppressWarnings("deprecation")
	protected void map(Object key, Text value, Context context) 
		throws IOException ,InterruptedException {
		
		// Parse the input string into a nice map
		Map<String, String> parsed = MRUtils.transformXmlToMap(value.toString());
	
		// Grab the "CreationDate" field,
		// since it is what we are grouping by
		String strDate = parsed.get("CreationDate");

		// Grab the comment to find the length
		String text = parsed.get("Text");

		// .get will return null if the key is not there
		if (strDate == null || text == null) {
			// skip this record
			return;
		}
		
		try {
			// get the hour this comment was posted in
			Date creationDate = frmt.parse(strDate);
			outHour.set(creationDate.getHours());
			
			// get the comment length
			outCountAverage.setCount(1);
			outCountAverage.setAverage(text.length());
			
			// write out the user ID with min max dates and count
			context.write(outHour, outCountAverage);
		} catch (ParseException e) {
			System.err.println(e.getMessage());
			return;	
		}
	};
}
