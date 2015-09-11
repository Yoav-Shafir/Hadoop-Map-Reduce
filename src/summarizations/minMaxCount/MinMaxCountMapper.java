package summarizations.minMaxCount;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import utils.MRUtils;

// The mapper will preprocess our input values by extracting the XML attributes
// from each input record: the creation data and the user identifier.

public class MinMaxCountMapper extends 
	Mapper<Object, Text, Text, MinMaxCountTuple>{
	
	// will be the mapper output key.
	private Text outUserId = new Text();
	// will be the mapper output value.
	private MinMaxCountTuple outTuple = new MinMaxCountTuple();
	
	// This object will format the creation date string into a Date object
	private final static SimpleDateFormat frmt =
		new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
	
	// key -> input key.
	// value -> input value.
	protected void map(Object key, Text value, Context context) 
		throws IOException ,InterruptedException {
		
		// xml to Map.
		Map<String, String> parsed = MRUtils.transformXmlToMap(value.toString());
		
		// Grab the "CreationDate" field since it is what we are finding
		// the min and max value of
		String strDate = parsed.get("CreationDate");
		// Grab the “UserID” since it is what we are grouping by
		String userId = parsed.get("UserId");
		
		// Parse the string into a Date object
		Date creationDate = null;
		try {
			creationDate = frmt.parse(strDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		// fields are stored in a custom Writable object of type MinMaxCountTuple.
		
		// important! - why the date is output twice?
		// so that we can take advantage of the combiner optimization.
		// if we have user with id 12345 and we have the following records:
		// userId	minimum	maximum count
		// 12345	10		10		1
		// 12345	8		8		1
		// 12345	21		21		1
		
		// we can use the reducer logic as a combiner
		// and aggregate the data in the mapper level,
		// making the reducer job easier.
		// userId	minimum	maximum count
		// 12345	8		21		3
		
		// Set the minimum and maximum date values to the creationDate.
		outTuple.setMin(creationDate);
		outTuple.setMax(creationDate);
		// Set the comment count to 1.
		outTuple.setCount(1);
		// Set user ID as the output key.
		outUserId.set(userId);
		
		context.write(outUserId, outTuple);
	};
}
