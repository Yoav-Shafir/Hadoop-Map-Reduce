package filtering.topten;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.MRUtils;

public class TopTenMapper
	extends Mapper<Object, Text, NullWritable, Text> {
	
	// Stores a map of user reputation to the record
	private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
	
	protected void map(Object key, Text value, Context context) 
		throws IOException ,InterruptedException {
		
		Map<String, String> parsed = MRUtils.transformXmlToMap(value.toString());
		
		String userId = parsed.get("Id");
		String reputation = parsed.get("Reputation");
		
		// A TreeMap is a subclass of Map that sorts on key. 
		// The default ordering of Integers is ascending.
		// Add the whole record to our map with the reputation as the key
		repToRecordMap.put(Integer.parseInt(reputation), new Text(value));
		
		// If we have more than ten records, remove the one with the lowest rep
		// As this tree map is sorted in descending order, the user with
		// the lowest reputation is the last key.
		if (repToRecordMap.size() > 10) {
			repToRecordMap.remove(repToRecordMap.firstKey());
		}
	};
	
	protected void cleanup(Context context) 
		throws IOException ,InterruptedException {
		
		// Output our ten records to the reducers with a null key
		for (Text record : repToRecordMap.values()) {
			context.write(NullWritable.get(), record);
		}
	};
}
