package filtering.topten;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.MRUtils;

// Because we configured our job to have one reducer using
// job.setNumReduceTasks(1) and we used NullWritable as our key, there will be one
// input group for this reducer that contains all the potential top ten records.

public class TopTenReducer 
	extends Reducer<NullWritable, Text, NullWritable, Text> {
	
	// Stores a map of user reputation to the record
	// Overloads the comparator to order the reputations in descending order
	private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
	
	protected void reduce(NullWritable key, Iterable<Text> values, Context context) 
		throws java.io.IOException ,InterruptedException {
		
		for (Text record : values) {
			Map<String, String> parsed = MRUtils.transformXmlToMap(record.toString());
			
			repToRecordMap.put(Integer.parseInt(parsed.get("Reputation")), new Text(record));
			// If we have more than ten records, remove the one with the lowest rep
			// As this tree map is sorted in descending order, the user with
			// the lowest reputation is the last key.
			if (repToRecordMap.size() > 10) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}
		
		for (Text record : repToRecordMap.descendingMap().values()) {
			// Output our ten records to the file system with a null key
			context.write(NullWritable.get(), record);
		}	
	};
}
