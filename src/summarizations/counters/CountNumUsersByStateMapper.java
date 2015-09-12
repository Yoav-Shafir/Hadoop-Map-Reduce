package summarizations.counters;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.MRUtils;

// The mapper reads each user record and gets his or her location. The location
// is split on white space and searched for something that resembles a state.

public class CountNumUsersByStateMapper 
	extends Mapper<Object, Text, NullWritable, NullWritable> {
	
	// counters.
	public static final String STATE_COUNTER_GROUP = "State";
	public static final String UNKNOWN_COUNTER = "Unknown";
	public static final String NULL_OR_EMPTY_COUNTER = "Null or Empty";
	
	// list of all states we we care about.
	private String[] statesArray = new String[] { "AL", "AK", "AZ", "AR",
			"CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
			"IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
			"MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND",
			"OH", "OK", "OR", "PA", "RI", "SC", "SF", "TN", "TX", "UT",
			"VT", "VA", "WA", "WV", "WI", "WY" };
	
	private HashSet<String> states = new HashSet<String>(Arrays.asList(statesArray));
	
	protected void map(Object key, Text value, Context context) 
		throws IOException ,InterruptedException {
		
		Map<String, String> parsed = MRUtils.transformXmlToMap(value.toString());
		
		// Get the value for the Location attribute
		String location = parsed.get("Location");
		
		// Look for a state abbreviation code if the
		// location is not null or empty
		if (location != null && !location.isEmpty()) {
			
			// Make location uppercase and split on white space
			String[] tokens = location.toUpperCase().split("\\s");
			
			// If a state is recognized, the counter for the 
			// state is incremented by one and the loop is broken.
			
			// For each token
			boolean unknown = true;
			for (String state : tokens) {
				// Check if it is a state
				if (states.contains(state)) {
					// If so, increment the state's counter by 1
					// and flag it as not unknown
					context.getCounter(STATE_COUNTER_GROUP, state).increment(1);
					unknown = false;
					break;
				}
			}
			
			// If the state is unknown, increment the UNKNOWN_COUNTER counter
			if (unknown) {
				context.getCounter(STATE_COUNTER_GROUP, UNKNOWN_COUNTER).increment(1);
			}
		} else {
			// If it is empty or null, increment the
			// NULL_OR_EMPTY_COUNTER counter by 1
			context.getCounter(STATE_COUNTER_GROUP,
			NULL_OR_EMPTY_COUNTER).increment(1);
		}
		
	}
	
}
