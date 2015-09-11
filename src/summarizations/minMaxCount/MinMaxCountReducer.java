package summarizations.minMaxCount;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


// The reducer iterates through the values to find the minimum and maximum
// dates, and sums the counts.

public class MinMaxCountReducer extends 
	Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
	
	// initializing the custom Writable output result for each input group.
	private MinMaxCountTuple result = new MinMaxCountTuple();
	
	protected void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) 
		throws IOException, InterruptedException {
		
		// Initialize the custom Writable result object.
		result.setMin(null);
		result.setMax(null);
		result.setCount(0);
		int sum = 0;
		
		// Iterate through all input values for this key.
		for (MinMaxCountTuple val : values) {
			
			// If the value's min is less than the result's min
			// Set the result's min to value's.
			if (result.getMin() == null || val.getMin().compareTo(result.getMin()) < 0) {
				result.setMin(val.getMin());
			}
			
			// If the value's max is more than the result's max
			// Set the result's max to value's.
			if (result.getMax() == null || val.getMax().compareTo(result.getMax()) > 0) {
				result.setMax(val.getMax());
			}
			
			// Add to our sum the count for value
			sum += val.getCount();
		}
		
		// Set our count to the number of input values
		result.setCount(sum);
		context.write(key, result);
	};
}
