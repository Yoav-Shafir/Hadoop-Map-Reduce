package summarizations.invertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// The reducer iterates through the set of input values and appends each row
// ID to a String, delimited by a space character. The input key is output along with this
// concatenation.

public class ConcatenatorReducer extends Reducer<Text, Text, Text, Text>{
	private Text result = new Text();
	
	protected void reduce(Text key, Iterable<Text> values, Context context) 
		throws IOException ,InterruptedException {
		
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		
		for (Text id : values) {
			if (first) {
				first = false;
			} else {
				sb.append(" ");
			}
			sb.append(id.toString());
		}
		
		result.set(sb.toString());
		context.write(key, result);
	};
}
