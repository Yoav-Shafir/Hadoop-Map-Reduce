package filtering.distributedGrep;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// As this is a map-only job, there is no combiner or reducer. All output records will be
// written directly to the file system.

public class GrepMapper 
	extends Mapper<Object, Text, NullWritable, Text> {
	
	private String mapRegex = null;
	
	// use the setup function to retrieve the map regex from the job configuration.
	// see job configuration.
	public void setup(Context context) 
		throws IOException, InterruptedException {
		
		mapRegex = context.getConfiguration().get("mapregex");
	}
	
	protected void map(Object key, Text value, Context context) 
		throws IOException ,InterruptedException {
		
		// run the test against the regex pattern.
		if (value.toString().matches(mapRegex)) {
			context.write(NullWritable.get(), value);
		}
	};
}
