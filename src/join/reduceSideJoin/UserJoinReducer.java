package join.reduceSideJoin;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserJoinReducer extends Reducer<Text, Text, Text, Text> {

	Text user;
	private ArrayList<Text> listC = new ArrayList<Text>();
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
	
		// Clear our lists
		user = null;
		listC.clear();
		
		// iterate through all our values, binning each record based on what
		// it was tagged with
		// make sure to remove the tag!
		for (Text t : values) {
			// this is the User record.
			if (t.charAt(0) == 'U') {
				user = new Text(t.toString().substring(1));
			// Comment records.
			} else if (t.charAt(0) == 'C') {
				listC.add(new Text(t.toString().substring(1)));
			}
		}
		
		for (Text comment : listC) {
			context.write(user, comment);
		}
	};
	
	
}
