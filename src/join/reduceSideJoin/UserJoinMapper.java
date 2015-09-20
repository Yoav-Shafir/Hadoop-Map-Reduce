package join.reduceSideJoin;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.MRUtils;

public class UserJoinMapper extends Mapper<Object, Text, Text, Text> {
	
	private Text outkey = new Text();
	private Text outvalue = new Text();
	
	protected void map(Object key, Text value, Context context) 
		throws IOException ,InterruptedException {
		
		// Parse the input string into a nice map
		Map<String, String> parsed = MRUtils.transformXmlToMap(value.toString());
		
		String userId = parsed.get("Id");
		
		if (userId == null) {
			return;
		}
		
		// The foreign join key is the user ID
		outkey.set(userId);
		
		// Flag this record for the reducer and then output.
		// U = User.
		outvalue.set("U" + value.toString());
		context.write(outkey, outvalue);
	};
}
