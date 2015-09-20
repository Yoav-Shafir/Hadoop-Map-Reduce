package join.reduceSideJoin;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.MRUtils;

public class CommentJoinMapper extends Mapper<Object, Text, Text, Text> {
	
	private Text outkey = new Text();
	private Text outvalue = new Text();
	
	protected void map(Object key, Text value, Context context) 
		throws IOException ,InterruptedException {
		
		// Parse the input string into a nice map
		Map<String, String> parsed = MRUtils.transformXmlToMap(value.toString());
		
		// get comment's User id.
		String userId = parsed.get("UserId");
		if (userId == null) {
			return;
		}
		
		// The foreign join key is the user ID
		outkey.set(userId);
		
		// Flag this record for the reducer and then output.
		// C - comment.
		outvalue.set("C" + value.toString());
		context.write(outkey, outvalue);
	};
}
