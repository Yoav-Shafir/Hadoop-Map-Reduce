package dataOrganization.structuredToHierarchical;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.MRUtils;

// there are two mapper classes, one for comments and one for
// posts. In both, we extract the post ID to use it as the output key. We output the input
// value prepended with a character ("P" for a post or "C" for a comment) so we know
// which data set the record came from during the reduce phase.

public class CommentMapper extends Mapper<Object, Text, Text, Text> {
	
	private Text outkey = new Text();
	private Text outvalue = new Text();
	
	public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
		
		Map<String, String> parsed = MRUtils.transformXmlToMap(value.toString());
		
		// The foreign join key is the post ID
		outkey.set(parsed.get("PostId"));
		
		// Flag this record for the reducer and then output
		outvalue.set("C" + value.toString());
		context.write(outkey, outvalue);
	}
}
