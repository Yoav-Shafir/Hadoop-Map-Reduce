package summarizations.invertedIndex;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.MRUtils;

public class WikipediaExtractorMapper extends 
	Mapper<Object, Text, Text, Text> {
	
	private Text link = new Text();
	private Text outkey = new Text();
	
	protected void map(Object key, Text value, Context context) 
		throws IOException ,InterruptedException {
		
		// XML attributes for the text, post type, and row ID are extracted.
		Map<String, String> parsed = MRUtils.transformXmlToMap(value.toString());
		
		// Grab the necessary XML attributes
		String txt = parsed.get("Body");
		String posttype = parsed.get("PostTypeId");
		String row_id = parsed.get("Id");
		
		// if the body is null, or the post is a question (1), skip,
		// else, we parse the text to find a Wikipedia URL
		if (txt == null || (posttype != null && posttype.equals("1"))) {
			return;
		}
		
		// Unescape the HTML because the SO data is escaped.
		// If a URL is found, the URL is output as the key and the row ID is output as the value.
		txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());
		link.set(MRUtils.getWikipediaURL(txt));
		outkey.set(row_id);
		context.write(link, outkey);
	};
}
