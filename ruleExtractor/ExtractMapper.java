package ruleExtractor;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import _core.AssociationRules;

/**Extracts the count and passes to the reducer.*/
public class ExtractMapper extends Mapper<Text, Text, Text, Text> {

	public void setup(Context context) throws IOException, InterruptedException {
		// no longer necessary
		AssociationRules.indexToSinglet.clear();
		AssociationRules.singletToIndex.clear();
		AssociationRules.singletIndexToLines.clear();
		AssociationRules.blacklist.clear();
	}
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		int count = value.toString().split(", ").length;
		String val = Integer.toString(count);
		context.write(new Text(key), new Text(val));
	}
	
}
