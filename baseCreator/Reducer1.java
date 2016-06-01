package baseCreator;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import _core.AssociationRules;

/** Outputs [Single item, line numbers] in addition to writing into the non-user assets. */
public class Reducer1 extends Reducer<Text, Text, Text, Text> {

	private int i = 0;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		// utility to collect all the baskets an item is in
		SortedSet<Integer> itemLines = new TreeSet<Integer>(); // goes into singlets and lines
		
		for (Text line : values) {
			itemLines.add(Integer.parseInt(line.toString()));
		}
		
		// if supported
		if (itemLines.size() >= AssociationRules.MIN_SUPPORT) {
			// singletsIndexes
			AssociationRules.singletToIndex.put(key.toString(), i);
			// indexesSinglets
			AssociationRules.indexToSinglet.put(i, key.toString());
			// singletsAndLines
			AssociationRules.singletIndexToLines.put(i, itemLines);
			i++;
			// context
			String arg1 = itemLines.toString();
			arg1 = arg1.substring(1, arg1.length() - 1); // seperator will be ", "
			context.write(new Text(key), new Text(arg1));
		}
	}
	
}