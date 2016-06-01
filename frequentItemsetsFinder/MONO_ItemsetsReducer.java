package frequentItemsetsFinder;

// general
import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

//Hadoop
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Project
import _core.AssociationRules;

/** This reducer adds non-frequent itemsets to the blacklist. 
 * In the next iteration, the mapper will deconsider itemsets with a subset in this blacklist. */
public class MONO_ItemsetsReducer extends Reducer<Text, Text, Text, Text> {
	
	public static enum Counters {
		MONO_LINES_COUNT
	}
	
	public void reduce(Text key, Iterable<Text> lines, Context context) throws IOException, InterruptedException {
		SortedSet<Integer> itemLines = new TreeSet<Integer>();
		for (Text line : lines) {
			itemLines.add(Integer.parseInt(line.toString()));
		}
		// filter
		if (itemLines.size() >= AssociationRules.MIN_SUPPORT) {
			String arg1 = itemLines.toString();
			arg1 = arg1.substring(1, arg1.length() - 1);
			context.write(new Text(key), new Text(arg1));
			context.getCounter(Counters.MONO_LINES_COUNT).increment(1);
		} else {
			AssociationRules.blacklist.add(stringToSet(key.toString()));
		}
	}
	
	private static SortedSet<String> stringToSet (String itemset) {
		SortedSet<String> result = new TreeSet<String>();
		String itemsetArray[] = itemset.split(", ");
		for (String item : itemsetArray) {
			result.add(item);
		}
		return result;
	}
	
}
