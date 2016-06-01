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

/** Same fashion as Reducer1 */
public class ItemsetsReducer extends Reducer<Text, Text, Text, Text> {
	
	public static enum Counters {
		LINES_COUNT
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
			context.getCounter(Counters.LINES_COUNT).increment(1);
		}
	}
}
