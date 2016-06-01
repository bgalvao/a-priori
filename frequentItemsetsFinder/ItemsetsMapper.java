package frequentItemsetsFinder;

// general
import java.io.IOException;
import java.util.Iterator;

// hadoop io
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// project
import _core.AssociationRules;

/**
 * Outputs [CANDIDATE SET, line].
 * This mapper takes the last item of a sorted itemset as a starting point.
 */
public class ItemsetsMapper extends Mapper<Text, Text, Text, Text> {

	public void map(Text key, Text lines, Context context) throws IOException, InterruptedException {

		// this is the current line of the file that we are at
		int[] currentLines = getCurrentLines(lines);
		String currentItemset = key.toString();
//		System.out.println("-----------------------------------------------------------------------------");
//		System.out.println("Generating next dimension itemsets from the itemset: {" + currentItemset + "}");

		// find the last item
		String itemset[] = currentItemset.split(", ");
		String lastItem = itemset[itemset.length - 1];
		int nextToLastIndex = AssociationRules.singletToIndex.get(lastItem) + 1;
		
		// for each singlet coming after
		for (int j = nextToLastIndex; j < AssociationRules.indexToSinglet.size(); j++) {
			// for each of the lines of the singlet
			Iterator<Integer> itr = AssociationRules.singletIndexToLines.get(j).iterator();
			int start = 0;
			while (itr.hasNext()) {
				int singletLine = itr.next();
				for (int i = start; i < currentLines.length; i++) {
					if (currentLines[i] == singletLine) { // if it finds at least one intersecting line, the new itemset is considered in the reducer
						String mapperOutputKey = currentItemset + ", " + AssociationRules.indexToSinglet.get(j);
						String mapperOutputValue = Integer.toString(currentLines[i]);
						context.write(new Text(mapperOutputKey), new Text(mapperOutputValue));
						start = i;
						break;
					}
				}
			}
		}
		
		// advantage of this for loop
		// - does not start over from the first singlet line
		// - does not start over from the first current line (with the statement "start = i")
	}
	
	/**Collects the basket keys from the file into an array.*/
	public int[] getCurrentLines(Text lines) {
		String aux[] = lines.toString().split(", ");
		int[] currentLines = new int[aux.length];
		for (int i = 0; i < aux.length; i++) {
			currentLines[i] = Integer.parseInt(aux[i]);
		}
		return currentLines;
	}

}
