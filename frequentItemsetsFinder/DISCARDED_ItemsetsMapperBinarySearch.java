package frequentItemsetsFinder;

// general
import java.io.IOException;

// hadoop io
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// project
import _core.AssociationRules;

/**
 * Same fashion: will output [CANDIDATE SET, line]. The next reducer gathers the
 * lines into a count and filters. This mapper will take the last item of a sorted itemset as a starting point.
 * More elaborate explanation is in the report.
 */
public class DISCARDED_ItemsetsMapperBinarySearch extends Mapper<Text, Text, Text, Text> {

	public void map(Text key, Text lines, Context context) throws IOException, InterruptedException {

		// this is the current line of the file that we are at
		int[] currentLines = getCurrentLines(lines);
		String currentItemset = key.toString();

		// find the last item
		String itemset[] = currentItemset.split(", ");
		String lastItem = itemset[itemset.length - 1];
		int nextToLastIndex = AssociationRules.singletToIndex.get(lastItem) + 1;

		// auxiliary for understanding
		// System.out.println("Looking up potential itemsets staring from the item: " + AssRules.indexToSinglets.get(nextToLastIndex));
		// System.out.println(AssRules.indexToSinglets);

		// Find intersecting lines
		// if it finds at least one intersecting line, the new itemset is considered in the reducer
		for (Integer n = nextToLastIndex; n < AssociationRules.indexToSinglet.size(); n++) {
			for (int i = 0; i < currentLines.length; i++) { // where i denotes a current line
				int size = AssociationRules.singletIndexToLines.get(n).size();
				Integer[] linesOfSinglet = AssociationRules.singletIndexToLines.get(n).toArray(new Integer[size]);
				if (isIn(linesOfSinglet, currentLines[i]) != -1) {
					String mapperOutputKey = currentItemset + ", " + AssociationRules.indexToSinglet.get(n);
					String mapperOutputValue = Integer.toString(currentLines[i]);
					context.write(new Text(mapperOutputKey), new Text(mapperOutputValue));
				}
			}
		}
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
	
	/** Binary search technique adapted. To be seen as "if key is in A, then return A[imid] = k; else return -1.*/
	public static int isIn(Integer[] A, int key) {
		int result = -1;
		int imin = 0;
		int imax = A.length - 1;
		while (imax >= imin) {
			int imid = (imin + imax) / 2; // System.out.println(imin + "\t" + imid + "\t" + imax);
			if (key == A[imid]) {
				result = A[imid];
				break;
			} else if (key < A[imid]) {
				imax = imid - 1;
			} else if (key > A[imid]) {
				imin = imid + 1;
			}
		}
		return result;
	}

}
