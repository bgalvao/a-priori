package frequentItemsetsFinder;

// general
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

// hadoop io
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// google
import com.google.common.collect.Sets;

// project
import _core.AssociationRules;

/**
 * Outputs [CANDIDATE SET, line]. This mapper takes the last item of a sorted itemset as a starting point.
 * This mapper ensures respecting monotonic property by consulting a blacklist the following way:
 *  if the blacklist includes one subset of the candidate set, then candidate set is infrequent.
 */
public class MONO_ItemsetsMapper extends Mapper<Text, Text, Text, Text> {

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
			
			// build candidate set
			SortedSet<String> candidateSet = stringToSet(currentItemset + ", " + AssociationRules.indexToSinglet.get(j));
//			System.out.println("Candidate Set: " + candidateSet);
			
			// Ensure monotone property
			boolean blacklistedSubset = false;
			Set<Set<String>> powerset = Sets.powerSet(candidateSet);
			Iterator<Set<String>> ptr = powerset.iterator();
			while (ptr.hasNext()) {
				Set<String> next = ptr.next();
				if (AssociationRules.blacklist.contains(next)) {
					blacklistedSubset = true;
					break;
				}
			}
			
			// Map the lines of approved candidate set
			if (blacklistedSubset == false) {
				// for each of the lines of the singlet
				Iterator<Integer> itr = AssociationRules.singletIndexToLines.get(j).iterator();
				int start = 0;
				while (itr.hasNext()) {
					int singletLine = itr.next();
					for (int i = start; i < currentLines.length; i++) {
						if (currentLines[i] == singletLine) { // if it finds at least one intersecting line, the new itemset is considered in the reducer
							String mapperOutputValue = Integer.toString(currentLines[i]);
							context.write(new Text(setToString(candidateSet)), new Text(mapperOutputValue));
							start = i;
							break;
						}
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

	private static SortedSet<String> stringToSet (String itemset) {
		SortedSet<String> result = new TreeSet<String>();
		String itemsetArray[] = itemset.split(", ");
		for (String item : itemsetArray) {
			result.add(item);
		}
		return result;
	}
	
	private static String setToString (SortedSet<String> itemset) {
		String result = itemset.toString();
		result = result.substring(1, result.length() - 1);
		return result;
	}
	
}
