package ruleExtractor;

// java
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

// hadoop
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// google
import com.google.common.collect.Sets;

// project package
import _core.AssociationRules;

public class ExtractReducerPowersetLookup extends Reducer<Text, Text, Text, Text> {

	static HashMap<String, Integer> itemsetsAndCounts = new HashMap<String, Integer>();
	
	/** This reducing method is limited to reading all the data into main memory - itemsetsAndCounts */
	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		// collecting all into a hashmap
		int count = 0;
		for (Text t : value) {
			count += Integer.parseInt(t.toString());
		}
		itemsetsAndCounts.put(key.toString(), count);
	}

	/** The cleanup handles the rule extraction. */
 	public void cleanup(Context context) throws IOException, InterruptedException {
 		
		Map<String, Double> confidentMap = new HashMap<>();
		
		// loop over all of the itemsets only once
		for (String k1 : itemsetsAndCounts.keySet()) {
			SortedSet<String> itemset = keyToSet(k1);
			
			// if eligible to build subsets from
			if (itemset.size() > 1) {
				
				// then get the powerset
				Set<Set<String>> subsets = Sets.powerSet(itemset);
				
				// and loop over its subsets
				Iterator<Set<String>> subtr = subsets.iterator();
				while (subtr.hasNext()) {
					
					// sort the elements to look up
					SortedSet<String> sortedSubset = new TreeSet<String>(subtr.next());
				
					// if a non empty set and different than the full set
					if (sortedSubset.size() > 0 && sortedSubset.size() < itemset.size()) {
						
						// Retrieve the count of itemset
						int count1 = itemsetsAndCounts.get(k1);
						
						// Retrieve the count of subset
						String k2 = sortedSubset.toString().substring(1, sortedSubset.toString().length() - 1);
						int count2 = itemsetsAndCounts.get(k2);
						
						// confidence
						double conf = (double) count1 / count2;
						if (conf > AssociationRules.MIN_CONF) {
							String keyRule = "{" + k2 + "} -> {" + getTarget(itemset, sortedSubset) + "}";
							confidentMap.put(keyRule, conf);
						}
						
					} // if non-empty set and different than the full set 
					
				} // loop over the subsets
				
			} // if eligible to build subsets from
			
		} // for (String k1 : itemsetsAndCounts.keySet())
		
		// writing in the final output
		Map<String, Double> sortedMap = sortByValues(confidentMap);
		for (String k : sortedMap.keySet()) {
			context.write(new Text(k), new Text(Double.toString(sortedMap.get(k))));
		}
		
	}

	/**Takes the key and converts it to a set for convenience of some operations*/
	private static SortedSet<String> keyToSet (String keySetOfCount) {
		SortedSet<String> result = new TreeSet<String>();
		String itemset[] = keySetOfCount.split(", ");
		for (String item : itemset) {
			result.add(item);
		}
		return result;
	}

	/**Retrieves the target of the association rule. By "target", one means {AD} in the rule "{BC} -> {AD}"*/
	private static String getTarget(SortedSet<String> itemset, SortedSet<String> subset) {
		String result = "";
		SortedSet<String> copy = new TreeSet<String>(itemset);
		copy.removeAll(subset);
		result = copy.toString();
		result = result.substring(1, result.length() - 1);
		return result;
	}
	
	/**Sorts the map by values. Taken from: http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html*/ 
	private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
		List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());
		Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {
			@Override
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});
		// LinkedHashMap will keep the keys in the order they are inserted
		// which is currently sorted on natural ordering
		Map<K, V> sortedMap = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

}