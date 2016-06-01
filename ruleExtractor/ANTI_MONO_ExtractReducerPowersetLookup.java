package ruleExtractor;

// java
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

// hadoop
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// google
import com.google.common.collect.Sets;

// project package
import _core.AssociationRules;

/***/
public class ANTI_MONO_ExtractReducerPowersetLookup extends Reducer<Text, Text, Text, Text> {

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
			SortedSet<String> itemset = stringToSet(k1);
			
			// if eligible to build subsets from
			if (itemset.size() > 1) {
				
				SortedMap<Integer, Set<SortedSet<String>>> RHSbySize = new TreeMap<Integer, Set<SortedSet<String>>>();
				
				// loop over its subsets once
				// to get all possible RHS rules and collect into SortedMap RHSbySize
				Set<Set<String>> subsets = Sets.powerSet(itemset);
				Iterator<Set<String>> subtr = subsets.iterator();
				while (subtr.hasNext()) {
					SortedSet<String> newRHS = getRHSasSet(itemset, subtr.next());
					int size = newRHS.size();
					if (size > 0 && size < itemset.size()) {
						if (RHSbySize.containsKey(size)) {
							RHSbySize.get(size).add(newRHS);
						} else {
							Set<SortedSet<String>> newSet = new HashSet<SortedSet<String>>();
							newSet.add(newRHS);
							RHSbySize.put(size, newSet);
						}
					}
				}
				
				// Now loop over the right hand side rules by size (increasing!)
				Set<SortedSet<String>> blacklistedRHS = new HashSet<SortedSet<String>>();
				Iterator<Integer> sizr = RHSbySize.keySet().iterator(); // the keySet() is the size of RHS
				while (sizr.hasNext()) {
					
					// for each RHS rule of the previously given size
					Iterator<SortedSet<String>> rhsr = RHSbySize.get(sizr.next()).iterator();
					while (rhsr.hasNext()) {
						SortedSet<String> nextRHS = rhsr.next();
						
						// first check if the rule is not pruned
						boolean prunedFlag = false;
						Iterator<SortedSet<String>> blkr = blacklistedRHS.iterator();
						while (blkr.hasNext()) {
							if (nextRHS.containsAll(blkr.next())) {
								prunedFlag = true;
							}
						}
						
						if (prunedFlag) {System.out.println("Skipping pruned rule");}
						
						// If rule is not pruned, then do the thing!
						if (prunedFlag != true) {
							// get count of itemset
							int count1 = itemsetsAndCounts.get(k1);
							// get count of subset
							String k2 = setToString(getSubset(itemset, nextRHS));
							int count2 = itemsetsAndCounts.get(k2);
							// confidence
							double conf = (double) count1 / count2;
							if (conf >= AssociationRules.MIN_CONF) {
								String keyRule = "{" + k2 + "} -> {" + setToString(nextRHS) + "}";
								confidentMap.put(keyRule, conf);
							} else {
								blacklistedRHS.add(nextRHS);
							}
						}
						
					} // rshr	
					
				} // sizr
			
			} // if eligible
		
		} // itemsets looper
				
		// writing in the final output
		Map<String, Double> sortedMap = sortByValues(confidentMap);
		for (String k : sortedMap.keySet()) {
			context.write(new Text(k), new Text(Double.toString(sortedMap.get(k))));
		}
				
 	} // cleanup

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
 	
 	private static SortedSet<String> getSubset (SortedSet<String> itemset, SortedSet<String> rhs) {
 		SortedSet<String> itemsetCopy = new TreeSet<String>(itemset);
 		SortedSet<String> rhsCopy = new TreeSet<String>(rhs);
 		itemsetCopy.removeAll(rhsCopy);
 		return itemsetCopy;
 	}
 	
	private static SortedSet<String> getRHSasSet(Set<String> itemset, Set<String> subset) {
		SortedSet<String> copy = new TreeSet<String>(itemset);
		copy.removeAll(subset);
		return copy;
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