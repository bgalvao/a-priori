# A-Priori-Algorithm-in-Hadoop
Big Data project for university. This is a university project coded in Java for Hadoop's MapReduce. The goal
is to extract association rules from a market-basket dataset using the A-Priori counting strategy. Implementation details are temporarily
all in the .pdf file - until the whole content is migrated to this file.

# Introduction
The goal of this project is to implement the A­Priori counting algorithm to be able to efficiently compute Association Rules in a basket dataset. As one usually finds out when implementing anything in code, we faced new challenges that we would like to point out:
- How to efficiently generate candidate itemsets from the bottom­up, and reliably?
- How to compute the Association Rules with the implementation of Anti­Monotone property?

These will be addressed along the following pages. Below is a macro picture of our MapReduce design, 
composed by three Map­Reducers hereby named after the packages contained in our Java src:
- baseCreator
- frequentItemsetsFinder
- ruleExtractor

Furthermore, there are two extra user parameters: booleans MONO and ANTI_MONO. When set to true, they make the program use our mappers and reducers that respect the properties they are named after ­ and thus the mappers and reducers of our final solution. They are built upon the mappers and reducers that do not follow the properties ­ which are accessed when the booleans are set to false ­ but provided the basis for the final mappers and reducers to be built. We left it in the code as we wanted to easily make performance comparisons if necessary or just out of curiosity.

# baseCreator
We figured that the best way to carry information throughout the MapReducers was by the lines, as each line represents a basket that can be coded as an integer a basketkey. Thus the baseCreator.map( ) collects as value the line number as it scans the document and outputs as key any item that was included in that line. The baseCreator.reduce( ) then proceeds to gather
all of the basketkeys belonging to a single key (i.e. item) and only allows those items with a
sufficient number of basketkeys (i.e. above minimum support) to be written in the context.
It is also at this stage that one gathers and stores information necessary for the iterative MapReducer
that follows and they are all related to our solution for building itemsets:
- TreeMap indexToSinglet: a sorted map with the singlets corresponding to an index key
- TreeMap singletToIndex: another sorted map with the index key corresponding to the singlets that are supported in the dataset. The reverse of the one made above for convenience.
- HashMap singletIndexToLines: for a key (index representing a supported singlet), it stores the sorted set of the basketkeys,
containing therefore information about which baskets a singlet is included in.

# frequentItemsetsFinder

# ruleExtractor

The mapper collects the counts. The reducer collects the itemsets (as keys) and respective
counts to a HashMap ­ itemsetsAndCounts ­ that will be used in the cleanup, which in its turn,
computes the association rules.

The baseline strategy (i.e. when ANTI_MONO = false) is to iterate once through frequent
itemsets, build the powerset (if applicable) and, iterate the subsets of the itemset, lookup the
subset in itemsetsAndCounts and compute the confidence. The problem is with the last loop:
it iterates through the subsets in a way that puts as many items on the right­hand side (RHS)
of a rule first, hence going completely against the anti­monotone property.

To implement a smarter reducer­cleanup ­ accessed when ANTI_MONO = true ­ the baseline strategy was adapted to iterate through the subsets in a way that puts as few items on the RHS
first. To this end, the set of “RHS itemsets“ is collected into a SortedMap, where the key is the
size of the RHS­itemsets. The next step is to iterate through the keySet() of this map
and iterate through the RHS­itemsets that have size = key.

Similarly to the process of finding frequent itemsets, a Set blacklistedRHS is kept, comprised of RHS­itemsets that do not satisfy minimum confidence. When iterating through RHS­itemsets and finding one that produces a low confidence rule this RHS­itemset will be stored in the in blacklistedRHS. At the next size of RHS­itemsets, it will check if each new RHS­itemset has a subset present in this blacklist. This way, respect to the anti­monotone property is granted. This addresses our approach to the second challenge pointed out.
