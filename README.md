# A-Priori-Algorithm-in-Hadoop
Big Data project for university. This is a university project coded in Java for Hadoop's MapReduce. The goal
is to extract association rules from a market-basket dataset using the A-Priori counting strategy. Implementation details are temporarily
all in the .pdf file - until the whole content is migrated this file.

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
