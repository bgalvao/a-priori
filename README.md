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
# frequentItemsetsFinder
# ruleExtractor
