### Yelp Data Analysis

Analyzed yelp data set to derive useful statistics about “business" and "review" entities.
• Data set was stored in Hadoop HDFS.
• Implemented Map Reduce java programs, Pig script and Mahout to do following analysis:
1. Inner Reduce Side Join - Since there were to data files, namely business.csv and review.csv and I required the data in one file to perform the map-reduce analysis, I implemented the Inner Reduce Side Join to produce single data set.
2. Simple Random Sampling - The type of filtering was used as I required a sub-sample of the both data sets to do the analysis.
3. Calculated average ratings for each business id using Map Reduce.
4. Counted number of businesses in a city using Map Reduce.
5. Calculated average ratings for each business id and listed top 20 using Pig script.
6. Item Based and User Based Recommendation using Mahout - Both the Recommendations, recommends items (in this case, businesses) based on the ratings.
