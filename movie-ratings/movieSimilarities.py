import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

def loadMovieNames():
    movieNames = {}
    with open("u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def removeDuplicate(tuplePair):
    p1, p2 = tuplePair[1]
    return p1[0] < p2[0]

def lookupSimilarMovie(pairRating):
    m1, m2 = pairRating[0]
    score, count = pairRating[1]
    return (m1 == movieID or m2 == movieID) and \
        score > scoreThreshold and \
        count > coOccurenceThreshold

def generateMoviePair(data):
    movie1, rating1 = data[1][0]
    movie2, rating2 = data[1][1]
    return ((movie1, movie2), (rating1, rating2))

def cosineSimilarity(ratingPairs):
    sumXX = sumYY = sumXY = 0
    for r1, r2 in ratingPairs:
        sumXX += r1 * r1
        sumYY += r2 * r2
        sumXY += r1 * r2
    score = 0
    denominator = float(sqrt(sumXX) * sqrt(sumYY))
    if denominator:
        score = sumXY / denominator
    return (score, len(ratingPairs))

conf = SparkConf().setMaster("local[*]").setAppName("MovieSimilarities")
#conf = SparkConf().setAppName("MovieSimilarities")
sc = SparkContext(conf = conf)

data = sc.textFile("file:///home/hduser/projects/spark/movie-ratings/u.data")

print("\nLoading movie names...")
nameDict = loadMovieNames()

# Map ratings to key / value pairs: user ID => movie ID, rating
ratingsByMovie = data.map(lambda line: line.split())\
                     .map(lambda parts:
                            (
                                int(parts[0]),
                                (
                                    int(parts[1]),
                                    float(parts[2])
                                )
                            ))
ratingPartitioned = ratingsByMovie.partitionBy(100)
ratingsByMoviePair = ratingsByMovie.join(ratingPartitioned)\
                                   .filter(removeDuplicate)
# At this point our RDD consists of
#   userID => ((movieID, rating), (movieID, rating))
# Now key by
#   (movie1, movie2) => (rating1, rating2) pairs.
moviePairRatingPairs = ratingsByMoviePair.map(generateMoviePair)
# We now have (movie1, movie2) => (rating1, rating2)
# Now collect all ratings for each movie pair and compute similarity
moviePairRatings = moviePairRatingPairs.groupByKey()
# We now have
#   (movie1, movie2) => (rating1, rating2), (rating1, rating2) ...
# Can now compute similarities.
moviePairSimilarities = moviePairRatings.mapValues(cosineSimilarity).cache()

scoreThreshold = 0.97
coOccurenceThreshold = 50

movieID = int(sys.argv[1])

lookupResults = moviePairSimilarities.filter(lookupSimilarMovie)

results = lookupResults.map(
        lambda pairSim: (pairSim[1], pairSim[0]))\
                       .sortByKey(ascending=False).take(10)

print("Top 10 similar movies for " + nameDict[movieID])
for result in results:
    (sim, pair) = result
    # Display the similarity result that isn't the movie we're looking at
    similarMovieID = pair[0]
    if (similarMovieID == movieID):
        similarMovieID = pair[1]
        print(nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))



