import sys
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS,\
    MatrixFactorizationModel, Rating
from math import sqrt


def loadMovieNames():
    movieNames = {}
    with open("u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def cosineSimilarity(target, reference):
    dotProduct = targetNorm = referNorm = 0.0
    for i in xrange(0, len(target)):
        dotProduct += target[i] * reference[i]
        targetNorm += target[i] * target[i]
        referNorm += reference[i] * reference[i]
    return dotProduct / (sqrt(targetNorm) * sqrt(referNorm))

conf = SparkConf().setMaster("local[*]")\
                  .setAppName("MovieRecommendation")
sc = SparkContext(conf = conf)

print("\nLoading movie names...")
nameDict = loadMovieNames()

rawData = sc.textFile("file:///home/hduser/projects/spark/movie-ratings/u.data")
# rawData is in form (userID, movieID, rating, timestamp),
# we don't need timestamp
rawRatings = rawData.map(lambda line: line.split()[ : -1])
# transform the raw rating data into the input form required
# by the ALS
ratings = rawRatings.map(lambda rating:
    Rating(rating[0], rating[1], float(rating[2])))
# train a model with 50 latent features(K = 50), 10
# iterations and a regularization factor of 0.01
K, iterations, regFactor = 50, 20, 0.01
model = ALS.train(ratings, K, iterations, regFactor)

# get top 10 recommended items for a user
#userID = sys.argv[1]
#top10Rec = model.recommendProducts(userID, 10)

# get top 10 similar movies for a movie
movieID = int(sys.argv[1])
movieMatrix = model.productFeatures()
reference = movieMatrix.lookup(movieID)[0]
movies = movieMatrix.filter(
        lambda movie: movie[0] != movieID)\
                    .mapValues(lambda target:\
                cosineSimilarity(target, reference))\
           .map(lambda movie: movie[::-1])\
           .top(10)
print("The top 10 similar movie to " + nameDict[movieID] + \
        "are:")
for movie in movies:
    print(nameDict[movie[1]] + "," + str(movie[0]))
