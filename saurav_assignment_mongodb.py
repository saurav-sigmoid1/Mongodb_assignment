import json
from bson import ObjectId
from pymongo import MongoClient
from bson.json_util import loads, dumps

#
client = MongoClient('localhost', 27017)
db = client.get_database('Mflix')


# print(db)
# ======================inserting bulk data from json file to mongodb collection========================================
# collection = db.get_collection('movies')
# data = []
# for line in open("sample_mflix/movies.json"):
# 	if line:
# 		# data.append(json.loads(line))
# 		new_data = json.loads(line)
# 		json_str = dumps(new_data)
# 		record2 = loads(json_str)
# 		data.append(record2)
# #
# for line in data[:2]:
#     print(line)
	# collection.insert_one(line)


# ========================inserting data into mongodb collection  using python method===================================


# def insert_users(new_data):
#     print(new_data)
#     user_collection = db.get_collection('users')
#     print(user_collection)
#     x = user_collection.insert_one(new_data)
#     print("inserted_id ", x.inserted_id)
#
#
# def insert_in_comments(new_data):
#     comments_collection = db.get_collection('comments')
#     x = comments_collection.insert_one(new_data)
#     print("inserted_id ", x.inserted_id)
#
#
# def insert_in_theaters(new_data):
#     theater_collection = db.get_collection('theaters')
#     x = theater_collection.insert_one(new_data)
#     print("inserted_id ", x.inserted_id)
#
#
# def insert_in_movies(new_data):
#     movies_collection = db.get_collection('movies')
#     x = movies_collection.insert_one(new_data)
#     print("inserted_id ", x.inserted_id)
#
#
# user_data = {
#     "_id" : ObjectId("59b99db4cfa9a34dcd7885b6"),
#     "name" : "Ned Stark",
#     "email" : "sean_bean@gameofthron.es",
#     "password" : "$2b$12$UREFwsRUoyF0CRqGNK0LzO0HM/jLhgUCNNIJ9RJAqMUQ74crlJ1Vu"
# }
# # insert_users(user_data)
#
# comment_data = {
#     "_id" : ObjectId("5a9427648b0beebeb69579cc"),
#     "name" : "Andrea Le",
#     "email" : "andrea_le@fakegmail.com",
#     "movie_id" : ObjectId("573a1390f29313caabcd418c"),
#     "text" : "Rem officiis eaque repellendus amet eos doloribus. Porro dolor voluptatum voluptates neque culpa molestias. Voluptate unde nulla temporibus ullam.",
#     "date" : ISODate("2012-03-26T23:20:16.000Z")
# }
# # insert_in_comments(comment_data)
#
# theater_data = {
#     "_id" : ObjectId("59a47286cfa9a3a73e51e72c"),
#     "theaterId" : 1000,
#     "location" : {
#         "address" : {
#             "street1" : "340 W Market",
#             "city" : "Bloomington",
#             "state" : "MN",
#             "zipcode" : "55425"
#         },
#         "geo" : {
#             "type" : "Point",
#             "coordinates" : [
#                 -93.24565,
#                 44.85466
#             ]
#         }
#     }
# }
# # insert_in_theaters(theater_data)
# #
# movie_data = {
#     "_id" : ObjectId("573a1390f29313caabcd4135"),
#     "plot" : "Three men hammer on an anvil and pass a bottle of beer around.",
#     "genres" : [
#         "Short"
#     ],
#     "runtime" : 1,
#     "cast" : [
#         "Charles Kayser",
#         "John Ott"
#     ],
#     "num_mflix_comments" : 1,
#     "title" : "Blacksmith Scene",
#     "fullplot" : "A stationary camera looks at a large anvil with a blacksmith behind it and one on either side. The smith in the middle draws a heated metal rod from the fire, places it on the anvil, and all three begin a rhythmic hammering. After several blows, the metal goes back in the fire. One smith pulls out a bottle of beer, and they each take a swig. Then, out comes the glowing metal and the hammering resumes.",
#     "countries" : [
#         "USA"
#     ],
#     "released" : ISODate("1893-05-09T00:00:00.000Z"),
#     "directors" : [
#         "William K.L. Dickson"
#     ],
#     "rated" : "UNRATED",
#     "awards" : {
#         "wins" : 1,
#         "nominations" : 0,
#         "text" : "1 win."
#     },
#     "lastupdated" : "2015-08-26 00:03:50.133000000",
#     "year" : 1893,
#     "imdb" : {
#         "rating" : 6.2,
#         "votes" : 1189,
#         "id" : 5
#     },
#     "type" : "movie",
#     "tomatoes" : {
#         "viewer" : {
#             "rating" : 3,
#             "numReviews" : 184,
#             "meter" : 32
#         },
#         "lastUpdated" : ISODate("2015-06-28T18:34:09.000Z")
#     }
# }
# print(movies_data)
# insert_in_movies(movies_data)

# =============== Query part of assignment===============================================================================

# query based on comment collection

# comment = db.get_collection('comments')
# # print(comment)

# print("_________________top 10 users who made the maximum number of comments___________________")
# top_10_user = comment.aggregate([
#     {"$project": {"_id": 0, "name": 1, 'email': 1}},
#     {"$group": {"_id": {"email": "$email"}, "name": {"$first": "$name"}, "total_comment": {"$sum": 1}}},
#     {"$sort": {"total_comment": -1}},
#     {"$project": {"_id": 0, "name": 1, "total_comment": 1}},
#     {"$limit": 10}
# ])
# for item in list(top_10_user):
#     print(item)

# print("_________________Top 10 movies with most comments________________________________________")
# most_commented_movie = comment.aggregate([
#     {"$project": {"_id": 0, "movie_id": 1}},
#     {"$group": {"_id": {"movie_id": "$movie_id"}, "comment_on_movie": {"$sum": 1}}},
#     {"$sort": {"comment_on_movie": -1}},
#     {"$project": {"comment_on_movie": 1}},
#     {"$limit": 10}
# ])
#
# for item in list(most_commented_movie):
#     print(item)
#
#
# print("_________________Total comment in a month in year________________________________________")
# total_comment_in_each_month_in_a_year=comment.aggregate([
#     {"$project":{"_id":0,"month":{"$month":"$date"},"year":{"$year":"$date"},"name":1,}},
#     {"$group":{"_id":{"year":"$year","month":"$month"},"total_comment_in_month":{"$sum":1}}},
#     {"$sort":{"_id":1}}
#     ])
# for item in list(total_comment_in_each_month_in_a_year):
#     print(item)


#Query for the movies section
# movies = db.get_collection('movies')

# 1.1
# print("======================Movies_with_hight_imdb_rating=========================================")
# movies_with_hight_imdb_rating=movies.aggregate([
#   {"$project":{"_id":0,"title":1,"year":1,"rating":{"$convert":{"input":"$imdb.rating","to":"double","onError":0.00}}}},
#    {"$sort":{"rating":-1}},
#    {"$limit":10}
#   ])
#
# for item in movies_with_hight_imdb_rating:
# 	print(item)

# 1.2
# print("======================Highest IMDB rating in a given year=========================================")
# highest_IMDB_rating_in_a_given_year=movies.aggregate([
#   {"$match":{"year":2014}},
#   {"$project":{"_id":0,"title":1,"rating":{"$convert":{"input":"$imdb.rating","to":"double","onError":0.00}}}},
#   {"$sort":{"rating":-1}},
#   {"$limit":10}
#   ])
# for item in highest_IMDB_rating_in_a_given_year:
# 	print(item)

# 1.3

# print("======================Highest IMDB rating with number of votes>1000=================================")
# highest_IMDB_rating_with_number_of_votes_gt_1000=movies.aggregate([
#   {"$project":{"_id":0,"voting":{"$convert":{"input":"$imdb.votes","to":"int","onError":0}},"rating":{"$convert":{"input":"$imdb.rating","to":"double","onError":0.00}}}},
#   {"$sort":{"rating":-1}},
#   {"$match":{"voting":{"$gt":1000}}},
#   {"$group":{"_id":{"vote":"$voting"},"max_rating":{"$max":"$rating"}}},
#   {"$sort":{"_id.vote":-1}},
#   {"$limit":10},
#   ])
# for item in highest_IMDB_rating_with_number_of_votes_gt_1000:
# 	print(item)

# 1.4
# print("======================Title matching a given pattern sorted by highest tomatoes ratings===============")
# title_matching_pattern_sort_highest_to_ratings=movies.aggregate([
#   {"$match":{"title":{"$regex":'Class'}}},
#   {"$project":{"_id":0,"title":1,"to_rating":{"$convert":{"input":"$tomatoes.viewer.rating","to":"double","onError":0.0,"onNull":0.0}}}},
#   {"$sort":{"to_rating":-1}},
#   {"$limit":10}
# ])
# for item in title_matching_pattern_sort_highest_to_ratings:
# 	print(item)


# 2.1
# print("======================directors created the maximum number of movies===============")
# dr_created_the_maximum_number_of_movies=movies.aggregate([
#      {"$unwind":"$directors"},
#      {"$project":{"_id":0,"movie_dir":{"$convert":{"input":"$directors","to":"string","onError":"unkonwn","onNull":"unkonwn"}},"title":1}},
#      {"$group":{"_id":{"direct":"$movie_dir"},"total_movies":{"$sum":1}}},
#      {"$sort":{"total_movies":-1}},
#      {"$limit":10}
#     ])
# for item in dr_created_the_maximum_number_of_movies:
# 	print(item)

# 2.2
# print("==============directors created the maximum number of movies in a given year==========")
# dr_created_the_maximum_number_of_movies_in_a_year=movies.aggregate([
#     {"$unwind":"$directors"},
# 	{"$match":{"year":2014}},
#     {"$project":{"_id":0,"movie_dir":{"$convert":{"input":"$directors","to":"string","onError":"unkonwn","onNull":"unkonwn"}},"title":1}},
#     {"$group":{"_id":{"direct":"$movie_dir"},"total_movies":{"$sum":1}}},
#     {"$sort":{"total_movies":-1}},
#     {"$limit":10}
#    ])
# for item in dr_created_the_maximum_number_of_movies_in_a_year:
# 	print(item)

# 2.3
# print("==============Directors created the maximum number of movies for a given genre==========")
# dr_created_the_maximum_number_of_movies_for_a_genre=movies.aggregate([
#        {"$unwind":"$genres"},
#        {"$unwind":"$directors"},
#        {"$match":{"genres":"Comedy"}},
#        {"$project":{"genres":1,"directors":1,"title":1}},
#        {"$group":{"_id":{"movie_dir":"$directors","genres":"$genres"},"total_movie":{"$sum":1}}},
#        {"$sort":{"total_movie":-1}},
#        {"$limit":10}
#       ])
# for item in dr_created_the_maximum_number_of_movies_for_a_genre:
# 	print(item)


# 3
# 3.1
# print("==============================Actors maximum number of movies============================")
# ac_maximum_number_of_movies=movies.aggregate([
#    {"$unwind":'$cast'},
#    {"$project":{"_id":0,"title":1,"cast":1}},
#    {"$group":{"_id":{"actor":"$cast"},"total_movies":{"$sum":1}}},
#    {"$sort":{"total_movies":-1}},
#    {"$limit":10},
# ])
#
# for item in ac_maximum_number_of_movies:
# 	print(item)


# 3.2
# print("=======================Actors maximum number of movies in a given year=====================")
# ac_maximum_number_of_movies_in_a_given_year=movies.aggregate([
#     {"$unwind":'$cast'},
#     {"$match":{"year":2014}},
#     {"$project":{"_id":0,"year":1,"title":1,"cast":1}},
#     {"$group":{"_id":{"actor":"$cast"},"total_movies":{"$sum":1}}},
#     {"$sort":{"total_movies":-1}},
#     {"$limit":10},
#
# ])
#
# for item in ac_maximum_number_of_movies_in_a_given_year:
# 	print(item)

# 3.3
# print("=====================Actors maximum number of movies for a given genre=====================")
# ac_maximum_number_of_movies_for_a_given_genre=movies.aggregate([
#     {"$unwind":'$cast'},
#     {"$unwind":'$genres'},
#     {"$match":{"genres":"Comedy"}},
#     {"$project":{"_id":0,"genres":1,"title":1,"cast":1}},
#     {"$group":{"_id":{"actor":"$cast"},"total_movies":{"$sum":1}}},
#     {"$sort":{"total_movies":-1}},
#     {"$limit":10},
# ])
#
# for item in ac_maximum_number_of_movies_for_a_given_genre:
# 	print(item)

# 4.
# print("===================`N` movies for each genre with the highest IMDB rating=====================")
# movies_for_each_genre_with_the_highest_IMDB_rating=movies.aggregate([
#     {"$unwind":'$genres'},
#     {"$project":{"_id":0,"genres":1,"title":1,
#         "rating":{"$convert":{"input":"$imdb.rating","to":"double","onError":0.00}}}},
#     {"$group":{"_id":{"genres":"$genres"},"movies":{"$first":"$title"},"max_rating":{"$max":"$rating"}}},
#     {"$sort":{"max_rating":-1}},
# ])
# for item in movies_for_each_genre_with_the_highest_IMDB_rating:
# 	print(item)


#theaters
# theaters=db.get_collection('theaters')
# print("===================top 10 cities with the maximum number of theatres=====================")
# 1.
# cities_with_the_maximum_number_of_theatres=theaters.aggregate([
# 	{"$project":{"_id":0,"city":"$location.address.city"}},
# 	{"$group":{"_id":{"city":"$city"},"count_of_theaters":{"$sum":1}}},
# 	{"$project":{"_id":0,"city":"$_id.city","count_of_theaters":1}},
# 	{"$sort":{"count_of_theaters":-1}},
# 	{"$limit":10}
# ])
# for item in cities_with_the_maximum_number_of_theatres:
# 	print(item)

# 2.
# print("=======================top 10 theatres nearby given coordinates===========================")
# theatres_nearby_given_coordinates=theaters.find({
#             "location.geo":{
#                     "$near":{
#                         "$geometry":{
#                             "type":"Point",
#                             "coordinates":[-93.24565,44.85466]
#                             },
#                             "$maxDistance":10000,
#                             "$minDistance":100
#                           }
#                         }
# }).limit(10)
#
# for item in theatres_nearby_given_coordinates:
# 	print(item)