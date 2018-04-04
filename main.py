from bson.json_util import dumps
from flask import Flask, redirect, send_from_directory
from flask_pymongo import PyMongo
from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import MatrixFactorizationModel
import math

sc = SparkContext()
model = MatrixFactorizationModel.load(sc, "datasets/col_filtering_best_model")

app = Flask(__name__, static_url_path='')
app.config['MONGO_HOST'] = 'db'
app.config['MONGO_DBNAME'] = 'movie_recommendation'
mongo = PyMongo(app, config_prefix='MONGO')

@app.route("/")
def root_redirect():
    return redirect("/static/index.html")

@app.route("/static/<path:filename>")
def get_home(filename):
    return send_from_directory('ui/dist', filename)

@app.route('/search/<path:text>')
def search_by_name(text):
    movies = mongo.db.movie_titles.find({
        'movie_name':{'$regex':'^{0}'.format(text)
    }}, {
        'movie_name': True, 
        'movie_id': True,
        '_id': False,
    }).limit(10)
    return dumps(movies)


@app.route('/search_similar/<path:movie_id>')
def search_for_similar_movies(movie_id):
    movie_id = str(movie_id)
    similar_object = []

    recommendations = mongo.db.recommendations.find(
        {
            "$and": [
                { "$or":[ {"movie_id_1": movie_id}, {"movie_id_2": movie_id}] },
                { "similarity": {'$gt': 0} }
            ]
        },
        {
            'movie_id_1': True, 
            'movie_id_2': True,
            'similarity': True,
            '_id': False,
        }
    ).sort("similarity", -1).limit(10)

    for recommendation in recommendations:
        if recommendation.get('movie_id_1') == movie_id:
            similar_object.append({
                'movie_id': recommendation.get('movie_id_2'),
                'similarity': recommendation.get('similarity')
            })
        else:
            similar_object.append({
                'movie_id': recommendation.get('movie_id_1'),
                'similarity': recommendation.get('similarity')
            })

    movies = list(mongo.db.movie_titles.find(
        {'movie_id': {'$in': map(lambda obj: obj.get('movie_id'), similar_object)}},
    ))

    for index, movie in enumerate(movies):
        movie['similarity'] = filter(lambda obj: obj.get('movie_id') == movie.get('movie_id'), similar_object)[0].get('similarity')
        movies[index] = movie

    movies = sorted(movies, key=lambda movie: movie.get('similarity'), reverse=True)
    return dumps(movies)

@app.route('/get_products/<path:user_id>')
def get_products_for_users(user_id):
    recommendation_objects = []

    user_id = int(user_id)
    recommendations = model.recommendProducts(user_id, 10)
    for recommendation in recommendations:
        recommendation_objects.append({
            'movie_id': str(recommendation[1]),
            'rating': recommendation[2]
        })

    movies = list(mongo.db.movie_titles.find(
        {'movie_id': {'$in': map(lambda obj: obj.get('movie_id'), recommendation_objects)}},
    ))

    for index, movie in enumerate(movies):
        movie['rating'] = filter(lambda obj: obj.get('movie_id') == movie.get('movie_id'), recommendation_objects)[0].get('rating')
        movies[index] = movie

    movies = sorted(movies, key=lambda movie: movie.get('rating'), reverse=True)

    return dumps(movies)

