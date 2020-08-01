import os
import datetime
import psycopg2

from dotenv import load_dotenv

load_dotenv()

connection=psycopg2.connect(os.environ["DATABASE_URl"])

def create_tables():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS movies ("
                "id SERIAL PRIMARY KEY,"
                " title TEXT,"
                " release_timestamp REAL)")
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS users ("
                "username TEXT PRIMARY KEY )")
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS watched ("
                "user_username TEXT,"
                " movie_id INTEGER,"
                " FOREIGN KEY (user_username) REFERENCES users(username),"
                " FOREIGN KEY (movie_id) REFERENCES movies(id)) ")


def add_movie(title, release_timestamp):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO movies (title, release_timestamp) VALUES (%s,%s);", (title,
                                                                                           release_timestamp))


def add_user(username):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO users VALUES (%s)", (username,))


def get_movies(upcoming=False):
    with connection.cursor() as cursor:
        if upcoming:
            today_timestamp = datetime.datetime.today().timestamp()
            cursor.execute("SELECT * FROM movies WHERE release_timestamp>%s;", (today_timestamp,))
        else:
            cursor.execute("SELECT * FROM movies;")
        return cursor.fetchall()


def get_watched_movies(watcher_name, upcoming=False):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT movies.*  FROM movies JOIN watched ON movies.id=watched.movie_id WHERE watched.user_username=%s",
            (watcher_name,))
        return cursor.fetchall()


def watch_movie(username, movie_id):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO watched (user_username, movie_id) VALUES (%s,%s)", (username, movie_id,))


def search_movie(title):
    with connection.cursor() as cursor:
        like_title = f"%{title}%"
        cursor.execute("SELECT * from movies WHERE title LIKE %s", (like_title,))
        return cursor.fetchall()