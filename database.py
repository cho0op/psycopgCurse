import sqlite3, datetime

connection = sqlite3.connect('data.db')


def create_tables():
    with connection:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS movies ("
            "id INTEGER PRIMARY KEY,"
            " title TEXT,"
            " release_timestamp REAL)")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS users ("
            "username TEXT PRIMARY KEY )")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS watched ("
            "user_username TEXT,"
            " movie_id INTEGER,"
            " FOREIGN KEY (user_username) REFERENCES users(username),"
            " FOREIGN KEY (movie_id) REFERENCES movies(id)) ")


def add_movie(title, release_timestamp):
    with connection:
        connection.execute("INSERT INTO movies (title, release_timestamp) VALUES (?,?);", (title,
                                                                                           release_timestamp))


def add_user(username):
    with connection:
        connection.execute("INSERT INTO users VALUES (?)", (username,))


def get_movies(upcoming=False):
    cursor = connection.cursor()
    if upcoming:
        today_timestamp = datetime.datetime.today().timestamp()
        cursor.execute("SELECT * FROM movies WHERE release_timestamp>?;", (today_timestamp,))
    else:
        cursor.execute("SELECT * FROM movies;")
    return cursor.fetchall()


def get_watched_movies(watcher_name, upcoming=False):
    cursor = connection.cursor()
    cursor.execute(
        "SELECT movies.*  FROM movies JOIN watched ON movies.id=watched.movie_id WHERE watched.user_username=?",
        (watcher_name,))
    return cursor.fetchall()


def watch_movie(username, movie_id):
    with connection:
        connection.execute("INSERT INTO watched (user_username, movie_id) VALUES (?,?)", (username, movie_id,))


def search_movie(title):
    like_title = f"%{title}%"
    cursor=connection.execute("SELECT * from movies WHERE title LIKE ?", (like_title,))
    return cursor.fetchall()