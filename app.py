import psycopg2


import datetime
import database

menu = """Select option:
1) Add new movie
2) View upcoming movies
3) View all movies
4) Watch a movie
5) View watched movies
6) Add new user
7) Search movies
8) Exit.
Your selection: """

database.create_tables()


def add_user_to_database():
    username = input("Username of new user:")
    database.add_user(username)


def add_movie_to_database():
    title = input("title: ")
    date = input("date (dd-mm-YYYY): ")
    parsed_date = datetime.datetime.strptime(date, "%d-%m-%Y")
    timestamp = parsed_date.timestamp()
    database.add_movie(title, timestamp)


def show_movies_from_database(header, movies):
    print(header, ":")
    for movie in movies:
        date = datetime.datetime.fromtimestamp(movie[2])
        human_date = date.strftime("%d-%m-%Y")
        print("id: {2}; title: {0}; date of release: {1} \n".format(movie[1], human_date, movie[0]))


# def show_watched_movies(username, movies):
#     print("\nWATCHED MOVIES of {0}".format(username))
#     for movie in movies:
#         print("title: {0}".format(movie[1]))


def mark_movie_as_watched():
    username = input("Username: ")
    movie_id = input("ID of movie you watched: ")
    database.watch_movie(username, movie_id)


while (user_input := input(menu)) != "8":
    if user_input == "1":
        add_movie_to_database()
    elif user_input == "2":
        upcoming_movies = database.get_movies(True)
        show_movies_from_database("UPCOMING MOVIES", upcoming_movies)
    elif user_input == "3":
        all_movies = database.get_movies(False)
        show_movies_from_database("ALL MOVIES", all_movies)
    elif user_input == "4":
        mark_movie_as_watched()
    elif user_input == "5":
        username = input("username: ")
        watched_movies = database.get_watched_movies(username)
        show_movies_from_database(f"\nwatched movies of {username}", watched_movies)
    elif user_input == "6":
        add_user_to_database()
    elif user_input == "7":
        title=input("title of movie: ")
        searched_movies=database.search_movie(title)
        show_movies_from_database("\nsearched movies:", searched_movies)
    else:
        print("Invalid input")
