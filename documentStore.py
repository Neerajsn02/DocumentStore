from pymongo import MongoClient

port = input("Enter port number:")
client = MongoClient(port)
db = client["291db"]

movies_collection = db["title_basics"]
ratings_collection = db["title_ratings"]
name_collection = db["name_basics"]
ratings_collection.create_index([("tconst", 1)])


def main_menu():
    user_input = 1
    while user_input != 6:
        print("*" * 22)
        print("Main Menu")
        print("1. Search for titles")
        print("2. Search for genres")
        print("3. Search for cast/crew members")
        print("4. Add a movie")
        print("5. Add a cast/crew member")
        print("6. End program")
        user_input = input("Select an option: ")
        user_input = int(user_input)
        if user_input == 1:
            searchtitles()
        elif user_input == 2:
            searchgenres()
        elif user_input == 3:
            searchcast()
        elif user_input == 4:
            if add_movie():
                print("The movie has been successfully added!")
            else:
                print("The movie has not been added")
        elif user_input == 5:
            add_cast()
        elif user_input == 6:
            print("Goodbye!")
        else:
            print("\nIncorrect. Please re-enter.")


def searchgenres():
    genre_input = input("Provide a genre: ")
    vote_input = input("Provide a minimum vote count: ")
    vote_input = int(vote_input)

    pipeline = [
        {"$lookup": {"from": "title_ratings",
                     "localField": "tconst",
                     "foreignField": "tconst",
                     "as": "movie_rate"}},
        {"$match": {"$and": [{"genres": {"$regex": genre_input, "$options": 'i'}},
                             {"movie_rate.numVotes": {"$gte": vote_input}}]}},
        {"$sort": {"movie_rate.averageRating": -1}}
    ]
    result = movies_collection.aggregate(pipeline)
    for item in result:
        print(item["primaryTitle"])


def searchcast():
    user_input = input("Enter cast/crew member name: ")
    profession_result = name_collection.find({"primaryName": user_input}, {"_id": 0, "primaryProfession": 1})
    print("Person's professions:")
    for profession in profession_result:
        print(profession)
    cast_id = name_collection.find_one({"primaryName": {"$regex": user_input, "$options": 'i'}},
                                       {"_id": 0, "nconst": 1})
    cast_id = cast_id["nconst"]
    title_principals_collection = db["title_principals"]
    for worked_on in title_principals_collection.find(
            {"$and": [{"nconst": cast_id}, {"$or": [{"job": {"$ne": "\\N"}}, {"characters": {"$ne": "\\N"}}]}]}):
        worked_on_id = worked_on["tconst"]
        movie_worked_on = movies_collection.find_one({"tconst": worked_on_id})
        print("-" * 20)
        print("Title", movie_worked_on["primaryTitle"])
        if (worked_on["job"] != "\\N"):
            print("Job: ", worked_on["job"])
        elif (worked_on["job"] == "\\N"):
            print("No jobs/Worked as an actor")
        if (worked_on["characters"][0] != "\\N"):
            print("Characters: ", worked_on["characters"])
        elif (worked_on["characters"][0] == "\\N"):
            print("No characters")


def searchtitles():
    keyword = input("Search for keyword: ")
    keyword = keyword.split()
    keyword = '\"' + '\" \"'.join(keyword) + '\"'

    title_collection = db["title_basics"]
    ratings_collection = db["title_ratings"]
    title_collection.create_index([("primaryTitle", "text"), ("startYear", "text")])

    seacrh_results = []
    i = 1
    for title in title_collection.find({"$text": {"$search": keyword}}):
        print(i, title["primaryTitle"], title["tconst"], title["isAdult"], title["startYear"], title["runtimeMinutes"],
              title["genres"])
        seacrh_results.append(title)
        i += 1
    if i == 1:
        print("No such title with these keywords")
        return

    selection_1 = int(input("Select a movie using index: "))
    selection_1 = selection_1 - 1
    print("Select a function: \n 1. Rating \n 2. Number of votes \n 3. Names of cast/crew")
    selection_2 = input("Make a selection: ")
    selection_2 = int(selection_2)
    title_code = seacrh_results[selection_1]["tconst"]
    if selection_2 == 1:
        movie_stats = ratings_collection.find_one({"tconst": title_code})
        print("Rating:", movie_stats["averageRating"])
    elif selection_2 == 2:
        movie_stats = ratings_collection.find_one({"tconst": title_code})
        print("Number of votes:", movie_stats["numVotes"])
    elif selection_2 == 3:
        roles_collection = db["title_principals"]
        name_collection = db["name_basics"]
        crew_list = []
        for crew in roles_collection.find({"tconst": title_code}):
            character = crew["characters"]
            crew2 = name_collection.find_one({"nconst": crew["nconst"]})
            name = crew2["primaryName"]
            if character[0] != "\\N":
                for item in character:
                    print("Name:", name, " | ", "Character:", item)
            elif character[0] == "\\N":
                print("Name:", name, " | ", "Character: No characters / is crew")


def add_movie():
    title_basics = db["title_basics"]
    movie_id = input("Enter a unique movie ID: ")
    movie_title = input("Enter the title of the movie to be added: ")
    movie_year = input("Enter the start year of the movie: ")
    movie_running_time = input("Enter the running time of the movie: ")
    movie_genres = []
    no_of_genres = int(input("Enter how many genres are there for this film: "))
    i = 1
    while i <= no_of_genres:
        genre = input("Enter the genre: ")
        movie_genres.append(genre)
        i += 1

    added_movie = [
        {
            "tconst": movie_id,
            "titleType": "movie",
            "primaryTitle": movie_title,
            "originalTitle": movie_title,
            "isAdult": "\\N",
            "startYear": movie_year,
            "endYear": "\\N",
            "runtimeMinutes": movie_running_time,
            "genres": movie_genres
        }
    ]
    title_basics.insert_one(added_movie[0])
    return True


def add_cast():
    title_collection = db["title_basics"]
    name_collection = db["name_basics"]

    cast_id = input("Provide cast/crew member id: ")
    title_id = input("Provide title id: ")
    category_inpt = input("Provide category: ")

    titles = title_collection.find_one({"tconst": title_id})
    if titles == None:
        print("No such title has been found in the database.")
        return

    names = name_collection.find_one({"nconst": cast_id})
    if names == None:
        print("No such name has been found in the database.")
        return

    roles_collection = db["title_principals"]

    result = roles_collection.aggregate(
        [{"$match": {"tconst": title_id}},
         {"$sort": {"ordering": -1}},
         {"$limit": 1},
         {"$project": {"ordering": 1}}
         ])
    value = None
    for item in result:
        value = item["ordering"]
    if value != None:
        value = int(value) + 1

    if value == None:
        value = 1

    inserted_id = roles_collection.insert_one(
        {"tconst": title_id, "ordering": value, "nconst": cast_id, "category": category_inpt, "job": "/N",
         "characters": "/N"})
    print("Successfully added!")
    return


def main():
    main_menu()


main()