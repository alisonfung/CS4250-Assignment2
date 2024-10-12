#-------------------------------------------------------------------------
# AUTHOR: Alison Fung
# FILENAME: db_connection_mongo.py
# SPECIFICATION: Performs database operations using Python Mongo Driver.
# FOR: CS 4250- Assignment #2
# TIME SPENT: 2 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
from pymongo import MongoClient  # import mongo client to connect
from string import punctuation

def connectDataBase():

    # Create a database connection object using pymongo
    client = MongoClient(host="localhost", port=27017)
    db = client.assignment2
    return db

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary (document) to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    terms = {}
    # remove punctuation, lowercase the string, split the string by spaces
    split_text = docText.translate(str.maketrans('', '', punctuation)).lower().split()
    # count terms in text
    for word in split_text:
        if word in terms:
            terms[word] += 1
        else:
            terms[word] = 1

    # create a list of dictionaries (documents) with each entry including a term, its occurrences, and its num_chars. Ex: [{term, count, num_char}]
    # --> add your Python code here
    list_of_terms = []
    for term in terms:
        list_of_terms.append({"term": term, "count": terms.get(term), "num_chars": len(term)})

    #Producing a final document as a dictionary including all the required fields
    document = {"_id": docId,
                "text": docText,
                "title": docTitle,
                "date": docDate,
                "category": docCat,
                "terms": list_of_terms}

    # Insert the document
    col.insert_one(document)


def deleteDocument(col, docId):

    # Delete the document from the database
    col.delete_one({"_id": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    deleteDocument(col, docId)

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3', ...}
    # We are simulating an inverted index here in memory.
    pipeline = [
        # create a document for every term in every document
        {"$unwind": "$terms" },
        # group each document by their title and their term, then sum the number of each
        {"$group": {"_id": {"term": "$terms.term", "title": "$title"},"count": {"$sum": "$terms.count"}}},
        # group again by the term, and create a list of the titles and counts
        {"$group": {"_id": "$_id.term", "index": {"$push": {"title": "$_id.title", "count": "$count"}}}},
        # include only the index and the id, which is the term
        {"$project": {"index": 1}}
    ]
    # apply aggregation
    index_data = col.aggregate(pipeline)
    index = {}
    # loop through each term
    for term in index_data:
        term_array = []
        # for each title that contains the term, add "title:count" to the list
        for title in term["index"]:
            term_array.append(f"{title['title']}:{title['count']}")
        # get the id (word) that term refers to and add the list to the index entry
        index[term["_id"]] = term_array

    return index

