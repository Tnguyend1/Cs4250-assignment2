#-------------------------------------------------------------------------
# AUTHOR: Thoa Nguyen
# FILENAME: db_connection_solution.py
# SPECIFICATION: use the corpus database tables created in question 1 to manage an inverted index
# FOR: CS 4250- Assignment #2
# TIME SPENT: 5
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
from psycopg2 import *

def connectDataBase():

    # Create a database connection object using psycopg2
    DB_name = "your database name"
    DB_user = "your username"
    DB_pass = "your password"
    DB_host = "your host"
    DB_port = "your port"
    try:
        conn = connect(
            database=DB_name,
            user=DB_user,
            password = DB_pass,
            host=DB_host,
            port=DB_port
        )
        #create tables if they dont exist
        createTables(conn)
        return conn
    except Exception as e:
        print(e)
#Create tables if not exist
def createTables(conn):
    try:
        curr = conn.cursor()
        curr.execute("CREATE TABLE IF NOT EXISTS categories (id integer NOT NULL, name text NOT NULL, PRIMARY KEY (id))")
        curr.execute("CREATE TABLE IF NOT EXISTS documents (doc integer NOT NULL, text text NOT NULL, title text NOT NULL, num_chars integer NOT NULL, date text NOT NULL, id_cat integer NOT NULL, PRIMARY KEY (doc), FOREIGN KEY (id_cat) REFERENCES categories(id))")
        curr.execute("CREATE TABLE IF NOT EXISTS terms (term text NOT NULL, num_chars integer NOT NULL, PRIMARY KEY (term))")
        curr.execute("CREATE TABLE IF NOT EXISTS documents_terms (term text NOT NULL, doc integer NOT NULL, term_count integer NOT NULL, PRIMARY KEY(term, doc), FOREIGN KEY (term) REFERENCES terms(term), FOREIGN KEY (doc) REFERENCES documents(doc))")
        conn.commit()
    except():
        print("Error to create the tables")

def createCategory(cur, catId, catName):

    # Insert a category in the database
    InsertCat = "INSERT INTO categories (id, name) VALUES (%(id)s, %(name)s)"
    params = {'id': catId, 'name': catName}
    try: 
        cur.execute(InsertCat, params)
    except Exception as e:
        print("Id exist already!")

def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    get_cat = "SELECT * FROM categories WHERE categories.name = %(docCat)s"
    get_cat_params = {'docCat':docCat}
    cur.execute(get_cat, get_cat_params)

    categ_id = cur.fetchone()

    # if not valid
    if not categ_id:
        print(docCat + " is  invalid category name")
        return

    # if valid
    categ_id = categ_id[0]

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    try:
        insertDoc = "INSERT INTO documents (doc, text, title, num_chars, date, id_cat) VALUES (%(doc)s, %(text)s, %(title)s, %(num_chars)s, %(date)s, %(id_cat)s);"
        insertDoc_params = {'doc':docId, 'text': docText, 'title':docTitle, 'num_chars':str(len(docText)), 'date': docDate, 'id_cat':categ_id}
    
        cur.execute(insertDoc, insertDoc_params)
    
 
    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    
    # Split into lowercased terms separated by space, remove non-alphanumeric characters
        terms = ["".join(char for char in term if char.isalnum()) for term in docText.lower().split()]

    # Iterate through each term, check if it exists in the database, if not add it
        for term in terms:
            cur.execute("SELECT * FROM terms WHERE terms.term = %(term)s;", {'term': term})
            term_set = cur.fetchall()

            if len(term_set) == 0:
                try:
                    cur.execute("INSERT INTO terms (term, num_chars) VALUES (%(term)s, %(num_chars)s)", {'term': term, 'num_chars': str(len(term))})
                except Exception as e:
                    print(f"Error inserting term: {e}")
    
        
    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database

    #get frequencies of terms in doc
        termFreq = {}
        for term in terms:
            termFreq[term] = 1 + termFreq.get(term, 0)
    #insert term and corresponding freqs for document into index

        for term, count in termFreq.items():
            cur.execute('INSERT INTO documents_terms (term, doc, term_count) VALUES (%(term)s, %(doc)s, %(term_count)s)', {'term': term, 'doc': docId, 'term_count': count})
    except:
        print("Invalid Document ID")    

def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    try:
        # 1 Query the index based on the document to identify terms
        cur.execute("SELECT term FROM documents_terms WHERE doc = %(docId)s", {'docId': docId})
        terms = cur.fetchall()

        # 1.1 For each term identified, delete its occurrences in the index for that document
        for term in terms:
            cur.execute("DELETE FROM documents_terms WHERE term = %(term)s AND doc = %(docId)s", {'term': term[0], 'docId': docId})

            # 1.2 Check if there are no more occurrences of the term in another document
            cur.execute("SELECT COUNT(*) FROM documents_terms WHERE term = %(term)s", {'term': term[0]})
            term_occurrences = cur.fetchone()[0]

            if term_occurrences == 0:
                # If no more occurrences, delete the term from the database
                cur.execute("DELETE FROM terms WHERE term = %(term)s", {'term': term[0]})

        # 2 Delete the document from the database
        cur.execute("DELETE FROM documents WHERE doc = %(docId)s", {'docId': docId})
    except Exception as e:
        print(f"Error deleting document: {e}")

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    createDocument(cur, docId, docText, docTitle, docDate, docCat)
def getIndex(cur):
    # Get all indexes with document title and term count in a single query
    cur.execute("""
        SELECT documents_terms.term, documents.title, documents_terms.term_count
        FROM documents_terms
        JOIN documents ON documents_terms.doc = documents.doc
    """)
    indexes = cur.fetchall()

    # Create a dictionary to store inverted indexes
    invertedIndexes = {}

    # Group indexes by term
    grouped_indexes = {}
    for term, title, term_count in indexes:
        if term not in grouped_indexes:
            grouped_indexes[term] = []
        grouped_indexes[term].append((title, term_count))

    # Sort and format the inverted indexes
    for term, occurrences in sorted(grouped_indexes.items()):
        occurrences.sort(key=lambda x: x[0])  # Sort by document title
        invertedIndexes[term] = ', '.join(f"{title}:{term_count}" for title, term_count in occurrences)

    return invertedIndexes
