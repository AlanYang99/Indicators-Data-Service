from flask import Flask, request
from flask_restplus import Api,Resource, fields
import requests
import sqlite3
import json
import datetime

#-----------Initialise Flask--------------

app = Flask(__name__)
api = Api(app, default = 'World Bank Indicators Queries and Updates' , title = 'World Bank API', description = 'Data Service for World Bank Economic Indicators')

#--------------SQL Commands--------------

def UpdateDatabase(Command):
    conn = sqlite3.connect('Indicators.db')
    cur = conn.cursor()
    cur.execute(Command)
    conn.commit()
    conn.close()

def QueryingDatabase(QueryCommand):
    conn = sqlite3.connect('Indicators.db')
    cur = conn.cursor()
    cur.execute(QueryCommand)
    Queries = cur.fetchall()
    conn.close()
    return Queries

#-----------Setting up SQL Database---------------

@app.before_first_request
def initialise_table():
    collections_table = """
                            CREATE TABLE IF NOT EXISTS COLLECTIONS(
                            ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL ,
                            INDICATOR VARCHAR(100) NOT NULL UNIQUE,
                            INDICATOR_VALUE VARCHAR(100) NOT NULL,
                            CREATION_TIME VARCHAR(100) NOT NULL
                            );"""

    UpdateDatabase(collections_table)

    indicators_table = """
                            CREATE TABLE IF NOT EXISTS INDICATORS(
                            ID INTEGER NOT NULL,
                            INDICATOR_NAME VARCHAR(100),
                            COUNTRY VARCHAR(100),
                            DATE INTEGER,
                            VALUE REAL,
                            FOREIGN KEY (ID) REFERENCES COLLECTIONS(ID)
                            );"""

    UpdateDatabase(indicators_table)

def inserting_entries(entries):
    time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-7] + 'Z'
    Query = """
                INSERT INTO COLLECTIONS(INDICATOR, INDICATOR_VALUE,CREATION_TIME)
                VALUES('{}','{}','{}')
            """.format(entries[0]['indicator']['id'],entries[0]['indicator']['value'],time)    
    UpdateDatabase(Query)

    Query = """
            SELECT ID FROM COLLECTIONS WHERE INDICATOR = '{}'
            """.format(entries[0]['indicator']['id'])
    id = QueryingDatabase(Query)[0][0]    

    for entry in entries:
        if(entry['value'] == None):
            continue
        Query = """
            INSERT INTO INDICATORS(ID, INDICATOR_NAME ,COUNTRY, DATE, VALUE)
            VALUES('{}','{}','{}','{}','{}')
        """.format(id,entry['indicator']['id'],entry['country']['value'].replace("'","''"),entry['date'],entry['value'])
        UpdateDatabase(Query)

    return {"uri" : "/collections/{}".format(id),
            "id" : id,
            "creation_time" : "{}".format(time),
            "indicator_id" : "{}".format(entries[0]['indicator']['id'])}

#--------------Validating Parameters---------------

def check_indicators(indicator):
    Query = """
            SELECT ID FROM COLLECTIONS WHERE INDICATOR = '{}'
            """.format(indicator)
    if(len(QueryingDatabase(Query)) == 0):
        return False
    return True

def check_id(id):
    Query = """
            SELECT ID FROM COLLECTIONS WHERE ID = '{}'
            """.format(id)
    if(len(QueryingDatabase(Query)) == 0):
        return False
    return True

#---------------Helper Functions--------------

def GetUrl(indicator,page = 1,start = 2012,end = 2017,content_format = 'json'):
    return f'http://api.worldbank.org/v2/countries/all/indicators/' + f'{indicator}?date={start}:{end}&format={content_format}&per_page={page}'

def SQLToJsonFormat1(SQLList):
    collections_list = []
    for collection in SQLList:
        collections_list.append({"uri" : "/collections/{}".format(collection[0]),
        "id" : collection[0],
        "creation_time" : "{}".format(collection[3]),
        "indicator" : "{}".format(collection[1])})
    if(len(collections_list) == 0):
        return collections_list , 204
    return collections_list , 200

def SQLToJsonFormat2(SQLList,option):
    entries = []
    if(option == 1):
        for entry in SQLList:
            entries.append({"country" : entry[0],
                            "date" : entry[1],
                            "value" : entry[2]})
    else:
        for entry in SQLList:
            entries.append({"country" : entry[0],"value" : entry[1]}) 
    return entries       

#----------------API Analysis-----------------

@api.route('/collections')
@api.response(200, 'OK')
@api.response(201, 'Created')
@api.response(204, 'No indicators in collections')
@api.response(400, 'Bad Request')
@api.response(404, 'Not Found')
class SimpleFunctions(Resource):

    #------Q1
    #Use the restplus interface to test it as it may occasionally get "405 Method Not Allowed"
    #Or could use the command 'curl -X POST http://127.0.0.1:5000/collections?indicator_id={indicator}' in the terminal
    @api.doc(params= {'indicator_id': {'description': 'Insert Indicator', 'in': 'query', 'type': 'string'}})
    def post(self):
        indicator_id = request.args.get('indicator_id')
        if(indicator_id == None):
            return {"message" : "Input for indicator_id is empty"}, 400
        collection_data = requests.get(GetUrl(indicator_id))
        if(len(collection_data.json()) == 1):
            return {"error" : "The indicator {} doesn't exist in the data source".format(indicator_id)} , 404
        if(check_indicators(indicator_id) == True):
            return {"message" : "{} is already imported".format(indicator_id)} , 200
        NumPages = collection_data.json()[0]['total']
        collection_data = requests.get(GetUrl(indicator_id,page = NumPages))
        return inserting_entries(collection_data.json()[1]) , 201

    # ------Q3
    # Use this format to test "http://127.0.0.1:5004/collections?order_by=+id,+creation_time,+indicator,-id,-creation_time,-indicator"
    # Replace the '+' by '%2B' as it is the encoding for '+'
    # same as q1, can test using the restplus interface or commandline
    @api.doc(params= {'order_by': {'description': 'Insert Ordering', 'in': 'query', 'type': 'string'}})
    def get(self):
        order_by = request.args.get('order_by')
        if(order_by == None):
            entries = QueryingDatabase("select * from collections order by ID")
            return SQLToJsonFormat1(entries) 
        valid_words = ['uri','id','creation_time','indicator_time','indicator']
        words = order_by.split(',')
        ordering = ''

        if(words[0][1:].lower() == 'uri'):
            ordering = "ID"
        else:
            if(words[0][1:].lower() not in valid_words):
                    return {"error" : "Invalid Column"} , 400                
            ordering = words[0][1:].upper()
        
        if(words[0][0] == '-'):
            ordering += " DESC"
        elif(words[0][0] == '+'):
            ordering += " ASC"
        else:
            return {"error" : "Invalid Column"} , 400

        for word in words[1:]:
            if(word[1:].lower() not in valid_words):
                    return {"error" : "Invalid Column"} , 400
            ordering += ", " 
            if(word[0] == '-'):
                if(word[1:].lower() == 'uri'):
                    ordering += "ID"
                else:
                    ordering += word[1:].upper()
                ordering += " DESC"           
            elif(word[0] == '+'):
                if(word[1:].lower() == 'uri'):
                    ordering += "ID"
                else:
                    ordering += word[1:].upper()
                ordering += " ASC"
            else:
                return {"error" : "Invalid Column"} , 400            

        entries = QueryingDatabase("select * from collections order by {}".format(ordering))
        return SQLToJsonFormat1(entries)

@api.route('/collections/<int:id>')
@api.response(200, 'OK')
@api.response(204, 'No Content - Where entries is empty')
@api.response(404, 'Not Found')
class MediumFunctions(Resource):

    #------Q2
    def delete(self,id):
        if(check_id(id) == False):
            return {"error" : "collections/{} doesnt exist".format(id)} , 404 
        Query = """
                DELETE FROM COLLECTIONS WHERE ID = '{}'
                """.format(id)
        UpdateDatabase(Query)
        Query = """
                DELETE FROM INDICATORS WHERE ID = '{}'
                """.format(id)
        UpdateDatabase(Query)
        return {"message" : "The collection {} was removed from the database!".format(id),
                "id" : id} , 200

    #------Q4
    def get(self,id):
        if(check_id(id) == False):
            return {"error" : "collections/{} doesnt exist".format(id)} , 404
        sql_entries = QueryingDatabase("select COUNTRY, DATE , VALUE from indicators where ID = {}".format(id))
        entries = SQLToJsonFormat2(sql_entries,1)      
        http_code = 200
        if(len(entries) == 0):
            http_code = 204
        indicator_info = QueryingDatabase("select * from COLLECTIONS where ID = {}".format(id))
        return {  
                "id" : id,
                "indicator": "{}".format(indicator_info[0][1]),
                "indicator_value": "{}".format(indicator_info[0][2]),
                "creation_time" : "{}".format(indicator_info[0][3]),
                "entries" : entries} , http_code

@api.route('/collections/<int:id>/<int:year>/<string:country>')
@api.response(200, 'OK')
@api.response(400, 'Bad Request')
@api.response(404, 'Not Found')
class AdvanceFunction(Resource):

    #------Q5
    def get(self,id,year,country):
        if(check_id(id) == False):
            return {"error" : "collections/{} doesnt exist".format(id)} , 404
        if(year > 2017 or year < 2012):
            return {"error" : "Year {} is not between 2012 and 2017 inclusive".format(year)} , 400
        entry = QueryingDatabase("select ID, INDICATOR_NAME, COUNTRY, DATE , VALUE from INDICATORS where ID = {} AND DATE = {} AND COUNTRY = '{}'".format(id,year,country))
        if(len(entry) == 0):
            return {"message" : "{} statistics for {} does not exist in collections/{}".format(year,country,id)} , 404
        return {"id" : id, "indicator" : entry[0][1],"country" : country, "year" : year,"value" : entry[0][4]} , 200

@api.route('/collections/<int:id>/<int:year>')
@api.response(200, 'OK')
@api.response(204, 'No Content - Where entries is empty')
@api.response(400, 'Bad Request')
@api.response(404, 'Not Found')
class ComplexFunction(Resource):

    #------Q6
    @api.doc(params= {'q': {'description': 'Query', 'in': 'query', 'type': 'string'}})
    def get(self,id,year):

        num = request.args.get('q')
        if(check_id(id) == False):
            return {"error" : "collections/{} doesnt exist".format(id)} , 404
        if(year > 2017 or year < 2012):
            return {"error" : "Year {} is not between 2012 and 2017 inclusive".format(year)} , 400
        entries = []
        if(num == None):
            Query = """
                    SELECT COUNTRY,VALUE FROM INDICATORS
                    WHERE DATE = '{}' AND ID = '{}'
                    """.format(year,id)
            entries = QueryingDatabase(Query)
        else:
            number = 0
            try:
                number = int(num)
            except ValueError:
                return {"error" : "q parameter is invalid"} , 400
            
            limit = ''
            if(number > 0):
                limit = 'DESC'
            else:
                number *= -1
                limit = 'ASC'

            Query = """
                select COUNTRY, VALUE FROM INDICATORS
                WHERE DATE = {} AND ID = {}
                ORDER BY VALUE {} LIMIT {}
                """.format(year,id,limit,number)   

            entries = QueryingDatabase(Query)         
        entries = SQLToJsonFormat2(entries,2)
        http_code = 200
        if(len(entries) == 0):
            http_code = 204

        Query = """
                select INDICATOR, INDICATOR_VALUE FROM COLLECTIONS
                WHERE ID = {}
                """.format(id)
        indicator_info = QueryingDatabase(Query)

        return {"indicator" : indicator_info[0][0], 
                "indicator_value" : indicator_info[0][1],
                "entries" : entries} , http_code       

if __name__ == '__main__':
    app.run(debug=True,port = 5001)
