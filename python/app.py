from flask import Flask, request, jsonify
import sqlite3
import json
import re  # regular expressions
from pprint import pprint


app = Flask(__name__)
DBPATH = "../database.db"


@app.route("/messages", methods=["GET"])
def messages_route():
    """
    Return all the messages
    """

    with sqlite3.connect(DBPATH) as conn:
        messages_res = conn.execute("select body from messages")
        messages = [m[0] for m in messages_res]
        
        # find default values for all state id's
        stateIdDefaultDic = getStateIdDefaultDic(messages)
       
        # Note: Below getStateIdDbValues queries state table only once! My initial implementation contained
        # separate query for every state id found in the returned messages which can be
        # costly (longer times), specially if tables are big. Using single query is way faster but still
        # could cause potential problems when large number of ids are present in the query. 
        # See https://social.technet.microsoft.com/Forums/en-US/1a22dc3e-2b64-4377-8714-b30cd0eb112d/in-clause-with-large-number-of-values-fails-in-sql-2016?forum=transactsql)
        # To work around this problem, next step would be store the items in the IN list in a temp table or can
        # break down query into multiple managable set of ids (bulk of few hundred or thousand at the time)

        # find db values for all state id's
        stateIdDbDic = getStateIdDbValues(stateIdDefaultDic.keys())
        
        # update mesages with id db or default values    
        updatedMsgs = map(lambda i: updateMessageVariables(i,stateIdDefaultDic, stateIdDbDic), messages)

    return jsonify(list(updatedMsgs)), 200


def updateMessageVariables(msg, stateIdDefaultDic, stateIdDbDic):
    """
    Updates message with db value or default value if none found in db
    """
    matches = re.findall(r'{(.*?)}', msg)
    for match in matches:
        matchStrings = match.split("|")
        id = matchStrings[0]
        defaultVal, dbValue = stateIdDefaultDic.get(id), stateIdDbDic.get(id)
        #use dbValue if exist otherwise defaultVal
        newValue = defaultVal if not dbValue else dbValue
        # replace match with new value
        msg = msg.replace(match, newValue)
        
    # replace all braces { } in the msg
    msg = msg.replace("{","").replace("}","")

    return msg

def getStateIdDefaultDic(messages):
    """
    Returns dictionary of ids and default values found in messages. 
    Some id may not have any default value.
    """
    idDefaultDic = {}
    for message in messages:
    # find all strings in between braces {} and for each
    # match split the string by "|" . First part is id and second is default value. Place id and defaultValue in idDefaultDic
        matches = re.findall(r'{(.*?)}', message)
        for match in matches:
            matchStrings = match.split("|")
            id, defaultValue = matchStrings
            idDefaultDic[id] = defaultValue
        
    return idDefaultDic


def getStateIdDbValues(ids):
    """
    Returns dictionary of id and db value pairs from the state table.
    Note: If id not found in db returned dictionary will not have the key for it.
    """
    idValueDic = {}
    with sqlite3.connect(DBPATH) as conn:
        query = "select id, value from state where id in (%s)" % ','.join('?' * len(ids))
        res = conn.execute(query, tuple(ids))

        for idVal in res:
            idValueDic[idVal[0]] = idVal[1]
        
    return idValueDic


@app.route("/search", methods=["POST"])
def search_route():
    """
    Search for answers!

    Accepts a 'query' as JSON post, returns the full answer.

    curl -d '{"query":"Star Trek"}' -H "Content-Type: application/json" -X POST http://localhost:5000/search
    """

    with sqlite3.connect(DBPATH) as conn:
        query = request.get_json().get("query")
        # handle query as empty or empty spaces padded string-> as bad request
        if not query or not query.strip():
            return jsonify({'status': 'FAILURE', 'message': 'Bad Request'}), 400

        # Answer must have all queryItems matched at some level.
        # It is hard and also expensive (believe me I tried) to execute sql query/ies
        # that would return appropriate content. It occurred to me that
        # there is no reason to look for all query items in a single query
        # but execute single query with any of the query items and then check/validate if result contains
        # other query items.
        queryItems = query.split()
        firstQueryItem = queryItems[0]
        res = conn.execute("select answers.id, answers.title, blocks.content \
                        from answers inner join blocks on answers.id = blocks.answer_id \
                        where title like ? or content like ?", [f"%{firstQueryItem}%", f"%{firstQueryItem}%"])
        answers = [{"id": r[0], "title": r[1],"content": json.loads(r[2])} for r in res]

        filteredAnswers = list(filter(lambda x: containsAllQueryItems(x, queryItems), answers))

        print("query string --->", query)
        pprint(filteredAnswers)
        return jsonify(filteredAnswers), 200


def containsAllQueryItems(answer, queryItems):
    """
    Checks if answer contains all queryItems at any of the level or the answer object
    It returns true if each of the items in queryItems is found at some level of the answer object
    """
    rc = True
    for queryItem in queryItems:
        # check if queryItem matches title first
        if queryItem.lower() in answer['title'].lower():
            continue
        elif hasQueryItem(answer['content'], queryItem):
            continue
        else:
            rc = False
            break

    return rc


def hasQueryItem(obj, queryItem):
    """
    Performs traversal of the passed in obj until we reach object of the string type and 
    checks if it matches queryItem.
    Object with (key)attribute 'type' are ignored as per requirement 
    """
    found = False
    if isinstance(obj, str):
        if queryItem.lower() in obj.lower():
            return True

    if isinstance(obj, dict):
        for k, v in obj.items():
            if k != 'type':
                if hasQueryItem(v, queryItem):
                    found = True
                    break

    elif isinstance(obj, list):
        for elem in obj:
            if hasQueryItem(elem, queryItem):
                found = True
                break

    return found

if __name__ == "__main__":
    app.run(debug=True)
