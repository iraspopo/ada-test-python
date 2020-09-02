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
    curl -d -H "Content-Type: application/json" -X GET http://localhost:5000/messages
    """

    with sqlite3.connect(DBPATH) as conn:
        #def execute(self, sql: str, parameters: Iterable = ...) -> Cursor: ...
        # -> indicates function annotation and in this case it means the type of a return value
        # class Cursor(Iterator[Any]):  
        #   arraysize = ...  # type: Any
        #   connection = ...  # type: Any
        #   description = ...  # type: Any
        # ...
        # when I examined the type of object returned by iterating over sqlite3.Cursor  
        # got the tuple, which makes sense => executes returns collection of records (tuples)
        # <class 'tuple'> ('With the {74c695031a554c2ebfdb2ee123c8b4f6|something} link, the 
        # chain is forged. The {74c695031a554c2ebfdb2ee123c8b4f6|} speech censured, the 
        # {74c695031a554c2ebfdb2ee123c8b4f6|} thought forbidden, the {74c695031a554c2ebfdb2ee123c8b4f6|} 
        # freedom denied - chains us all irrevocably. ',)       //NOTICE ', tuple with one item always have , at the end 
        # so m[0] gets first and only item in the tuple
        messages_res = conn.execute("select body from messages")
        messages = [m[0] for m in messages_res]
        
        # find default values for all state id's
        state_id_default_dic = get_state_id_default_dic(messages)
       
        # Note: Below get_state_id_db_values queries state table only once! My initial implementation contained
        # separate query for every state id found in the returned messages which can be
        # costly (longer times), specially if tables are big. Using single query is way faster but still
        # could cause potential problems when large number of ids are present in the query. 
        # See https://social.technet.microsoft.com/Forums/en-US/1a22dc3e-2b64-4377-8714-b30cd0eb112d/in-clause-with-large-number-of-values-fails-in-sql-2016?forum=transactsql)
        # To work around this problem, next step would be store the items in the IN list in a temp table or can
        # break down query into multiple managable set of ids (bulk of few hundred or thousand at the time)

        # find db values for all state id's
        state_id_db_dic = get_state_id_db_values(state_id_default_dic.keys())
        
        # update mesages with id db or default values    
        updated_msgs = map(lambda i: update_message_variables(i,state_id_default_dic, state_id_db_dic), messages)

    return jsonify(list(updated_msgs)), 200

#
def update_message_variables(msg, state_id_default_dic, state_id_db_dic):
    """
    Updates message with db value or default value if none found in db
    """
    matches = re.findall(r'{(.*?)}', msg)
    for match in matches:
        match_strings = match.split("|")
        id = match_strings[0]
        default_val, db_value = state_id_default_dic.get(id), state_id_db_dic.get(id)
        #use db_value if exist otherwise default_val
        new_value = default_val if not db_value else db_value
        # replace match with new value
        msg = msg.replace(match, new_value)
        
    # replace all braces { } in the msg
    msg = msg.replace("{","").replace("}","")

    return msg

def get_state_id_default_dic(messages):
    """
    Returns dictionary of ids and default values found in messages. 
    Some id may not have any default value.
    """
    id_default_dic = {}
    for message in messages:
    # find all strings in between braces {} and for each
    # match split the string by "|" . First part is id and second is default value. Place id and default_value in id_default_dic
        matches = re.findall(r'{(.*?)}', message) #r means treat next as raw string -do not escape any char
        for match in matches:
            match_strings = match.split("|")
            id, default_value = match_strings
            id_default_dic[id] = default_value
        
    return id_default_dic


def get_state_id_db_values(ids):
    """
    Returns dictionary of id and db value pairs from the state table.
    Note: If id not found in db returned dictionary will not have the key for it.
    """
    id_value_dic = {}
    with sqlite3.connect(DBPATH) as conn:
        #formating string in python using C format style (%s and % https://realpython.com/python-string-formatting/
        query = "select id, value from state where id in (%s)" % ','.join('?' * len(ids))
        #print(query)
        #select id, value from state where id in (?,?,?,?,?,?)
        
        #From https://docs.python.org/2/library/sqlite3.html
        #Put ? as a placeholder wherever you want to use a value, and then provide a tuple of values as the second argument to the cursor’s execute() method.
        res = conn.execute(query, tuple(ids))

        for id_val in res:
            id_value_dic[id_val[0]] = id_val[1]
        
    return id_value_dic


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

        # Answer must have all query_items matched at some level.
        # It is hard and also expensive (believe me I tried) to execute sql query/ies
        # that would return appropriate content. It occurred to me that
        # there is no reason to look for all query items in a single query
        # but execute single query with any of the query items and then check/validate if result contains
        # other query items.
        query_items = query.split()
        first_query_item = query_items[0]
        res = conn.execute("select answers.id, answers.title, blocks.content \
                        from answers inner join blocks on answers.id = blocks.answer_id \
                        where title like ? or content like ?", [f"%{first_query_item}%", f"%{first_query_item}%"])
        #use of f"string{var}" -formating -http://zetcode.com/python/fstring/#:~:text=It%20uses%20the%20%25%20operator%20and,as%20%25s%20and%20%25d%20.&text=Since%20Python%203.0%2C%20the%20format,to%20provide%20advance%20formatting%20options.&text=Python%20f%2Dstrings%20are%20available,uses%20%7B%7D%20to%20evaluate%20variables.
        answers = [{"id": r[0], "title": r[1],"content": json.loads(r[2])} for r in res]
        #filter ansers with filter(function, iterable) where function can be lambda func that check the condition we want
        filtered_answers = list(filter(lambda x: contains_all_query_items(x, query_items), answers))

        print("query string --->", query)
        pprint(filtered_answers)
        return jsonify(filtered_answers), 200


def contains_all_query_items(answer, query_items):
    """
    Checks if answer contains all queryItems at any of the level or the answer object
    It returns true if each of the items in queryItems is found at some level of the answer object
    """
    rc = True
    for query_item in query_items:
        # check if query_item matches title first
        if query_item.lower() in answer['title'].lower():
            continue
        elif has_query_item(answer['content'], query_item):
            continue
        else:
            rc = False
            break

    return rc


def has_query_item(obj, query_item):
    """
    Performs traversal of the passed in obj until we reach object of the string type and 
    checks if it matches query_item.
    Object with (key)attribute 'type' are ignored as per requirement 
    """
    found = False
    if isinstance(obj, str):
        if query_item.lower() in obj.lower():
            return True

    if isinstance(obj, dict):
        for k, v in obj.items():
            if k != 'type':
                if has_query_item(v, query_item):
                    found = True
                    break

    elif isinstance(obj, list):
        for elem in obj:
            if has_query_item(elem, query_item):
                found = True
                break

    return found

if __name__ == "__main__":
    app.run(debug=True)
