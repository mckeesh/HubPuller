from urllib.request import urlopen
from operator import itemgetter
from datetime import datetime

import requests
import MySQLdb
import time
import uuid


GITHUB_KEY = '97545ccc3291cf6fe86effb5a8201e182da3ba71'
conn = None #connection
db = None

def main():
    global conn
    global db

    database = 'angular'
    db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                     user="root",         # your username
                     passwd="pass",  # your password
                     db="angular",
                     charset="utf8")        # name of the data base
    conn = db.cursor()

    # load_issue_events()
    load_issues()

def load_issue_events():
    page_num = 567
    while True:
        jsonData = get_issue_events_json(page_num)
        if len(jsonData) == 0:
            break

        while rate_limit_exceeded():
            print('GitHub Rate Limit Exceeded. Sleeping for a minute.')
            time.sleep(60)

        for jsonEvent in jsonData:
            db_issue_event_writer(jsonEvent)

        page_num += 1

def load_issues():
    page_num = 1
    while True:
        jsonData = get_issues_json(page_num)
        if len(jsonData) == 0:
            break

        while rate_limit_exceeded():
            print('GitHub Rate Limit Exceeded. Sleeping for a minute.')
            time.sleep(60)

        for jsonEvent in jsonData:
            db_issue_writer(jsonEvent)

        page_num += 1

# def get_actors():
#     page_num = 1
#     actors = {}
#     while True:
#         print("Page %d:" % page_num)
#         jsonData = get_issue_events_json(page_num)

#         if len(jsonData) == 0:
#             break

#         for each in jsonData:
#             actor_name = each['actor']['login']

#             if actor_name in actors:
#                 actors[actor_name] += 1
#             else:
#                 actors[actor_name] = 1

#         pretty_print(actors)
#         print("")
#         print("")
#         print("")
#         page_num += 1

#     return actors

def get_issues_json(page_num):
    print('Getting issue page %d' % page_num)
    return requests.get('https://api.github.com/repos/angular/angular/issues?access_token=%s&page=%d' % (GITHUB_KEY, page_num)).json()

def get_issue_events_json(page_num):
    print('Getting issue event page %d' % page_num)
    return requests.get('https://api.github.com/repos/angular/angular/issues/events?access_token=%s&page=%d' % (GITHUB_KEY, page_num)).json()


def pretty_print(d):
    sorted_items = sorted(d.items(), key=itemgetter(1))
    for each in sorted_items:
        print(each)

def db_issue_writer(json_issue):
    global conn
    global db

    try:

        user_obj = json_issue['user']
        if user_obj != None:
            user_id = json_issue['user']['id']
        else:
            user_id = None

        assignee = json_issue['assignee']
        if assignee != None:
            assignee_id = assignee['id']
        else:
            assignee_id = None

        milestone = json_issue['milestone']
        if milestone != None:
            milestone_id = milestone['id']
        else:
            milestone_id = None

        issueID = json_issue['id']
        url = json_issue['url']
        number = json_issue['number']
        title = json_issue['title']
        state = json_issue['state']
        locked = json_issue['locked']
        assignees = json_issue['assignees'] #???
        comments = json_issue['comments']
        created_at = json_issue['created_at']
        updated_at = json_issue['updated_at']
        closed_at = json_issue['closed_at']
        labels = json_issue['labels']
        repo_url = json_issue['repository_url']
        labels_url = json_issue['labels_url']
        comments_url = json_issue['comments_url']
        events_url = json_issue['events_url']
        html_url = json_issue['html_url']

        body = sanitizeBody(json_issue['body'])

        if 'closed_by' in json_issue:
            closed_by = json_issue['closed_by']
        else:
            closed_by = None

    except TypeError as e:
        print(str(e))
        print('FAILING ISSUE:')
        print(json_issue)
        return

    db_labels_writer(issueID, labels)
    created_at_object = getTimeWrapper(created_at)
    updated_at_object = getTimeWrapper(updated_at)
    closed_at_object = getTimeWrapper(closed_at)

    try:
        query = "INSERT INTO issues VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        conn.execute(query, (issueID,url,number,title,state,locked,user_id,assignee_id,milestone_id,int(comments),created_at_object,updated_at_object,closed_at_object,closed_by,body,repo_url,labels_url,comments_url,events_url,html_url))
        db.commit()
    except Exception as e:
        print(str(e))
        import pdb; pdb.set_trace()
        db.rollback()

def db_user_writer(json_user):
    """
    "login": "vsavkin",
    "id": 35996,
    "avatar_url": "https://avatars.githubusercontent.com/u/35996?v=3",
    "gravatar_id": "",
    "url": "https://api.github.com/users/vsavkin",
    "html_url": "https://github.com/vsavkin",
    "followers_url": "https://api.github.com/users/vsavkin/followers",
    "following_url": "https://api.github.com/users/vsavkin/following{/other_user}",
    "gists_url": "https://api.github.com/users/vsavkin/gists{/gist_id}",
    "starred_url": "https://api.github.com/users/vsavkin/starred{/owner}{/repo}",
    "subscriptions_url": "https://api.github.com/users/vsavkin/subscriptions",
    "organizations_url": "https://api.github.com/users/vsavkin/orgs",
    "repos_url": "https://api.github.com/users/vsavkin/repos",
    "events_url": "https://api.github.com/users/vsavkin/events{/privacy}",
    "received_events_url": "https://api.github.com/users/vsavkin/received_events",
    "type": "User",
    "site_admin": false,
    "contributions": 758
    """

    login = json_user['login']
    userID = json_user['id']
    url = json_user['url']
    followers_url = json_user['followers_url']
    following_url = json_user['following_url']
    gists_url = json_user['gists_url']
    starred_url = json_user['starred_url']
    subscriptions_url = json_user['subscriptions_url']
    organizations_url = json_user['organizations_url']
    repos_url = json_user['repos_url']
    events_url = json_user['events_url']
    received_events_url = json_user['received_events_url']
    userType = json_user['type']
    site_admin = json_user['site_admin']
    contributions = int(json_user['contributions'])



def getTimeWrapper(datetimeStr):
    if datetimeStr != None:
        datetimeObj = datetime.strptime(datetimeStr,'%Y-%m-%dT%H:%M:%SZ')
    else:
        datetimeObj = None

    return datetimeObj

def sanitizeBody(body):
    return ""
    # if body != None:
    #     return body.encode('utf-8')
    # else:
    #     return None

def db_issue_event_writer(json_event):
    global conn
    global db

    try:

        actor = json_event['actor']
        if actor != None:
            actorID = json_event['actor']['id']
        else:
            actorID = None

        issue = json_event['issue']
        if issue != None:
            issueID = json_event['issue']['id']
        else:
            issueID = None

        eventID = json_event['id']
        eventURL = json_event['url']
        eventType = json_event['event']
        created_at = json_event['created_at']
        commit_id = json_event['commit_id']
        commit_url = json_event['commit_url']
        date_object = datetime.strptime(created_at,'%Y-%m-%dT%H:%M:%SZ')

    except TypeError as e:
        print(str(e))
        print('FAILING ISSUE EVENT:')
        print(json_event)
        return

    try:
        query = "INSERT INTO issue_events VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        conn.execute(query, (eventID,actorID,issueID,eventURL,eventType,date_object,commit_id,commit_url))
        db.commit()
    except Exception as e:
        print(str(e))
        db.rollback()

def db_labels_writer(issue_id, json_labels):
    for label in json_labels:
        url = label['url']
        name = label['name']
        color = label['color']
        label_id = uuid.uuid4()

        try:
            query = "INSERT INTO issue_labels VALUES (%s,%s,%s,%s,%s)"
            conn.execute(query, (label_id, url, name, color, issue_id))
            db.commit()
        except Exception as e:
            print(str(e))
            db.rollback()

# def db_user_writer(json_actor):
#     global conn
#     global db

#     try:
#        x.execute("INSERT INTO anooog1 VALUES (%s,%s)" % (188,90))
#        conn.commit()
#     except:
#        db.rollback()

def rate_limit_exceeded():
    rate_limit_obj = requests.get('https://api.github.com/rate_limit').json()
    return rate_limit_obj['resources']['core']['remaining'] == 0
    

main()