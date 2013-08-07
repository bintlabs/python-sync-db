import sys, os

cwd = u"".join(reversed(os.getcwd()))
test_dir = "tset-ppa"
try:
    base_dir = "".join(reversed(cwd[cwd.index(test_dir) + len(test_dir):]))
except ValueError:
    base_dir = os.getcwd()

if base_dir not in sys.path:
    sys.path.append(base_dir)

import json
from flask import Flask, request
import models

from dbsync import models as synchmodels, server


app = Flask(__name__)


def enc(string):
    table = {"<": "&lt;",
             ">": "&gt;"}
    return u"".join(table.get(c, c) for c in string)


@app.route("/")
def root():
    return 'Pull: GET <a href="/pull">/pull</a><br />'\
        'Push: POST /push<br />'\
        'Query: GET <a href="/query">/query</a><br />'\
        'Synch query: GET <a href="/synch">/synch</a>'


@app.route("/pull", methods=["GET"])
def pull():
    return json.dumps(server.handle_pull(request.args))


@app.route("/push", methods=["POST"])
def push():
    return json.dumps(server.handle_push(request.json))


@app.route("/query", methods=["GET"])
def query():
    session = models.Session()
    return u"<strong>Cities:</strong><pre>{0}</pre><hr />"\
        u"<strong>Houses:</strong><pre>{1}</pre><hr />"\
        u"<strong>Persons:</strong><pre>{2}</pre><hr />".format(
        u"\n".join(enc(repr(x)) for x in session.query(models.City)),
        u"\n".join(enc(repr(x)) for x in session.query(models.House)),
        u"\n".join(enc(repr(x)) for x in session.query(models.Person)))


@app.route("/synch", methods=["GET"])
def synch():
    session = models.Session()
    return u"<strong>Content Types:</strong><pre>{0}</pre><hr />"\
        u"<strong>Versions:</strong><pre>{1}</pre><hr />"\
        u"<strong>Operations:</strong><pre>{2}</pre><hr />".format(
        u"\n".join(enc(repr(x)) for x in session.query(synchmodels.ContentType)),
        u"\n".join(enc(repr(x)) for x in session.query(synchmodels.Version)),
        u"\n".join(enc(repr(x)) for x in session.query(synchmodels.Operation)))


if __name__ == "__main__":
    app.run(debug=True)
