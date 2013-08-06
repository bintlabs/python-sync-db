from flask import Flask, request
import models


app = Flask(__name__)


@app.route("/")
def root():
    return 'Pull: GET <a href="/pull">/pull</a><br />'\
        'Push: POST /push<br />'\
        'Query: GET <a href="/query">/query</a>'


@app.route("/pull", methods=["GET"])
def pull():
    return "Pull placeholder"


@app.route("/push", methods=["POST"])
def push():
    return "Push placeholder"


@app.route("/query", methods=["GET"])
def query():
    session = models.Session()
    return u"<strong>Cities:</strong><pre>{0}</pre>"\
        u"<strong>Houses:</strong><pre>{1}</pre>"\
        u"<strong>Persons:</strong><pre>{2}</pre>".format(
        u"\n".join(session.query(models.City)),
        u"\n".join(session.query(models.House)),
        u"\n".join(session.query(models.Person)))


if __name__ == "__main__":
    app.run()
