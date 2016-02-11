# coding:utf-8

# python
import asyncio
import random

# 3rd party
from aiopg.sa import create_engine
import sqlalchemy as sa

# config
import config as c

# tornado
import tornado.concurrent
import tornado.ioloop
import tornado.web
import tornado.platform.asyncio
import tornado.httpclient
from tornado.httpserver import HTTPServer


# --------------------------------------------------------------------SQLAlchemy
metadata = sa.MetaData()

tbl = sa.Table(
    'tbl', metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('val', sa.String(255))
)


# ---------------------------------------------------------engine and sqla usage
@asyncio.coroutine
def get_engine():
    engine = yield from create_engine(user='shandec',
                                      database='testdb',
                                      host='127.0.0.1',
                                      password='')
    return engine


@asyncio.coroutine
def go():
    engine = yield from get_engine()
    with (yield from engine) as conn:
        #yield from conn.execute(tbl.insert().values(val='abc'))
        res = yield from conn.execute("select * from tbl limit %s", 2)
        #res = yield from conn.execute(tbl.select().where(tbl.c.val=='abc'))
        data = [row for row in res]
        return data


# -----------------------------------------------------------------------Handler
class MainHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        sleep_time = random.randint(3,5)
        yield from asyncio.sleep(sleep_time)
        data = yield from go()
        self.write("sleep for {} and data {}\n".format(sleep_time, data))


# -------------------------------------------------------------------------- App
def make_app():
    tornado.platform.asyncio.AsyncIOMainLoop().install()
    app = tornado.web.Application([
        (r"/", MainHandler)
    ])
    http_server = HTTPServer(app)
    http_server.listen(c.PORT)
    asyncio.get_event_loop().run_forever()


#def benchmark():
    #for i in range(5):
        #r = requests.get('127.0.0.1:{}'.format(c.PORT))
        #print(r, r.text)


if __name__ == '__main__':
    make_app()
    #benchmark()
