#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
import re
import riak
import sys
import uuid

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
PROJECTS_DIR = os.path.dirname(PROJECT_DIR)
TESTS_DIR = os.path.dirname(PROJECTS_DIR)

sys.path.insert(0, TESTS_DIR)

from  config import (
    BUCKET,
    HOST,
    PB_PORT,
)

QF_RE = re.compile(r"(['%\s])")

def quote_field(f):
    def qf(m):
        return '%%%02x' % ord(m.group(1))

    s = QF_RE.sub(qf, f)

    if isinstance(s, str):
        return unicode(s, 'utf-8')
    return s


UF_RE = re.compile(r'''%[0-9a-f]{2}''')

def unquote_field(f):
    def uf(m):
        g1 = m.group(0)
        return chr(int(g1[1:], 16))

    return UF_RE.sub(uf, f)


class Transaction(object):
    def __init__(self):
        self.custom_timers = {}
        self.client = riak.RiakClient(host=HOST, port=PB_PORT, transport_class=riak.RiakPbcTransport)
        self.bucket = self.client.bucket(BUCKET)
        self.owner = "/c67256bbd9100aa480e11a0e8234e74a9"
        self.parent = "/c67256bbd91003e7b0e1135096c51f49b"
        self.file_idx = 0


    def search(self, name):
        mr = riak.RiakMapReduce(self.client)
        mr.search(BUCKET, "p:%s AND n:'%s'" % (self.parent, quote_field(name)))
        r = []
        queryset = mr.run()
        if queryset:
            for row in queryset:
                key = str(row.get_key())
                r.append(key)
        return r


    def _name(self):
        self.file_idx += 1
        return "file_%08d" % self.file_idx
    name = property(_name)

    def _random_name(self):
        self.file_idx += 1
        return uuid.uuid1().hex
    random_name = property(_random_name)

    def _key(self):
        return "f%s" % uuid.uuid1().hex[::-1]
    key = property(_key)

    def _make_data(self, name):
        return {
            "a_num": 1307375046,
            "al": [],
            "b_num": 294,
            "c_num": 1307375046,
            "cn": "ObjectInfo",
            "ct": "application/octet-stream",
            "gr": "",
            "m_num": 1307375046,
            "m5": "0926719f1656140bb9071cf1652136db",
            "n": quote_field(name),
            "nsh_num": 0,
            "o": self.owner,
            "p": self.parent,
            "pu": False,
            "sc": {
                "module-data": "opaque module data",
                "module-id": "test_module"
            },
            "sh": False,
            "t": "",
            "v_num": 0
        }


    def run(self):
        #name = self.name
        name = self.random_name
        res = self.search(name)
        if res:
            key = res[0].strip('/')
        else:
            key = self.key

        data = json.dumps(self._make_data(name))
        obj = self.bucket.new_binary(key, data=data,
            content_type='application/json')
        obj.store()

if __name__ == '__main__':
    trans = Transaction()
    trans.run()
