# -*- encoding: utf-8 -*-

"""
        persistentdict module $Revision: 65 $ $Date: 2011-06-08 20:57:07 +0200 (wo, 08 jun 2011) $

        (c) 2011 Michel J. Anders

        This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""

from collections import UserDict
import sqlite3 as sqlite
from pickle import dumps,loads
import threading 

class PersistentDict(UserDict):

        """
        PersistentDict  a MutableMapping that provides a thread safe,
                                        SQLite backed persistent storage.
        
        db              name of the SQLite database file, e.g.
                        '/tmp/persists.db', default to 'persistentdict.db'
        table   name of the table that holds the persistent data,
                        usefull if more than one persistent dictionary is
                        needed. defaults to 'dict'
        
        PersistentDict tries to mimic the behaviour of the built-in
        dict as closely as possible. This means that keys should be hashable.
        
        Usage example:
        
        >>> from persistentdict import PersistentDict
        >>> a=PersistentDict()
        >>> a['number four'] = 4
        
        ... shutdown and then restart applicaion ...
        
        >>> from persistentdict import PersistentDict
        >>> a=PersistentDict()
        >>> print(a['number four'])
        4
        
        Tested with Python 3.2 but should work with 3.x and 2.7.x as well.
        
        run module directly to run test suite:
        
        > python PersistentDict.py
        
        """
        
        def __init__(self, filename="temp.db", dict=None, **kwargs):
                
                self.db    = kwargs.pop('db',filename)
                self.table = kwargs.pop('table','dict')
                self.local = threading.local()
                
                with self.connect() as conn:
                        conn.execute('create table if not exists %s (hash unique not null,key,value);'%self.table)
                                
                if dict is not None:
                        self.update(dict)
                if len(kwargs):
                        self.update(kwargs)
        
        def connect(self):
                try:
                        return self.local.conn
                except AttributeError:
                        self.local.conn = sqlite.connect(self.db)
                return self.local.conn
                        
        def __len__(self):
                cursor = self.connect().cursor()
                cursor.execute('select count(*) from %s'%self.table)
                return cursor.fetchone()[0]
        
        def __getitem__(self, key):
                cursor = self.connect().cursor()
                h=hash(key)
                cursor.execute('select value from %s where hash = ?'%self.table,(h,))
                try:
                        return loads(cursor.fetchone()[0])
                except TypeError:
                        if hasattr(self.__class__, "__missing__"):
                                return self.__class__.__missing__(self, key)
                raise KeyError(key)
                
        def __setitem__(self, key, item):
                h=hash(key)
                with self.connect() as conn:
                        conn.execute('insert or replace into %s values(?,?,?)'%self.table,(h,dumps(key),dumps(item)))

        def __delitem__(self, key):
                h=hash(key)
                with self.connect() as conn:
                        conn.execute('delete from %s where hash = ?'%self.table,(h,))

        def __iter__(self):
                cursor = self.connect().cursor()
                cursor.execute('select key from %s'%self.table)
                for row in cursor.fetchall():
                        yield loads(row[0])

        def __contains__(self, key):
                h=hash(key)
                cursor = self.connect().cursor()
                cursor.execute('select value from %s where hash = ?'%self.table,(h,))
                return not ( None is cursor.fetchone())

        def __repr__(self):
                return "\n".join(self)

        # not implemented def __repr__(self): return repr(self.data)
        
        def copy(self):
                c = self.__class__(db=self.db)
                for key,item in self.items():
                        c[key]=item
                return c

# end of coverage

if __name__ == "__main__":
        
        import unittest
        import trace
        from collections import defaultdict
        from re import compile,match
        from os import unlink
        eoc=compile(r'^\s*# end of coverage')
        ignore=compile(r'(^\s*$)|(^\s*((def)|(import)|(from)|(class)))|(^\s*#)')
        triplequote=compile('(""")|(\'\'\')')
        class A:
                def __init__(self,a): self.a=a
                
        class PersistentDictGeneralTest(unittest.TestCase):
                def setUp(self):
                        self.db=PersistentDict(db=':memory:')
                
                def test_basic(self):
                        self.db['a']=1
                        self.assertEqual(self.db['a'],1)
                        self.assertEqual(len(self.db),1)
                        self.db['a']=2
                        self.assertEqual(self.db['a'],2)
                        self.assertEqual(len(self.db),1)
                        self.db['b']=3
                        self.assertEqual(len(self.db),2)
                        with self.assertRaises(KeyError):
                                v=self.db['c']

                def test_delete(self):
                        self.db['a']=1
                        del self.db['a']
                        self.assertEqual(len(self.db),0)
                
                def test_iter(self):
                        d={'a':1,'b':2}
                        for k,v in d.items():
                                self.db[k]=v

                        n=0
                        for key in self.db:
                                n+=1
                                self.assertEqual(d[key],self.db[key])
                        self.assertEqual(len(d),n)
                
                        n=0
                        for k,v in self.db.items():
                                n+=1
                                self.assertEqual(d[k],v)
                        self.assertEqual(len(d),n)
                
                def test_implied(self):
                        self.db['a']=1
                        self.assertTrue('a' in self.db)
                        self.assertFalse('b' in self.db)
                        self.db.update({'b':2})
                        self.assertEqual(self.db,{'a':1,'b':2}) # this is a subtle one, it not only tests that update worked but also that PersistentDict is recognizable a dict to assertEqual()
                        self.assertEqual(self.db.get('c',3),3)
                        self.assertEqual(self.db.pop('a'),1)
                        self.assertEqual(self.db,{'b':2})
                        
                def test_multiple(self):
                        self.db2=PersistentDict(db=':memory:')
                        self.db['a']=1
                        self.db2['a']=2
                        self.assertEqual(self.db['a'],1)
                        self.assertEqual(self.db2['a'],2)
                        self.db3=self.db.copy()
                        self.assertEqual(self.db['a'],1)
                        self.assertEqual(self.db3['a'],1)
                        self.db3['a']=2
                        self.assertEqual(self.db['a'],1)
                        self.assertEqual(self.db3['a'],2)
        
        class PersistentDictAltCreationTest(unittest.TestCase):
        
                def test_table(self):
                        db  = PersistentDict(db=':memory:')
                        db2 = PersistentDict(db=':memory:',table='test')
                        a={'a':1}
                        b={'b':2}
                        db.update(a)
                        db2.update(b)
                        self.assertEqual(db,a)
                        self.assertEqual(db2,b)

                def test_keyword(self):
                        db3 = PersistentDict(a=1,db=':memory:',)
                        self.assertEqual(db3['a'],1)
        
                def test_dict(self):
                        db = PersistentDict({'a':1},db=':memory:')
                        self.assertEqual(db,{'a':1})
                        
        class PersistentDictTypesTest(unittest.TestCase):
                def setUp(self):
                        self.db=PersistentDict(db=':memory:')
                
                def test_tuple(self):
                        ob=(1,2)
                        self.db[1]=ob
                        self.assertEqual(self.db[1],ob)
                        self.db[ob]=3
                        self.assertEqual(self.db[ob],3)
                        
                def test_list(self):
                        ob=[1,2]
                        self.db[1]=ob
                        self.assertEqual(self.db[1],ob)
                        with self.assertRaises(TypeError):
                                self.db[ob]=3
                        
                def test_hash(self):
                        ob={'a':1}
                        self.db[1]=ob
                        self.assertEqual(self.db[1],ob)
                        with self.assertRaises(TypeError):
                                self.db[ob]=3
                        
                def test_class(self):
                        # a class to be pickled must be globally defined.
                        ob=A(a=42)
                        self.db[1]=ob
                        # object returned will not be identical (is different instance) but fields should be.
                        self.assertEqual(self.db[1].a,ob.a)
                        self.db[ob]=3
                        self.assertEqual(self.db[ob],3)
                
                def test_unicode(self):
                        ob="ö\N{GREEK CAPITAL LETTER DELTA}ç"
                        self.db[1]=ob
                        self.assertEqual(self.db[1],ob)
                        self.db[ob]=3
                        self.assertEqual(self.db[ob],3)

        class PersistentDictSubclassTest(unittest.TestCase):
                
                def test_subclass(self):
                        class A(PersistentDict):
                                def __missing__(self,key):
                                        return 'oink'
                        ob={'a':1,'b':2}
                        d=A(ob)
                        self.assertEqual(d['a'],1)
                        self.assertEqual(d['c'],'oink')
        
        class PersistentDictThreadTest(unittest.TestCase):
                
                def test_thread(self):
                        #multithreading doesn't work w. :memory: databases, or rather each separate connection
                        #will have a separate database so we need a file based db here
                        try:
                                unlink('threadtest.db')
                        except:
                                pass
                        def run():
                                d=PersistentDict(db='threadtest.db')
                                self.assertEqual(d[1],2)
                        d=PersistentDict(db='threadtest.db')
                        d[1]=2
                        threading.Timer(0.0,run)
                        
        t=trace.Trace(count=1,trace=0)
        t.runfunc(unittest.main,exit=False)
        r=t.results()
 
        linecount = defaultdict(int)
        for line in r.counts:
                if line[0]==__file__:
                        linecount[line[1]]=r.counts[line]
 
        print('Lines not covered in testing:')
        n=0
        triplequotes=0
        with open(__file__) as f:
                for linenumber,line in enumerate(f,start=1):
                        if eoc.search(line) : break
                        m = len(triplequote.findall(line))
                        triplequotes+=m
                        if (m%2)==1:    continue
                        if linecount[linenumber]==0 and not ignore.match(line) and (triplequotes%2)!=1:
                                print("%02d %s"%(linenumber,line),end='')
                                n+=1
        print('None' if n==0 else '%d lines total.'%n)
