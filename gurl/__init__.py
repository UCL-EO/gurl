#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import urlpath
import stat

import urllib
from pathlib import PosixPath, _PosixFlavour, PurePath
from pathlib import Path
from cylog import Cylog

import urllib.parse
import requests
from bs4 import BeautifulSoup
import io

'''
class derived from urlpath to provide pathlib-like
interface to url data
'''

__author__    = "P. Lewis"
__email__     = "p.lewis@ucl.ac.uk"
__date__      = "28 Aug 2020"
__copyright__ = "Copyright 2020 P. Lewis"
__license__   = "GPLv3"

def clean(*args):
  '''
  delete trailing / that 
  otherwise can cause problems
  '''
  args = list(*args)
  for i,arg in enumerate(args):
    arg = str(arg)
    while arg[-1] == '/':
      if len(arg) == 1:
        break
      arg = arg[:-1]
    args[i] = arg
  args = tuple(args)
  return args

class Zerostat(object):
  def __init__(self,*args):
    if len(args):
      self.st_mode = int(args[0])
    else:
      self.st_mode = 0

class URL(urlpath.URL,urllib.parse._NetlocResultMixinStr, PurePath):
  '''
  Derived from 
  https://raw.githubusercontent.com/chrono-meter/urlpath/master/urlpath.py

  to provide more compatibility with pathlib.Path functionality

  If a request is made to access the URL read_bytes() or read_text()
  and self.cache is True, then self.local_file is used as a cache

  Keywords:
    
  suffix_value
  ------------
  If suffix_value is set to a string (e.g. ".store") then this 
  is appended to the cached filename. This can be useful for
  distinguishing cached files from other types.

  suffix_value=None : If suffix_value is set to a string (e.g. ".store")
                      then this is appended to the cached filename. 
                      This can be useful for distinguishing cached files 
                      from other types.
  verbose=True      : verbose switch
  log=None          : set to string to send verboise output to file
  pwr=False         : password required (pwr). Sewt to True is the URL
                      requires username and password. You can then either
                      set 

                          self.with_userinfo(username, password)

                      explicitly, or be prompted and store in local
                      cylog file (encrypted). The use of cylog is
                      the default if a password is required. Note that
                      if a call to get() without using a password fails,
                      then the code will next try it with a password i.e.
                      try to guess that you meant pwr=True. You can stop
                      that behaviour by setting pwr=None. 
  binary=False      : Set to true if you want to retrieve a binary file.
  cache=False       : Set to True if you want to cache any retrieved 
                      results. The cached filename is self.local_file
  local_dir="."     : Set the name of the root directory for cached
                      files. The default is ".".
                      self.local_file is derived from this, being the
                      local_dir followed by the URL filename path.     

  '''
  # new type
  def __new__(cls,*args,**kwargs):
      '''
      make a new URL, without trailing '/'
      or return Path() if you refer to a local file
      '''
      args = clean(args)
      self = super(URL, cls).__new__(cls,*args)
      # test to see if we are a real URL
      if (self.scheme == '') or (self.scheme == 'file'):
        self = Path(*args,**kwargs)
      else:
        self.init(**kwargs)
      return self

  def __init__(self,*args,**kwargs):
    '''
    initialisation
    '''
    self.init(**kwargs)
    return

  def __del__(self):
    if 'log' in self.__dict__:
      del self.stderr  

  def init(self,*args,**kwargs):
    '''
    initialisation to update local info
    '''
    self.__dict__.update(kwargs)
    if 'verbose' not in self.__dict__:
      self.verbose = True
    if 'log' not in self.__dict__:
      self.stderr = sys.stderr
    else:
      self.stderr = Path(self.log).open("w+")
      self.msg(f"log file {self.log.as_posix()}")
    if 'pwr' not in self.__dict__:
      # login 
      self.pwr = False
    if 'binary' not in self.__dict__:
      self.binary = False
    if 'cache' not in self.__dict__:
      self.cache = False
    if 'local_dir' not in self.__dict__:
      self.local_dir = Path('.')
    self.local_file = Path(self.local_dir,self.components[2][1:])

    if ('suffix_value' in self.__dict__) and \
        (type(self.suffix_value) is str) and \
        (self.local_file) and \
        (self.local_file.name.find(self.suffix_value) < 0):
      self.local_file = \
        Path(self.local_file.as_posix() + self.suffix_value)
    self.local_file = self.local_file.absolute()

    if self.pwr:
      self.login()
    self.update()

  def login(self):
    if not (self.username and self.password):
      uinfo = Cylog(self.anchor).login()
      self._username,self._password = \
           uinfo[0].decode('utf-8'),uinfo[1].decode('utf-8')
    else:
      self._username,self._password = \
        self.username,self.password
    return self._username,self._password

  def update(self):
    st_mode = self.stat().st_mode
    self.readable = bool((st_mode & stat.S_IRUSR) /stat.S_IRUSR )
    self.writeable = bool((st_mode & stat.S_IWUSR) /stat.S_IWUSR )

  def stat(self):
    '''
    Return the result of the stat() system call on the local
    file for this path, like os.stat() does.
    '''
    if self.local_file.exists():
      return self.local_file.stat()
    else:
      return Zerostat(0)

  def get_data_without_login(self):
    '''
    get data from URL without using login

    '''
    data = None
    # get data from url
    # without login
    r = self.get()
    self.msg(f'get(): requests.Response code {r.status_code}')
    if r.status_code == 200:
      self.msg(f'retrieving content')
      if self.binary:
        data = r.content
      else:
        data = r.text
      self.msg(f'got {len(data)} bytes')
    else:
      self.msg(f'data not rerieved using no-password.')
      self.msg(f'Can try password login, but for future reference')    
      self.msg(f'you should set pwr=True if you think a password is needed.')
      self.msg(f'and/or you should check URL {str(self)}')
    
    return data

  def get_data(self,data):
    '''
    get data from URL

    Arguments:
      data : set to not None to skip

    '''
    if data:
      return data
    if not self.pwr:
      data = self.get_data_without_login()
    if (not data) and (self.pwr is not None): 
      # try pwr mode
      data = self.get_data_with_login()
    return data

  def read_bytes(self):
    '''
    Open the URL in bytes mode, read it, and close the file.
    '''
    if not self.binary:
      self.init(binary=True) 
    return self.read()

  def read_text(self):
    '''
    Open the URL in text mode, read it, and close the file.
    '''
    if self.binary:
      self.init(binary=False)
    return self.read()

  def read(self):
    '''
    Open the URL read it, and close the file.
    '''
    renew = False
    data = None

    if self.cache and self.readable:
      # look in cache for file
      self.msg(f"looking in cache {self.local_file.as_posix()}")
      if self.binary:
        data = self.local_file.read_bytes()
      else:
        data = self.local_file.read_text()

    if data is None:
      # get data from url
      self.msg(f"getting data from URL {str(self)}")
      data = self.get_data(data)
      renew = True

    if renew and self.cache:
      # save cache file
      self.msg(f"saving in cache")
      self.write(data,local=True)

    if data:
      self.msg("done")
    return data

  def write(self,data,local=True):
    '''
    Write data to local file if local=True
    '''
    if local and data:
      self.msg(f'writing data to cache file {self.local_file.as_posix()}')
      try:
        self.local_file.parent.mkdir(parents=True,exist_ok=True)
        if self.binary:
          self.local_file.write_bytes(data)
        else:
          self.local_file.write_text(data)
        # reset everything if we change the cache file
        if self.cache:
          self.update()
      except:
        self.msg(f"unable to save file {self.local_file.as_posix()}")
        return ''

    if data == None:
      self.msg(f"error calling write for None")
      return ''

    if local == False:
      self.msg(f"self.write(data,local=False) not yet implemented")
    
    return data

  def get_data_with_login(self,head=False):
    '''
    get data from URL with login. Wew try several strategies
    with the login that should allow for redirection
    and 'awkward' logins.

    '''
    data = None
    with requests.Session() as session:
      session.auth = self.login()
      self.msg(f'logging in to {self.anchor}')
      try:
        r = session.request('get',str(self))
        if r.status_code == 200:
          self.msg(f'data read get request from {self.anchor}')
          if self.binary:
            data = r.content
          else:
            data = r.text
          self.msg(f'got {len(data)} bytes')
          return data

        self.msg(f"request code {r.status_code}")
        self.msg(f"tried get request for data read from {self.anchor}")
        # try encoded login
        if head:
          self.msg(f"try to retrieve head")
          r = session.head(r.url)
        else:
          self.msg(f"try to retrieve data")
          r = session.get(r.url)
        if r.status_code == 302:
          self.msg(f"try to retrieve data")
          r = session.get(r.url)
          # redirection
          if type(r) == requests.models.Response:
            self.msg(f'sucessful response with get() from {self.anchor}')
            if self.binary:
              data = r.content
            else:
              data = r.text
            self.msg(f'got {len(data)} bytes')
            return data
        self.msg(f"request code {r.status_code}")

        if r.status_code == 200:
          if type(r) == requests.models.Response:
            self.msg(f'sucessful response with get() from {self.anchor}')
            if self.binary:
              data = r.content
            else:
              data = r.text
            self.msg(f'got {len(data)} bytes')
            return data
        self.msg(f"request code {r.status_code}")
      except:
        try:
          self.msg(f"request code {r.status_code}")
        except:
          pass

    self.msg(f'failure reading data from {self.anchor}')
    self.msg(f'data not rerieved.\nCheck URL {str(self)}')
    self.msg(f'and your login self._username, self._password')
    self.msg(f'If you have an incorrect password in the database')
    self.msg(f'run Cylog("{str(self)}").login(force=True)')
    return None

  def msg(self,*args,stderr=sys.stderr):
    '''
    messaging
    '''
    if self.verbose:
      print('>>>>',*args,file=stderr)

  def write_bytes(self,data):
    '''
    Open the URL as local file in bytes mode, write it, and close the file.
    '''
    if not self.binary:
      self.init(binary=True)
    return len(self.write(data,local=True))
 
  def write_text(self,data):
    '''
    Open the URL as local file in text mode, write it, and close the file.
    '''
    if self.binary:
      self.init(binary=False)
    return len(self.write(data,local=True))

  def clear(self):
    '''
    clear cached file
    '''
    if self.cache and self.local_file.exists():
      try:
        self.local_file.unlink()
      except:
        pass
    # reset everything if we change the cache file
    self.update()

def main():
  u='https://e4ftl01.cr.usgs.gov/MOTA/MCD15A3H.006/2003.12.11/MCD15A3H.A2003345.h09v06.006.2015084002115.hdf'
  url = URL(u)
  data = url.read_bytes()
  assert len(data) == 3365255
  print('passed 1')

  url = URL(u,cache=True,pwr=True,binary=True)
  data = url.read_bytes()
  assert len(data) == 3365255
  print('passed 2')

  url = URL(u,pwr=True,binary=True)
  data = url.read_bytes()
  assert len(data) == 3365255
  print('passed 3')


if __name__ == "__main__":
    main()

