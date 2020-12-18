#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import urlpath
import stat

import urllib
from pathlib import PosixPath, _PosixFlavour, PurePath
from pathlib import Path
from cylog import Cylog
from gurl.list_file import ListPath
import urllib.parse
import requests
from bs4 import BeautifulSoup
import io
import numpy as np
import os
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
    while (len(arg) > 0) and (arg[-1] == '/'):
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
  and self.cache is True, then self.local_file_* is used as a cache.

  The cache is defined relative to self.local_dir (ListPath, a list of Paths)
  The reason this is a list is to allow the concept of some global
  cache database (where the user might not have write permission, but will 
  have read permission). So, the cache filename may be different
  for read and write. Because of this, the cache i
  mechanism allows for two concepts of self.local_file:
  - self.local_file (read)  and 
  - self.local_file_write (write)
  These may of course be the same in some cases.

  Keywords:
   
  ofile=filename    : filename to save downloaded URL to 
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
                      results. The cached filename is self.local_file (read)
                      or self.local_file_write (write)
  local_dir=ListPath(".")     
                    : Set the name of the root directory for cached
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
      # close the open log file
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

    if 'ofile' in self.__dict__:
      self.local_dir = Path(self.ofile).parent
      self.local_file,self.local_file_write = \
                       Path(self.ofile),Path(self.ofile)
    elif self.cache:
      if 'local_dir' not in self.__dict__:
        self.local_dir = '.'
      self.local_dir = self.sort_local_dir()
      self.local_file,self.local_file_write = self.sort_local_file()

    if self.pwr:
      self.login()
    if ('ofile' in self.__dict__) or self.cache:
      self.save = True
      self.update()
    else:
      self.save = False

  def sort_local_dir(self):
    '''
    deal with mechanics of local_dir
    '''
    if ('CACHE_FILE' in os.environ):
      local_dir = ListPath([os.environ['CACHE_FILE'],\
                                  list(self.local_dir)])
    else:
      local_dir =  ListPath(self.local_dir)
    return ListPath(local_dir)   
 
  def sort_local_file(self):
    '''
    deal with mechanics of local_file
    '''
    # consider possible cache files
    # some will be readable, some writeable
    components = self.components[2]

    if len(components) and (components[0] == '/'):
      components = components[1:]
    local_files = ListPath([l.joinpath(components).absolute()
                            for l in self.local_dir])

    try:
      local_file_write = np.array(local_files)[local_files.write][0]
    except:
      local_file_write = None

    if local_file_write is None:
      # try parent
      try:
        local_file_write = np.array(local_files)[local_files.parentwrite][0]
      except:
        local_file_write = None

    try:
      local_file_read = np.array(local_files)[local_files.read][0]
    except:
      local_file_read = local_file_write

    return local_file_read,local_file_write

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
    stat_read,stat_write = self.stat()
    self.readable = bool((stat_read.st_mode & stat.S_IRUSR) /stat.S_IRUSR )
    self.writeable = bool((stat_write.st_mode & stat.S_IWUSR) /stat.S_IWUSR )

  def stat(self):
    '''
    Return the result of the stat() system call on the local
    file for this path, like os.stat() does. 

    If the file doesnt exist, then we also try appending
    index.html on the end to see if that exists (in case we request
    a directory, in which case it is cached as index.html)
    '''
    s1,s2 = Zerostat(0),Zerostat(0)

    if 'local_file' not in self.__dict__:
      return s1,s2
    if self.local_file.exists():
      if self.local_file.is_dir():
        # maybe its index.html??
        try_file = Path(self.local_file,"index.html")
        if try_file.exists() and try_file.is_file():
          self.binary = False
          if (self.suffix != ".html") and \
             (self.suffix != ".htm"):
            self.local_file = Path(self.local_file,"index.html")
          self.update()
      s1 = self.local_file.stat()

    if self.local_file_write.exists():
      if self.local_file_write.is_dir():
        # maybe its index.html??
        try_file = Path(self.local_file_write,"index.html")
        if try_file.exists() and try_file.is_file():
          self.binary = False
          if (self.suffix != ".html") and \
             (self.suffix != ".htm"):
            self.local_file_write = Path(self.local_file_write,"index.html")
          self.update()
      s2 =  self.local_file_write.stat()

    return s1,s2

  def set_directory(self,r):
    '''
    If the url is to a httpd/unix-directory object
    then set the type as text and set the local filename
    as index.html. This avoids problems with the cache
    structure.

    '''
    self.content_type = r.headers['Content-Type']
    self.msg(f"content type: {self.content_type}")
    if (self.content_type == 'httpd/unix-directory') or \
       (self.content_type.split(';')[0] == 'text/html'):
      self.binary = False
      if (self.suffix != ".html") and \
         (self.suffix != ".htm"):
        if self.save:
          self.local_file = Path(self.local_file,"index.html")
      if self.save:
        self.update()

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
      self.set_directory(r)
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
    self.get_links(data)
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
    data = self.read()
    self.get_links(data)
    return data

  def is_html(self,data):
    '''
    True if data is html
    '''
    start = '<!DOCTYPE HTML'
    len_start = len(start)

    if (len(data) < len_start):
      return False
    if (type(data) != str):
      return False
    if (data[:len_start] == start):
      return True
    return False

  def get_links(self,data,force=False):
    '''
    if the type is httpd/unix-directory or
    text/html, then set self.links to any relative links
    found in the file
    '''
    if ('links' in self.__dict__) and (not force):
      return self.links
    
    self.links = []
    if not self.is_html(data):
      return self.links

    # get relative links (filter out those that start ?
    # or http
    if (type(data) is str) or ((self.content_type == 'httpd/unix-directory') or \
       (self.content_type.split(';')[0] == 'text/html')):
      links = np.array([mylink.attrs['href'] \
         for mylink in BeautifulSoup(data,'lxml').find_all('a')])
      self.links = [str(l).rstrip('/#') for l in links \
                    if ((l[0] != '?') and (l[:len('http')] != 'http'))]
      self.msg(f'found {len(self.links)} links')
      self.links = [URL(self,i,pwr=self.pwr,cache=self.cache,binary=True) for i in self.links]
    return self.links

  def read(self):
    '''
    Open the URL read it, and close the file.
    '''
    renew = False
    data = None

    if self.cache and self.readable and self.local_file.is_file():
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
      self.get_links(data)
      self.msg("done")
    return data

  def write(self,data,local=True,ofile=None):
    '''
    Write data to local file if local=True

    Output filename can be set from ofile keyword
    otherwise it is self.local_file_write
    '''
    if ('local_file_write' not in self.__dict__) and (ofile == None):
      self.msg(f'cannot write: no ofile or cache (local_dir) defined')
      return ''

    if self.cache and self.local_file_write.is_dir():
        # cant use as cache
        if (self.content_type == 'httpd/unix-directory') or \
           (self.content_type.split(';')[0] == 'text/html'):
          if len(self.components[2]) == 0:
            # maybe it should be index.html??
            try_file = Path(self.local_file_write,"index.html")
            self.binary = False
            self.local_file_write = Path(self.local_file_write,"index.html")
          elif (self.suffix != ".html") and \
             (self.suffix != ".htm"):
            self.local_file_write = Path(self.local_file_write,"index.html")
          self.update()

    if ofile is None:
      ofile = self.local_file_write
 
    if local and data:
      self.msg(f'writing data to cache file {ofile.as_posix()}')
      try:
        ofile.parent.mkdir(parents=True,exist_ok=True)
        if self.binary:
          ofile.write_bytes(data)
        else:
          ofile.write_text(data)
        # reset everything if we change the cache file
        if self.cache:
          self.update()
      except:
        self.msg(f"unable to save file {ofile.as_posix()}")
        self.msg(f'data: type: {type(data)}; binary? {self.binary}')
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
          self.set_directory(r)
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
            self.set_directory(r)
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

  def write_bytes(self,data,ofile=None):
    '''
    Open the URL as local file in bytes mode, write it, and close the file.
    '''
    if not self.binary:
      self.init(binary=True)
    return len(self.write(data,local=True))
 
  def write_text(self,data,ofile=None):
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
    if self.cache and self.local_file_write.exists():
      try:
        self.local_file_write.unlink()
      except:
        pass
    # reset everything if we change the cache file
    self.update()

def main():
  print('test 3: defaults, read hdf')
  u='https://e4ftl01.cr.usgs.gov/MOTA/MCD15A3H.006/2003.12.11/MCD15A3H.A2003345.h09v06.006.2015084002115.hdf'
  url = URL(u)
  data = url.read_bytes()
  assert len(data) == 3365255
  print('passed 1')

  print('test 2: cache, password required, read hdf')
  url = URL(u,cache=True,pwr=True,binary=True)
  data = url.read_bytes()
  assert len(data) == 3365255
  print('passed 2')

  print('test 3: no cache, password required, read hdf')
  url = URL(u,pwr=True,binary=True)
  data = url.read_bytes()
  assert len(data) == 3365255
  print('passed 3')

  print('test 4: no cache, password required, read html')
  u='https://e4ftl01.cr.usgs.gov/MOTA/MCD15A3H.006/2003.12.11/'
  url = URL(u,pwr=True)
  data = url.read_text()
  assert len(data) == 210369
  print('passed 4')

  print('test 5: no cache, password required, read html')
  u='https://e4ftl01.cr.usgs.gov/MOTA/MCD15A3H.006/'
  url = URL(u,pwr=True)
  data = url.read_text()
  assert len(data) == 200239
  print('passed 5')

  print('test 6: multi-level, no cache, password required, read html')
  u='https://e4ftl01.cr.usgs.gov/'
  url = URL(u,pwr=True)
  data = url.read_text()
  print(url)
  # read next level
  data = url.links[5].read()
  print(url.links[5])
  # next level
  data = url.links[5].links[1].read()
  print(url.links[5].links[1])
  # next level
  data = url.links[5].links[1].links[1].read()
  print(url.links[5].links[1].links[1])
  # next level ... now a jpg file
  data = url.links[5].links[1].links[1].links[1].read() 
  print(url.links[5].links[1].links[1].links[1])
  assert len(data) == 15718
  print('passed 6')

  print('test 7: multi-level, with cache, password required, read html')
  u='https://e4ftl01.cr.usgs.gov/'
  url = URL(u,pwr=True,cache=True)
  data = url.read_text()
  print(url)
  # read next level
  data = url.links[5].read()
  print(url.links[5])
  # next level
  data = url.links[5].links[1].read()
  print(url.links[5].links[1])
  # next level
  data = url.links[5].links[1].links[1].read()
  print(url.links[5].links[1].links[1])
  # next level ... now a jpg file
  data = url.links[5].links[1].links[1].links[1].read()  
  print(url.links[5].links[1].links[1].links[1])
  assert len(data) == 15718
  print('passed 7')




if __name__ == "__main__":
    main()

