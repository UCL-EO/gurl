gurl
====

general URL class. 

class URL(urlpath.URL, urllib.parse._NetlocResultMixinStr, pathlib.PurePath)
 |  URL(*args, **kwargs)
 |  
 |  Derived from 
 |  https://raw.githubusercontent.com/chrono-meter/urlpath/master/urlpath.py
 |  
 |  to provide more compatibility with pathlib.Path functionality
 |  
 |  If a request is made to access the URL read_bytes() or read_text()
 |  and self.cache is True, then self.local_file is used as a cache
 |  
 |  Keywords:
 |    
 |  suffix_value
 |  ------------
 |  If suffix_value is set to a string (e.g. ".store") then this 
 |  is appended to the cached filename. This can be useful for
 |  distinguishing cached files from other types.
 |  
 |  suffix_value=None : If suffix_value is set to a string (e.g. ".store")
 |                      then this is appended to the cached filename. 
 |                      This can be useful for distinguishing cached files 
 |                      from other types.
 |  verbose=True      : verbose switch
 |  log=None          : set to string to send verboise output to file
 |  pwr=False         : password required (pwr). Sewt to True is the URL
 |                      requires username and password. You can then either
 |                      set 
 |  
 |                          self.with_userinfo(username, password)
 |  
 |                      explicitly, or be prompted and store in local
 |                      cylog file (encrypted). The use of cylog is
 |                      the default if a password is required. Note that
 |                      if a call to get() without using a password fails,
 |                      then the code will next try it with a password i.e.
 |                      try to guess that you meant pwr=True. You can stop
 |                      that behaviour by setting pwr=None. 
 |  binary=False      : Set to true if you want to retrieve a binary file.
 |  cache=False       : Set to True if you want to cache any retrieved 
 |                      results. The cached filename is self.local_file
 |  local_dir="."     : Set the name of the root directory for cached
 |                      files. The default is ".".
 |                      self.local_file is derived from this, being the
 |                      local_dir followed by the URL filename path.
 |  
 |  Method resolution order:
 |      URL
 |      urlpath.URL
 |      urllib.parse._NetlocResultMixinStr
 |      urllib.parse._NetlocResultMixinBase
 |      urllib.parse._ResultMixinStr
 |      pathlib.PurePath
 |      builtins.object
 |  Methods defined here:
 |  
 |  __del__(self)
 |  
 |  __init__(self, *args, **kwargs)
 |      initialisation
 |  
 |  clear(self)
 |      clear cached file
 |  
 |  get_data(self, data)
 |      get data from URL
 |      
 |      Arguments:
 |        data : set to not None to skip
 |  
 |  get_data_with_login(self, head=False)
 |      get data from URL with login. Wew try several strategies
 |      with the login that should allow for redirection
 |      and 'awkward' logins.
 |  
 |  get_data_without_login(self)
 |      get data from URL without using login
 |  
 |  init(self, *args, **kwargs)
 |      initialisation to update local info
 |  
 |  login(self)
 |  
 |  msg(self, *args, stderr=<_io.TextIOWrapper name='<stderr>' mode='w' encoding='UTF-8'>)
 |      messaging
 |  
 |  read(self)
 |      Open the URL read it, and close the file.
 |  
 |  read_bytes(self)
 |      Open the URL in bytes mode, read it, and close the file.
 |  
 |  read_text(self)
 |      Open the URL in text mode, read it, and close the file.
 |  
 |  stat(self)
 |      Return the result of the stat() system call on the local
 |      file for this path, like os.stat() does.
 |  
 |  update(self)
 |  
 |  write(self, data, local=True)
 |      Write data to local file if local=True
 |  
 |  write_bytes(self, data)
 |      Open the URL as local file in bytes mode, write it, and close the file.
 |  
 |  write_text(self, data)
 |      Open the URL as local file in text mode, write it, and close the file.
 |  
 |  ----------------------------------------------------------------------
 |  Static methods defined here:
 |  
 |  __new__(cls, *args, **kwargs)
 |      make a new URL, without trailing '/'
 |      or return Path() if you refer to a local file
 |  
 |  ----------------------------------------------------------------------
 |  Methods inherited from urlpath.URL:
 |  
 |  __bytes__(self)
 |      Return the bytes representation of the path.  This is only
 |      recommended to use under Unix.
 |  
 |  __str__(self)
 |      Return str(self).
 |  
 |  add_query(self, query=None, **kwargs)
 |      Return a new url with the query ammended.
 |  
 |  as_uri(self)
 |      Return URI.
 |  
 |  delete(self, **kwargs)
 |      Sends a DELETE request.
 |      
 |      :param \*\*kwargs: Optional arguments that ``request`` takes.
 |      :return: :class:`Response <Response>` object
 |      :rtype: requests.Response
 |  
 |  get(self, params=None, **kwargs)
 |      Sends a GET request.
 |      
 |      :param params: (optional) Dictionary or bytes to be sent in the query string for the :class:`Request`.
 |      :param \*\*kwargs: Optional arguments that ``request`` takes.
 |      :return: :class:`Response <Response>` object
 |      :rtype: requests.Response
 |  
 |  get_json(self, name='', query='', keys='', overwrite=False)
 |      Runs a url with a specific query, amending query if necessary, and returns the result after applying a
 |      transformer
 |  
 |  get_text(self, name='', query='', pattern='', overwrite=False)
 |      Runs a url with a specific query, amending query if necessary, and returns the resulting text
 |  
 |  head(self, **kwargs)
 |      Sends a HEAD request.
 |      
 |      :param \*\*kwargs: Optional arguments that ``request`` takes.
 |      :return: :class:`Response <Response>` object
 |      :rtype: requests.Response
 |  
 |  options(self, **kwargs)
 |      Sends a OPTIONS request.
 |      
 |      :param \*\*kwargs: Optional arguments that ``request`` takes.
 |      :return: :class:`Response <Response>` object
 |      :rtype: requests.Response
 |  
 |  patch(self, data=None, **kwargs)
 |      Sends a PATCH request.
 |      
 |      :param data: (optional) Dictionary, bytes, or file-like object to send in the body of the :class:`Request`.
 |      :param \*\*kwargs: Optional arguments that ``request`` takes.
 |      :return: :class:`Response <Response>` object
 |      :rtype: requests.Response
 |  
 |  post(self, data=None, json=None, **kwargs)
 |      Sends a POST request.
 |      
 |      :param data: (optional) Dictionary, bytes, or file-like object to send in the body of the :class:`Request`.
 |      :param json: (optional) json data to send in the body of the :class:`Request`.
 |      :param \*\*kwargs: Optional arguments that ``request`` takes.
 |      :return: :class:`Response <Response>` object
 |      :rtype: requests.Response
 |  
 |  put(self, data=None, **kwargs)
 |      Sends a PUT request.
 |      
 |      :param data: (optional) Dictionary, bytes, or file-like object to send in the body of the :class:`Request`.
 |      :param \*\*kwargs: Optional arguments that ``request`` takes.
 |      :return: :class:`Response <Response>` object
 |      :rtype: requests.Response
 |  
 |  resolve(self)
 |      Resolve relative path of the path.
 |  
 |  with_components(self, *, scheme=<object object at 0x7f96ae1d6a40>, netloc=<object object at 0x7f96ae1d6a40>, username=<object object at 0x7f96ae1d6a40>, password=<object object at 0x7f96ae1d6a40>, hostname=<object object at 0x7f96ae1d6a40>, port=<object object at 0x7f96ae1d6a40>, path=<object object at 0x7f96ae1d6a40>, name=<object object at 0x7f96ae1d6a40>, query=<object object at 0x7f96ae1d6a40>, fragment=<object object at 0x7f96ae1d6a40>)
 |      Return a new url with components changed.
 |  
 |  with_fragment(self, fragment)
 |      Return a new url with the fragment changed.
 |  
 |  with_hostinfo(self, hostname, port=None)
 |      Return a new url with the hostinfo changed.
 |  
 |  with_name(self, name)
 |      Return a new url with the file name changed.
 |  
 |  with_netloc(self, netloc)
 |      Return a new url with the netloc changed.
 |  
 |  with_query(self, query=None, **kwargs)
 |      Return a new url with the query changed.
 |  
 |  with_scheme(self, scheme)
 |      Return a new url with the scheme changed.
 |  
 |  with_suffix(self, suffix)
 |      Return a new url with the file suffix changed (or added, if none).
 |  
 |  with_userinfo(self, username, password)
 |      Return a new url with the userinfo changed.
 |  
 |  ----------------------------------------------------------------------
 |  Data descriptors inherited from urlpath.URL:
 |  
 |  __dict__
 |      dictionary for instance variables (if defined)
 |  
 |  __weakref__
 |      list of weak references to the object (if defined)
 |  
 |  components
 |      Url components, `(scheme, netloc, path, query, fragment)`.
 |  
 |  form
 |      The query parsed by `urllib.parse.parse_qs` of url.
 |  
 |  form_fields
 |      The query parsed by `urllib.parse.parse_qsl` of url.
 |  
 |  fragment
 |      The fragment of url.
 |  
 |  hostinfo
 |      The hostinfo of url. "hostinfo" is hostname and port.
 |  
 |  hostname
 |      The hostname of url.
 |  
 |  jailed
 |  
 |  name
 |      The final path component, if any.
 |  
 |  netloc
 |      The scheme of url.
 |  
 |  parts
 |      An object providing sequence-like access to the
 |      components in the filesystem path.
 |  
 |  password
 |      The password of url.
 |  
 |  path
 |      The path of url, it's with trailing sep.
 |  
|  query
 |      The query of url.
 |  
 |  scheme
 |      The scheme of url.
 |  
 |  trailing_sep
 |      The trailing separator of url.
 |  
 |  username
 |      The username of url.
 |  
 |  ----------------------------------------------------------------------
 |  Data descriptors inherited from urllib.parse._NetlocResultMixinBase:
 |  
 |  port
 |  
 |  ----------------------------------------------------------------------
 |  Methods inherited from urllib.parse._ResultMixinStr:
 |  
 |  encode(self, encoding='ascii', errors='strict')
 |  
 |  ----------------------------------------------------------------------
 |  Methods inherited from pathlib.PurePath:
 |  
 |  __eq__(self, other)
 |      Return self==value.
 |  
 |  __fspath__(self)
 |  
 |  __ge__(self, other)
 |      Return self>=value.
 |  
 |  __gt__(self, other)
 |      Return self>value.
 |  
 |  __hash__(self)
 |      Return hash(self).
 |  
 |  __le__(self, other)
 |      Return self<=value.
 |  
 |  __lt__(self, other)
 |      Return self<value.
 |  
 |  __reduce__(self)
 |      Helper for pickle.
 |  
 |  __repr__(self)
 |      Return repr(self).
 |  
 |  __rtruediv__(self, key)
 |  
 |  __truediv__(self, key)
 |  
 |  as_posix(self)
 |      Return the string representation of the path with forward (/)
 |  
 |  is_absolute(self)
 |      True if the path is absolute (has both a root and, if applicable,
 |      a drive).
 |  
 |  is_reserved(self)
 |      Return True if the path contains one of the special names reserved
 |      by the system, if any.
 |  
 |  joinpath(self, *args)
 |      Combine this path with one or several arguments, and return a
 |      new path representing either a subpath (if all arguments are relative
 |      paths) or a totally different path (if one of the arguments is
 |      anchored).
 |  
 |  match(self, path_pattern)
 |      Return True if this path matches the given pattern.
 |  
 |  relative_to(self, *other)
 |      Return the relative path to another path identified by the passed
 |      arguments.  If the operation is not possible (because this is not
 |      a subpath of the other path), raise ValueError.
 |  
 |  ----------------------------------------------------------------------
 |  Data descriptors inherited from pathlib.PurePath:
 |  
 |  anchor
 |      The concatenation of the drive and root, or ''.
 |  
 |  drive
 |      The drive prefix (letter or UNC path), if any.
 |  
 |  parent
 |      The logical parent of the path.
 |  
 |  parents
 |      A sequence of this path's logical parents.
 |  
 |  root
 |      The root of the path, if any.
 |  
 |  stem
 |      The final path component, minus its last suffix.
 |  
 |  suffix
 |      The final component's last suffix, if any.
 |      
 |      This includes the leading period. For example: '.txt'
 |  
 |  suffixes
 |      A list of the final component's suffixes, if any.
 |      
 |      These include the leading periods. For example: ['.tar', '.gz']
