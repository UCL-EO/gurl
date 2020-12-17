#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
from pathlib import PosixPath, _PosixFlavour, PurePath
from pathlib import Path
from urlpath import URL
import sys
import stat

'''
listfile utilities class based on list

This is a particular type of list where we 
assume all items are filenames, URLs or similar objects.

The class ensures that Paths to ant parent 
directories are generated if possible. Read and
Write permissions are nioted in the class, as are
existence and whether or not it is a directory.

Treats input as a flat list of filenames and provides
information on permissions etc. 

If name is set as keyword, then any items
that are directories have the name appended

The return value is a list of resolved Path objects.

'''

__author__    = "P. Lewis"
__email__     = "p.lewis@ucl.ac.uk"
__date__      = "28 Aug 2020"
__copyright__ = "Copyright 2020 P. Lewis"
__license__   = "MIT License"

class ListPath(list):
    '''
    listfile utilities class based on list

    Treats input as a flat list of filenames and provides
    information on permissions etc. 

    If name is set as keyword, then any items
    that are directories have the name appended
    '''
    def __init__(self,*args,**kwargs):
        '''
        Generate list of filenames and associated data
        
        Arguments:
            list or list-like items. A string is *not* treated 
            as a list of characters, but as a filename. Any ragged list
            is flattened.
            
            The items can be directory or file names. 
            If they are directory names, then if kwargs['name']
            is set, this is taken as the filename.
            
            Permissions etc are set for ease of access (see self.report())
        
        Keywords:
            unique: if True, then remove duplicates from list
            name:   str to be used if items are directories 
                    
        '''
        args = self.flatten_list(*args)
        if 'name' in kwargs.keys():
            self.name = kwargs['name']
            listfile = self.name_resolve(args,name=self.name)
        else:
            self.name = None
            listfile = self.resolve(args)
            
        # make sure it is flat
        arr = np.array(listfile,dtype=np.object)
        listfile = [Path(i) for i in list(arr.ravel())]
        
        if ('unique' in kwargs) and (kwargs['unique'] is True):
            listfile = self.remove_duplicates(listfile)
            
        # set list 
        super(ListPath, self).__init__(listfile) 
        # get read/write permissions 
        self.list_info()
    
    def len(self):
        return len(self)

    def type(self):
        '''
        return types for all members in list
        '''   
        return [type(i) for i in self]

    def joinpath(self,*args):
        '''
        Path.joinpath for all items
        '''
        for i,j in enumerate(self):
          self[i] = self[i].joinpath(*args)

    def parents(self):
        '''
        parents for all elements
        '''
        for i,j in enumerate(self):
          self[i] = self[i].parent

    def absolute(self):
        '''
        Path.absolute for all items
        '''
        for i,j in enumerate(self):
          self[i] = self[i].absolute()


    def resolve(self,listfile):
        '''
        resolve raggedy listfile into
        list of absolute filenames as PosixPath type

        Arguments:
        listfile : One of:
                   - None
                   - string
                   - PosixPath
                   - urlpath.URL
                   - list (or numpy array) of filenames

        Returns:
        listfile : list of absolute filenames as mixed
        urlpath.URL/PosixPath type
        '''
        if type(listfile) is np.ndarray:
            listfile = listfile.as_list()

        if type(listfile) is list:
            listfile = [str(f) for f in listfile if f]
        elif type(listfile) is str:
            listfile = [listfile]
        elif type(listfile) is PosixPath:
            listfile = [listfile.as_posix()]
        else:
          self.msg("unrecognised type for resolve:listfile")
          self.msg(f"{type(listfile)}")
        #listfile  = self.remove_duplicates(listfile)
        listfile = [Path(f).expanduser().absolute().resolve() for f in listfile]
        return listfile

    def name_resolve(self,*listfile,name='file.dat'):
        '''
        resolve listfile to ensure all items 
        are filenames, not directories. And make sure
        that directory structure exists.

        If they are directories, then add name to them.


        Arguments:
            *listfile : tuple of files

        Keywords:
            name : use this as a filename
                   if you happen to be a directory. 
                   Default file.dat

        Returns:
            listfile : list of filenames
        '''
        if name == None:
            name='file.dat'
            
        if listfile is None:
            return None

        listfile = self.resolve(*listfile)

        for i,f in enumerate(listfile):
            # in case its a dir accidently
            if f.exists() and f.is_dir():
                f = Path(f,name)

            # check that parent is a directory
            # this check shouldnt be needed
            # but keep for historical reasons
            parent = f.parent
            if parent.exists() and (not parent.is_dir()):
                try:
                    parent.unlink()
                except:
                    print(f"Parental problem: {parent}")
                    print(f"Error in lists."+\
                          f"name_resolve({str(listfile)},name={name})")
                    sys.exit(1)
            try:
                parent.mkdir(parents=True,exist_ok=True)
            except:
                pass
            listfile[i]  = f
        return listfile
    
    def flatten_list(self,_2d_list):
        '''
        based on 
        https://stackabuse.com/python-how-to-flatten-list-of-lists/
        '''
        flat_list = []
        # Iterate through the outer list
        if type(_2d_list) is str:
            return self.flatten_list([_2d_list])
        for element in _2d_list:
            if type(element) is list:
                # If the element is of type list, iterate through the sublist
                for item in element:
                    if type(item) is str:
                        item = [item]
                    if type(item) is list:
                        flat_list.extend(self.flatten_list(item))
                    else:
                        flat_list.append(item)
            else:
                flat_list.append(element)
        return flat_list

    def list_info(self):
        '''
        resolve self and get read and write permissions.
        If a file doesn't yet exist, then write permission
        is assumed, so long as the user has write 
        permission to the directory.

        Sets:
        self.isfile- Bool Ture if object if file
        self.isdir - Bool Ture if object if directory
        self.exists- Bool Ture if object exists
        self.read  - Bool list of user read permissions
        self.write - Bool list of user write permissions
        self.parentwrite
                   - Bool list of user write permissions
                     for the parent directory
        '''
        listfile  = np.array(self,dtype=np.object)
        readlist  = np.zeros_like(listfile).astype(np.bool)
        writelist = np.zeros_like(listfile).astype(np.bool)
        isdir     = np.zeros_like(listfile).astype(np.bool)
        isfile    = np.zeros_like(listfile).astype(np.bool)
        exists    = np.zeros_like(listfile).astype(np.bool)
        parentwrite= np.zeros_like(listfile).astype(np.bool)

 
        # get permissions
        for i,f in enumerate(listfile):
            f = Path(f)
            if f.exists():
                exists[i] = True
                if f.is_dir():
                    isdir[i] = True
                else:
                    isfile[i] = True
          
                st_mode = f.stat().st_mode
                readlist[i]  = bool((st_mode & stat.S_IRUSR) /stat.S_IRUSR )
                writelist[i] = bool((st_mode & stat.S_IWUSR) /stat.S_IWUSR )
                parent = f.parent
                st_mode = parent.stat().st_mode
                parentwrite[i] = bool((st_mode & stat.S_IWUSR) /stat.S_IWUSR )
            else:
              parent = f.parent
              try:
                # file doesnt yet exist, so make directory
                # and set writelist[i] if parent is writeable
                parent.mkdir(parents=True,exist_ok=True)
                st_mode = parent.stat().st_mode
                writelist[i] = bool((st_mode & stat.S_IWUSR) /stat.S_IWUSR )
                isfile[i] = True
                parentwrite[i] = writelist[i]
              except:
                # file doesnt yet exist, and we cant make
                # the parent, so this is not going to
                # be writeable
                writelist[i] = False
                    
        self.exists = exists
        self.isdir  = isdir
        self.isfile = isfile
        self.read   = readlist
        self.write  = writelist
        self.parentwrite = parentwrite
        return

    def remove_duplicates(self,listfile):
        '''
        remove duplicates in self and return
        new ListPath
        '''
        if len(listfile) == 0:
            return listfile
        return ListPath(np.unique(np.array(listfile,dtype=np.object)).ravel())
    
    def report(self,stderr=sys.stderr):
        print(f'info: read  : {self.read}',file=stderr)
        print(f'info: write : {self.write}',file=stderr)
        print(f'info: isdir : {self.isdir}',file=stderr)
        print(f'info: isfile: {self.isfile}',file=stderr)
        print(f'info: exists: {self.exists}',file=stderr)
    
def test1():
    msg = '''
    Test for a mixed bag of directories and files
    
    The result should be a flattened list
    '''
    print(msg)
    xin = [['.',['..'],'a b',['.','demo.ipynb',['notebooks/demo.ipynb']]],'hello']
    xout = ListPath(xin)
    print(f'{xin} -> {xout}')
    xout.report()
    
def test2():
    msg = '''
    give it a string. We want this maintained as
    a string, not split into chars (as with normal list)
    '''
    print(msg)
    xin = 'hello world'
    xout = ListPath(xin)
    print(f'{xin} -> {xout}')
    xout.report()
    
def test3():
    msg = '''
    Remove duplicates test
    '''
    print(msg)
    xin = ['hello world',['.','./hello world'],'.']
    xout = ListPath(xin,unique=True)
    print(f'{xin} -> {xout}')
    xout.report()

def test4():
    msg = '''
    test write permission and use of name= kwarg
    '''
    print(msg)
    xin = ['/tmp','/doesnt exist/x']
    xout = ListPath(xin,name='tester.dat')
    print(f'{xin} -> {xout}')
    xout.report()

    
def main():
    # tests
    test1()
    test2()
    test3()
    test4()

if __name__ == "__main__":
    main()
