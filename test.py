#!/usr/bin/env python
# -*- coding: utf-8 -*-

from gurl import URL
print('test 7: multi-level, with cache, password required, read html')
u='https://e4ftl01.cr.usgs.gov/'
url = URL(u,pwr=True,cache=True)
data = url.read_text()
