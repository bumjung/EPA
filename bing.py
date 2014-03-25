#!/usr/bin/env python

# Python bindings to the Google search engine
# Copyright (c) 2009-2013, Mario Vilas
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice,this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the copyright holder nor the names of its
#       contributors may be used to endorse or promote products derived from
#       this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

__all__ = ['search']

import os
import sys
import time
import random

if sys.version_info[0] > 2:
    from http.cookiejar import LWPCookieJar
    from urllib.request import Request, build_opener, HTTPCookieProcessor
    from urllib.parse import quote_plus, urlparse, parse_qs
else:
    from cookielib import LWPCookieJar
    from urllib import quote_plus
    from urllib2 import Request, build_opener, HTTPCookieProcessor
    from urlparse import urlparse, parse_qs

# Try to use BeautifulSoup 4 if available, fall back to 3 otherwise.
try:
    from bs4 import BeautifulSoup
except ImportError:
    from BeautifulSoup import BeautifulSoup

# URL templates to make Google searches.
url_home        = "http://www.bing.com/"
url_search      = "http://www.bing.com/search?q=%(query)s+language:%(lang)s"
url_next_page   = "http://www.bing.com/search?q=%(query)s+language:%(lang)s&first=%(start)d"

# Cookie jar. Stored at the user's home folder.
home_folder = os.getenv('HOME')
if not home_folder:
    home_folder = os.getenv('USERHOME')
    if not home_folder:
        home_folder = '.'   # Use the current folder on error.
cookie_jar = LWPCookieJar(os.path.join(home_folder, '.bing-cookie'))
try:
    cookie_jar.load()
except Exception:
    pass

def get_rand_header():
    """
    Return random user-agent header
    
    @rtype:  str
    @return: user-agent header for web browser request
    """
    headers = [ 
        ('Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) '
            'Chrome/19.0.1084.36 Safari/536.5'),
        ('Mozilla/5.0 (Windows NT 6.1) AppleWebKit/535.1 (KHTML, like Gecko) '
            'Chrome/14.0.812.0 Safari/535.1'),
        ('Mozilla/4.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/5.0)'),
        ('Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; ' 
            'SLCC2; Media Center PC 6.0; InfoPath.2; MS-RTC LM 8'),
        ('Mozilla/5.0 (Windows; U; Windows NT 6.1; tr-TR) AppleWebKit/533.20.25 '
            '(KHTML, like Gecko) Version/5.0.4 Safari/533.20.27'),
        ('Mozilla/5.0 (Windows; U; Windows NT 6.1; sv-SE) AppleWebKit/533.19.4 '
            '(KHTML, like Gecko) Version/5.0.3 Safari/533.19.4'),
        ('Mozilla/5.0 (Windows NT 6.1; de;rv:12.0) Gecko/20120403211507 '
            'Firefox/12.0'),
        ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:9.0) Gecko/20100101 '
            'Firefox/9.0'),
        ('Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; de) Presto/2.9.168 '
            'Version/11.52'),
        ('Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.1.16) Gecko/20120421 '
            'Gecko Firefox/11.0')
    ]
    return random.choice(headers)

# Request the given URL and return the response page, using the cookie jar.
def get_page(url):
    """
    Request the given URL and return the response page, using the cookie jar.

    @type  url: str
    @param url: URL to retrieve.

    @rtype:  str
    @return: Web page retrieved for the given URL.

    @raise IOError: An exception is raised on error.
    @raise urllib2.URLError: An exception is raised on error.
    @raise urllib2.HTTPError: An exception is raised on error.
    """    
    opener = build_opener(HTTPCookieProcessor(cookie_jar))
    request = Request(url)
    request.add_header('User-Agent', get_rand_header())    
    response = opener.open(request)
    
    html = response.read()
    response.close()
    cookie_jar.save()
    return html

# Filter links found in the Google result pages HTML code.
# Returns None if the link doesn't yield a valid result.
def filter_result(link):
    try:
        o = urlparse(link, 'http')
        if o.netloc and 'bing' not in o.netloc:
            return link

        # Decode hidden URLs.
        if link.startswith('/url?'):
            link = parse_qs(o.query)['q'][0]

            o = urlparse(link, 'http')
            if o.netloc and 'bing' not in o.netloc:
                return link

    # Otherwise, or on error, return None.
    except Exception:
        pass
    return None

# Returns a generator that yields URLs.
def search(query, lang='en', start=0, n=10, pause=1.0):
    """
    Search the given query string using Google.

    @type  query: str
    @param query: Query string. Must NOT be url-encoded.

    @type  lang: str    
    @param lang: Language.

    @type  start: int
    @param start: First result to retrieve.

    @type  n: int
    @param n: Number of results to retrieve.
        None to keep searching forever.

    @type  pause: float
    @param pause: Lapse to wait between HTTP requests.
        A lapse too long will make the search slow, but a lapse too short may
        cause Google to block your IP. Your mileage may vary!

    @rtype:  generator
    @return: Generator (iterator) that yields found URLs. If the C{stop}
        parameter is C{None} the iterator will loop forever.
    """

    # Set of hashes for the results found.
    # This is used to avoid repeated results.
    hashes = set()
    
    if n is not None:
        stop = start + n # get last result number

    # Prepare the search string.
    query = quote_plus(query)

    # Grab the cookie from the home page.
    get_page(url_home % vars())

    # Prepare the URL of the first request.
    if start:
        url = url_next_page % vars()
    else:
        url = url_search % vars()
       
    # Loop until we reach the maximum result, if any (otherwise, loop forever).
    while not stop or start < stop:
        # Sleep between requests.
        time.sleep(pause)

        # Request the Google Search results page.
        html = get_page(url)

        # Parse the response and process every anchored URL.
        soup = BeautifulSoup(html)

        # If there are no results
        if soup.find(id='no_results'):
            break

        anchors = soup.find(id='results').findAll('a')
        for a in anchors:
            # Get the URL from the anchor tag.
            try:
                link = a['href']
            except KeyError:
                continue

            # Filter invalid links and links pointing to Google itself.
            link = filter_result(link)
            if not link:
                continue

            # Discard repeated results.
            h = hash(link)
            if h in hashes:
                continue
            hashes.add(h)

            # Yield the result.
            start += 1 
            yield link

            if start >= stop:
                break

        # End if there are no more results.
        if not soup.find(class_='sb_pagN'):
            break

        # Prepare the URL for the next request.
        url = url_next_page % vars()

if __name__ == "__main__":
    from argparse import ArgumentParser, RawDescriptionHelpFormatter
    import textwrap

    parser = ArgumentParser(prog='bing', 
        formatter_class=RawDescriptionHelpFormatter,
        description=textwrap.dedent('''\
            Python script to use the Bing search engine
            Original script for Google by Mario Vilas
            Modified for Bing by Michael Noukhovitch'''))
    parser.add_argument("query", help="query to search bing")
    parser.add_argument("--start", metavar="NUM", type=int, default=0,
                      help="first result to retrieve [default: 0]")
    parser.add_argument("--n", metavar="NUM", type=int, default=10,
                      help="number of results to retrieve [default: 10]")
    parser.add_argument("--pause", metavar="SEC", type=float, default=1.0,
                      help="pause between HTTP requests [default: 1.0]")
    args = parser.parse_args(['hello', '--n', '5'])
    query = args.query
    if not query:
        parser.print_help()
        sys.exit(2)

    # Run the query.
    for url in search(**args.__dict__):
        print(url)