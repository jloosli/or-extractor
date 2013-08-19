#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
SYNOPSIS

    extractprintdata [-h,--help] [-v,--verbose] [--version] [-o, --output] [directory|filename]

DESCRIPTION

    Extracts data from roadware reports printed to a file.

EXIT STATUS

    0: Success
    1: Failure

AUTHOR

    Jared Loosli <jloosli@tcstire.com>

LICENSE

    TCS Only

VERSION

    $Id$
"""

import sys, os, traceback, argparse, csv, time, re
from functools import reduce
from itertools import groupby, count, repeat
from random import choice



def fixedExtract(s, *args):
    left = 0
    result = []
    for right in args:
        result.append(s[left:right])
        left = right
    return [x.strip() for x in result]

def extractData(filename):
    global options
    try:
        with open(filename, 'r') as f:
            page = 0
            inLoc = -1  # 0: heading, 1: THead, 2: data, 3: footer, -1: nowhere
            haveHeaderInfo = False
            title = ""
            titleRE = re.compile(r"\*---(.*)---\*")
            THead = []
            datalines = []

            for line in f:

                # Form feed signifies new page
                if "\f" in line:
                    inLoc = 0
                    hCount = 0
                    inHeading = True
                    inTHead = False
                    page = page + 1
                    continue

                if page > 0:  # We're outside the debug job header area

                    if line.startswith("#####"):
                        if inLoc != -1:
                            yield [title, THead, datalines]
                            inLoc == -1
                            haveHeaderInfo = False
                            title = ""
                            THead = []
                            datalines = []
                            page = 0
                        continue

                    # Move through Sections
                    if "===========" in line:
                        inLoc += 1
                        if inLoc == 2:
                            haveHeaderInfo = True

                    elif inLoc == 0:  # In Heading Area
                        if title in ["BUFF SPECIFICATIONS", "CUSTOMER PROFILE"]:  # Have to add this since these reports don't have the top ====== line for the header
                            inLoc += 1

                        # Find Title to report
                        elif not haveHeaderInfo and title == "" and "*" in line:
                            match = re.search(titleRE, line)
                            if match:
                                title = match.group(1).strip("\n ")

                        # Check if we're still of the header area
                    elif inLoc == 1:  # In THead

                        if not haveHeaderInfo and "CUSTOMER PROFILE" not in title:
                            if line == "":
                                continue
                            THead.append(line.strip("\n"))

                    elif inLoc == 2:  # In data section
                        if line.startswith("***"):  # Some reports have summaries that start with multiple stars
                            continue
                        datalines.append(line.strip("\n"))
                    elif inLoc == 3:  # In Footer (typically at the end of a report)
                        continue  # do nothing for now

    except Exception as e:
        print(str(e))
        print('Couldn\'t open filename %s' % filename)

    yield [title, THead, datalines]


def processData(items):
    global options
    maxwidth = 134

    (title, THead, datalines) = items

    if "CUSTOMER PROFILE" in title:
        headings = ["CUSTOMER NO.", "XREF", "B_NAME", "B_ADD1", "B_ADD2",
                    "B_CITY", "B_STATE", "B_ZIP", "S_NAME","S_ADD1", "S_ADD2",
                    "S_CITY", "S_STATE", "S_ZIP"]
        entry = {
            "no" : "",
            "xref": "",
            "b_name" : "",
            "b_add1" : "", 
            "b_add2": "",
            "b_city": "",
            "b_state": "",
            "b_zip": "", 
            "s_name": "",
            "s_add1": "", 
            "s_add2": "",
            "s_city": "",
            "s_state": "",
            "s_zip" : ""
        }
        results=[]
        cust_no_tag= re.compile(r'CUSTOMER NO:\s+([0-9]+)\s+(.+)')
        names=re.compile(r'B +([A-Za-z0-9 :_\.-]*) +S(?![A-Za-z0-0\.]) *([A-Za-z0-9 :_\.-]*)')
        add1=re.compile(r'I T +([A-Za-z0-9 :_\.-]*) +H T *([A-Za-z0-9 :_\.-]*)')
        add2=re.compile(r'L O +([A-Za-z0-9 :_\.-]*) +I O *([A-Za-z0-9 :_\.-]*)')
        city_state_zip=re.compile(r'L +([0-9A-Za-z\.]*(?: [0-9A-Za-z\.]+)*) +([A-Z]{0,2}) +([0-9]*) +P *([0-9A-Za-z\.]*(?: [0-9A-Za-z\.]+)*) *([A-Z]{0,2}) *([0-9]*)')
        city_state_zip=re.compile(
            r"""L\ + # L followed by 1 or more spaces
            ([0-9A-Za-z\.-]*(?:\ [0-9A-Za-z\.-]+)*) # 0 or more alphanumeric characters and single spaces-no multiple spaces (the city)
            \ + # 1 or more spaces
            ([A-Z]{0,2}) # 0-2 upper-case characters (the state)
            \ + # 1 or more spaces
            ([0-9]*) # 0 or more numbers (the zip)
            \ + # 1 or more spaces
            P\ * # P followed by 0 or more spaces
            ([0-9A-Za-z\.-]*(?:\ [0-9A-Za-z\.-]+)*) # 0 or more alphanumeric characters and single spaces-no multiple spaces (the city)
            \ * # 0 or more spaces
            ([A-Z]{0,2}) # 0-2 upper-case characters (the state)
            \ * # 0 or more spaces
            ([0-9]*) # 0 or more numbers (the zip)
            """, re.VERBOSE)
        for line in datalines:
            matches=re.search(cust_no_tag,line)
            if matches:
                entry['no'],entry['xref'] = matches.groups()
                continue

            matches=re.search(names,line)
            if matches:
                entry['b_name'],entry['s_name'] = matches.groups()
                continue

            matches=re.search(add1,line)
            if matches:
                entry['b_add1'],entry['s_add1'] = matches.groups()
                continue

            matches=re.search(add2,line)
            if matches:
                entry['b_add2'],entry['s_add2'] = matches.groups()
                continue

            matches=re.search(city_state_zip,line)
            if matches:
                entry['b_city'], entry['b_state'],entry['b_zip'],entry['s_city'], \
                entry['s_state'],entry['s_zip'] = matches.groups()
                results.append([
                    entry['no'],
                    entry['xref'],
                    entry['b_name'],
                    entry['b_add1'],
                    entry['b_add2'],
                    entry['b_city'],
                    entry['b_state'],
                    entry['b_zip'],
                    entry['s_name'],
                    entry['s_add1'],
                    entry['s_add2'],
                    entry['s_city'],
                    entry['s_state'],
                    entry["s_zip"]
                ])
        return [title, headings, results]

    elif "BUFF SPECIFICATIONS" in title:
        headings = "BRAND|SIZE CODE|DESCRIPTION|ORIGINAL CASING MODEL|TREAD SIZE|MOLDCURE BUFF DIAM|PRECURE BUFF DIAM|BUFFED RADIUS|MOLD CAVITY|BEAD PLATE"
        headings += "|PRIMARY TR SIZE|PRIMARY PGM#|ALTERNATE TR SIZE|ALTERNATE PGM#"

        print(headings)
        headings = headings.split("|")
        print(headings)
        results = []
        widths = (11,16,31,53,62,73,84,92,99,106,114,120,128,132)
        for line in datalines:
            results.append(fixedExtract(line,*widths))
        return [title, headings, results]

    elif "FINISHED GOODS MASTER FILE" in title:
        headings = "ITEM NO.|TIRE SIZE|TREAD ABB|TREAD SIZE|TREAD DEPTH|CASE CIRC|LIST PRICE|CASING PRICE|STK QOH|RET QOH|C&C QOH"
        headings += "|CAT|MATERIAL COST|AVG COST|INV VALUE|C&C ITEM|RET ITEM"
        headings = headings.split("|")

        results=[]
        widths = (15,30,44,52,59,64,74,83,88,94,100,106,114,123,132)
        item_details = re.compile(r'^ +C&C ITEM: (.+) RET ITEM: (.+)$') #      C&C ITEM: **************     RET ITEM: MISC
        temp = []
        for line in datalines:
            matches=re.search(item_details,line)
            if matches:
                temp += [x.strip() for x in matches.groups()]
                results.append(temp)
                temp = []
            else:
                temp =fixedExtract(line,*widths)

        return [title, headings, results]

    elif "CUSTOMER MASTER FILE" in title:
        headings = "CUSTOMER NO.|CUSTOMER NAME|SHORT NAME|ALTERNATE CUST NO.|SHIP ZONE|TAX CUST|TAX CODE|SALESMAN|TERMS"
        headings = headings.split("|")

        results=[]
        widths = (10,41,52,64,72,75,87,113, 132)
        for line in datalines:
            results.append(fixedExtract(line,*widths))
        return [title, headings, results]

    elif "VENDOR MASTER FILE" in title:
        headings = "SHORT NAME|NUMBER|NAME|ADDR 1|CITY|ST|ZIP|PHONE"
        headings = headings.split("|")

        results=[]
        widths = (7,18,49,80,96,99,110, 132)
        for line in datalines:
            results.append(fixedExtract(line,*widths))
        return [title, headings, results]


    #
    # Everything else goes here
    #
    spaces = []

    # Find the spaces in the headings
    for idx, i in enumerate(THead):
        spaces.append(set([])) # @todo: Do I still need this?
        spaces[idx] = set(findIndexes(i," "))

    # If multiple lines of headings, find the intersection of those spaces
    intersect = reduce(lambda x, y:x & y, spaces)
    groups = setToRanges(intersect)
    spacegroups = []

    # If it's a single line, throw out the single spaces
    for i in groups:
        if min(i) == max(i) and len(THead)==1:
            continue
        else:
            spacegroups.append(range(min(i), max(i)+1))

    # Figure out Headings
    inverted = invertRanges(spacegroups,maxwidth)
    headings = [""] * len(inverted)
    for idx, theRange in enumerate(inverted):

        for line in THead:
            headings[idx] = ("" if headings[idx] == "" else headings[idx] + " ") + line[min(theRange):max(theRange)+1].strip("\n ")

    #Get data
    cols=[]
    if(len(datalines) != 0):
        cols = reduce(lambda x,y: set(x) & set(y), map(findIndexes, map(str.ljust,datalines, repeat(maxwidth)), repeat(" ")))
    else:
        print("No Datalines")
        print(title)
        print(headings)

    supercols = set([item for sublist in spacegroups for item in sublist]) & set(cols)

    extractedData = []
    for d in datalines:
        tmp = []
        for theRange in invertRanges(setToRanges(supercols), maxwidth):
            tmp.append(d[min(theRange):max(theRange)+1].strip("\n "))
        extractedData.append(tmp)

    return [title, headings, extractedData]

'''
Write CSV file
'''
def writeCSV(filename, data):
    (title, THead, datalines) = data
    with open(filename + '.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # writer = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(THead)
        for line in datalines:
            writer.writerow(line)


'''
Convert set to list of ranges
'''
def setToRanges(theSet):
    theSet=sorted(theSet)
    groups = []
    uniquekeys = []
    for k, g in groupby(theSet, lambda n, c=count(): n-next(c)):
        groups.append(list(g))
        uniquekeys.append(k)
    ranges = []
    for i in groups:
        ranges.append(range(min(i),max(i)+1))
    return ranges


'''
Invert a list of ranges
e.g. invertRanges([range(7,9), range(12,15)], 20)
return = [range(0,7), range(10,12), range(16,20)]
'''
def invertRanges(ranges, maxRange):
    inverted = []
    if ranges == []:
        return [range(maxRange)] # If no ranges, return the whole range
    for idx, i in enumerate(ranges):
        i = sorted(i)
        inverted.append(range(0 if idx == 0 else max(ranges[idx-1])+1,min(i)))
    try:
        inverted.append(range(max(ranges[-1])+1, maxRange))
    except Exception as e:
        print(str(e))
        print (ranges)
        traceback.print_exc()
    return inverted

'''
Returns index of each occurance of character in string
'''
def findIndexes(s, ch):
    return [i for i, ltr in enumerate(s) if ltr == ch]


'''
Prints out only if verbose option used
'''
def debug(*objects, **args):
    global options
    if options.verbose: 
        #traceback.print_stack()
        print(*objects, **args)

def main ():

    global options

    if not os.path.exists(options.output):
        os.makedirs(options.output)

    path = os.path.abspath(options.output)
    for i in extractData(options.inputfile):
        try:
            results = processData(i)
            print(i[0])
            filename=i[0].replace(" ","_").replace("/","-")
            writeCSV(os.path.join(path,filename), results)
        except Exception as e:
            print(str(e))
            traceback.print_exc()

if __name__ == '__main__':
    try:
        start_time = time.time()
        parser = argparse.ArgumentParser()
        parser.add_argument ('-v', '--verbose', action='store_true', default=False, help='verbose output')
        parser.add_argument ('-o', '--output', default='output', help='verbose output')
        parser.add_argument ('inputfile')
        options = parser.parse_args()
        print(options.inputfile)
        #if len(args) < 1:
        #    parser.error ('missing argument')
        if options.verbose: print(time.asctime())
        main()
        if options.verbose: print(time.asctime())
        if options.verbose: print('TOTAL TIME IN SECONDS:', end=' ')
        if options.verbose: print((time.time() - start_time))
        sys.exit(0)
    except KeyboardInterrupt as e: # Ctrl-C
        raise e
    except SystemExit as e: # sys.exit()
        raise e
    except Exception as e:
        print('ERROR, UNEXPECTED EXCEPTION')
        print(str(e))
        traceback.print_exc()
        os._exit(1)
