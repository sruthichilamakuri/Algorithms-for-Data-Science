# -*- coding: utf-8 -*-
"""
Created on Sun Mar 04 17:55:02 2018

@author: Sruthi
"""
from __future__ import division
import re

class HeavyHitter(object):
    
    def __init__(self,k):
        self.k = k
        self.counterdict = {}
        self.count = 0
        self.m = 0

    def UpdateDict(self,s):
        #If item is present in dictionary, increment its counter by 1
        if s in self.counterdict.keys():
            self.counterdict[s] += 1
            return
        #If item is not present in the dictionary, and dictionary size has not reached k
        #add it to the dictionary with counter set to 1
        if len(self.counterdict) < self.k:
            self.counterdict[s] = 1
            return
        #If dictionary size has reached k values
        if len(self.counterdict) == self.k:
            #Decrement all counters by 1
            for key in self.counterdict.keys():
                self.counterdict[key] -= 1
            #Delete items in the dictionary with zero counter value
            for key in self.counterdict.keys():
                if self.counterdict[key]== 0:
                    del self.counterdict[key]

    def ParseData(self):
        self.count = 0
        with open("tweetstream.txt",'r') as fp:
            for line in fp:
                #Parse tweets to find the hashtags
                strings = re.split(';| |"|,|\*|\n', line.strip().lower())
                
                for s in strings:
                    if len(s) > 1 and s[0] == '#':
                        #Increment counter for number of elements/hashtags in stream
                        self.m += 1
                        self.count += 1
                        #Update counter for each hashtag according to the algorithm
                        self.UpdateDict(s)
                if self.count >= 1000:
                    return
                
    def ItemsWithFrequencyF(self,f):
        #f is the frequency (int or float)
        self.f = f    
        itemlist = []
        count = 0
        for key in self.counterdict.keys():
            if (self.counterdict[key]/self.m) >= self.f:
                itemlist.append(key)
                count += 1
        self.numofitemswithfreqf = count
        return itemlist
    
def main():
    myclass = HeavyHitter(k=500)
    myclass.ParseData() 
    maxfreqitems = myclass.ItemsWithFrequencyF(0.002)
    #print myclass.m,len(myclass.counterdict)
    myfile = open('FrquentHashtags.txt', 'w')
    myfile.write("Total number of hashtags in stream: %d\n" % myclass.m)
    myfile.write("Number of hashtags present in dictionary: %d\n" % len(myclass.counterdict))
    myfile.write("Number of items with frequency greater than %f: %d\n\n" 
                 % (myclass.f, myclass.numofitemswithfreqf))
    myfile.write("List of frequent hashtags:\n\n")
    for item in maxfreqitems:
        myfile.write("%s\n" % item)
    myfile.write("\nHashtags and their frequencies:\n\n")
    for key, value in reversed(sorted(myclass.counterdict.iteritems(), key=lambda (k,v): (v,k))):
        myfile.write("%s: %s\n" % (key, value))   
        

if __name__ == "__main__":
    main()