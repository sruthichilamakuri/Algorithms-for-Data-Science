# -*- coding: utf-8 -*-
"""
Created on Thu Mar 29 13:15:09 2018

@author: Sruthi
"""
import numpy as np

def GenerateData(above=0,below=5000,arrsize=1000):
    x = np.random.randint(low=above,high=below,size=arrsize)
    np.save('testdata.npy',x)

class MapReduce(object):
    
    def __init__(self):
        data = np.load('testdata.npy')
        #Assumption: one machine does not have the memory to store all of data
        #We will split into two chunks to simulate Map Reduce
        self.chunk1 = data[:len(data)/2]
        self.chunk2 = data[len(data)/2:]
        
    def LargestInt(self):
        """
        Computes the largest number in the data
        """
        #MAP STEP
        #Input = Array
        #Emit the input as it is
        #First chunk of emitted data = self.chunk1
        #Second chunk of emitted data = self.chunk2
              
        #SHUFFLE STEP
        #Assign emit1 to processor 1 and emit2 to processor 2

        #REDUCE STEP
        #First processor - return max of self.chunk1
        output1 = max(self.chunk1)
        #Second processor - return max of self.chunk2
        output2 = max(self.chunk2)
        
        #COMBINE STEP
        #Return max of two numbers
        output = max(output1,output2)
        return output
    
    def AverageInt(self):
        """
        Computes the average of all the integers in the data
        """
        #MAP STEP
        #Input = Array
        #Emit the input as it is
        #First chunk of emitted data = self.chunk1
        #Second chunk of emitted data = self.chunk2
              
        #SHUFFLE STEP
        #Assign emit1 to processor 1 and emit2 to processor 2

        #REDUCE STEP
        #First processor - return sum of self.chunk1 and number of elements in self.chunk1
        output1 = (sum(self.chunk1),len(self.chunk1))
        #Second processor - return sum of self.chunk2 and number of elements in self.chunk2
        output2 = (sum(self.chunk2),len(self.chunk2))
        
        #COMBINE STEP
        totalsum = output1[0] + output2[0]
        totalnum = output1[1] + output2[1]
        return totalsum/float(totalnum)
    
    def SetInt(self):
        """
        Returns the same set of integers, but with each integer appearing only once
        """
        #MAP STEP
        #Input = Array
        #Emit the input as it is
        #First chunk of emitted data = self.chunk1
        #Second chunk of emitted data = self.chunk2
              
        #SHUFFLE STEP
        #Assign emit1 to processor 1 and emit2 to processor 2

        #REDUCE STEP
        #First processor - remove duplicates in self.chunk1 and return list
        output1 = list(set(self.chunk1))
        #Second processor - remove duplicates in self.chunk2 and return list
        output2 = list(set(self.chunk2))
        
        #COMBINE STEP
        #Remove duplicates in list output1 + output2
        output = list(set(output1 + output2))
        numofdistinctint = len(output)
        return output,numofdistinctint
        
    
       
def main():
    #GenerateData()
    x = np.load('testdata.npy')
    MR = MapReduce()
    print "Largest integer computed using Numpy:", np.amax(x)
    li = MR.LargestInt()
    print "Largest integer computed using Map Reduce:", li
    print "Average of all the integers computed using Numpy:", np.mean(x)
    ai = MR.AverageInt()
    print "Average of all the integers computed using Map Reduce:", ai
    setint,numdistinctint = MR.SetInt()
    #Not printing returned value as there are 10,000 numbers in the test data
    print "Set of integers returned correctly?", np.array_equal(np.unique(x),sorted(setint))
    print "Number of distinct integers computed using Numpy:", len(np.unique(x))
    print "Number of distinct integers computed using Map Reduce:", numdistinctint


if __name__ == "__main__":
    main()