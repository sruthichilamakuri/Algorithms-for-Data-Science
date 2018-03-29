# -*- coding: utf-8 -*-
"""
Created on Thu Mar 29 11:56:11 2018

@author: Sruthi
"""
import re

class MapReduce(object):
    
   def __init__(self):
       f = open('Email-EuAll.txt','r')
       databegin = False
       data = []
       for line in f:
           if line.find('FromNodeId') != -1:
               databegin = True
               continue
           if databegin:    
               newline = re.sub('\s+', ' ', line.strip()).split(' ')
               data.append({'from':newline[0],'to':newline[1]})
       #Assumption: one machine does not have the memory to store all of data
       #We will split into two chunks to simulate Map Reduce
       self.chunk1 = data[:len(data)/2]
       self.chunk2 = data[len(data)/2:]
       
   def ComputeNumNodes(self):
       """
       Computes number of nodes in the graph
       """
       #MAP STEP
       #Input = [{'from':intx,'to':'inty},{'from':intw,'to':'intz},...]
       #Emit = [intx,inty,intw,intz,...]
       #First chunk of emitted data
       emit1 = []
       for i in range(len(self.chunk1)): 
           emit1.append(self.chunk1[i]['from'])
           emit1.append(self.chunk1[i]['to'])
       #Second chunk of emitted data
       emit2 = []
       for i in range(len(self.chunk2)):
           emit2.append(self.chunk2[i]['from'])
           emit2.append(self.chunk2[i]['to'])
           
       #SHUFFLE STEP
       #Assign emit1 to processor 1 and emit2 to processor 2

       #REDUCE STEP
       #First processor - remove duplicates from input
       output1 = list(set(emit1))
       #Second processor - remove duplicates from input
       output2 = list(set(emit2))
       
       #COMBINE STEP - combine nodes and remove duplicates
       output = list(set(output1 + output2))
       self.numnodes = len(output) #Store the number of nodes
       self.nodes = output #Store the list of nodes
       return self.numnodes 
   
   def AvgMedianInOutDegree(self):
       """
       Computes average and median in degree and out degree
       Computes number of nodes with in degree > 100
       """
       #Creating a list of all nodes if not already created
       #Relying on the assumption that a machine can handle |V| but not |E| in the graph
       if not hasattr(self, 'nodes'):
           self.ComputeNumNodes()
           
       #MAP STEP
       #Input = [{'from':intx,'to':'inty},{'from':intw,'to':'intz},...]
       #Emit the input as it is
       #First chunk of emitted data = self.chunk1
       #Second chunk of emitted data = self.chunk2
          
       #SHUFFLE STEP
       #Assign emit1 to processor 1 and emit2 to processor 2

       #REDUCE STEP
       #Input = [{'from':intx,'to':'inty},{'from':intw,'to':'intz},...]
       #Output = {intx:{'in':num1,'out':num2},inty:{'in':'num3','out':num4},...}
       #First processor 
       output1 = {i:{'in':0,'out':0} for i in self.nodes}
       for i in range(len(self.chunk1)):
          output1[self.chunk1[i]['from']]['out'] += 1
          output1[self.chunk1[i]['to']]['in'] += 1
       #Second processor 
       output2 = {i:{'in':0,'out':0} for i in self.nodes}
       for i in range(len(self.chunk2)):  
          output2[self.chunk2[i]['from']]['out'] += 1
          output2[self.chunk2[i]['to']]['in'] += 1
       
       #COMBINE STEP
       #combine dictionaries by summing in and out degrees of each key common to both dictionaries
       output = output1.copy()
       for key in output2.keys():
          output[key]['in'] += output2[key]['in']
          output[key]['out'] += output2[key]['out']
       
       #Compute median in degree and out degree
       medin,medout = 0,0
       medin_tuplelist = sorted(output.items(), key=lambda (k,v): v['in'])
       medout_tuplelist = sorted(output.items(),key=lambda (k,v): v['out'])
       #Sample content of medin_tuplelist ('5983', {'out': 1, 'in': 0})
       if self.numnodes%2 == 0:
           medin = (medin_tuplelist[(self.numnodes/2)-1][1]['in'] + medin_tuplelist[(self.numnodes/2)][1]['in'])/float(2)
           medout = (medout_tuplelist[(self.numnodes/2)-1][1]['out'] + medout_tuplelist[(self.numnodes/2)][1]['out'])/float(2)
       else:
           medin = medin_tuplelist[(self.numnodes/2)][1]['in']
           medout = medout_tuplelist[self.numnodes/2][1]['out']
       
        #Compute in degree count, out degree count and number of nodes with in degree > 100    
       incount,outcount,numnodes100in = 0,0,0
       for key in output.keys():
           incount += output[key]['in']
           outcount += output[key]['out']
           if output[key]['in'] > 100:
               numnodes100in += 1
       return incount/float(self.numnodes),outcount/float(self.numnodes),medin,medout,numnodes100in

   def AvgMedTwoHops(self):
       """
       Computes the average and median number of nodes reachable in two hops
       """
       #Creating a list of all nodes if not already created
       #Relying on the assumption that a machine can handle |V| but not |E| in the graph
       if not hasattr(self, 'nodes'):
           self.ComputeNumNodes()
           
       #MAP STEP
       #Input = [{'from':intx,'to':'inty},{'from':intw,'to':'intz},...]
       #Emit the input as it is
       #First chunk of emitted data = self.chunk1
       #Second chunk of emitted data = self.chunk2
          
       #SHUFFLE STEP
       #Assign emit1 to processor 1 and emit2 to processor 2

       #REDUCE STEP
       #Input = [{'from':intx,'to':'inty},{'from':intw,'to':'intz},...]
       #Output = {intx:{'firsthop':[],'secondhop':[],'numnode2hops':0},...}
       #First processor 
       output1 = {i:{'firsthop':[],'secondhop':[],'numnode2hops':0} for i in self.nodes}
       for i in range(len(self.chunk1)):
          output1[self.chunk1[i]['from']]['firsthop'].append(self.chunk1[i]['to'])
       for key in output1.keys():
          for node in output1[key]['firsthop']:
              output1[key]['secondhop'] = list(set(output1[key]['secondhop'] + output1[node]['firsthop']))
       
       #Second processor 
       output2 = {i:{'firsthop':[],'secondhop':[],'numnode2hops':0} for i in self.nodes}
       for i in range(len(self.chunk2)):  
          output2[self.chunk2[i]['from']]['firsthop'].append(self.chunk2[i]['to'])
       for key in output2.keys():
           for node in output2[key]['firsthop']:
               output2[key]['secondhop'] = list(set(output2[key]['secondhop'] + output2[node]['firsthop']))
       
       #COMBINE STEP
       #combine dictionaries by summing first hop and second hop nodes of each key common to both dictionaries
       output = output1.copy()
       for key in output2.keys():
          output[key]['firsthop'] = list(set(output[key]['firsthop'] + output2[key]['firsthop']))
          output[key]['secondhop'] = list(set(output[key]['secondhop'] + output2[key]['secondhop']))
       
       #Compute the average
       avg = 0
       for key in output.keys():
           output[key]['numnode2hops'] = len(output[key]['secondhop'])
           avg += output[key]['numnode2hops']
       avg /= float(self.numnodes)
       
       #Compute the median 
       med = 0
       med_tuplelist = sorted(output.items(), key=lambda (k,v): v['numnode2hops'])
       for i in range(10):
           print med_tuplelist[i]
       #Sample content of med_tuplelist ('5983', {'firsthop': [], 'secondhop': [],'numnode2hops':0})
       if self.numnodes%2 == 0:
           med = (med_tuplelist[(self.numnodes/2)-1][1]['numnode2hops'] + med_tuplelist[(self.numnodes/2)][1]['numnode2hops'])/float(2)
       else:
           med = med_tuplelist[(self.numnodes/2)][1]['numnode2hops']
       return avg, med    

def main():
    MR = MapReduce()
    numnodes = MR.ComputeNumNodes()
    print "Number of nodes in the graph:", numnodes
    avgin,avgout,medin,medout,nodesingt100 = MR.AvgMedianInOutDegree()
    print "Average in degree:", avgin
    print "Average out degree:", avgout
    print "Median in degree:", medin
    print "Median out degree:", medout
    print "Number of nodes with in degree > 100:", nodesingt100
    avg,med = MR.AvgMedTwoHops()
    print "Average number of nodes reachable in two hops:", avg
    print "Median of number of nodes reachable in two hops:", med

if __name__ == "__main__":
    main()
           
        
