########################################
## Template Code for Big Data Analytics
## assignment 1, at Stony Brook Univeristy
## Fall 2016

## <Amit Gupta,110900982>

##Matrix Multiplication
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint
import numpy as np
from scipy.sparse import coo_matrix


##########################################################################
##########################################################################
# PART I. MapReduce

class MyMapReduce:
    __metaclass__ = ABCMeta
    
    def __init__(self, data, num_map_tasks=4, num_reduce_tasks=3): 
        self.data = data  #the "file": list of all key value pairs
        self.num_map_tasks=num_map_tasks #how many processes to spawn as map tasks
        self.num_reduce_tasks=num_reduce_tasks # " " " as reduce tasks

    ###########################################################   
    #programmer methods (to be overridden by inheriting class)
    @abstractmethod
    def map(self, k, v): 
        print ("Need to override map")    
    @abstractmethod
    def reduce(self, k, vs): 
        print ("Need to override reduce")       

    ###########################################################
    #System Code: What the map reduce backend handles

    def mapTask(self, data_chunk, map_to_reducer): 
        #runs the mappers and assigns each k,v to a reduce task
        for (k, v) in data_chunk:
            #run mappers:
            mapped_kvs = self.map(k, v)
            #print(mapped_kvs)
            #assign each kv pair to a reducer task
            for (k, v) in mapped_kvs:
                map_to_reducer.append((self.partitionFunction(k), (k, v)))

    @abstractmethod
    def partitionFunction(self,k): 
        #genarate hash and return k
        #given a key returns the reduce task to send it
        print("Need to override partitionFunction")


    def reduceTask(self, kvs, from_reducer):
        mydict={}
        for(k,vs) in kvs:
            if k in mydict.keys():
                mydict[k].append(vs)
            else:
                mydict[k]=[vs]
        #pprint(mydict)       
        for k in mydict:
            from_reducer.append(self.reduce(k, mydict[k]))
            
        
    def runSystem(self):
        map_to_reducer = Manager().list() #stores the reducer task assignment and 
                                          #each key-value pair returned from mappers
                                          #in the form: [(reduce_task_num, (k, v)), ...]
        from_reducer = Manager().list() #stores key-value pairs returned from reducers
                                        #in the form [(k, v), ...]
        
        
        chunksize = len(self.data)//self.num_map_tasks        
        p=[]
        k=0
        for i in range(self.num_map_tasks):
            chunk= [] 
            for j in range(chunksize):
                chunk.append(self.data[j+k]) 
            k+=chunksize    
            p.append(Process(target=self.mapTask, args=(chunk,map_to_reducer)))
            p[i].start()
            #print("current chunk%d",(i))
            #print(chunk)
            
        #join map task processes back
        
        for i in range(self.num_map_tasks):
            p[i].join()

        #print output from map tasks 
        
        print ("map_to_reducer after map tasks complete:")
        map_to_reducer=sorted(list(map_to_reducer))
        pprint(map_to_reducer)
        #print(len(map_to_reducer))

        #"send" each key-value pair to its assigned reducer by placing each 
        #into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
        to_reduce_task=[]
        temp=[]
        count=0
        for (k,v) in map_to_reducer:
            #print(k,v)
            if(k==count):
                temp.append(v)
                #print(temp)
            else:
                count+=1
                to_reduce_task.append(temp)
                temp=[]
                temp.append(v)
        to_reduce_task.append(temp)        
        #pprint(to_reduce_task)        

        R=[]
        #launch the reduce tasks as a new process for each. 
        for i in range(self.num_reduce_tasks):
            R.append(Process(target=self.reduceTask, args=(to_reduce_task[i],from_reducer)))
            R[i].start()
        #join the reduce tasks back
        for i in range(self.num_reduce_tasks):
            R[i].join()
        #print output from reducer tasks 
        
        print ("from_reducer after reduce tasks complete:")
        pprint(sorted(list(from_reducer)))

        #return all key-value pairs:
        
        return from_reducer

##Map Reducers:
            
##################################################################################

class MatrixMultMR(MyMapReduce):
    def __init__(self,data,num_map_tasks,num_reduce_tasks):
        #print(data)
        hr=0
        hc=0
        for (k,v)in data:
            if k[0]=='m':
                if k[1]>hr:
                    hr=k[1]
            else:
                if k[2]>hc:
                    hc=k[2]
        self.row=hr+1
        self.col=hc+1            
                        
        super().__init__(data, num_map_tasks, num_reduce_tasks)
    def map(self,key,value):
        l=[]
        #print("matrix",self.tup2)
        if key[0]=='m':
            for k in range(self.col): # (j,k) in n
                l.append(((key[1],k),('m',key[2],value)))
        else:
            for i in range(self.row):
                l.append(((i,key[2]),('n',key[1],value)))                     
        return l
    def reduce(self,key,value):
        s=0
        mid=len(value)//2
        j=mid;
        for i in range(mid):
            s+=value[i][2]*value[j][2]
            j+=1
            
        #print(key,s)             
        return (key,s)
    def partitionFunction(self,k):
        sum=k[0]+k[1]
        return (sum%self.num_reduce_tasks)      
    
    
def matrixToCoordTuples(label, m): #given a dense matrix, returns ((row, col), value), ...
    cm = coo_matrix(np.array(m))
    return (list(zip(zip([label]*len(cm.row), cm.row, cm.col), cm.data)))               


##########################################################################


if __name__ == "__main__": #[DONE: Uncomment peices to test]
                
    ####################
    ##run MatrixMultiply
    data1 = matrixToCoordTuples('m', [[1, 2], [3, 4]]) + matrixToCoordTuples('n', [[1, 2], [3, 4]])
    data2 = matrixToCoordTuples('m', [[1, 2, 3], [4, 5, 6]]) + matrixToCoordTuples('n', [[1, 2], [3, 4], [5, 6]])
    data3 = matrixToCoordTuples('m', np.random.rand(20,5)) + matrixToCoordTuples('n', np.random.rand(5, 40))
    mrObject = MatrixMultMR(data1, 2, 2)
    mrObject.runSystem() 
    mrObject = MatrixMultMR(data2, 2, 2)
    mrObject.runSystem() 
    mrObject = MatrixMultMR(data3, 6, 6)
    mrObject.runSystem() 

   
      