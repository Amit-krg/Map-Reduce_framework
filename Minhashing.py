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
            

#Method to generate prime number
def gen_primes():   
    D = {}    
    q = 2    
    while True:
        if q not in D:
            yield q
            D[q * q] = [q]
        else:           
            for p in D[q]:
                D.setdefault(p + q, []).append(p)
            del D[q]
        
        q += 1

def minhash(documents, k=5): 
    #returns a minhashed signature for each document
    #documents is a list of strings
    #k is an integer indicating the shingle size
    shingles=set()
    for row in documents:
        row=row.replace(" ","")
        row=row.replace(".","")
        row=row.replace(",","")
        row=row.replace("-","")
        row=row.replace("'","")
        for i in range(0,len(row)-1):
            if(i+5< len(row)):
                shingles.add(row[i:i+5])    
    pprint(shingles)
    print(len(shingles))
    #print(len(s)) 
    cm=[]    
    #print(cm)
    shingles=list(shingles)
    for i in range(0,len(shingles)):
        temp=[]
        for j in range(len(documents)):
            if shingles[i] in documents[j]:
                temp.append(1)
            else:
                temp.append(0) 
        cm.append(temp)         
    #print(cm)
    #cm=np.array(cm)
    pprint(cm)
    h=[]  # h(i)= row of signature matix*hash function number mod h   
    count=0
    for p in gen_primes():
        if count<100:
            h.append(p)
            count+=1
        else:                  
            break;
    print(h)                    
    signature_m = np.zeros((100,3))#the signature matrix, created a 100x3 matrix initialized with 0
    for i in range(100):
        for j in range(3):
            signature_m[i][j]=10000  #initialized each value with infinity
    for i in range(len(shingles)):
        for col in range(len(documents)):
            if cm[i][col]==1:
                for k in range (100):
                    if signature_m[k][col] > (i*k)%h[k]: #h(x)= i*x % primeno
                        signature_m[k][col]= (i*k)%h[k]
    print(signature_m)                     
    return signature_m #a minhash signature for each document


##########################################################################


if __name__ == "__main__": #[DONE: Uncomment peices to test]
                
   
    documents = ["The horse raced past the barn fell. The complex houses married and single soldiers and their families",
                  "There is nothing either good or bad, but thinking makes it so. I burn, I pine, I perish. Come what come may, time and the hour runs through the roughest day",
                  "Be a yardstick of quality. A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful. I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."]
    sigs = minhash(documents, 5)
      