
import time
from datetime import datetime,timezone,timedelta
import multiprocessing as mp
import numpy as np
from queue import Queue
import threading

def bubblesort(data):
    n = len(data)
    for i in range(n-1):                   
        for j in range(n-i-1):              
            if data[j] > data[j+1]:        
                data[j], data[j+1] = data[j+1], data[j] # swap

def merge(left, right):
    result = []
    l = 0
    r = 0
    max = len(left) + len(right)
    for i in range( max ):
        if ( l < len(left) ) and ( r < len(right) ):
            if left[l] <= right[r]:
                result.append(left[l])
                l = l + 1
        
            else:
                result.append(right[r])
                r = r + 1

        else:
            if l < len(left):
                result.append(left[l])
                l = l + 1

            else:
                result.append(right[r])
                r = r + 1

    return result

def kmerge(final_list, k):
    tempList = final_list.copy()
    while k > 1:
        j = 0
        mergeList = tempList.copy()
        tempList.clear()
        while (j + 1) < k:
            temp = merge(mergeList[j], mergeList[j+1])
            tempList.append(temp)
            j = j + 2

        if (k%2) == 1:
            tempList.append( mergeList[k-1] )
        
        mergeList.clear()
        k = len(tempList)
    
    return tempList

def mergetask( queue, newQueue ):
    left = queue.get()
    right = queue.get()
    temp = merge( left, right )
    newQueue.put( temp )

def mergeProcess(queue, k):
    manager = mp.Manager()
    newQueue = manager.Queue()

    while k > 1:
        j = 0
        i = 0
        pool = mp.Pool( mp.cpu_count() )
        pool.starmap( mergetask, [( queue, newQueue ) for j in range(0, k-1, 2) ] )
        pool.close()
        pool.join()

        '''
        processList = []
        while (j + 1) < k:
            processList.append(mp.Process(target=mergetask, args=(queue, newQueue)))
            processList[i].start()
            j = j + 2
            i = i + 1

        for p in range( i ):
            processList[p].join()
        '''

        if (k%2) == 1:
            newQueue.put( queue.get() )

        while not newQueue.empty():
            queue.put( newQueue.get() )

        k = queue.qsize()

def mergeThread(queue, k):
    newQueue = Queue()

    while k > 1:
        j = 0
        i = 0
        threadList = []
        while (j + 1) < k:
            threadList.append(threading.Thread(target=mergetask, args=(queue, newQueue)))
            threadList[i].start()
            j = j + 2
            i = i + 1

        for t in range( i ):
            threadList[t].join()

        if (k%2) == 1:
            newQueue.put( queue.get() )

        while not newQueue.empty():
            queue.put( newQueue.get() )

        k = queue.qsize()

def method2task( final_list, k, answer ):
    for i in range( len(final_list) ):
        bubblesort( final_list[i] )
    
    t = kmerge(final_list, k)
    answer.put(t[0])

def bubbletask( tempList, answer ):
    bubblesort( tempList )
    answer.put( tempList )


def out(fileName, method, text, cputime):
    name = fileName + "_output" + method + ".txt"
    dt1 = datetime.utcnow().replace(tzinfo=timezone.utc)
    dt2 = dt1.astimezone(timezone(timedelta(hours=8))) # 轉換時區 -> 東八區
    with open(name, 'w') as f:
        print( "Sort : ", file = f )
        for num in range( len(text) ):
            print( text[num], file = f )
        
        print( 'CPU Time : %f'%(cputime), file = f )
        print('datetime : %s'%(dt2), file = f)


def method1(text, fileName):
    start = time.time()
    bubblesort(text)
    end = time.time()
    cputime = end - start
    out( fileName, "1", text, cputime )

def method2(text, fileName):
    k = input( "請輸入要切成幾分:\n" )
    k = int(k)
    start = time.time()
    final_list = np.array_split(text,k)
    manager = mp.Manager()
    answer = manager.Queue()
    p1 = mp.Process(target=method2task, args=(final_list, k, answer))
    p1.start()
    p1.join()
    end = time.time()
    cputime = end - start
    out( fileName, "2", answer.get(), cputime )

def method3(text, fileName):
    k = input( "請輸入要切成幾分:\n" )
    k = int(k)
    start = time.time()
    final_list = np.array_split(text,k)
    manager = mp.Manager()
    answer = manager.Queue()

    pool = mp.Pool( mp.cpu_count() )
    pool.starmap( bubbletask, [( final_list[i], answer ) for i in range(k) ] )
    pool.close()
    pool.join()
    '''
    processList = []
    for i in range( k ):
        processList.append(mp.Process(target=bubbletask, args=(final_list[i], answer)))
        processList[i].start()

    for i in range( k ):
        processList[i].join()
    '''

    mergeProcess( answer, k )
    end = time.time()
    cputime = end - start
    out( fileName, "3", answer.get(), cputime )

def method4(text, fileName):
    k = input( "請輸入要切成幾分:\n" )
    k = int(k)
    start = time.time()
    final_list = np.array_split(text,k)
    answer = Queue()
    
    threadList = []
    for i in range( k ):
        threadList.append(threading.Thread(target=bubbletask, args=(final_list[i], answer)))
        threadList[i].start()

    for i in range( k ):
        threadList[i].join()
        
    mergeThread( answer, k )
    end = time.time()
    cputime = end - start
    out( fileName, "4", answer.get(), cputime )

if __name__ == '__main__':
    keep = True
    while keep:
        fileName = input( "請輸入檔案名稱:\n" )
        name = fileName + ".txt"
        f = None
        text = []
        try:
            f = open(name, 'r')
            for line in f.readlines():
                num = line.split( '\n' )
                if not num[0] == '':
                    text.append(int(num[0]))
        
        except IOError:
            print('ERROR: can not found ' + name)
            if f:
                f.close()
        finally:
            if f:
                f.close()
        
        #final_list = np.array_split(text,1)
        #print( final_list[0] )

        if text:
            method = input( "請輸入方法編號:\n" )
            if method == "1":
                method1(text, fileName)
        
            elif method == "2":
                method2(text, fileName)

            elif method == "3":
                method3(text, fileName)

            elif method == "4":
                method4(text, fileName)

            else:
                print( "no such method" )

        else:
            print( "empty file or no such file" )

        text.clear()
        cmd = input( "繼續請輸入1\n" )
        if cmd == "1":
            keep = True

        else:
            keep = False
