import numpy as np
import random
import pickle
import timer
if __name__=='__main__':
    import matplotlib.pyplot as plt

shuf = []

def verify_normal():
    sigma = 3000
    mu = 0
    s = np.random.default_rng().normal(mu,sigma,1000)
    s = abs(s)
    count, bins, ignored = plt.hist(s, 100, density=True)
    plt.plot(bins, 1/(sigma * np.sqrt(2 * np.pi)) *
                np.exp( - (bins - mu)**2 / (2 * sigma**2) ),
            linewidth=2, color='r')
    plt.show()

def verify_pareto():
    a, m = 1., 1.
    s = (np.random.default_rng().pareto(a,int(1000*1.2))+1)*m
    s.sort()
    s = s[:1000]
    count, bins, _ = plt.hist(s, 100, density=True)
    fit = a*m**a / bins**(a+1)
    plt.plot(bins, max(count)*fit/max(fit), linewidth=2, color='r')
    plt.show()


def nextOrderPositionCount():
    return min(int(np.random.default_rng().pareto(1.6)+1),100)

def nextProduct():
    x = None
    while x is None or x >= 10000:
        x = int(abs(np.random.default_rng().normal(0,3000)))
    return x

def pickProducts( count ):
    pr = []
    for x in range(count):
        while True:
            p = nextProduct()
            if not p in pr:
                pr.append(p)
                break
    res = [shuf[p] for p in pr]  
    return res

def pickProducts2( count ):
    pr = []
    prev = -1
    for x in range(count):
        at = 0
        while True:
            if prev > 4000:
                p = random.randint(prev,10000-1)
            else:
                p = nextProduct()
            if x>0 and p<=pr[x-1]:
                at = at + 1
                if at > 10:
                    break
                continue
            if not p in pr:
                pr.append(p)
                prev = p
                break
        if at > 10:
            break
    res = [shuf[p] for p in pr]  
    return res


def newShuffle():
    x = [x for x in range(10000)]
    random.shuffle(x)
    with open('shuffle.pd', 'wb') as fp:
        pickle.dump(x, fp)
    
def loadShuffle():
    global shuf
    with open ('shuffle.pd', 'rb') as fp:
        shuf = pickle.load(fp)

loadShuffle()

if __name__=='__main__':
    verify_normal()
    #for x in range(100):
    #    print( nextOrderPositionCount())


    # timer.timer_on = True
    # with timer.MyTimer('Pick'):
    #     print(pickProducts(10))