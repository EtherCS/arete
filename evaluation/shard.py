import math
import scipy . special
import numpy
from fractions import Fraction

# Compute probability of having too many corrupted parties in committee with
# n = total population
# s = number of corruptions in total population
# m = committee size
# f = corruption ration in a committee
# h = minimum number of honest parties required in committee
# pMax = maximal value for which pFail returns correct value . Output is 1 if pFail > pMax .
def pFail(n, s, m, h, pMax):
    p = 0
    denom = scipy.special.comb(n, m, exact = True) # compute n choose m as exact integer
    for i in range(m - h + 1, m + 1):
        p += Fraction(scipy.special.comb(s, i, exact = True) * scipy.special.comb(n - s, m - i, exact = True), denom)
        if p > pMax:
            return 1
    return p

# Find minimum committee size with corruption ration f such that pFail <= 2^{ -k}
def minCSize(n, s, f, k):
    pMax = Fraction(1, 2 ** k)
    for m in range(1, n + 1):
        h = math.ceil((1 - f) * m) # we want at least h honest parties to not violate corruption threshold
        if pFail(n, s, m, h, pMax) <= pMax :
            # pRemove(n, s, m, h, f)
            return m

# Compute probability of forming a shard with corruption between s~mf, i.e., remove corruptions from the node pool to a new shard
def pRemove(n, s, m, h, f):
    p = 0
    lower_bound = math.ceil(m*s/n)
    upper_bound = math.ceil(m*f)
    # lower_bound = math.ceil(0)
    # upper_bound = math.ceil(m*s/n)
    denom = scipy.special.comb(n, m, exact = True) # compute n choose m as exact integer
    for i in range(lower_bound, upper_bound):
        p += Fraction(scipy.special.comb(s, i, exact = True) * scipy.special.comb(n - s, m - i, exact = True), denom)
    print("lower_bound=", lower_bound, ", upper_bound=", upper_bound, ", m=", m, ", pr=", float(p))

# Compute analytical upper bound, nodes can be put back to the pool (replacement)
def analyticBound(n , s , f , k):
    p = Fraction(s , n)
    q = f
    alpha = q - p
    beta = (math.e * math.sqrt(q) * (1 - p)) / (2 * math.pi * alpha * math.sqrt(1 - q))
    bound = math.ceil(k / math.log((q / p) ** q * ((1 - q)/(1 - p))**(1 - q), 2))
    betaBound = math.ceil(beta * beta)
    return max(bound, betaBound)

# Compute values for 10000 total parties , 30% corruption , and 60 bit security
n = 10000
s = 3000
k = 60

# corruption threshold percentages we are interested in
crps = range(99 , 32 , -1)

print ("cr\t min\t bound ")
for crp in crps:
    f = Fraction(crp, 100) # convert percentage to fraction
    print (float(f) , "\t", minCSize(n, s, f, k) , "\t", analyticBound(n, s, f, k))