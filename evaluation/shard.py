import math
import scipy . special
import numpy
from fractions import Fraction

# Compute probability of having too many corrupted parties in committee with
# n = total population
# t = number of corruptions in total population
# s = committee size
# h = minimum number of honest parties required in committee
# pMax = maximal value for which pFail returns correct value . Output is 1 if pFail > pMax .
def pFail (n , t , s , h , pMax ) :
    p = 0
    denom = scipy . special . comb (n , s , exact = True ) # compute n choose s as exact integer
    for i in range ( s - h + 1 , s +1) :
        p += Fraction ( scipy . special . comb (t , i , exact = True ) *
        scipy . special . comb (n -t , s -i , exact = True ) , denom )
    if p > pMax :
        return 1
    return p

# Find minimum committee size with corruption ration cr such that pFail <= 2^{ -k}
def minCSize (n , t , cr , k ) :
    pMax = Fraction (1 , 2** k )
    for s in range (1 , n +1) :
        h = math . ceil ((1 - cr ) * s ) # we want at least h honest parties to not violate corruption threshold
    if pFail (n , t , s , h , pMax ) <= pMax :
        return s

# Compute analytical upper bound
def analyticBound (n , t , cr , k ) :
    p = Fraction (t , n )
    q = cr
    alpha = q - p
    beta = ( math . e * math . sqrt ( q ) * (1 - p ) ) / (2 * math . pi * alpha
    * math . sqrt (1 - q ) )
    bound = math . ceil ( k / math . log (( q / p ) ** q * ((1 - q ) /(1 - p ) ) **(1 - q ) ,
    2) )
    betaBound = math . ceil ( beta * beta )
    return max ( bound , betaBound )

# Compute values for 10000 total parties , 30% corruption , and 60 bit security
n = 10000
t = 3000
k = 60

# corruption threshold percentages we are interested in
crps = range (99 , 32 , -1)

print ("cr\t min\t bound ")
for crp in crps :
    cr = Fraction ( crp , 100) # convert percentage to fraction
    print ( float ( cr ) , "\t", minCSize (n , t , cr , k ) , "\t",
    analyticBound (n , t , cr , k ) )