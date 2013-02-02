import operator as op
import itertools as it
import random as rnd
import os as os
from functools import reduce
from functools import partial
import multiprocessing as mp

def convert_to_sparse_matrix(matrix):
    N = len(matrix)
    M = len(matrix[0])
    return { pt: matrix[pt[0]][pt[1]] for pt in it.product(range(N), range(M)) }

def Lmax(p1, p2):
    return max(
        map(
            abs,
            map(
                op.sub,
                p1, 
                p2
            )
        )
    )

def printer(var):
    print(var)
    return var

class wordsolver(object):
    def __init__(self):
        self.words_set = set(
            map(
                op.methodcaller("rstrip"),
                open('words.dat')
            )
        )
        self.partial_words_set = set(
            it.chain(*
                map(
                    it.accumulate, 
                    self.words_set
                )
            )
        )

    def is_word(self, word):
        return word in self.words_set

    def is_partial_word(self, word):
        return word in self.partial_words_set

    def solve(self, matrix):
        def solve_from(pt, free_pts = set(matrix.keys()), init=""):
            word = init + matrix[pt]
            if(self.is_partial_word(word)):
                distance = partial(Lmax, pt)
                neighbor = ( lambda p: distance(p) == 1 )
                solve_from_next = partial(
                    solve_from, 
                    free_pts = ( free_pts - set([pt,]) ),
                    init = word
                )
                return reduce(
                    op.or_,
                    filter(
                        bool, #not Null
                        map(
                            solve_from_next,
                            filter(
                                neighbor,
                                free_pts
                            )
                        )
                    ),
                    ( set(), set([word,]) )[ self.is_word(word) ] #basecase
                )

        return reduce(
            op.or_, 
            map(
                solve_from,
                matrix.keys()
            )
        )

def random_matrix(N, M):
    letters = "ABCDEFGHIJKLMNOPQRSTUVXYZ"
    while True:
        yield { p: rnd.choice(letters) for p in it.product(range(N), range(M)) }

def filename(matrix):
    return "".join(
        map( #sorted dict
            lambda p:matrix[p], 
            sorted(matrix.keys())
        )
    )+".dat" 

if __name__ == "__main__":
    N = 4
    M = 4
    folder = "${N}x${M}".format(N=N, M=M) 
    ws=wordsolver()

    for m in random_matrix(N, M):
        print(m)
        print(len(ws.solve(m)))

