import operator as op
import itertools as it
import copy as cp
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
        #print("is_word", word)
        #if(isinstance(word, str)):
        return word in self.words_set
        #elif(isinstance(word, set)):
        #    return True #assume a set already contains valid words
        #else:
        #    return False

    def is_partial_word(self, word):
        return word in self.partial_words_set

    def solve(self, matrix):
        matrix = convert_to_sparse_matrix(matrix)

        def solve_from(pt, free_pts = set(matrix.keys()), init=""):
            word = init + matrix[pt]
            if(self.is_partial_word(word)):
                #if(self.is_word(word)):
                #    print(word)
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
                        bool,
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


if __name__ == "__main__":
    m=["ABCD","EFGH","IJKL","MNOP"];

    ws=wordsolver()
    print(ws.solve(m))


