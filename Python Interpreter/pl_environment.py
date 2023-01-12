#!/usr/bin/env python
""" generated source for module Environment """
#  (C) 2013 Jim Buffenbarger
#  All rights reserved.
from pl_evalexception import EvalException

class Environment(object):
    """ generated source for class Environment """

    def __init__(self):
        """ generated source for method __init__ """
        self.dict = dict()

    def put(self, var, val):
        """ generated source for method put """
        self.dict[var] = val
        return None

    def get(self, pos, var):
        """ generated source for method get """
        if var not in self.dict:
            return EvalException(pos, str(var) + " not in environment!")
        else: 
            return self.dict[var]
