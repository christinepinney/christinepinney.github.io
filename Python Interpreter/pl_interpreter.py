#!/usr/bin/env python
""" generated source for module Interpreter """
#  (C) 2013 Jim Buffenbarger
#  All rights reserved.
from pl_parser import Parser
from pl_syntaxexception import SyntaxException
from pl_environment import Environment

class Interpreter(object):
    """ generated source for class Interpreter """
    @classmethod
    def main(cls, args):
        """ generated source for method main """
        parser = Parser()
        env = Environment()
        for prog in args:
            try:
                node = parser.parse(prog)
                node.eval(env)
            except SyntaxException as e:
                print(e)

if __name__ == '__main__':
    import sys
    Interpreter.main(sys.argv[1:])

