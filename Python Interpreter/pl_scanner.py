#!/usr/bin/env python
""" generated source for module Scanner """
#  (C) 2013 Jim Buffenbarger
#  All rights reserved.
from pl_syntaxexception import SyntaxException
from pl_token import Token
import sys


class Scanner(object):
    """ generated source for class Scanner """
    program = ""
    whitespace = {' ','\n','\t'}
    digits = set("0123456789")
    letters = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
    legits = set("_").union(digits).union(letters)
    symbols = set("@[\]^_`!\"#$%&',)(*+-./:;<=>?")
    operators = {'=',';','(',')','+','-','*','/','<','<=','>','>=','<>','=='}
    keywords = {'wr', 'rd', 'else', 'begin', 'end', 'then', 'if', 'while', 'do'}
    token = ""
    lexeme = ""


    def __init__(self, program):
        """ generated source for method __init__ """
        self.program = program
        self.pos = 0
        self.token = None

    def done(self):
        """ generated source for method done """
        return self.pos >= len(self.program)

    def many(self, s):
        """ generated source for method many """
        while not self.done() and self.program[self.pos] in s:
            self.pos += 1

    def past(self, c):
        """ generated source for method past """
        while not self.done() and c != self.program[self.pos]:
            self.pos += 1
        if not self.done() and c == self.program[self.pos]:
            self.pos += 1
    
    def nextNumber(self):
        """ generated source for method nextNumber """
        old = self.pos
        self.many(self.digits)
        if not self.done() and self.program[self.pos] == '.':
            self.pos += 1
            self.many(self.digits)
        self.token = Token("num", self.program[old:self.pos])

    def nextKwId(self):
        """ generated source for method nextKwId """
        old = self.pos
        self.many(self.letters)
        self.many(self.legits)
        lexeme = self.program[old:self.pos]
        if lexeme == 'wr':
            self.token = Token('wr', 'wr')
        elif lexeme == 'rd':
            self.token = Token('rd', 'rd')
        elif lexeme == 'if':
            self.token = Token('if', 'if')
        elif lexeme == 'else':
            self.token = Token('else', 'else')
        elif lexeme == 'then':
            self.token = Token('then', 'then')
        elif lexeme == 'while':
            self.token = Token('while', 'while')
        elif lexeme == 'do':
            self.token = Token('do', 'do')
        elif lexeme == 'begin':
            self.token = Token('begin', 'begin')
        elif lexeme == 'end':
            self.token = Token('end', 'end')
        else:
            self.token = Token('id', lexeme)

    def nextOp(self):
        """ generated source for method nextOp """
        old = self.pos
        self.pos = old + 2
        if not self.done():
            self.lexeme = self.program[old: self.pos]
            if self.lexeme in self.operators:
                self.token = Token(self.lexeme)
                return
        self.pos = old + 1
        self.lexeme = self.program[old: self.pos]
        self.token = Token(self.lexeme)

    def next(self):
        """ generated source for method next """
        self.many(self.whitespace)
        if self.done():
            self.token = Token('')
            return False
        c = self.program[self.pos]
        if c in self.digits:
            self.nextNumber()
        elif c in self.letters:
            self.nextKwId()
        elif c in self.operators:
            self.nextOp()
        elif c == "#":
            self.pos += 1
            self.past('\n')
            return self.next()
        elif c in self.keywords:
            self.nextKwId()
        else:
            print("illegal character " + self.program[self.pos] + " at position " + str(self.pos))
            self.pos += 1
            return self.next()
        return True

    def match(self, t):
        """ generated source for method match """
        if not t == self.curr():
            raise SyntaxException(self.pos, t, self.curr())
        self.next()

    def curr(self):
        """ generated source for method curr """
        if self.token == None:
            raise SyntaxException(self.pos, Token("ANY"), Token("EMPTY"))
        return self.token

    def position(self):
        """ generated source for method pos """
        return self.pos


if __name__ == '__main__':
    scanner = Scanner(sys.argv[1])
    while scanner.next():
        print(scanner.curr())

