#!/usr/bin/env python

from pl_syntaxexception import SyntaxException
from pl_node import *
from pl_scanner import Scanner
from pl_token import Token

class Parser(object):
    """ generated source for class Parser """
    def __init__(self):
        self.scanner = None

    def match(self, s):
        """ generated source for method match """
        self.scanner.match(Token(s))

    def curr(self):
        """ generated source for method curr """
        return self.scanner.curr()

    def pos(self):
        """ generated source for method pos """
        return self.scanner.position()

    def parseMulOp(self):
        """ generated source for method parseAssn """
        if self.curr() == Token('*'):
            mulop = self.curr()
            self.match('*')
            return mulop.lex()
        elif self.curr() == Token('/'):
            mulop = self.curr()
            self.match('/')
            return mulop.lex()

        return None
    
    def parseAddOp(self):
        """ generated source for method parseAssn """
        if self.curr() == Token('+'):
            addop = self.curr()
            self.match('+')
            return addop.lex()
        elif self.curr() == Token('-'):
            addop = self.curr()
            self.match('-')
            return addop.lex()

        return None

    def parseRelOp(self):
        """ generated source for method parseAssn """
        if self.curr() == Token('<'):
            lessOp = self.curr()
            self.match('<')
            return lessOp.lex()
        elif self.curr() == Token('<='):
            lessEqOp = self.curr()
            self.match('<=')
            return lessEqOp.lex()
        elif self.curr() == Token('>'):
            greatOp = self.curr()
            self.match('>')
            return greatOp.lex()
        elif self.curr() == Token('>='):
            greatEqOp = self.curr()
            self.match('>=')
            return greatEqOp.lex()
        elif self.curr() == Token('<>'):
            notEqOp = self.curr()
            self.match('<>')
            return notEqOp.lex()
        elif self.curr() == Token('=='):
            eqOp = self.curr()
            self.match('==')
            return eqOp.lex()

        return None

    def parseFact(self):
        """ generated source for method parseAssn """
        fact = None
        if self.curr().tok() == 'id':
            nid = self.curr()
            self.match('id')
            return NodeFactID(nid.lex())
        elif self.curr().tok() == 'num':
            num = self.curr()
            self.match('num')
            return NodeFactNum((int)(num.lex()))
        elif self.curr().tok() == '(':
            self.match('(')
            expr = self.parseExpr()
            self.match(')')
            return NodeFactExpr(expr)
        elif self.curr().tok() == '-':
            self.match('-')
            fact = self.parseFact()
            return NodeFactFact(fact)

        return None
    
    def parseTerm(self):
        """ generated source for method parseAssn """
        fact = self.parseFact()
        mulop = self.parseMulOp()
        if mulop == None:
            return NodeTerm(fact, None, None)
        term = self.parseTerm()
        term.append(NodeTerm(fact, mulop, None))

        return term
    
    def parseExpr(self):
        """ generated source for method parseAssn """
        term = self.parseTerm()
        addop = self.parseAddOp()
        if addop == None:
            return NodeExpr(term, None, None)
        expr = self.parseExpr()
        expr.append(NodeExpr(term, addop, None))

        return expr

    def parseAssn(self):
        """ generated source for method parseAssn """
        nid = self.curr()
        self.match('id')
        self.match('=')
        expr = self.parseExpr()

        return NodeAssn(nid.lex(), expr)

    def parseRead(self):
        self.match('rd')
        _id = self.curr()
        self.match('id')
        
        return NodeRead(_id.lex())

    def parseWrite(self):
        self.match('wr')
        expr = self.parseExpr()
        
        return NodeWr(expr)

    def parseBoolExpr(self):
        expr = self.parseExpr()
        relop = self.parseRelOp()
        expr2 = self.parseExpr()

        return NodeBoolExpr(expr, relop, expr2)

    def parseIf(self):
        self.match('if')
        boolEx = self.parseBoolExpr()
        stmt = None
        stmt2 = None
        self.match('then')
        stmt = self.parseStmt()
        if self.curr().tok() == 'else':
            self.match('else')
            stmt2 = self.parseStmt()
        
        return NodeIf(boolEx, stmt, stmt2)

    def parseWhile(self):
        self.match('while')
        boolEx = self.parseBoolExpr()
        self.match('do')
        stmt = self.parseStmt()
        
        return NodeWhile(boolEx, stmt)

    def parseStmt(self):
        """ generated source for method parseStmt """
        if self.curr() == Token('wr'):
            wr = self.parseWrite()
            return NodeStmt(wr)
        if self.curr() == Token('rd'):
            rd = self.parseRead()
            return NodeStmt(rd)
        if self.curr() == Token('id'): 
            assn = self.parseAssn()
            return NodeStmt(assn)
        if self.curr() == Token('if'): 
            _if = self.parseIf()
            node = NodeStmt(_if)
            if node == None:
                return None
            else:
                return NodeStmt(_if)
        if self.curr() == Token('then'): 
            self.match('then')
            stmt = self.parseStmt()
            return NodeStmt(stmt)
        if self.curr() == Token('while'): 
            _while = self.parseWhile()
            return NodeStmt(_while)
        if self.curr() == Token('begin'):
            self.match('begin')
            block = self.parseBlock()
            return NodeStmt(block)

        return None

    def parseBlock(self):
        """ generated source for method parseBlock """
        stmt = self.parseStmt()
        rest = None
        if self.curr() == Token(';'):
            self.match(';')
            rest = self.parseBlock()
        if self.curr() == Token('end'):
            self.match('end')

        return NodeBlock(stmt, rest)


    def parse(self, program):
        """ generated source for method parse """
        if program == '': return None
        self.scanner = Scanner(program)
        self.scanner.next()

        return self.parseBlock()

