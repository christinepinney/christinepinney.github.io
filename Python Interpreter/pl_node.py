#!/usr/bin/env python
""" generated source for module Node """

#  (C) 2013 Jim Buffenbarger
#  All rights reserved.
from pl_evalexception import EvalException
from pl_environment import *

class Node(object):
    """ generated source for class Node """
    pos = 0

    def __str__(self):
        """ generated source for method toString """
        result = ""
        result += str(self.__class__.__name__)
        result += " ( "
        fields = self.__dict__
        for field in fields:
            result += "  "
            result += str(field)
            result += str(": ")
            result += str(fields[field])
        result += str(" ) ")
        return result

    def eval(self, env):
        """ generated source for method eval """
        raise EvalException(self.pos, "cannot eval() node!")

class NodeAssn(Node):
    """ generated source for class NodeAssn """

    def __init__(self, id, expr):
        """ generated source for method __init__ """
        super(NodeAssn, self).__init__()
        self.id = id
        self.expr = expr

    def eval(self, env):
        """ generated source for method eval """
        return env.put(self.id, self.expr.eval(env))

class NodeBlock(Node):
    """ generated source for class NodeBlock """

    def __init__(self, stmt, block):
        """ generated source for method __init__ """
        super(NodeBlock, self).__init__()
        self.stmt = stmt
        self.block = block

    def eval(self, env):
        retVal = None
        if self.stmt is not None:
            retVal = self.stmt.eval(env)
        if self.block is not None:
            retVal = self.block.eval(env)
        return retVal

class NodeStmt(Node):
    """ generated source for class NodeStmt """

    def __init__(self, assn):
        """ generated source for method __init__ """
        super(NodeStmt, self).__init__()
        self.assn = assn

    def eval(self, env):
        """ generated source for method eval """
        retVal = self.assn.eval(env)
        return retVal

class NodeExpr(Node):
    """ generated source for class NodeExpr """

    def __init__(self, term, addop, expr):
        """ generated source for method __init__ """
        super(NodeExpr, self).__init__()
        self.term = term
        self.addop = addop
        self.expr = expr

    def eval(self, env):
        """ generated source for method eval """
        term = self.term.eval(env)
        retVal = term
        if self.addop is not None:
            expr = self.expr.eval(env)
            if self.addop == '+':
                retVal = expr + term
            else:
                retVal = expr - term
        return retVal
        
    def append(self, expr):
        if self.expr is None:
            self.addop = expr.addop
            self.expr = expr
            expr.addop = None
        else:
            self.expr.append(expr)

class NodeWr(Node):
    """ generated source for class NodeWr """

    def __init__(self, expr):
        """ generated source for method __init__ """
        super(NodeWr, self).__init__()
        self.expr = expr

    def eval(self, env):
        """ generated source for method eval """
        result = self.expr.eval(env)
        print(result)
        return result

class NodeTerm(Node):
    """ generated source for class NodeTerm """

    def __init__(self, fact, mulop, term):
        """ generated source for method __init__ """
        super(NodeTerm, self).__init__()
        self.fact = fact
        self.mulop = mulop
        self.term = term

    def eval(self, env):
        """ generated source for method eval """
        fact = self.fact.eval(env)
        retVal = fact
        if self.mulop is not None:
            term = self.term.eval(env)
            if self.mulop == '*':
                retVal = term * fact
            else:
                retVal = term / fact
        return retVal

    def append(self, term):
        if self.term is None:
            self.mulop = term.mulop
            self.term = term
            term.mulop = None
        else:
            self.term.append(term)

class NodeFactID(Node):
    """ generated source for class NodeFactID """

    def __init__(self, id):
        """ generated source for method __init__ """
        super(NodeFactID, self).__init__()
        self.id = id

    def eval(self, env):
        """ generated source for method eval """
        retVal = env.get(self.pos, self.id)
        return retVal

class NodeFactNum(Node):
    """ generated source for class NodeFactNum """

    def __init__(self, num):
        """ generated source for method __init__ """
        super(NodeFactNum, self).__init__()
        self.num = num

    def eval(self, env):
        """ generated source for method eval """
        return self.num

class NodeFactExpr(Node):
    """ generated source for class NodeFactExpr """

    def __init__(self, expr):
        """ generated source for method __init__ """
        super(NodeFactExpr, self).__init__()
        self.expr = expr

    def eval(self, env):
        """ generated source for method eval """
        return self.expr.eval(env)

class NodeFactFact(Node):

    def __init__(self, fact):
        super(NodeFactFact, self).__init__()
        self.fact = fact

    def eval(self, env):

        return -self.fact.eval(env)

class NodeRead(Node):

    def __init__(self, id):

        super(NodeRead, self).__init__()
        self.id = id

    def eval(self, env):
        r = input()
        return env.put(self.id, float(r))

class NodeIf(Node):

    def __init__(self, boolEx, stmt, stmt2):
        
        super(NodeIf, self).__init__()
        self.boolEx = boolEx
        self.stmt = stmt
        self.stmt2 = stmt2

    def eval(self, env):

        if self.boolEx.eval(env) == None and self.stmt2 == None:
            return None
        elif self.boolEx.eval(env) == None and self.stmt2 != None:
            return self.stmt2.eval(env)
        else:
            return self.stmt.eval(env)
         

class NodeWhile(Node):

    def __init__(self, boolEx, stmt):

        super(NodeWhile, self).__init__()
        self.boolEx = boolEx
        self.stmt = stmt

    def eval(self, env):

        retVal = None
        while self.boolEx.eval(env) != None:
            retVal = self.stmt.eval(env)
        return retVal

class NodeBoolExpr(Node):

    def __init__(self, expr, relop, expr2):

        super(NodeBoolExpr, self).__init__()
        self.expr = expr
        self.relop = relop
        self.expr2 = expr2

    def eval(self, env):
        expr = self.expr.eval(env)
        expr2 = self.expr2.eval(env)
        if self.relop == '<':
            if expr < expr2:
                return True
        if self.relop == '<=':
            if expr <= expr2:
                return True
        if self.relop == '>':
            if expr > expr2:
                return True
        if self.relop == '>=':
            if expr >= expr2:
                return True
        if self.relop == '<>':
            if expr != expr2:
                return True
        if self.relop == '==':
            if expr == expr2:
                return True
        return None

class NodeAddOp(Node):
    """ generated source for class NodeAddOp """

    def __init__(self, op):
        """ generated source for method __init__ """
        super(NodeAddOp, self).__init__()
        self.op = op

    def eval(self, env):
        """ generated source for method eval """
        return self.op

class NodeMulOp(Node):
    """ generated source for class NodeMulOp """

    def __init__(self, op):
        """ generated source for method __init__ """
        super(NodeMulOp, self).__init__()
        self.op = op

    def eval(self, env):
        """ generated source for method eval """
        return self.op