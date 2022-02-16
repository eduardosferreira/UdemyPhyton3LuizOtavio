import sys
from datetime import datetime
from traceback import format_exc

class Node:
    def __init__(self, val):
        self.left = None
        self.right = None
        self.val = val

class Tree:
    def __init__(self):
        self.parent = None

    def getRoot(self):
        return self.parent

    def add(self, val):
        if self.parent is None:
            self.parent = Node(val)
        else:
            self._add(val, self.parent)

    def _add(self, val, node):
        if val < node.val:
            if node.left is not None:
                self._add(val, node.left)
            else:
                node.left = Node(val)
        else:
            if node.right is not None:
                self._add(val, node.right)
            else:
                node.right = Node(val)

    def find(self, val):
        if self.parent is not None:
            return self._find(val, self.parent)
        else:
            return None

    def _find(self, val, node):
        if val == node.val:
            return node
        elif (val < node.val and node.left is not None):
            return self._find(val, node.left)
        elif (val > node.val and node.right is not None):
            return self._find(val, node.right)

    def deleteTree(self):
        # garbage collector will do this for us. 
        self.parent = None

    def printTree(self):
        if self.parent is not None:
            self._printTree(self.parent)

    def _printTree(self, node):
        if node is not None:
            self._printTree(node.left)
            print(str(node.val) + ' ')
            self._printTree(node.right)


class Ex2TreeNode:
    def __init__(self, value):
        self.left = None
        self.right = None
        self.val = value

class Ex2Tree:
    def __init__(self):
        self.parent = None

    def addNode(self, node, value):
        if(node is None):
            self.parent = Ex2TreeNode(value)
        else:
            if(value < node.val):
                if(node.left is None):
                    node.left = Ex2TreeNode(value)
                else:
                    self.addNode(node.left, value)
            else:
                if(node.right is None):
                    node.right = Ex2TreeNode(value)
                else:
                    self.addNode(node.right, value)

    def printInorder(self, node):
        if(node is not None):
            self.printInorder(node.left)
            print(node.val)
            self.printInorder(node.right)

class Ex1Tree:
    def __init__(self, val=None):
        if val is not None:
            self.val = val
        else:
            self.val = None

        self.left = None
        self.right = None

    def insert(self, val):
        if not self.val:
            self.val = val
            return
        if val < self.val:
            if self.left is None:
                self.left = Ex1Tree(val)
                return
            self.left.insert(val)
            return
        if val > self.val:
            if self.right is None:
                self.right = Ex1Tree(val)
                return
            self.right.insert(val)
            return

    def printValues(self):
        if self.left:
            self.left.printValues()

        print(self.val)

        if self.right:
            self.right.printValues()

def fnc_exemplo_01():
    pass
    print('\n')
    # print('new value', test.next())
    tree = Ex1Tree(20)
    tree.left = Ex1Tree(18)
    tree.right = Ex1Tree(22)
    tree.insert(19)
    tree.insert(24)
    tree.insert(5)
    tree.insert(21)

    tree.printValues()


def fnc_exemplo():
    print('\n')
    #     3
    # 0     4
    #   2      8
    tree = Tree()
    tree.add(3)
    tree.add(4)
    tree.add(0)
    tree.add(8)
    tree.add(2)
    tree.printTree()
    print(tree.find(3).val)
    print(tree.find(10))
    tree.deleteTree()
    print('\n')
    

def df(p_dt_valor: datetime = datetime.now()) -> str:
    return p_dt_valor.strftime('%d/%m/%Y %H:%M:%S')


def main(*args, **kwargs) -> int:
    v_nr_ret = 0
    try:
        for idx, parametros_entrada in enumerate(args):
            print(df(), "(", idx, ")", parametros_entrada)
            for index, valor in enumerate(parametros_entrada):
                print(df(), "(", idx, ".", index, ")", valor)
        for index, valor in kwargs:
            print(df(), "[", index, "]", valor)

        fnc_exemplo()

    except Exception as ds_err:
        v_nr_ret = 1
        ds_err_trace = format_exc()
        print(df(), '<<ERR>>', 'STOP', ds_err_trace, ds_err)

    return v_nr_ret


if __name__ == "__main__":
    """Procedimentos a serem acionados
    """
    print(df(), "Start")
    v_nr_ret = main(sys.argv[1:])
    if not isinstance(v_nr_ret, int):
        v_nr_ret = 1
    print(df(), "End", v_nr_ret)
    sys.exit(v_nr_ret)
