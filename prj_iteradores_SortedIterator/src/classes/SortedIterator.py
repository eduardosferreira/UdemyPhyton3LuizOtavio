"""Classe
"""
import sys
from datetime import datetime
from traceback import format_exc
from typing import Any, List
from abc import ABC, abstractmethod, abstractproperty

class ClassAbstractIterator(ABC):
    nr_instance = 0

    @staticmethod
    def fnc_add_nr_instance(p_nr_add: int = 1):
        ClassAbstractIterator.nr_instance += p_nr_add
    
    @abstractmethod
    def __init__(self, *args, **kwargs):
        self.ob_list = []
        

    @property
    def ob_list(self):
        return self.__ob_list

    @ob_list.setter
    def ob_list(self, p_ob_list):
        self.__ob_list = p_ob_list
    
    def __iter__(self):
        return self

    def hashNext(self) -> bool:
        if self.ob_list is None or len(self.ob_list) == 0:
            return False
        return True

    def __next__(self):
        if not self.hashNext():
            self.ob_list = None
            raise StopIteration
        value = self.ob_list.pop()
        return value

    def next(self):
        return self.__next__()

    def __str__(self) -> str:
        cc_value = ""
        for value in self.ob_list:
            cc_value = str(value) + ("," if cc_value else "") + cc_value   
        return cc_value

    
class ClassIterator(ClassAbstractIterator):
    def __init__(self, *args, **kwargs):
        def __fnc_value(p_cc_value: Any, p_ob_list: List):
            if isinstance(p_cc_value, (list, tuple, set)):
                p_ob_list.extend((x for x in p_cc_value))
            else:
                p_ob_list.append(p_cc_value)
        super().__init__(*args, **kwargs)
        for value in args:
            __fnc_value(value, self.ob_list)
        for _, value in kwargs.items():
            __fnc_value(value, self.ob_list)


class ClassSortedIterator(ClassAbstractIterator):
    def __init__(self, *args, **kwargs):
        def __fnc_value(p_cc_values: Any, p_ob_lists: List):
            def __fnc_value_iterator(p_cc_value: Any, p_ob_list: List):
                if type(p_cc_value) is ClassIterator:
                    p_ob_list.extend((x for x in p_cc_value))
                else:
                    raise ValueError("ERR! Value not 'Iterator'")
            if isinstance(p_cc_values, (list, tuple, set)):
                for value in p_cc_values:
                    __fnc_value_iterator(value, p_ob_lists)
            else:
                __fnc_value_iterator(p_cc_values, p_ob_lists)
        super().__init__(*args, **kwargs)
        for value in args:
            __fnc_value(value, self.ob_list)
        for _, value in kwargs.items():
            __fnc_value(value, self.ob_list)
        self.ob_list.sort(reverse=True)


class Iterator():
    """Classe Iterator
    """
    def __init__(self, *args, **kwargs):
        def __fnc_value(p_cc_value: Any, p_ob_list: List):
            if isinstance(p_cc_value, (list, tuple, set)):
                p_ob_list.extend((x for x in p_cc_value))
            else:
                p_ob_list.append(p_cc_value)
        self.__ob_list = []
        for value in args:
            __fnc_value(value, self.__ob_list)
        for _, value in kwargs.items():
            __fnc_value(value, self.__ob_list)

    def __iter__(self):
        return self

    def hashNext(self) -> bool:
        if self.__ob_list is None or len(self.__ob_list) == 0:
            return False
        return True

    def __next__(self):
        if not self.hashNext():
            self.__ob_list = None
            raise StopIteration
        value = self.__ob_list.pop()
        return value

    def next(self):
        return self.__next__()

    def __str__(self) -> str:
        cc_value = ""
        for value in self.__ob_list:
            cc_value =  str(value) + ("," if cc_value else "") + cc_value   
        return cc_value

class SortedIterator():
    """Classe SortedIterator
    """

    def __init__(self, *args, **kwargs):
        def __fnc_value(p_cc_values: Any, p_ob_lists: List):
            def __fnc_value_iterator(p_cc_value: Any, p_ob_list: List):
                if type(p_cc_value) is Iterator:
                    p_ob_list.extend((x for x in p_cc_value))
                else:
                    raise ValueError("ERR! Value not 'Iterator'")
            if isinstance(p_cc_values, (list, tuple, set)):
                for value in p_cc_values:
                    __fnc_value_iterator(value, p_ob_lists)
            else:
                __fnc_value_iterator(p_cc_values, p_ob_lists)
        self.__ob_list = []
        for value in args:
            __fnc_value(value, self.__ob_list)
        for _, value in kwargs.items():
            __fnc_value(value, self.__ob_list)
        self.__ob_list.sort(reverse=True)

    def __iter__(self):
        return self

    def hashNext(self) -> bool:
        if self.__ob_list is None or len(self.__ob_list) == 0:
            return False
        return True

    def __next__(self):
        if not self.hashNext():
            self.__ob_list = None
            raise StopIteration
        value = self.__ob_list.pop()
        return value

    def next(self):
        return self.__next__()

    def __str__(self) -> str:
        cc_value = ""
        for value in self.__ob_list:
            cc_value =  str(value) + ("," if cc_value else "") + cc_value   
        return cc_value



def fnc_exemplo_01():
    pass
    test = ClassSortedIterator(ClassIterator([11, 6, 2]), ClassIterator([1, 4, 3,  11, 16, 12]))
    # test = ClassAbstractIterator()
    # test = ClassIterator({11, 6, 2}, p1=[1,2,3,4,5])
    # test = SortedIterator(Iterator([11, 6, 2]), Iterator([1, 4, 3,  11, 16, 12]))
    # test = Iterator({11, 6, 2}, p1=[1,2,3,4,5])
    print('\n')
    print(test.next(), test, sep= '>>')
    # for t in test:
    #     print(t, end=',')
    print('\n')    
    # print('new value', test.next())

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

        fnc_exemplo_01()

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
