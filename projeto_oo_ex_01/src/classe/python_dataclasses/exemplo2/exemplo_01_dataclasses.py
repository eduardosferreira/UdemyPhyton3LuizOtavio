# -*- coding: utf-8 -*-
"""Exemplo do uso dataclasses
"""
from pickle import TRUE
import sys
from traceback import format_exc
from inspect import getmembers
from dataclasses import asdict, dataclass, field, astuple, asdict
from datetime import datetime


def df():
    return datetime.now().strftime('%d/%m/%Y %H:%M:%S')


@dataclass(frozen=True)
class Frozen:
    """nao pode alterar"""
    valor: int = 9
    

@dataclass(order=True)
class Person:
    first_name: str
    last_name: str
    full_name: str = field(init=False, repr=True)
    sort_index: int = field(init=False, repr=False)

    def __post_init__(self):
        if not isinstance(self.first_name, str):
            raise ValueError("Invalid! [first_name] " + str(self.first_name))
        if not isinstance(self.last_name, str):
            raise ValueError("Invalid! [last_name] " + str(self.last_name))
        self.sort_index = (self.last_name if self.last_name else "")\
            + ", " + (self.first_name if self.first_name else "")
        self.full_name = (self.first_name if self.first_name else "")\
            + " " + (self.last_name if self.last_name else "")


def main(*args, **kwargs):
    """Main
    """
    try:
        for index, value in enumerate(args):
            print(df(), "(", index, ")", value)
        for index, value in kwargs:
            print(df(), "[", index, "]", value)
    
    
        # print(df(), getmembers(Frozen))
        # print(df(), getmembers(Person))
        print(df())
        person_01 = Person("Eduardo", "A")
        person_02 = Person("Eduardo", "B")
        person_03 = Person("Eduardo", "A")
        print(df(), person_01, person_02,\
            'A=B: '+str("OK" if person_01 == person_02 else "NOK"),\
            'A=A: '+str("OK" if person_01 == person_03 else "NOK"),\
            sorted([person_01, person_02, person_03], key=lambda p: p.sort_index, reverse=True),\
            '')
        z = Frozen()
        print(df(), z)
        print(df(), astuple(person_01), asdict(person_01))
        
        # z.date_time_active = df()
        return 0
    except (ValueError) as err:
        print(df(), '<<ERR>>', 'STOP', 'ValueError',  err)
        return 1
    except (Exception) as err:
        ds_err_trace = format_exc()
        print(df(), '<<ERR>>', 'STOP', ds_err_trace, err)
        return 1


if __name__ == "__main__":
    print(df(), "Start")
    v_nr_ret = main(sys.argv)
    if not isinstance(v_nr_ret, int):
        v_nr_ret = 1
    print(df(), "End")
    sys.exit(v_nr_ret)
