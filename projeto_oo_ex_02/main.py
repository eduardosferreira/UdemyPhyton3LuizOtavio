# -*- coding: utf-8 -*-
"""Script para para validação
"""
##############################################################
import sys
#import os
#def __fnc_load_path():
#    """
#        Funcao de carregamento path
#    """
#    os.gv_cc_sep_dir = ('/' if os.name == 'posix' else '\\')
#    os.gv_cc_path = ""
#    for index, dir in \
#            enumerate(os.path.dirname(__file__).split(os.gv_cc_sep_dir)):
#        if index == 0:
#            os.gv_cc_path += dir+os.gv_cc_sep_dir
#            continue
#        if dir.upper().strip() == "SRC":
#            sys.path.append(os.gv_cc_path)
#            break
#        os.gv_cc_path = os.path.join(os.gv_cc_path, dir)
#        #print(os.gv_cc_path)
#            
## Carrega os dados principais
#__fnc_load_path()
##############################################################
from src.classes.cliente import Cliente


def main(*args, **kwargs):
    """Acionamento da funcao principal
    """
    x = Cliente("Teste", "J", '1')
    y = Cliente("Teste", "J", '2')
    # x.tipo_cliente="x"
    print(x)
    print(y)
    print(y==x)
    c2 = eval(repr(x))
    print(c2)
    
    # print(help(tratamento_exception))
    pass


if __name__ == '__main__':
    main(sys.argv)
