# -*- coding: utf-8 -*-
"""
------------------------------------------------------------------------------
MODULO ...: <<MODULO>>
SCRIPT ...: main.py
CRIACAO ..: DD/MM/YYYY
AUTOR ....: EDUARDO DA SILVA FERREIRA
            / KYROS TECNOLOGIA (eduardof@kyros.com.br)
DESCRICAO.: Este script .....
------------------------------------------------------------------------------

------------------------------------------------------------------------------
  HISTORICO :
  DD/MM/YYYY : EDUARDO DA SILVA FERREIRA
            / KYROS TECNOLOGIA
            (eduardof@kyros.com.br)
        - Criação do Script.
------------------------------------------------------------------------------
"""
import os
import sys
# sys.path.insert(0, '..')


def __fnc_load_path():
    """
        Funcao de carregamento path
    """
    os.gv_cc_sep_dir = ('/' if os.name == 'posix' else '\\')
    os.gv_cc_path = ""
    for index, dir in \
            enumerate(os.path.dirname(__file__).split(os.gv_cc_sep_dir)):
        if index == 0:
            os.gv_cc_path += dir+os.gv_cc_sep_dir
            continue
        if dir.upper().strip() == "SRC":
            sys.path.append(os.gv_cc_path)
            break
        os.gv_cc_path = os.path.join(os.gv_cc_path, dir)


# Carrega os dados principais
__fnc_load_path()


def main(*args, **kwargs):
    """
        Funcao principal
    """
    for arg in args:
        print(arg)
    print('sep_dir:', os.gv_cc_sep_dir)
    print('path:', os.gv_cc_path)
    print('file:', __file__)
    print('basename:', os.path.basename(__file__))
    print('dirname:', os.path.dirname(__file__))
    print('abspath:', os.path.abspath(__file__))
    print('abs dirname:', os.path.dirname(os.path.abspath(__file__)))


def fnc_teste_01(p_vl_x: float, p_vl_y: float) -> float:
    """
        Funcao de teste
    """
    return p_vl_x + p_vl_y


# Processa os dados quando acionado
if __name__ == '__main__':
    main(sys.argv)
