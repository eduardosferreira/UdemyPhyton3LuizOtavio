import os
import sys


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
    # print('sep_dir:', os.gv_cc_sep_dir)
    # print('path:', os.gv_cc_path)
    # print('file:', __file__)
    # print('basename:', os.path.basename(__file__))
    # print('dirname:', os.path.dirname(__file__))
    # print('abspath:', os.path.abspath(__file__))
    # print('abs dirname:', os.path.dirname(os.path.abspath(__file__)))
    # print(os.path.realpath('.'))


# Carrega os dados principais
__fnc_load_path()


from src.comum.comum\
    import fnc_data_hora_atual\
    , fnc_decoradora_tempo_processamento\
    , fnc_gere_cnpf_valido


@fnc_decoradora_tempo_processamento
def main(*args, **kwargs):
    """
        Funcao principal
    """
    pass
    # print(sys.path)
    print(fnc_data_hora_atual(),\
         fnc_gere_cnpf_valido(10), sep=' -> ')
    # for arg in args:
    #    print(arg)


# Processa os dados quando acionado
if __name__ == '__main__':
    main(sys.argv)
