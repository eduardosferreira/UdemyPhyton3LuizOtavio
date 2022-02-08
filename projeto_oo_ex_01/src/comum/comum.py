from functools import reduce
import re
from time import time
import datetime
import traceback
from random import randint
import string
import random


def fnc_data_hora_atual() -> str:
    """
    Retorna a data atual no formato texto dd/mm/yyyy hh:mm:ss
    """
    return str(datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S'))


def fnc_gera_codigo_aleatorio(p_nr_tamanho_codigo: int = 10) -> str:
    """
    Retorna codigo aleatorio
    """
    return ''.join(random.SystemRandom()
                   .choice(string.ascii_letters + string.digits)
                   for _ in range(p_nr_tamanho_codigo))


def fnc_apenas_numero(p_cc_dado: str) -> str:
    """
        Retorna somente numero do que foi repassado
    """
    try:
        return re.sub(r'[^0-9]', '', p_cc_dado)
    except Exception:
        return ""


def fnc_decoradora_tempo_processamento(p_ob_funcao: object) -> object:
    """
    Função decoradora: Verifica o tempo que uma função leva para executar
    """
    def __fnc_dependente(*args, **kwargs):
        """ Função que envolve e executa outra função """
        # Tempo inicial
        print(fnc_data_hora_atual(), "Inicio", sep=' -> ')
        v_nr_start = time()
        # Executa a função
        v_ob_resultado = p_ob_funcao(*args, **kwargs)
        # Tempo final
        v_nr_end = time()
        # Resultado de tempo em ms
        v_nr_tempo = (v_nr_end - v_nr_start) * 1000
        print(fnc_data_hora_atual(), "Fim", sep=' -> ')
        # Mostra o tempo
        print(f'\nA função levou {v_nr_tempo:.2f}ms para ser executada.')
        # Retorna a função original executada
        return v_ob_resultado
    # Retorna a função que __fnc_dependente
    return __fnc_dependente


def fnc_valide_cnpj(p_cc_dado: str) -> str:
    """
        Retorna a validação do cnpj,
        caso retorna algum dado, é a mensagem invalida,
        senão "" (vazio) é SUCESSO
    """
    try:
        v_cc_dado = fnc_apenas_numero(p_cc_dado).strip()
        if len(v_cc_dado) != 14:
            return f'ERRO: Validar o CNPJ {p_cc_dado}! Tamanho inválido ..'
        v_ob_sequencia = [str(x)*14 for x in range(0, 10)]
        if str(v_cc_dado) in v_ob_sequencia:
            return f'ERRO: Validar o CNPJ {p_cc_dado}! Sequencial é inválido ..'

        v_cc_dado_aux = str(v_cc_dado)[:-2]
        v_ob_sequencia = [str(x)*12 for x in range(0, 10)]
        if str(v_cc_dado_aux) in v_ob_sequencia:
            return f'ERRO: Validar o CNPJ {v_cc_dado_aux}! Sequencial é inválido ..'

        v_nr_digito = ''
        for i in range(0, 2):
            v_ob_lista_1 = []
            v_ob_lista_1.extend([str(x) for x in range(5+i, 1, -1)])
            v_ob_lista_1.extend([str(x) for x in range(9, 1, -1)])
            v_nr_digito = int(11) - int((reduce(lambda x, y: x+y, [int(x[0])*int(x[1])
                                                                   for x in list(zip(str(v_cc_dado_aux), v_ob_lista_1))])) % 11)
            v_nr_digito = 0 if v_nr_digito > 9 else v_nr_digito
            v_cc_dado_aux += str(v_nr_digito)
            # print(v_cc_dado_aux)
        if v_cc_dado_aux.strip() == v_cc_dado.strip():
            return ""
        else:
            return f'ERRO: Validar o CNPJ {v_cc_dado}!\
                 cálculo inválido ..{v_cc_dado_aux}'
    except Exception as err:
        v_ds_err_desc_trace = traceback.format_exc()
        return f'ERRO: Validar o CNPJ {p_cc_dado}!\
         {err} - {v_ds_err_desc_trace}'


def fnc_formata_cnpj(p_cc_dado: str) -> str:
    """
        Retorna a formatacao do cnpj,
    """
    try:
        return '{}.{}.{}/{}-{}'.format(p_cc_dado[:2], p_cc_dado[2:5], p_cc_dado[5:8], p_cc_dado[-6:-2], p_cc_dado[-2:])
    except Exception:
        return p_cc_dado


def fnc_gere_cnpj_valido(p_qt_elementos: int = 1) -> list:
    """
        Retorna um cnpj valido
    """
    v_ob_lista = list()
    while len(v_ob_lista) < p_qt_elementos:
        v_cc_cnpj = ''.join([str(randint(0, 9)) for _ in range(0, 14)])
        if not fnc_valide_cnpj(v_cc_cnpj):
            v_ob_lista.append(v_cc_cnpj)
    return list(map(fnc_formata_cnpj, v_ob_lista))


if __name__ == '__main__':
    print(fnc_data_hora_atual())
