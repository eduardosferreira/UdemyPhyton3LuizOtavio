"""Exemplo do uso dataclasses
"""
import sys
from time import time
from datetime import datetime, timedelta
from traceback import format_exc


def df(p_dt_valor: datetime = datetime.now()) -> str:
    return p_dt_valor.strftime('%d/%m/%Y %H:%M:%S')


def fd(p_ds_data: str = df(), p_ds_formato: str = '%d/%m/%Y %H:%M:%S') -> datetime:
    return datetime.strptime(p_ds_data, p_ds_formato)


def fnc_decoradora_tempo_processamento(p_ob_funcao: object) -> object:
    """
    Função decoradora: Verifica o tempo que uma função leva para executar
    """
    def __fnc_dependente(*args, **kwargs):
        """ Função que envolve e executa outra função """
        # Tempo inicial000
        print(df(), "Inicio", p_ob_funcao.__name__, sep=' -> ')
        v_nr_start = time()
        # Executa a função
        v_ob_resultado = p_ob_funcao(*args, **kwargs)
        # Tempo final
        v_nr_end = time()
        # Resultado de tempo em ms
        v_nr_tempo = (v_nr_end - v_nr_start) * 1000
        print(df(), "Fim", p_ob_funcao.__name__, sep=' -> ')
        # Mostra o tempo
        print(f'\nA função ({p_ob_funcao.__name__}) levou {v_nr_tempo:.2f}ms para ser executada.')
        # Retorna a função original executada
        return v_ob_resultado
    # Retorna a função que __fnc_dependente
    return __fnc_dependente

@fnc_decoradora_tempo_processamento
def main(*args, **kwargs):
    """Main
    """
    try:
        for index, value in enumerate(args):
            print(df(), "(", index, ")", value)
        for index, value in kwargs:
            print(df(), "[", index, "]", value)

        print(df(),\
            fd(),\
            datetime.now().strftime('%a'),\
            (datetime.now()+(datetime.now()
            - (datetime.strptime('11/02/2022','%d/%m/%Y')\
            + timedelta(hours=20, minutes=2, microseconds=80)))),\
            fd().timestamp(),\
            datetime.fromtimestamp(fd().timestamp()),\
            (datetime.now()-(datetime.now()+timedelta(seconds=80))).total_seconds(),\
            sep='\n')
        
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
