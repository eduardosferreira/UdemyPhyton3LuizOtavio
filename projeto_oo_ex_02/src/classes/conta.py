# -*- coding: utf-8 -*-
"""Classe abstrata para Conta
"""
from abc import ABC, abstractmethod
from random import randint
from re import sub
##############################################################
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
#Carrega os dados principais
__fnc_load_path()
##############################################################
from src.classes.enumeracao.tipo_conta import TipoConta
from src.classes.excecao.tratamento_exception import TratamentoException


class Conta(ABC):
    """Classe Abstrata de Conta

    Args:
        ABC ([type]): [description]
    """
    __NR_INSTANCIA = 0
    __VL_MIN_SALDO = 0

    @staticmethod
    def _fnc_gera_conta_aleatorio(p_nr_tamanho: int = 10) -> str:
        """Gera conta aleatorio[0-9] para controle

        Args:
            p_nr_tamanho (int, optional): Tamanho desejado. Defaults to 10.

        Returns:
            str: Retorna conta aletorio
        """
        return ''.join(str(randint(0, 9)) for _ in range(p_nr_tamanho))

    def __fnc_gera_conta(self) -> str:
        """Gera conta proprio para controle da classe
        Returns:
            str: Retorna conta
        """
        return Conta._fnc_gera_conta_aleatorio()


    def __new__(cls, *args, **kwargs):
        """Controle de instancia antes do acionamento

        Args:
            cls (type[Self]): A propria classe

        Returns:
            Self: A propria classe a ser instanciada
        """
        Conta.__NR_INSTANCIA += 1
        # SINGLETON
        # if not hasattr(cls, '__fl_ja_existe'):
        cls.__fl_ja_existe = super().__new__(cls)
        return cls.__fl_ja_existe

    def __init__(self,
                 p_cc_agencia: str,
                 p_tp_conta: str = '037',
                 p_cc_conta: str = None,
                 p_vl_saldo: float = 0) -> None:
        """Inicializacao da classe

        Args:
            p_cc_agencia ([str(0-9)]): campo agencia
            p_tp_conta ([str(0-9)]): campo tipo de conta. Defaults to 037.
            p_cc_conta ([str(0-9)]): campo conta. Defaults to None.
            p_vl_saldo ([float]): campo saldo. Defaults to 0.
        """
        super().__init__()
        self.__cc_agencia = p_cc_agencia
        self.__tp_conta = p_tp_conta
        if not p_cc_conta:
            self.__cc_conta = self.__fnc_gera_conta()
        else:
            self.__cc_conta = p_cc_conta
        self.vl_saldo = p_vl_saldo

    @property
    def tipo_conta(self) -> str:
        """Retorna o valor do atributo do campo "tipo conta"

        Returns:
            str: campo tipo de conta
        """
        return self.__tp_conta

    @property
    def conta(self) -> str:
        """Retorna o valor do atributo do campo "conta"

        Returns:
            str: campo conta
        """
        return self.__cc_conta

    @property
    def agencia(self) -> str:
        """Retorna o valor do atributo do campo "agencia"

        Returns:
            str: campo agencia
        """
        return self.__cc_agencia

    @property
    def vl_saldo(self) -> float:
        """Retorna o valor do atributo do campo "saldo"

        Returns:
            str: campo saldo
        """
        return self.__vl_saldo

    @vl_saldo.setter
    def vl_saldo(self, p_vl_saldo: float):
        """Atribui valor para o campo "saldo"

        Args:
            p_vl_saldo (float): valor do atributo "saldo"

        """
        if not isinstance(p_vl_saldo, (int, float))\
                or p_vl_saldo < 0:
            raise TratamentoException(
                f"ERRO: Informar dados correto [saldo]:{p_vl_saldo}")
        self.__vl_saldo = p_vl_saldo

    def __setattr__(self, __name: str, __value) -> None:
        """Valida os dados atribuidos

        Args:
            __name (str): agencia do atributo
            __value (Any): Descricao do atributo

        Returns:
            [type]: Dados atributos
        """
        # print(f'[{__name}] = {__value}')
        if __name.endswith('__cc_agencia')\
                or __name.endswith('__cc_conta')\
                or __name.endswith('__tp_conta'):
            if not isinstance(__value, str)\
                    or not str(__value).strip()\
                    or not sub(r'[^0-9]', '', str(__value).strip())\
                    or not str(__value).strip().isnumeric:
                raise TratamentoException(
                    f"ERRO: Informar dados correto [{__name}]:{__value}")
        elif __name.endswith('__tp_conta')\
                and not TipoConta.fnc_dict().get(str(__value).strip(), ""):
            raise TratamentoException(
                f"ERRO: Informar dados correto [{__name}]:{__value}")
        elif __name.endswith('__vl_saldo'):
            if not isinstance(__value, (int, float)) or __value < 0:
                raise TratamentoException(
                    f"ERRO: Informar dados correto [{__name}]:{__value}")
        elif __name.endswith('__VL_MIN_SALDO'):
            raise TratamentoException(
                f"ERRO: Não pode alterar dados [{__name}]:{__value}")

        self.__dict__[__name] = __value

        return super().__setattr__(__name, __value)

    @abstractmethod
    def mostrar(self):
        pass

    def fnc_sacar(self, p_vl_saque):
        """Realiza a adicao do saldo

        Args:
            p_vl_saque (float, optional): [valor do deposito]. Defaults to 0.

        Raises:
            TratamentoException: dados incorreto para deposito
        """
        if not isinstance(self.vl_saldo, (int, float)):
            raise TratamentoException(
                f"ERRO: Informar dados correto [saldo]:{self.vl_saldo}")
        
        if not isinstance(p_vl_saque, (int, float))\
                or p_vl_saque < 0:
            raise TratamentoException(
                f"ERRO: Informar dados correto [saque]:{p_vl_saque}")
        elif p_vl_saque > self.vl_saldo:
            raise TratamentoException(
                f"FALHA: Valor [{self.vl_saldo}] insuficiente [saque]:{p_vl_saque}")
        # print(':old.saque',self.vl_saldo, p_vl_saque)
        self.vl_saldo -= p_vl_saque
        # print(':new.saque',self.vl_saldo, p_vl_saque)

    def fnc_deposito(self, p_vl_deposito: float = 0):
        """Realiza a adicao do saldo

        Args:
            p_vl_deposito (float, optional): [valor do deposito]. Defaults to 0.

        Raises:
            TratamentoException: dados incorreto para deposito
        """
        if not isinstance(self.vl_saldo, (int, float)):
            raise TratamentoException(
                f"ERRO: Informar dados correto [saldo]:{self.vl_saldo}")
        
        if not isinstance(p_vl_deposito, (int, float))\
                or p_vl_deposito < 0:
            raise TratamentoException(
                f"ERRO: Informar dados correto [deposito]:{p_vl_deposito}")
        # print(':old.deposito', self.vl_saldo, p_vl_deposito)
        self.vl_saldo += p_vl_deposito
        # print(':new.deposito', self.vl_saldo, p_vl_deposito)
        
    def __str__(self) -> str:
        """Retorna a descricao e atributos da classe
        """
        return "(\n  Agencia = '{}'\n, Tipo de Conta = '{}'\n, Conta = '{}'\n, Saldo = {}\n)".\
            format(self.agencia, TipoConta.fnc_dict().get(self.tipo_conta, ""), self.conta, self.vl_saldo)

    def __repr__(self) -> str:
        """Retorna a descricao e atributos da classe
        """
        v_ds_classe = type(self).__name__
        return "{}('{}', '{}', '{}', {})".format(v_ds_classe, 
                                        self.agencia,
                                        self.tipo_conta,
                                        self.conta, 
                                        self.vl_saldo)

    def __del__(self):
        """Deleta / Apaga os objetos da classe
        """
        Conta.__NR_INSTANCIA -= 1

    def __eq__(self, __o) -> bool:
        """compara conta e o agencia

        Args:
            __o (object): Classe comparadora

        Returns:
            bool: True/False
        """
        try:
            if self.conta == __o.conta and self.agencia == __o.agencia:
                return True
            else:
                return False
        except Exception:
            return False


class ContaCorrenteDePessoaFisica(Conta):
    def __init__(self, p_cc_agencia: str, p_cc_conta: str = None, p_vl_saldo: float = 0) -> None:
        super().__init__(p_cc_agencia, '001', p_cc_conta, p_vl_saldo)
    
    def mostrar(self):
        pass


class ContaCorrenteDePessoaJuridica(Conta):
    def __init__(self, p_cc_agencia: str, p_cc_conta: str = None, p_vl_saldo: float = 0) -> None:
        super().__init__(p_cc_agencia, '003', p_cc_conta, p_vl_saldo)
    
    def mostrar(self):
        pass


class PoupancaDePessoaFisica(Conta):
    def __init__(self, p_cc_agencia: str, p_cc_conta: str = None, p_vl_saldo: float = 0) -> None:
        super().__init__(p_cc_agencia, '013', p_cc_conta, p_vl_saldo)
    
    def mostrar(self):
        pass

class PoupancaDePessoaJuridica(Conta):
    def __init__(self, p_cc_agencia: str, p_cc_conta: str = None, p_vl_saldo: float = 0) -> None:
        super().__init__(p_cc_agencia, '022', p_cc_conta, p_vl_saldo)

    def mostrar(self):
        pass
        

def main(*args, **kwargs):
    """Acionamento da funcao principal
    """
    pass
    c2 = ContaCorrenteDePessoaJuridica("071")
    c2.fnc_deposito(100)
    c2.fnc_deposito(13.21)
    c2.fnc_deposito(11.11)
    c2.fnc_deposito(12.11)
    c2.fnc_sacar(100)
    print(c2,'\n' ,repr(c2))
    
    pass


if __name__ == '__main__':
    main(sys.argv)
