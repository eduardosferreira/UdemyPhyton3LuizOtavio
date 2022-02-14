# -*- coding: utf-8 -*-
"""Classe abstrata para Pessoa
"""
from abc import ABC, abstractmethod
from string import ascii_letters, digits
from random import SystemRandom
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


# Carrega os dados principais
__fnc_load_path()
##############################################################
from src.classes.excecao.tratamento_exception import TratamentoException
from src.classes.enumeracao.tipo_cliente import TipoCliente


class Pessoa(ABC):
    """Classe Abstrata de Pessoa

    Args:
        ABC ([type]): [description]
    """
    __NR_INSTANCIA = 0

    @staticmethod
    def _fnc_gera_codigo_aleatorio(p_nr_tamanho_codigo: int = 10) -> str:
        """Gera codigo aleatorio[a-z|A-Z] para controle

        Args:
            p_nr_tamanho_codigo (int, optional): Tamanho desejado. Defaults to 10.

        Returns:
            str: Retorna codigo aletorio
        """
        return ''.join(SystemRandom().choice(ascii_letters + digits)
                       for _ in range(p_nr_tamanho_codigo))

    def __fnc_gera_codigo(self) -> str:
        """Gera codigo proprio para controle da classe
        Returns:
            str: Retorna codigo juntamente com nome da classe
        """
        return str(self.__class__.__name__) + "_" + Pessoa._fnc_gera_codigo_aleatorio()

    def __new__(cls, *args, **kwargs):
        """Controle de instancia antes do acionamento

        Args:
            cls (type[Self]): A propria classe

        Returns:
            Self: A propria classe a ser instanciada
        """
        Pessoa.__NR_INSTANCIA += 1
        # SINGLETON
        # if not hasattr(cls, '_fl_ja_existe'):
        cls._fl_ja_existe = super().__new__(cls)
        return cls._fl_ja_existe

    def __init__(self, p_nm_nome: str, p_cd_codigo: str = None) -> None:
        """Inicializacao da classe

        Args:
            p_nm_nome ([str]): campo nome
            p_cd_codigo ([str]): campo codigo. Defaults to None.
        """
        super().__init__()
        self.nome = p_nm_nome
        if not p_cd_codigo:
            self.__cd_codigo = self.__fnc_gera_codigo()
        else:
            self.__cd_codigo = p_cd_codigo

    @classmethod
    def __init_nome_codigo__(cls, p_nm_nome: str, p_cd_codigo: str):
        """Inicializacao da classe, repassando nome e codigo

        Args:
            p_nm_nome ([str]): campo nome
            p_cd_codigo ([str]): campo codigo
        """
        if not isinstance(p_cd_codigo, str) or not str(p_cd_codigo).strip():
            raise TratamentoException(
                f"ERRO: Informar dados correto [codigo]:{p_cd_codigo}")
        return cls(p_nm_nome, p_cd_codigo)

    @property
    def codigo(self) -> str:
        """Retorna o valor do atributo do campo "codigo"

        Returns:
            str: campo codigo
        """
        return self.__cd_codigo

    @property
    def nome(self) -> str:
        """Retorna o valor do atributo do campo "nome"

        Returns:
            str: campo nome
        """
        return self.__nm_nome

    @nome.setter
    def nome(self, p_nm_nome: str):
        """Atribui valor para o campo "nome"

        Args:
            p_nm_nome (str): valor do atributo "nome"

        """
        if not isinstance(p_nm_nome, str) or not str(p_nm_nome).strip():
            raise TratamentoException(
                f"ERRO: Informar dados correto [nome]:{p_nm_nome}")
        self.__nm_nome = p_nm_nome

    def __setattr__(self, __name: str, __value) -> None:
        """Valida os dados atribuidos

        Args:
            __name (str): Nome do atributo
            __value (Any): Descricao do atributo

        Returns:
            [type]: Dados atributos
        """
        # print(f'[{__name}] = {__value}')
        if __name.endswith('__nm_nome')\
                or __name.endswith('__cd_codigo'):
            if not isinstance(__value, str) or not str(__value).strip():
                raise TratamentoException(
                    f"ERRO: Informar dados correto [{__name}]:{__value}")
        self.__dict__[__name] = __value

        return super().__setattr__(__name, __value)

    @abstractmethod
    def fnc_acao(*args, **kwargs):
        """Funcao abstrata a ser construido para quem herdar
        """
        pass

    # mesma coisa, __str__ caso nao exista, porem nao precisa retorna uma atring
    def __repr__(self) -> str:
        """Retorna a descricao e atributos da classe
        """
        v_ds_nome_da_classe = type(self).__name__
        return "{}('{}', '{}')".format(v_ds_nome_da_classe, self.nome, self.codigo)

    def __del__(self):
        """Deleta / Apaga os objetos da classe
        """
        Pessoa.__NR_INSTANCIA -= 1

    def __eq__(self, __o) -> bool:
        """compara codigo e o nome

        Args:
            __o (object): Classe comparadora

        Returns:
            bool: True/False
        """
        try:
            if self.codigo == __o.codigo and self.nome == __o.nome:
                return True
            else:
                return False
        except Exception:
            return False


class Cliente(Pessoa):
    """Classe Cliente

    Args:
        Pessoa ([type]): Herda da Classe Pessoa
    """
    def fnc_acao(*args, **kwargs):
        """Funcao herdada
        """
        pass

    def __init__(self,
                 p_nm_nome: str,
                 p_tp_cliente: str = 'F',
                 p_cd_codigo: str = None) -> None:
        """Construtora da classe cliente

        Args:
            p_nm_nome (str): campo nome
            p_tp_cliente (str, optional): campo tipo.
                                    Defaults to 'F'.
            p_cd_codigo (str, optional): campo codigo.
                Defaults to None.Gerado automaticamente
        """
        super().__init__(p_nm_nome, p_cd_codigo)
        self.tipo_cliente = p_tp_cliente

    @property
    def tipo_cliente(self) -> str:
        """Retorna o valor do atributo do campo "tipo_cliente"

        Returns:
            str: campo tipo de cliente
        """
        return self.__tp_cliente

    @tipo_cliente.setter
    def tipo_cliente(self, p_tp_cliente: str = 'F'):
        if not isinstance(p_tp_cliente, str)\
                or not str(p_tp_cliente).strip()\
                or not TipoCliente.fnc_dict().get(str(p_tp_cliente).strip(), ""):
            raise TratamentoException(
                f"ERRO: Informar dados correto [tipo_cliente]:{p_tp_cliente}")

        self.__tp_cliente = p_tp_cliente

    def __setattr__(self, __name: str, __value) -> None:
        """Valida os dados atribuidos

        Args:
            __name (str): Nome do atributo
            __value (Any): Descricao do atributo

        Returns:
            [type]: Dados atributos
        """
        super().__setattr__(__name, __value)
        # print(f'[{__name}] = {__value}')
        if __name.endswith('__tp_cliente'):
            if not isinstance(__value, str) or not str(__value).strip()\
                    or not TipoCliente.fnc_dict().get(str(__value).strip(), ""):
                raise TratamentoException(
                    f"ERRO: Informar dados correto [{__name}]:{__value}")
        self.__dict__[__name] = __value

        return super().__setattr__(__name, __value)

    def __repr__(self) -> str:
        """Retorna a descricao e atributos da classe
        """
        v_ds_nome_da_classe = type(self).__name__
        return "{}('{}', '{}', '{}')".format(v_ds_nome_da_classe, self.nome, self.tipo_cliente, self.codigo)

    def __eq__(self, __o) -> bool:
        """compara codigo, nome e o tipo do cliente

        Args:
            __o (object): Classe comparadora

        Returns:
            bool: True/False
        """
        try:
            if not super().__eq__(__o):
                return False
            if self.tipo_cliente == __o.tipo_cliente:
                return True
            else:
                return False
        except Exception:
            return False


def main(*args, **kwargs):
    """Acionamento da funcao principal
    """
    pass
    # x = Cliente("Teste", "J", '1')
    # y = Cliente("Teste", "J", '2')
    # # x.tipo_cliente="x"
    #print(x)
    # print(y)
    # print(y==x)
    # c2 = eval(repr(x))
    # print(c2)

    # print(help(tratamento_exception))
    pass


if __name__ == '__main__':
    main(sys.argv)