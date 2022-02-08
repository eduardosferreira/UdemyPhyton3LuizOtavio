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

from src.comum.comum import fnc_data_hora_atual, fnc_gera_codigo_aleatorio


"""
public, private, protected - nao existe em python, apenas em convensao por anotacoes
_ (um unico underline na frente), apenas informativo que é privado, porem pode acessar
__ (dois underlines na frente), tb informado, mas para acessa-lo, 
    deverá colocar _NOMEDACLASSE__NOMEATRITO
"""


class Pessoa:

    NR_INSTANCIA = 0

    def pessoa(self) -> str:
        return f'Pessoa....{self.nm_pessoa}'

    @staticmethod
    def __fnc_gera_codigo() -> str:
        return fnc_gera_codigo_aleatorio(10)

    @staticmethod
    def __mensagem__() -> str:
        return f'Olá...'

    @staticmethod
    def fnc_busca_instancia() -> int:
        Pessoa.NR_INSTANCIA += 1
        return Pessoa.NR_INSTANCIA

    def __init__(self, p_nm_pessoa: str, p_ds_usuario: str = None):
        """
        Construtora
        """
        self.__nm_pessoa = p_nm_pessoa
        # PRIVADOS
        self.__cd_codigo = str(self.__class__.__name__) + "_" + Pessoa.__fnc_gera_codigo()
        self.__dt_criacao = fnc_data_hora_atual()
        # PRIVADOS, mas apenas informativo
        if isinstance(p_ds_usuario, str) and str(p_ds_usuario).strip:
            self._ds_usuario = p_ds_usuario
        else:
            self._ds_usuario = None
        self.__nm_classe__ = self.__class__.__name__
        self.NR_INSTANCIA += 1
        Pessoa.NR_INSTANCIA += 1

    # Funcoes para Get
    @property
    def cd_codigo(self) -> str:
        """
        Retorna codigo
        """
        return self.__cd_codigo

    @property
    def nm_pessoa(self) -> str:
        """
        Retorna nome da pessoa
        """
        return self.__nm_pessoa

    @property
    def dt_criacao(self) -> str:
        """
        Retorna data de criacao
        """
        return self.__dt_criacao

    @property
    def usuario(self) -> str:
        """
        Retorna ano base
        """
        return self._ds_usuario

    # Funcoes Setter, para atribuição
    @nm_pessoa.setter
    def nm_pessoa(self, p_nm_pessoa: str):
        if not isinstance(p_nm_pessoa, str) or not str(p_nm_pessoa).strip:
            raise ValueError("Nome obrigatório")
        self.__nm_pessoa = p_nm_pessoa

    @usuario.setter
    def usuario(self, p_ds_usuario: str):
        if not isinstance(p_ds_usuario, str) or not str(p_ds_usuario).strip:
            raise ValueError("Nome do usuário obrigatório")
        self._ds_usuario = p_ds_usuario

    @classmethod
    def fnc_sobrecarga_construtora_por_usuario(cls, p_nm_pessoa: str, p_ds_usuario: str):
        """
        Sobre carga da construtora da classe para utilizar outros atributos
        """
        return cls(p_nm_pessoa, p_ds_usuario)

#Herança Simples
class Cliente(Pessoa):

    def pessoa(self) -> str:
        return f'Cliente....{self.nm_pessoa}'
    
    def __init__(self, p_nm_pessoa: str, p_tp_cliente: str = 'F'):
        super().__init__(p_nm_pessoa)
        self.tipo = p_tp_cliente

    @property
    def tipo(self) -> int:
        return self.__tp_cliente

    @tipo.setter
    def tipo(self, p_tp_cliente: str):
        self.__tp_cliente = p_tp_cliente

#Herança Multipla
class ClienteJuridico(Cliente):
    def __init__(self, p_nm_pessoa: str, p_cc_cnpj: str):
        super().__init__(p_nm_pessoa)
        self.tipo = "J"
        self.__cc_cnpj = p_cc_cnpj

    @property
    def cnpj(self) -> str:
        return self.__cc_cnpj


if __name__ == '__main__':
    print()

    p0 = Pessoa("Eduardo")
    p0.usuario = "ferreedu"

    p2 = Pessoa.fnc_sobrecarga_construtora_por_usuario("Eduardo", "user")
    p2._ds_usuario = ""
    p2.usuario = "teste"
    p2.__nm_pessoa = "p_nm_pess"
    p2.__cd_codigo = 0

    #p2.__codigo__ = 1
    print(fnc_data_hora_atual(), p2.cd_codigo, p2.nm_pessoa, 'codigo', p2.__cd_codigo, p2._Pessoa__cd_codigo, p2.__nm_classe__, 'pessoa',
          p2.__nm_pessoa, p2._Pessoa__nm_pessoa, p2._ds_usuario, p2.usuario, Pessoa.NR_INSTANCIA, p2.NR_INSTANCIA, p2.dt_criacao, sep=" -> ")
    print()
    p0.nm_pessoa = "tEste"
    x = p0.fnc_busca_instancia()
    print(fnc_data_hora_atual(), p0.cd_codigo, p0.nm_pessoa,
          p0._ds_usuario, p0.dt_criacao, Pessoa.NR_INSTANCIA, p0.NR_INSTANCIA)

    a0 = Cliente("Kyros")
    a0.tipo = 'J'
    print(fnc_data_hora_atual(),a0.pessoa(), a0.cd_codigo, a0.nm_pessoa,a0.dt_criacao,a0.tipo,a0.__nm_classe__)
