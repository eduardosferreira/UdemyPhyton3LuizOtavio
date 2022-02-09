# -*- coding: utf-8 -*-
"""Classe Enumeracao Tipo de Cliente
"""
from enum import Enum


class TipoClienteExtendedEnum(Enum):
    """Extended Enum
    """
    @classmethod
    def fnc_dict(cls):
        return {x[0]: x[1] for x in list(map(lambda c: c.value, cls))}


class TipoCliente(TipoClienteExtendedEnum):
    """Tipos de clientes

    Args:
        Enum ([type]): Fisico/Juridico
    """
    Fisico: tuple = ('F', 'Pessoa Fisica')
    Jurico: tuple = ('J', 'Pessoa Juridica')
    Estrangeiro: tuple = ('EX', 'Estrangeiro')
