# -*- coding: utf-8 -*-
"""Classe Enumeracao Tipo de Cliente
"""
from enum import Enum

class TipoContaExtendedEnum(Enum):
    """Extended Enum
    """    
    @classmethod
    def fnc_dict(cls):
        return {x[0]: {'descricao': x[1],
                    'tipo': x[2],
                    'vl_adicional': x[3],
                    'cliente_tipo': x[4]
                    }
        for x in list(map(lambda c: c.value, cls))}


    @classmethod
    def fnc_ds(cls, key: str) -> str:
        try:
            return cls.fnc_dict().get(key).get('descricao')
        except Exception:
            return ""
    

    @classmethod
    def fnc_tp(cls, key: str) -> str:
        try:
            return cls.fnc_dict().get(key).get('tipo')
        except Exception:
            return ""
    

    @classmethod
    def fnc_tp_cliente(cls, key: str) -> list:
        try:
            return cls.fnc_dict().get(key).get('cliente_tipo')
        except Exception:
            return []
    

    @classmethod
    def fnc_vl_add(cls, key: str) -> float:
        try:
            return float(cls.fnc_dict().get(key).get('vl_adicional'))
        except Exception:
            return 0.0
    

class TipoConta(TipoContaExtendedEnum):
    """Tipos de contas
    """    
    ContaCorrenteDePessoaFisica: tuple = ('001', 'Conta Corrente de Pessoa Física', 'CC', 1000.00, ['F','EX'])
    ContaSimplesDePessoaFisica: tuple = ('002', 'Conta Simples de Pessoa Física', 'CC', 1000.00, ['F','EX'])
    ContaCorrenteDePessoaJuridica: tuple = ('003', 'Conta Corrente de Pessoa Jurídica', 'CC', 1000.00, ['J'])
    EntidadesPublicas: tuple = ('006', 'Entidades Públicas', 'NA', 0, ['J'])
    DepositosInstituicoesFinanceiras: tuple = ('007', 'Depósitos Instituições Financeiras', 'NA', 0, ['J'])
    PoupancaDePessoaFisica: tuple = ('013', 'Poupança de Pessoa Física', 'CP', 0, ['F','EX'])
    PoupancaDePessoaJuridica: tuple = ('022', 'Poupança de Pessoa Jurídica', 'CP', 0, ['F','EX'])
    ContaCaixaFacil: tuple = ('023', 'Conta Caixa Fácil', 'NA', 0, ['F','EX'])
    PoupancaDeCreditoImobiliario: tuple = ('028', 'Poupança de Crédito Imobiliário', 'CP', 0, ['J','F','EX'])
    ContaInvestimentoPessoaFisica: tuple = ('032', 'Conta Investimento Pessoa Física', 'NA', 0, ['F','EX'])
    ContaInvestimentoPessoaJuridica: tuple = ('034', 'Conta Investimento Pessoa Jurídica', 'NA', 0, ['F','EX'])
    ContaSalario: tuple = ('037', 'Conta Salário', 'NA', 0, ['F','EX'])
    DepositosLotericos: tuple = ('043', 'Depósitos Lotéricos', 'NA', 0, ['F','EX','J'])
    PoupancaIntegrada: tuple = ('131', 'Poupança Integrada', 'NA', 0, ['F','EX','J'])
