# -*- coding: utf-8 -*-
"""Classe Enumeracao Tipo de Cliente
"""
from enum import Enum

class TipoContaExtendedEnum(Enum):
    """Extended Enum
    """    
    @classmethod
    def fnc_dict(cls):
        return {x[0]:x[1] for x in list(map(lambda c: c.value, cls))}

class TipoConta(TipoContaExtendedEnum):
    """Tipos de contas
    """    
    ContaCorrenteDePessoaFisica: tuple = ('001', 'Conta Corrente de Pessoa Física')
    ContaSimplesDePessoaFisica: tuple = ('002', 'Conta Simples de Pessoa Física')
    ContaCorrenteDePessoaJuridica: tuple = ('003', 'Conta Corrente de Pessoa Jurídica')
    EntidadesPublicas: tuple = ('006', 'Entidades Públicas')
    DepositosInstituicoesFinanceiras: tuple = ('007', 'Depósitos Instituições Financeiras')
    PoupancaDePessoaFisica: tuple = ('013', 'Poupança de Pessoa Física')
    PoupancaDePessoaJuridica: tuple = ('022', 'Poupança de Pessoa Jurídica')
    ContaCaixaFacil: tuple = ('023', 'Conta Caixa Fácil')
    PoupancaDeCreditoImobiliario: tuple = ('028', 'Poupança de Crédito Imobiliário')
    ContaInvestimentoPessoaFisica: tuple = ('032', 'Conta Investimento Pessoa Física')
    ContaInvestimentoPessoaJuridica: tuple = ('034', 'Conta Investimento Pessoa Jurídica')
    ContaSalario: tuple = ('037', 'Conta Salário')
    DepositosLotericos: tuple = ('043', 'Depósitos Lotéricos')
    PoupancaIntegrada: tuple = ('131', 'Poupança Integrada')
