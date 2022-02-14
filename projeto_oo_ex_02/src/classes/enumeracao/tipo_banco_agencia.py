# -*- coding: utf-8 -*-
"""Classe Enumeracao Tipo de Cliente
"""
from enum import Enum


class TipoBancoAgenciaExtendedEnum(Enum):
    """Extended Enum
    """
    @classmethod
    def fnc_dict(cls):
        return {x[0]: {'descricao': x[1], 'lista_agencia': x[2]} for x in list(map(lambda c: c.value, cls))}

    @classmethod
    def fnc_ds(cls, key: str) -> str:
        try:
            return cls.fnc_dict().get(key).get('descricao')
        except Exception:
            return ""
    
    @classmethod
    def fnc_list(cls, key: str) -> list:
        try:
            return cls.fnc_dict().get(key).get('lista_agencia')
        except Exception:
            return []
    

class TipoBancoAgencia(TipoBancoAgenciaExtendedEnum):
    """Tipos Banco Agencia
    """
    BancoDoBrasil:              tuple = ('001', 'Banco do Brasil S.A.', [
                                         '0001', '0002', '0003', '0004'])
    BancoSantander:             tuple = ('033',
                                         'Banco Santander (Brasil) S.A.', [
                                             '0011', '0012', '0013', '0014'])
    CaixaEconomicaFederal:      tuple = ('104', 'Caixa Econômica Federal', [
        '0021', '0022', '0023', '0024'])
    BancoBradesco:              tuple = ('237', 'Banco Bradesco S.A.', [
        '1001', '1002', '1003', '1004'])
    BancoItau:                  tuple = ('341', 'Banco Itaú S.A.', [
        '0101', '0102', '0103', '0104'])
    BancoMercantil:             tuple = ('389', 'Banco Mercantil do Brasil S.A.', [
        '2001', '2002', '2003', '2004'])
    HSBCBankBrasil:             tuple = ('399', 'HSBC Bank Brasil S.A. – Banco Múltiplo', [
        '0201', '0202', '0203', '0204'])
    BancoSafra:                 tuple = ('422', 'Banco Safra S.A.', [
        '50001', '50002', '50003', '50004'])
    BancoRural:                 tuple = ('453', 'Banco Rural S.A.', [
        '0401', '0402', '0403', '0404'])
    BancoRendimento:            tuple = ('633', 'Banco Rendimento S.A.', [
        '0601', '0602', '0603', '0604'])
    ItauUnibanco:               tuple = ('652', 'Itaú Unibanco Holding S.A.', [
        '0701', '0702', '0703', '0704'])
