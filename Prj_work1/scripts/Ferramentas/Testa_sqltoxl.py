#!/usr/local/bin/python3.7
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 16 15:40:09 2021
@author: Airton
"""
import sys
import os
dir_base = os.path.join( os.path.realpath('.').split('/PROD/')[0], 'PROD') if os.path.realpath('.').__contains__('/PROD/') else os.path.join( os.path.realpath('.').split('/DEV/')[0], 'DEV')
sys.path.append(dir_base)
import configuracoes
import comum
import sql
comum.log.gerar_log_em_arquivo = False

import sqltoxl

comum.carregaConfiguracoes(configuracoes)

dir_saida = configuracoes.dir_saida
#dir_saida = os.getcwd()

if (configuracoes.ambiente == 'DEV'):
    dir_saida = os.getcwd()


vIE = "77452443"
vDataIni = "01/06/2017"

query =  """SELECT
                emps_cod,
                fili_cod_insest,
                codigo,
                item,
                seq,
                TO_CHAR(data,'DD/MM/YYYY'),
                gia2_ocor,
                gia2_valor
            FROM
                openrisow.gia2
            WHERE 1=1
                AND emps_cod = 'TBRA'
                AND fili_cod_insest = '%s'
                AND data >= TO_DATE('%s','DD/MM/YYYY')
                AND data <  ADD_MONTHS(TO_DATE('%s','DD/MM/YYYY'),1)
                AND codigo in('002', '003', '006')
         """%(vIE,vDataIni,vDataIni)
         
sqltoxl.qtoxl(configuracoes, query, "teste.xlxs")
