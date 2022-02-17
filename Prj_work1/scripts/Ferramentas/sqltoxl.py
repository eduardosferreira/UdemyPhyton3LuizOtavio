#!/usr/local/bin/python3.7
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 16 15:30:56 2021

@author: Airton
"""
import sql
import re
from openpyxl import Workbook

def qtoxl(configuracoes,query,saida,aba="Resultado da query"):


    bd=sql.geraCnxBD(configuracoes)
    bd.executa(query)
    result = bd.fetchall()
    if (result == None):
        return(None)

    arq_xl = Workbook()
    aba_xl = arq_xl.active
    aba_xl.title = aba


    define_coluna(aba_xl)
    input("VAI MOSTRAR DESCRIÇÃO DAS COLUNAS:")
    for row in bd.description():
        print(row[0])

    input("AGORA VAI MOSTRAR OS DADOS:")
    print(result)

    arq_xl.save(saida)


    
def define_coluna(planilha):
    for col in planilha.columns:
        max_lenght = 0
        print(col[0])
        col_name = re.findall('\w\d', str(col[0]))
        col_name = col_name[0]
        col_name = re.findall('\w', str(col_name))[0]
        print(col_name)
        for cell in col:
            try:
                if len(str(cell.value)) > max_lenght:
                    max_lenght = len(cell.value)
            except:
                pass
        adjusted_width = (max_lenght+2)
        planilha.column_dimensions[col_name].width = adjusted_width
