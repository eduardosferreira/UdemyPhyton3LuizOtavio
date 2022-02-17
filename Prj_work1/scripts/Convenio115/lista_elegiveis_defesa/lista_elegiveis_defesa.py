#!/usr/local/bin/python3.7
# -*- coding: utf-8 -*-
"""
------------------------------------------------------------------------------
MODULO ...: TESHUVA
SCRIPT ...: lista_elegiveis_defesa.py
CRIACAO ..: 20/01/2022
AUTOR ....: EDUARDO DA SILVA FERREIRA 
            / KYROS TECNOLOGIA (eduardof@kyros.com.br)
DESCRICAO.: Este script possibilita a execução 
            dos objetos para comparação de mestre e item
------------------------------------------------------------------------------

------------------------------------------------------------------------------
  HISTORICO : 
  18/01/2022 : EDUARDO DA SILVA FERREIRA 
            / KYROS TECNOLOGIA (eduardof@kyros.com.br) 
        - Criação do Script.
                 
------------------------------------------------------------------------------
"""
#### PATRONIZACAO PARA O PAINEL DE EXECUCOES....
import sys
import os
gv_cc_sep_dir = ('/' if os.name == 'posix' else '\\')
gv_cc_dir_bse = os.path.join( \
    os.path.realpath('.').split( \
    gv_cc_sep_dir+'PROD'+gv_cc_sep_dir)[0], 'PROD') \
    if os.path.realpath('.').__contains__( \
        gv_cc_sep_dir+'PROD'+gv_cc_sep_dir) \
    else os.path.join( os.path.realpath('.').split( \
        gv_cc_sep_dir+'DEV'+gv_cc_sep_dir)[0], 'DEV')
sys.path.append(gv_cc_dir_bse)
# imports do sparta
import configuracoes
from comum import \
    log \
    , carregaConfiguracoes\
    , buscaDadosSerie             
log.gerar_log_em_arquivo = True
carregaConfiguracoes(configuracoes)
# demais imports
import traceback
# imports do projeto
import compara_conv_115_mestre
import compara_conv_115_item
import compara_conv115_x_conv39


gv_ob_dados_serie = {}

def processo_principal(p_id_serie,p_cc_pleito=""):
    """
    Compara dados de Mestre e Item
    """
    v_nr_quantidade_autenticos = 0
    v_nr_compara_01 = 0
    v_nr_compara_02 = 0
    v_nr_compara_03 = 0
    v_nr_compara_04 = 0
    v_nr_compara_05 = 0
    global gv_ob_dados_serie
    global gv_cc_sep_dir
    try:
        if not p_id_serie:
            log("ERRO >> ID SERIE Inexistente ! " + str(p_id_serie) + " >> ")
            return None              
        
        if not p_cc_pleito:
            log("ERRO >> PLEITO Inexistente ! " + str(p_cc_pleito) + " >> ")
            return None              

        gv_ob_dados_serie =\
        buscaDadosSerie(p_id_serie)  

        if not gv_ob_dados_serie:
            log("ERRO >> Não existe dados da serie! >> ")
            return None

        if not gv_ob_dados_serie.get("uf",""):
            log("ERRO >> Não existe dados da UF na serie! >> ")
            return None
        
        if not gv_ob_dados_serie.get("ano",""):
            log("ERRO >> Não existe dados da ANO na serie! >> ")
            return None

        if not gv_ob_dados_serie.get("dir_serie",""):
            log("ERRO >> Não existe dados do DIRETORIO na serie! >> ")
            return None

        v_diretorio_conv39 = getattr(configuracoes, 'diretorio_pleito',"")
        v_diretorio_conv39 = v_diretorio_conv39.replace('<<UF>>',gv_ob_dados_serie['uf'])
        v_diretorio_conv39 = v_diretorio_conv39.replace('<<anoMesPleito>>', p_cc_pleito)
        log("diretorio_conv39 >> " + str(v_diretorio_conv39 ))
        if not v_diretorio_conv39:
            log("ERRO >> Não existe dados do DIRETORIO PLEITO ! >> ")
            return None
        if  not os.path.exists(v_diretorio_conv39):
            log("ERRO >> Diretorio Inexistente [PLEITO] ! " + str(v_diretorio_conv39) + " >> ")
            return None

        v_arquivo_conv39 = ""
        v_nm_arquivo_conv39 = ""
        v_ob_lista_arquivos = os.listdir(v_diretorio_conv39) 
        if not v_ob_lista_arquivos:
            log("ERRO >> Arquivos Inexistente ! " + str(v_diretorio_conv39) + " >> ")
            return None
        log("lista_arquivos.... " + str(v_ob_lista_arquivos))
        for cc_arquivo in v_ob_lista_arquivos:
            try:
                v_nm_arquivo_conv39 = cc_arquivo.split(gv_cc_sep_dir)[-1]
            except:
                v_nm_arquivo_conv39 = ""            
            if not v_nm_arquivo_conv39:
                log('Ignorando arquivo com nome fora do padrao', cc_arquivo + " ...")
                continue
            try:
                if str(v_nm_arquivo_conv39[-4:]).strip().upper() != ".TXT":
                    log('Ignorando arquivo com nome fora do padrao', cc_arquivo + " ...")
                    v_nm_arquivo_conv39 = ""            
                    continue
            except:
                v_nm_arquivo_conv39 = ""            
                continue
            if v_nm_arquivo_conv39:
                v_arquivo_conv39 = os.path.join( v_diretorio_conv39, v_nm_arquivo_conv39)
                if  not os.path.isfile(v_arquivo_conv39):
                    log('Ignorando arquivo com nome fora do padrao', cc_arquivo + " ...")
                    v_arquivo_conv39 = ""
                    v_nm_arquivo_conv39 = ""
                    continue
                break
        
        log("v_arquivo_conv39 .." + str(v_arquivo_conv39))
        if not v_arquivo_conv39:
            log("ERRO >> Arquivos Inexistente (TXT) ! " + str(v_diretorio_conv39) + " >> ")
            return None  

        v_diretorio_saida = getattr(configuracoes, 'diretorio_saida',"")
        v_diretorio_saida = str(v_diretorio_saida).replace("<<id_serie>>",p_id_serie).strip()
        log("diretorio_saida >> " + str(v_diretorio_saida ))
        if  not os.path.exists(v_diretorio_saida):
            log("Diretorio Inexistente ! " + str(v_diretorio_saida) + " >> ")
            os.makedirs(v_diretorio_saida)
        v_arq_saida = getattr(configuracoes, 'arq_saida',"")
        if not v_arq_saida:
            log("ERRO >> Arquivo Inexistente [SAIDA] ! " + str(v_arq_saida) + " >> ")
            return None

        log(" - Acionamento processo do mestre >> " + str(v_nr_compara_01 ))
        v_nr_compara_01 = compara_conv_115_mestre.comparaMestre(p_id_serie)
        if not v_nr_compara_01:
            log("FALHA/NENHUM RESULTADO no processo de geração de dados mestre >> " + str(v_nr_quantidade_autenticos ))
            if v_nr_compara_01 is None:
                return None
        else:    
            log(" - Quantidade de registros retornados processo mestre >> " + str(v_nr_compara_01 ))
            v_nr_quantidade_autenticos += v_nr_compara_01
            log(" - Acionamento processo do mestre >> " + str(v_nr_compara_02 ))
            v_nr_compara_02 = compara_conv_115_item.comparaItem(p_id_serie)
            if not v_nr_compara_02:
                log("FALHA/NENHUM RESULTADO no processo de geração de dados itens >> " + str(v_nr_quantidade_autenticos ))
                if v_nr_compara_02 is None:
                    return None
            else:
                log(" - Quantidade de registros retornados processo itens >> " + str(v_nr_compara_02 ))
                v_nr_quantidade_autenticos += v_nr_compara_02            
                log(" - Acionamento processo do conv39 >> " + str(v_nr_compara_03 ))
                v_nr_compara_03 = compara_conv115_x_conv39.comparaConv39(p_id_serie,p_cc_pleito)
                if not v_nr_compara_03:
                    log("FALHA/NENHUM RESULTADO no processo de geração de dados conv39 >> " + str(v_nr_quantidade_autenticos ))
                    if v_nr_compara_03 is None:
                        return None
                else:
                    log(" - Quantidade de registros retornados processo conv39 >> " + str(v_nr_compara_03 ))
                    v_nr_quantidade_autenticos += v_nr_compara_03            

        log("*"*150)
        log("")
        log(" - Quantidade de registros >> " + str(v_nr_quantidade_autenticos ))
        log(" - 01 ) Numero auxiliar >> " + str(v_nr_compara_01 ))
        log(" - 02 ) Numero auxiliar >> " + str(v_nr_compara_02 ))
        log(" - 03 ) Numero auxiliar >> " + str(v_nr_compara_03 ))
        log(" - 04 ) Numero auxiliar >> " + str(v_nr_compara_04 ))
        log(" - 05 ) Numero auxiliar >> " + str(v_nr_compara_05 ))
        log("")
        log("*"*150)
        return v_nr_quantidade_autenticos

    except Exception as err:
        err_desc_trace = traceback.format_exc()
        log("ERRO >> COMPARAR ARQUIVOS >> "\
        + str(err) + " - TRACE - " + err_desc_trace + " >> ")
               
        return None

if __name__ == "__main__":
    if len(sys.argv) < 3:
        log('#### ERRO ') 
        log('-'* 100)
        log('QUANTIDADE DE PARAMETROS INVALIDA')
        log('-'* 100)
        log('EXEMPLO')
        log('-'* 100)
        log( '%s <ID SERIE LEVANTAMENTO> <PLEITO>'%( sys.argv[0] ) )
        log('-'* 100)
        sys.exit(99)
    else:
        v_id_serie = sys.argv[1]
        log("v_id_serie: " + str(v_id_serie))        
        v_cc_pleito = sys.argv[2]
        log("v_PLEITO: " + str(v_cc_pleito))        
        v_cc_retorno = processo_principal(v_id_serie,v_cc_pleito)
        log(v_cc_retorno)
        if v_cc_retorno is None:
            sys.exit(99)
        else:
            sys.exit(0)    