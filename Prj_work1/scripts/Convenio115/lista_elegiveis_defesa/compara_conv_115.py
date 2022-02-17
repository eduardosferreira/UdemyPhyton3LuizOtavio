#!/usr/local/bin/python3.7
# -*- coding: utf-8 -*-
"""
------------------------------------------------------------------------------
MODULO ...: TESHUVA
SCRIPT ...: compara_conv_115.py
CRIACAO ..: 18/01/2022
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
from layout import \
    carregaLayout\
    , quebraRegistro       
from comum import \
    log \
    , carregaConfiguracoes\
    , buscaDadosSerie\
    , encodingDoArquivo             
log.gerar_log_em_arquivo = True
carregaConfiguracoes(configuracoes)
carregaLayout()
# demais imports
import traceback
from glob import glob
import fnmatch


def compara_dados_mestre_item(p_id_serie,p_cc_pleito=""):
    """
    Compara dados de Mestre e Item
    """
    # Funcoes comuns
    # Filtra nomes dos arquivos
    def fnc_filtra_arquivos(p_nm_arquivo):
        if not p_nm_arquivo.startswith(configuracoes.gv_ob_dados_serie["uf"]):
            return False   
                    
        try:
            if p_nm_arquivo[-4] != ".":
                return False
            if int(p_nm_arquivo[-3:]) < 1:    
                return False
            if not p_nm_arquivo[configuracoes.gv_nr_pos] == "M":
                return False
        except:
            return False       
        
        return True
    #substituir a posicao da string    
    def fnc_replace(p_ds_dado, p_new_string, p_nr_posicao, p_fl_no_fail=False):
        try:
            # raise an error if index is outside of the string
            if not p_fl_no_fail and p_nr_posicao not in range(len(p_ds_dado)):
                return p_ds_dado #raise ValueError("index outside given string")
    
            # if not erroring, but the index is still not in the correct range..
            if p_nr_posicao < 0:  # add it to the beginning
                return p_new_string + p_ds_dado
            if p_nr_posicao > len(p_ds_dado):  # add it to the end
                return p_ds_dado + p_new_string
    
            # insert the new string between "slices" of the original
            return p_ds_dado[:p_nr_posicao] + p_new_string + p_ds_dado[p_nr_posicao + 1:]        
        except:
            return p_ds_dado    
    # abrir o arquivo        
    def fnc_abrir_arquivo(p_ds_arquivo):
        try:
            return open(p_ds_arquivo, 'r', encoding=encodingDoArquivo( p_ds_arquivo ))
        except:
            return None
    #inicializa
    v_nr_quantidade_autenticos = 0
    v_cc_pleito = p_cc_pleito
    v_fl_pleito = False
    v_nr_compara_01 = 0
    v_nr_compara_02 = 0
    v_nr_compara_03 = 0
    v_nr_compara_04 = 0
    v_nr_compara_05 = 0
    
    try:
        
        if not p_id_serie:
            log("ERRO >> ID SERIE Inexistente ! " + str(p_id_serie) + " >> ")
            return None              

        configuracoes.gv_ob_dados_serie =\
        buscaDadosSerie(p_id_serie)  

        if not configuracoes.gv_ob_dados_serie:
            log("ERRO >> Não existe dados da serie! >> ")
            return None

        if not configuracoes.gv_ob_dados_serie.get("uf",""):
            log("ERRO >> Não existe dados da UF na serie! >> ")
            return None
        
        if not configuracoes.gv_ob_dados_serie.get("ano",""):
            log("ERRO >> Não existe dados da ANO na serie! >> ")
            return None

        if not configuracoes.gv_ob_dados_serie.get("dir_serie",""):
            log("ERRO >> Não existe dados do DIRETORIO na serie! >> ")
            return None

        if not v_cc_pleito:
            v_cc_pleito = str(configuracoes.gv_ob_dados_serie.get("ano","")).strip()\
                        + str(configuracoes.gv_ob_dados_serie.get("mes","")).strip()

        v_arquivo_conv39 = ""
        v_nm_arquivo_conv39 = ""
        v_diretorio_conv39 = getattr(configuracoes, 'diretorio_pleito',"")
        v_diretorio_conv39 = v_diretorio_conv39.replace('<<UF>>',configuracoes.gv_ob_dados_serie['uf'])
        v_diretorio_conv39 = v_diretorio_conv39.replace('<<anoMesPleito>>', v_cc_pleito)
        log("diretorio_conv39 >> " + str(v_diretorio_conv39 ))
        if not v_diretorio_conv39:
            log("Não existe dados do DIRETORIO PLEITO ! >> ")
        elif  not os.path.exists(v_diretorio_conv39):
            log("Diretorio Inexistente [PLEITO] ! " + str(v_diretorio_conv39) + " >> ")
        else:
            v_ob_lista_arquivos_conv39 = os.listdir(v_diretorio_conv39) 
            if not v_ob_lista_arquivos_conv39:
                log("Arquivos Inexistente ! " + str(v_diretorio_conv39) + " >> ")
            else:
                for cc_arquivo in v_ob_lista_arquivos_conv39:
                    try:
                        v_nm_arquivo_conv39 = cc_arquivo.split(gv_cc_sep_dir)[-1]
                        if str(v_nm_arquivo_conv39[-4:]).strip().upper() != ".TXT":
                            v_arquivo_conv39 = ""
                            v_nm_arquivo_conv39 = ""
                            continue
                        if not str(v_nm_arquivo_conv39).strip():
                            v_arquivo_conv39 = ""
                            v_nm_arquivo_conv39 = ""
                            continue
                        v_arquivo_conv39 = os.path.join( v_diretorio_conv39, v_nm_arquivo_conv39)
                        if  not os.path.isfile(v_arquivo_conv39):
                            v_arquivo_conv39 = ""
                            v_nm_arquivo_conv39 = ""
                            continue
                        break
                    except:
                        v_arquivo_conv39 = ""
                        v_nm_arquivo_conv39 = ""
                                   
        if v_arquivo_conv39:
            v_fl_pleito = True    
        else:
            v_arquivo_conv39 = ""
            v_nm_arquivo_conv39 = ""

        v_diretorio_saida = getattr(configuracoes, 'diretorio_saida',"")
        v_diretorio_saida = str(v_diretorio_saida).replace("<<id_serie>>",p_id_serie).strip()
        
        if  not os.path.exists(v_diretorio_saida):
            log("Diretorio Inexistente ! " + str(v_diretorio_saida) + " >> ")
            os.makedirs(v_diretorio_saida)
        v_diretorio_saida_geral = str(getattr(configuracoes, 'diretorio_saida',"")).replace("<<id_serie>>","").strip()
        
        v_arq_saida = getattr(configuracoes, 'arq_saida',"")
        if not v_arq_saida:
            log("ERRO >> Arquivo Inexistente [SAIDA] ! " + str(v_arq_saida) + " >> ")
            return None
        v_nm_arq_saida_resultado_defesa = str(v_arq_saida).replace("<<TIPO>>","M_I_" + ("P" if v_fl_pleito else "") + "_" + str(p_id_serie)).strip()   
        v_nm_arq_saida_resultado_defesa_geral = str(v_arq_saida).replace("<<TIPO>>","M_I_" + ("P" if v_fl_pleito else "")).strip()   
        
        v_nm_arq_saida_resultado_defesa_00 = str(v_arq_saida).replace("<<TIPO>>","M" + "_" + str(p_id_serie)).strip()   
        v_arq_saida_resultado_01 = os.path.join( v_diretorio_saida, v_nm_arq_saida_resultado_defesa)
        log("v_arq_saida_resultado_01 >> " + str(v_arq_saida_resultado_01 ))
        if not v_arq_saida_resultado_01:
            log("ERRO >> FALHA arquivo DEFESA [SAIDA] ! " + str(v_arq_saida_resultado_01) + " >> ")
            return None
        fd_arquivo_saida = None
        v_fl_existe_saida = False        
        v_arq_saida_resultado_00 = os.path.join( v_diretorio_saida, v_nm_arq_saida_resultado_defesa_00)        
        v_arq_saida_resultado_geral = os.path.join( v_diretorio_saida_geral, v_nm_arq_saida_resultado_defesa_geral)        
        fd_arquivo_saida_geral = None
        v_fl_existe_cabecalho_geral = False   
        if os.path.isfile(v_arq_saida_resultado_geral):
            v_fl_existe_cabecalho_geral = True   
        fd_arquivo_saida_geral = open(v_arq_saida_resultado_geral, 'a', newline='')#encoding=ob_encoding_arquivo_saida)
                                                            

        v_LayoutMestre         = 'LayoutMestre' + ("" if int(configuracoes.gv_ob_dados_serie.get("ano",0)) >= 2017 else "_Antigo")
        v_LayoutItem           = 'LayoutItem' + ("" if int(configuracoes.gv_ob_dados_serie.get("ano",0)) >= 2017 else "_Antigo")
        v_LayoutCadastro       = 'LayoutCadastro' + ("" if int(configuracoes.gv_ob_dados_serie.get("ano",0)) >= 2017 else "_Antigo")
        v_layout_arq_conv39    = 'LayoutConv39'
        v_dic_campos = dict()
        try:
            v_dic_campos[v_LayoutMestre]       = v_dic_campos.get(v_LayoutMestre, carregaLayout.dic_layouts[v_LayoutMestre]['dic_campos']) 
            v_dic_campos[v_LayoutItem]         = v_dic_campos.get(v_LayoutItem, carregaLayout.dic_layouts[v_LayoutItem]['dic_campos']) 
            v_dic_campos[v_LayoutCadastro]     = v_dic_campos.get(v_LayoutCadastro, carregaLayout.dic_layouts[v_LayoutCadastro]['dic_campos']) 
            v_dic_campos[v_layout_arq_conv39]  = v_dic_campos.get(v_layout_arq_conv39, carregaLayout.dic_layouts[v_layout_arq_conv39]['dic_campos']) 
        except:
            log("ERRO >> Carregamento Layout ! " + str(configuracoes.gv_ob_dados_serie.get("ano",0)) + " >> ")
            return None
            
        v_nm_diretorio_atual_ti = configuracoes.gv_ob_dados_serie.get("dir_serie","") + str(gv_cc_sep_dir) + 'OBRIGACAO'
        v_nm_diretorio_ultima_entrega = configuracoes.gv_ob_dados_serie.get("dir_serie","") + str(gv_cc_sep_dir) + 'ULTIMA_ENTREGA'
        
        if  not os.path.exists(v_nm_diretorio_atual_ti):
            log("ERRO >> Diretorio Inexistente ! " + str(v_nm_diretorio_atual_ti) + " >> ")
            return None

        if  not os.path.exists(v_nm_diretorio_ultima_entrega):
            log("ERRO >> Diretorio Inexistente ! " + str(v_nm_diretorio_ultima_entrega) + " >> ")
            return None

        configuracoes.gv_nr_pos = 28 if int(configuracoes.gv_ob_dados_serie.get("ano",0)) >= 2017 else 10        
        
        v_ob_lista_arquivos_atual_ti = list(filter(fnc_filtra_arquivos,  os.listdir(v_nm_diretorio_atual_ti)))
        if  not v_ob_lista_arquivos_atual_ti:
            log("ERRO >> Arquivos Inexistente ! " + str(v_nm_diretorio_atual_ti) + " >> ")
            return None
        
        v_ob_lista_arquivos_ultima_entrega = list(filter(fnc_filtra_arquivos,  os.listdir(v_nm_diretorio_ultima_entrega)))
        if  not v_ob_lista_arquivos_ultima_entrega:
            log("ERRO >> Arquivos Inexistente ! " + str(v_nm_diretorio_ultima_entrega) + " >> ")
            return None
        
        v_ob_lista_arquivos = list()
        
        for arq in v_ob_lista_arquivos_atual_ti:
            v_nm_arquivo_atual = arq
            v_ob_dic_arq = dict()
            if not os.path.isfile(os.path.join( v_nm_diretorio_atual_ti, v_nm_arquivo_atual)):
                continue
            if not os.path.isfile(os.path.join( v_nm_diretorio_atual_ti, fnc_replace(v_nm_arquivo_atual,"D",configuracoes.gv_nr_pos))):
                continue            
            if not os.path.isfile(os.path.join( v_nm_diretorio_atual_ti, fnc_replace(v_nm_arquivo_atual,"I",configuracoes.gv_nr_pos))):
                continue
            v_nm_arquivo_ultimo = v_nm_arquivo_atual
            if not os.path.isfile(os.path.join( v_nm_diretorio_ultima_entrega, v_nm_arquivo_ultimo)):
                if int(configuracoes.gv_ob_dados_serie.get("ano",0)) >= 2017:
                    try:
                        v_ds_mascara = v_nm_arquivo_ultimo[:25] + '*' + v_nm_arquivo_ultimo[28:] 
                        #log("v_ds_mascara",v_ds_mascara)
                        v_nm_arquivo_ultimo = ""
                        for arq_ultima_entrega in v_ob_lista_arquivos_ultima_entrega:
                            pass #log("v_ds_mascara",v_ds_mascara,"arq_ultima_entrega",arq_ultima_entrega)
                            if  fnmatch.fnmatch(arq_ultima_entrega,v_ds_mascara):
                                if os.path.isfile(os.path.join( v_nm_diretorio_ultima_entrega, arq_ultima_entrega)):
                                    v_nm_arquivo_ultimo = arq_ultima_entrega
                                    break
                        if not v_nm_arquivo_ultimo:
                            continue
                    except:
                        continue
                else:    
                    continue
            if not os.path.isfile(os.path.join( v_nm_diretorio_ultima_entrega, fnc_replace(v_nm_arquivo_ultimo,"D",configuracoes.gv_nr_pos))):
                continue            
            if not os.path.isfile(os.path.join( v_nm_diretorio_ultima_entrega, fnc_replace(v_nm_arquivo_ultimo,"I",configuracoes.gv_nr_pos))):
                continue           
            
            v_ob_dic_arq['arquivo_mestre_atual'] = os.path.join( v_nm_diretorio_atual_ti, v_nm_arquivo_atual)
            v_ob_dic_arq['arquivo_cadastro_atual'] = os.path.join( v_nm_diretorio_atual_ti, fnc_replace(v_nm_arquivo_atual,"D",configuracoes.gv_nr_pos))
            v_ob_dic_arq['arquivo_item_atual'] = os.path.join( v_nm_diretorio_atual_ti, fnc_replace(v_nm_arquivo_atual,"I",configuracoes.gv_nr_pos))
            
            v_ob_dic_arq['arquivo_mestre_ultima_entrega'] = os.path.join( v_nm_diretorio_ultima_entrega, v_nm_arquivo_ultimo)
            v_ob_dic_arq['arquivo_cadastro_ultima_entrega'] = os.path.join( v_nm_diretorio_ultima_entrega, fnc_replace(v_nm_arquivo_ultimo,"D",configuracoes.gv_nr_pos))
            v_ob_dic_arq['arquivo_item_ultima_entrega'] = os.path.join( v_nm_diretorio_ultima_entrega, fnc_replace(v_nm_arquivo_ultimo,"I",configuracoes.gv_nr_pos))
            
            v_ob_dic_arq['arquivo_pleito'] = v_arquivo_conv39

            v_ob_dic_arq['encontrou_mestre'] = 0
            v_ob_dic_arq['encontrou_item'] = 0
            
            v_ob_lista_arquivos.append(v_ob_dic_arq)
        
        if not v_ob_lista_arquivos:
            log("ERRO >> Arquivos Inexistente ! " + str(v_nm_diretorio_ultima_entrega) + " >> ")
            return None
            
        v_ob_lista_arquivos_ordenada  = sorted(v_ob_lista_arquivos,key=lambda x:x.get('arquivo_mestre_atual',''),reverse=False)
                
        log("inicio da analise.....")
        v_DADO_EMPRESA = configuracoes.gv_ob_dados_serie.get("empresa","")
        v_DADO_UF = configuracoes.gv_ob_dados_serie.get("uf","")
        v_fl_erro = 0        
        v_ob_lista_encontrou = []
        fd_arquivo_pleito = None
        reg_pleito = None 
        v_ob_reg_arquivo_pleito = dict()                           
        for index, ob_lista in enumerate(v_ob_lista_arquivos_ordenada):
            try:
                v_nr_linha_mestre_atual = 0    
                fd_arquivo_mestre_atual            = fnc_abrir_arquivo(ob_lista['arquivo_mestre_atual'])  
                fd_arquivo_cadastro_atual          = fnc_abrir_arquivo(ob_lista['arquivo_cadastro_atual'])
                fd_arquivo_item_atual              = None
                fd_arquivo_mestre_ultima_entrega   = fnc_abrir_arquivo(ob_lista['arquivo_mestre_ultima_entrega'])
                fd_arquivo_cadastro_ultima_entrega = fnc_abrir_arquivo(ob_lista['arquivo_cadastro_ultima_entrega'])
                fd_arquivo_item_ultima_entrega     = None
                v_ob_reg_arquivo_item_atual = dict()
                v_ob_reg_arquivo_item_ultima_entrega = dict()                                                        
                if fd_arquivo_mestre_atual and fd_arquivo_cadastro_atual and fd_arquivo_mestre_ultima_entrega and fd_arquivo_cadastro_ultima_entrega:##<TROCA_MEMORIA> and fd_arquivo_item_atual and fd_arquivo_item_ultima_entrega:
                    linha_cadastro_atual           = fd_arquivo_cadastro_atual.readline()              
                    linha_mestre_ultima_entrega    = fd_arquivo_mestre_ultima_entrega.readline()   
                    linha_cadastro_ultima_entrega  = fd_arquivo_cadastro_ultima_entrega.readline() 
                    if linha_cadastro_atual and linha_mestre_ultima_entrega and linha_cadastro_ultima_entrega:##<TROCA_MEMORIA> and linha_item_atual and linha_item_ultima_entrega:
                        for linha_mestre_atual in fd_arquivo_mestre_atual:                                            
                            if linha_mestre_atual and linha_cadastro_atual  and linha_mestre_ultima_entrega and linha_cadastro_ultima_entrega:##<TROCA_MEMORIA>  and linha_item_atual and linha_item_ultima_entrega:
                                v_nr_linha_mestre_atual += 1
                            else:
                                break
                            reg_mestre_atual             = quebraRegistro(linha_mestre_atual, v_LayoutMestre)     
                            reg_cadastro_atual           = quebraRegistro(linha_cadastro_atual, v_LayoutCadastro)                
                            reg_item_atual = None
                            reg_mestre_ultima_entrega    = quebraRegistro(linha_mestre_ultima_entrega, v_LayoutMestre)     
                            reg_cadastro_ultima_entrega  = quebraRegistro(linha_cadastro_ultima_entrega, v_LayoutCadastro)   
                            reg_item_ultima_entrega = None
                            v_fl_encontrou_item = False                            
                            v_fl_encontrou_mestre_cadastro = False
                            if str(reg_mestre_atual[v_dic_campos[v_LayoutMestre]['HASH_CODE_ARQ']-1]) == str(reg_mestre_ultima_entrega[v_dic_campos[v_LayoutMestre]['HASH_CODE_ARQ']-1]):
                                if str(reg_cadastro_atual[v_dic_campos[v_LayoutCadastro]['CodigoAutentRegistro']-1]) == str(reg_cadastro_ultima_entrega[v_dic_campos[v_LayoutCadastro]['CodigoAutentRegistro']-1]):
                                    v_fl_encontrou_mestre_cadastro = True
                                    v_ob_lista_arquivos_ordenada[index]['encontrou_mestre'] = 1
                            
                            v_fl_linha_item_atual = False
                            v_fl_linha_item_ultima_entrega = False
                            if v_fl_encontrou_mestre_cadastro:
                                v_DADO_NUMERO_NF                    =   str(reg_mestre_atual[v_dic_campos[v_LayoutMestre]['NUMERO_NF']-1]).strip() 
                                v_DADO_DATA_EMISSAO                 =   str(reg_mestre_atual[v_dic_campos[v_LayoutMestre]['DATA_EMISSAO']-1]).strip()
                                v_DADO_SERIE                        =   str(reg_mestre_atual[v_dic_campos[v_LayoutMestre]['SERIE']-1]).strip()
                                v_DADO_MODELO                       =   str(reg_mestre_atual[v_dic_campos[v_LayoutMestre]['MODELO']-1]).strip() 
                                if not fd_arquivo_item_atual:
                                    v_ob_reg_arquivo_item_atual = dict()
                                    fd_arquivo_item_atual              = fnc_abrir_arquivo(ob_lista['arquivo_item_atual'])
                                    for linha_item_atual in fd_arquivo_item_atual:
                                            reg_item_atual = quebraRegistro(linha_item_atual, v_LayoutItem)
                                            v_cc_chave = ""
                                            v_cc_chave +=  str(reg_item_atual[v_dic_campos[v_LayoutItem]['NUMERO_NF']-1]).strip() + "|" 
                                            v_cc_chave +=  str(reg_item_atual[v_dic_campos[v_LayoutItem]['DATA_EMISSAO']-1]).strip() + "|"
                                            v_cc_chave +=  str(reg_item_atual[v_dic_campos[v_LayoutItem]['SERIE']-1]).strip() + "|"
                                            v_cc_chave +=  str(reg_item_atual[v_dic_campos[v_LayoutItem]['MODELO']-1]).strip() + "|"
                                            v_ob_lista_chave = list()
                                            if v_cc_chave in v_ob_reg_arquivo_item_atual:
                                                v_ob_lista_chave = v_ob_reg_arquivo_item_atual[v_cc_chave]
                                            v_ob_lista_chave.append(reg_item_atual)
                                            v_ob_reg_arquivo_item_atual[v_cc_chave] = v_ob_lista_chave
                                    try:
                                        fd_arquivo_item_atual.close()              
                                    except:
                                        pass                        
                                if not fd_arquivo_item_ultima_entrega: 
                                    v_ob_reg_arquivo_item_ultima_entrega = dict()
                                    fd_arquivo_item_ultima_entrega     = fnc_abrir_arquivo(ob_lista['arquivo_item_ultima_entrega'])
                                    for linha_item_ultima_entrega in fd_arquivo_item_ultima_entrega:
                                        reg_item_ultima_entrega = quebraRegistro(linha_item_ultima_entrega, v_LayoutItem)
                                        v_cc_chave = ""
                                        v_cc_chave +=  str(reg_item_ultima_entrega[v_dic_campos[v_LayoutItem]['NUMERO_NF']-1]).strip() + "|" 
                                        v_cc_chave +=  str(reg_item_ultima_entrega[v_dic_campos[v_LayoutItem]['DATA_EMISSAO']-1]).strip() + "|"
                                        v_cc_chave +=  str(reg_item_ultima_entrega[v_dic_campos[v_LayoutItem]['SERIE']-1]).strip() + "|"
                                        v_cc_chave +=  str(reg_item_ultima_entrega[v_dic_campos[v_LayoutItem]['MODELO']-1]).strip() + "|"
                                        v_ob_lista_chave = list()
                                        if v_cc_chave in v_ob_reg_arquivo_item_ultima_entrega:
                                            v_ob_lista_chave = v_ob_reg_arquivo_item_ultima_entrega[v_cc_chave]
                                        v_ob_lista_chave.append(reg_item_ultima_entrega)
                                        v_ob_reg_arquivo_item_ultima_entrega[v_cc_chave] = v_ob_lista_chave
                                    try:
                                        fd_arquivo_item_ultima_entrega.close()              
                                    except:
                                        pass
                                reg_item_atual = None
                                reg_item_ultima_entrega = None
                                v_cc_chave = ""
                                v_cc_chave +=  str(v_DADO_NUMERO_NF).strip() + "|" 
                                v_cc_chave +=  str(v_DADO_DATA_EMISSAO).strip() + "|"
                                v_cc_chave +=  str(v_DADO_SERIE).strip() + "|"
                                v_cc_chave +=  str(v_DADO_MODELO).strip() + "|"
                                
                                if v_cc_chave in v_ob_reg_arquivo_item_atual and v_cc_chave in v_ob_reg_arquivo_item_ultima_entrega:
                                    v_fl_linha_item_atual = True
                                    v_fl_linha_item_ultima_entrega = True                                                    
                                    for reg_item_atual in v_ob_reg_arquivo_item_atual[v_cc_chave]:
                                        for reg_item_ultima_entrega in v_ob_reg_arquivo_item_ultima_entrega[v_cc_chave]:
                                            if str(reg_item_ultima_entrega[v_dic_campos[v_LayoutItem]['HASH_CODE_ARQ']-1]).strip()\
                                            == str(reg_item_atual[v_dic_campos[v_LayoutItem]['HASH_CODE_ARQ']-1]).strip():
                                                pass
                                                v_fl_encontrou_item = True
                                                v_ob_lista_arquivos_ordenada[index]['encontrou_item'] = 1
                                                v_DADO_NUM_ITEM =   str(reg_item_atual[v_dic_campos[v_LayoutItem]['NUM_ITEM']-1]).strip() 
                                                v_cc_chave_item =  v_cc_chave + str(v_DADO_NUM_ITEM).strip() + "|"
                                                v_achou_nota = False    
                                                
                                                if v_fl_pleito:
                                                    if not fd_arquivo_pleito:
                                                        fd_arquivo_pleito            = fnc_abrir_arquivo(ob_lista['arquivo_pleito'])  
                                                        for linha_pleito in fd_arquivo_pleito:
                                                            reg_pleito = quebraRegistro(linha_pleito, v_layout_arq_conv39)
                                                            v_cc_chave_aux = ""
                                                            v_cc_chave_aux +=  str(reg_pleito[v_dic_campos[v_layout_arq_conv39]['NUMERO_NF']-1]).strip() + "|" 
                                                            v_cc_chave_aux +=  str(reg_pleito[v_dic_campos[v_layout_arq_conv39]['DATA_EMISSAO']-1]).strip() + "|"
                                                            v_cc_chave_aux +=  str(reg_pleito[v_dic_campos[v_layout_arq_conv39]['SERIE']-1]).strip() + "|"
                                                            v_cc_chave_aux +=  str(reg_pleito[v_dic_campos[v_layout_arq_conv39]['MODELO']-1]).strip() + "|"
                                                            v_cc_chave_aux +=  str(reg_pleito[v_dic_campos[v_layout_arq_conv39]['NUM_ITEM']-1]).strip() + "|"
                                                            v_ob_reg_arquivo_pleito[v_cc_chave_aux] = reg_pleito
                                                        try:
                                                            fd_arquivo_pleito.close()              
                                                        except:
                                                            pass
                                                    reg_pleito = None
                                                    if v_cc_chave_item in v_ob_reg_arquivo_pleito:
                                                        reg_pleito = v_ob_reg_arquivo_pleito[v_cc_chave_item]
                                                        v_achou_nota = True
                                                else:
                                                    v_achou_nota = True        

                                                if v_achou_nota:
                                                    v_nr_quantidade_autenticos += 1 
                                                    #log(v_arq_saida_resultado_01.split(gv_cc_sep_dir)[-1],v_nr_quantidade_autenticos,'NF', str(v_DADO_EMPRESA), str(v_DADO_UF) , str(v_DADO_NUMERO_NF)  , str(v_DADO_SERIE) , str(v_DADO_DATA_EMISSAO) , str(v_DADO_MODELO) , str(v_DADO_NUM_ITEM))
                                                    if v_nr_quantidade_autenticos == 1:
                                                        fd_arquivo_saida = open(v_arq_saida_resultado_01, 'w', newline='')#encoding=ob_encoding_arquivo_saida)
                                                        v_fl_existe_saida = True

                                                    v_ds_cabecalho = ""
                                                    v_ds_linha     = ""                                
                                                                                                            
                                                    if v_nr_quantidade_autenticos==1:
                                                        v_ds_cabecalho += '"EMPRESA"' + ';'
                                                        v_ds_cabecalho += '"UF"' + ';'
                                                        v_ds_cabecalho += '"NUMERO_NF"' + ';'
                                                        v_ds_cabecalho += '"SERIE"' + ';'
                                                        v_ds_cabecalho += '"DATA_EMISSAO"' + ';'
                                                        v_ds_cabecalho += '"MODELO"' + ';'
                                                        v_ds_cabecalho += '"NUM_ITEM"' + ';'
                                                    
                                                    v_ds_linha += '"' + str(v_DADO_EMPRESA) + '"' + ';' 
                                                    v_ds_linha += '"' + str(v_DADO_UF) + '"' + ';'
                                                    v_ds_linha += '"' + str(v_DADO_NUMERO_NF) + '"' + ';' 
                                                    v_ds_linha += '"' + str(v_DADO_SERIE) + '"' + ';'
                                                    v_ds_linha += '"' + str(v_DADO_DATA_EMISSAO) + '"' + ';'
                                                    v_ds_linha += '"' + str(v_DADO_MODELO) + '"' + ';'
                                                    v_ds_linha += '"' + str(v_DADO_NUM_ITEM) + '"' + ';'
                                                    
                                                    for i,(k,v) in enumerate(v_dic_campos[v_LayoutMestre].items()):    
                                                        if v_nr_quantidade_autenticos==1:
                                                            if not i:
                                                                v_ds_cabecalho += '"NOME_ARQUIVO_MESTRE_M_ATUAL"' + ';'
                                                                v_ds_cabecalho += '"NOME_ARQUIVO_MESTRE_M_ENTREGUE"' + ';'
                                                            v_ds_cabecalho += '"' + str(k) + '_M_ATUAL"' + ';'                                    
                                                            v_ds_cabecalho += '"' + str(k) + '_M_ENTREGUE"' + ';'                                    
                                                        
                                                        if not i:
                                                            v_ds_linha += '"' + str(v_ob_lista_arquivos_ordenada[index]['arquivo_mestre_atual'].split(gv_cc_sep_dir)[-1]) + '"' + ';' 
                                                            v_ds_linha += '"' + str(v_ob_lista_arquivos_ordenada[index]['arquivo_mestre_ultima_entrega'].split(gv_cc_sep_dir)[-1]) + '"' + ';' 
                                
                                                        v_ds_linha += \
                                                            '"' + (str(reg_mestre_atual[int(v)-1])\
                                                            .replace(chr(13), "")\
                                                            .replace(chr(10), "")\
                                                            .replace('\n', '')\
                                                            .replace('\r', '')\
                                                            .replace(";", "")\
                                                            .strip()\
                                                            ) + '"' + ';'
                    
                                                        v_ds_linha += \
                                                            '"' + (str(reg_mestre_ultima_entrega[int(v)-1])\
                                                            .replace(chr(13), "")\
                                                            .replace(chr(10), "")\
                                                            .replace('\n', '')\
                                                            .replace('\r', '')\
                                                            .replace(";", "")\
                                                            .strip()\
                                                            ) + '"' + ';'
                    
                                                    for i,(k,v) in enumerate(v_dic_campos[v_LayoutCadastro].items()):    
                                                        if v_nr_quantidade_autenticos==1:
                                                            if not i:
                                                                v_ds_cabecalho += '"NOME_ARQUIVO_CADASTRO_D_ATUAL"' + ';'
                                                                v_ds_cabecalho += '"NOME_ARQUIVO_CADASTRO_D_ENTREGUE"' + ';'
                                                            
                                                            v_ds_cabecalho += '"' + str(k) + '_D_ATUAL"' + ';'                                    
                                                            v_ds_cabecalho += '"' + str(k) + '_D_ENTREGUE"' + ';'                                    
                                                        
                                                        if not i:
                                                            v_ds_linha += '"' + str(v_ob_lista_arquivos_ordenada[index]['arquivo_cadastro_atual'].split(gv_cc_sep_dir)[-1]) + '"' + ';' 
                                                            v_ds_linha += '"' + str(v_ob_lista_arquivos_ordenada[index]['arquivo_cadastro_ultima_entrega'].split(gv_cc_sep_dir)[-1]) + '"' + ';' 
                    
                                                        v_ds_linha += \
                                                            '"' + (str(reg_cadastro_atual[int(v)-1])\
                                                            .replace(chr(13), "")\
                                                            .replace(chr(10), "")\
                                                            .replace('\n', '')\
                                                            .replace('\r', '')\
                                                            .replace(";", "")\
                                                            .strip()\
                                                            ) + '"' + ';'
                    
                                                        v_ds_linha += \
                                                            '"' + (str(reg_cadastro_ultima_entrega[int(v)-1])\
                                                            .replace(chr(13), "")\
                                                            .replace(chr(10), "")\
                                                            .replace('\n', '')\
                                                            .replace('\r', '')\
                                                            .replace(";", "")\
                                                            .strip()\
                                                            ) + '"' + ';'                               
                    
                                                    for i,(k,v) in enumerate(v_dic_campos[v_LayoutItem].items()):    
                                                        if v_nr_quantidade_autenticos==1:
                                                            if not i:
                                                                v_ds_cabecalho += '"NOME_ARQUIVO_ITEM_I_ATUAL"' + ';'
                                                                v_ds_cabecalho += '"NOME_ARQUIVO_ITEM_I_ENTREGUE"' + ';'
                                                            v_ds_cabecalho += '"' + str(k) + '_I_ATUAL"' + ';'                                    
                                                            v_ds_cabecalho += '"' + str(k) + '_I_ENTREGUE"' + ';'                                    
                                                        
                                                        if not i:
                                                            v_ds_linha += '"' + str(v_ob_lista_arquivos_ordenada[index]['arquivo_item_atual'].split(gv_cc_sep_dir)[-1]) + '"' + ';' 
                                                            v_ds_linha += '"' + str(v_ob_lista_arquivos_ordenada[index]['arquivo_item_ultima_entrega'].split(gv_cc_sep_dir)[-1]) + '"' + ';' 
                                
                                                        v_ds_linha += \
                                                            '"' + (str(reg_item_atual[int(v)-1])\
                                                            .replace(chr(13), "")\
                                                            .replace(chr(10), "")\
                                                            .replace('\n', '')\
                                                            .replace('\r', '')\
                                                            .replace(";", "")\
                                                            .strip()\
                                                            ) + '"' + ';'
                    
                                                        v_ds_linha += \
                                                            '"' + (str(reg_item_ultima_entrega[int(v)-1])\
                                                            .replace(chr(13), "")\
                                                            .replace(chr(10), "")\
                                                            .replace('\n', '')\
                                                            .replace('\r', '')\
                                                            .replace(";", "")\
                                                            .strip()\
                                                            ) + '"' + ';'
                    
                                                    if v_fl_pleito:
                                                        for i,(k,v) in enumerate(v_dic_campos[v_layout_arq_conv39].items()):    
                                                            if v_nr_quantidade_autenticos==1:
                                                                if not i:
                                                                    v_ds_cabecalho += '"NOME_ARQUIVO_CONV39"' + ';'
                                                                v_ds_cabecalho += '"' + str(k) + '_CONV39"' + ';'                                    
                                                        
                                                            if not i:
                                                                v_ds_linha += '"' + str(v_ob_lista_arquivos_ordenada[index]['arquivo_pleito'].split(gv_cc_sep_dir)[-1]) + '"' + ';' 
                                                        
                                                            v_ds_linha += \
                                                                '"' + (str(reg_pleito[int(v)-1])\
                                                                .replace(chr(13), "")\
                                                                .replace(chr(10), "")\
                                                                .replace('\n', '')\
                                                                .replace('\r', '')\
                                                                .replace(";", "")\
                                                                .strip()\
                                                                ) + '"' + ';'

                                                        
                                                    # 3. grava as linhas
                                                    if v_nr_quantidade_autenticos==1:
                                                        if v_ds_cabecalho:
                                                            fd_arquivo_saida.write(v_ds_cabecalho + '\n')    
                                                            if not v_fl_existe_cabecalho_geral:
                                                                fd_arquivo_saida_geral.write(v_ds_cabecalho + '\n')   
         
                                                    if v_ds_linha:
                                                        fd_arquivo_saida.write(v_ds_linha + '\n')
                                                        fd_arquivo_saida_geral.write(v_ds_linha + '\n')
                                                    pass

                            linha_cadastro_atual           = fd_arquivo_cadastro_atual.readline()              
                            linha_mestre_ultima_entrega    = fd_arquivo_mestre_ultima_entrega.readline()   
                            linha_cadastro_ultima_entrega  = fd_arquivo_cadastro_ultima_entrega.readline() 

                    if not v_nr_linha_mestre_atual:
                        log("NAO FOI ENCONTRADO REGISTROS >> LEITURA ABERTURA DAS LINHAS DO ARQUIVO  >> " + str(ob_lista['arquivo_mestre_atual']))
                else:
                    v_fl_erro += 1
                    log("ERRO >> ABERTURA DOS ARQUIVOS >> ")
            except Exception as err:
                err_desc_trace = traceback.format_exc()
                log("ERRO >> COMPARAR ARQUIVOS >> "\
                + str(err) + " - TRACE - " + err_desc_trace + " >> ")
                v_fl_erro += 1
            finally:
                try:
                    fd_arquivo_mestre_atual.close()            
                except:
                    pass
                try:
                    fd_arquivo_cadastro_atual.close()          
                except:
                    pass
                try:
                    fd_arquivo_item_atual.close()              
                except:
                    pass
                try:
                    fd_arquivo_mestre_ultima_entrega.close()   
                except:
                    pass
                try:
                    fd_arquivo_cadastro_ultima_entrega.close() 
                except:
                    pass
                try:
                    fd_arquivo_item_ultima_entrega.close()     
                except:
                    pass
                try:
                    fd_arquivo_pleito.close()              
                except:
                    pass
                
        #if not v_ob_lista_encontrou:
        if not v_nr_quantidade_autenticos:
            log("NAO FOI ENCONTRADO REGISTROS para COMPARAÇÃO ")
            if not v_fl_existe_saida:
                try:
                    fd_arquivo_saida = open(v_arq_saida_resultado_00, 'w', newline='')#encoding=ob_encoding_arquivo_saida)
                except:
                    pass    
        else:
            log("QTDE REGISTROS para COMPARAÇÃO " + str(v_nr_quantidade_autenticos))
                                                                       

        try:
            fd_arquivo_saida.close()
        except:
            pass
        try:
            fd_arquivo_saida_geral.close()
        except:
            pass        
        log("*"*150)
        log("")
        log(" - Quantidade de registros autenticos >> " + str(v_nr_quantidade_autenticos ))
        if v_nr_quantidade_autenticos > 0:
            try:
                log(" - Arquivo de Saida >> " + str(v_arq_saida_resultado_01 ))
            except:
                pass
            try:
                log(" - Arquivo de Saida [GERAL] >> " + str(v_arq_saida_resultado_geral ))
            except:
                pass
        
        log("")
        
        log("*"*150)
        return v_nr_quantidade_autenticos

    except Exception as err:
        err_desc_trace = traceback.format_exc()
        log("ERRO >> COMPARAR ARQUIVOS >> "\
        + str(err) + " - TRACE - " + err_desc_trace + " >> ")
        try:
            fd_arquivo_mestre_atual.close()            
        except:
            pass
        try:
            fd_arquivo_cadastro_atual.close()          
        except:
            pass
        try:
            fd_arquivo_item_atual.close()              
        except:
            pass
        try:
            fd_arquivo_mestre_ultima_entrega.close()   
        except:
            pass
        try:
            fd_arquivo_cadastro_ultima_entrega.close() 
        except:
            pass
        try:
            fd_arquivo_item_ultima_entrega.close()     
        except:
            pass
        try:
            fd_arquivo_pleito.close()              
        except:
            pass
        try:
            fd_arquivo_saida.close()
        except:
            pass
        try:
            fd_arquivo_saida_geral.close()
        except:
            pass        
        
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2:#3:
        log('#### ERRO ') 
        log('-'* 100)
        log('QUANTIDADE DE PARAMETROS INVALIDA')
        log('-'* 100)
        log('EXEMPLO')
        log('-'* 100)
        log( '%s <ID SERIE LEVANTAMENTO> <OPCIONAL PLEITO YYYYMM>'%( sys.argv[0] ) )
        log('-'* 100)
        sys.exit(99)
    else:
        v_id_serie = sys.argv[1]
        log("serie: " + str(v_id_serie))
        v_cc_pleito = sys.argv[2] if len(sys.argv) > 2 else ""    
        log("pleito: " + str(v_cc_pleito))
        v_cc_retorno = compara_dados_mestre_item(v_id_serie,v_cc_pleito)
        log(v_cc_retorno)
        if v_cc_retorno is None:
            sys.exit(99)
        else:
            sys.exit(0)    