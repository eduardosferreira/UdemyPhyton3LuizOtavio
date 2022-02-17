#!/usr/local/bin/python3.7
# -*- coding: utf-8 -*-
"""
------------------------------------------------------------------------------
MODULO ...: TESHUVA
SCRIPT ...: compara_conv115_x_conv39.py
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
from pathlib import Path
from glob import glob

gv_ob_dados_serie = {}


def comparaConv39(p_id_serie,p_cc_pleito=""):
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

        v_layout_arq_conv39 = 'LayoutConv39'
        v_dic_campos = dict()
        try:
            v_dic_campos[v_layout_arq_conv39] =\
            v_dic_campos.get(v_layout_arq_conv39, carregaLayout.dic_layouts[v_layout_arq_conv39]['dic_campos'])        
        except:
            log("ERRO >> Carregamento LayoutConv39 ! " + str(gv_ob_dados_serie.get("ano",0)) + " >> ")
            return None        
        log("dic_campos >> " + str(v_dic_campos)) 

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
            return None
        v_arq_saida = getattr(configuracoes, 'arq_saida',"")
        if not v_arq_saida:
            log("ERRO >> Arquivo Inexistente [SAIDA] ! " + str(v_arq_saida) + " >> ")
            return None

        v_diretorio_saida_defesa = getattr(configuracoes, 'diretorio_saida',"")
        v_diretorio_saida_defesa = str(v_diretorio_saida_defesa).replace("<<id_serie>>",p_id_serie).strip()        
        v_diretorio_saida_defesa_geral = str(getattr(configuracoes, 'diretorio_saida',"")).replace("<<id_serie>>","").strip()        
        
        v_nm_arq_saida_resultado_defesa = v_arq_saida
        v_nm_arq_saida_resultado_defesa = str(v_nm_arq_saida_resultado_defesa).replace("<<TIPO>>","DEFESA").strip()
        v_arq_saida_resultado_defesa = os.path.join( v_diretorio_saida_defesa, v_nm_arq_saida_resultado_defesa)
        v_arq_saida_resultado_defesa_geral = os.path.join( v_diretorio_saida_defesa_geral, v_nm_arq_saida_resultado_defesa)
        
        log("v_arq_saida_resultado_defesa >> " + str(v_arq_saida_resultado_defesa ))
        log("v_arq_saida_resultado_defesa_geral >> " + str(v_arq_saida_resultado_defesa_geral ))
        
        if not v_arq_saida_resultado_defesa:
            log("ERRO >> FALHA arquivo DEFESA [SAIDA] ! " + str(v_arq_saida_resultado_defesa) + " >> ")
            return None

        #abre o arq_saida_resultado_Item_identicos tipo I para leitura no diretorio_saida
        v_nm_arq_saida_resultado_Item_identicos = v_arq_saida
        v_nm_arq_saida_resultado_Item_identicos = str(v_nm_arq_saida_resultado_Item_identicos).replace("<<TIPO>>","I").strip()
        v_arq_saida_resultado_Item_identicos = os.path.join( v_diretorio_saida, v_nm_arq_saida_resultado_Item_identicos)
        log("v_arquivo_saida_mestre_identicos >> " + str(v_arq_saida_resultado_Item_identicos ))
        if  not os.path.isfile(v_arq_saida_resultado_Item_identicos):
            log("ERRO >> Arquivo Inexistente [SAIDA] ! " + str(v_arq_saida_resultado_Item_identicos) + " >> ")
            return None
        ob_encoding_arq_saida_resultado_Item_identicos = encodingDoArquivo( v_arq_saida_resultado_Item_identicos )
        fd_arq_saida_resultado_Item_identicos = open(v_arq_saida_resultado_Item_identicos, 'r', encoding=ob_encoding_arq_saida_resultado_Item_identicos)
        linha_arq_saida_resultado_Item_identicos = fd_arq_saida_resultado_Item_identicos.readline()  
        if not linha_arq_saida_resultado_Item_identicos:
            try:
                fd_arq_saida_resultado_Item_identicos.close()
            except:
                pass                
            log("ERRO >> Cabeçalho >> Inexistente de dados [Arquivo SAIDA] ! " + str(v_arq_saida_resultado_Item_identicos) + " >> ")
            return None
        else:
            v_ds_cabecalho = linha_arq_saida_resultado_Item_identicos
        linha_arq_saida_resultado_Item_identicos = fd_arq_saida_resultado_Item_identicos.readline()  
        if not linha_arq_saida_resultado_Item_identicos:
            try:
                fd_arq_saida_resultado_Item_identicos.close()
            except:
                pass                
            log("ERRO >> Registros >> Inexistente de dados [registros] [Arquivo SAIDA] ! " + str(v_arq_saida_resultado_Item_identicos) + " >> ")
            return None
        else:
            v_ds_linha = linha_arq_saida_resultado_Item_identicos
        
        # pos = 28 if int(dados_serie[‘ano’]) >= 2017 else 10
        pos = 28\
        if int(gv_ob_dados_serie.get("ano",0)) >= 2017 else 10        

        v_campos_mestre_validar = { 'CNPJ_CPF':-1, 'RAZAO_SOCIAL':-1, 'UF':-1, 'IE':-1 ,'HASH_COD_NF':-1}
        v_campos_cad_validar = {'ENDERECO':-1, 'CEP':-1, 'BAIRRO':-1, 'MUNICIPIO':-1 }
        v_campos_item_validar = {'DATA_EMISSAO':-1,'MODELO':-1,'SERIE':-1,'NUMERO_NF':-1,'NUM_ITEM':-1}
        if len(v_ds_cabecalho.split(";")) <=0:
            try:
                fd_arq_saida_resultado_Item_identicos.close()
            except:
                pass                
            log("ERRO >> Inexistente de dados [registros] [Arquivo SAIDA] ! " + str(v_arq_saida_resultado_Item_identicos) + " >> ")
            return None
        
        v_colunas = [str(campo).replace(chr(13), "")\
                                .replace(chr(10), "")\
                                .replace('\n', '')\
                                .replace('\r', '')\
                                .replace(";", "")\
                                .replace('"', '')\
                                .replace("'", "")\
                                .upper()\
                                .strip()                
                    for campo in v_ds_cabecalho.split(";")]
        
        v_ob_colunas_dic = dict()
        for posicao, coluna in enumerate(v_colunas):
            v_ob_colunas_dic[coluna] = posicao
            if coluna.endswith("_M_ATUAL"):
                for k in v_campos_mestre_validar.keys():
                    if coluna.startswith(str(k)+"_"):
                        if v_campos_mestre_validar[k] < 0:
                            v_campos_mestre_validar[k] = posicao
                            continue
            if coluna.endswith("_D_ATUAL"):
                for k in v_campos_cad_validar.keys():
                    if coluna.startswith(str(k)+"_"):
                        if v_campos_cad_validar[k] < 0:
                            v_campos_cad_validar[k] = posicao
                            continue
            if coluna.endswith("_I_ATUAL"):
                for k in v_campos_item_validar.keys():
                    if coluna.startswith(str(k)+"_"):
                        if v_campos_item_validar[k] < 0:
                            v_campos_item_validar[k] = posicao
                            continue
        
        #log(v_ds_cabecalho)
        log(v_ob_colunas_dic)        
        #log(v_campos_mestre_validar)
        #log(v_campos_cad_validar)
        log(v_campos_item_validar)
        #for k,v in v_campos_mestre_validar.items():
        #    if v < 0:
        #        try:
        #            fd_arq_saida_resultado_Item_identicos.close()
        #        except:
        #            pass                
        #        log("ERRO >> Posicao inválido [MESTRE] : " + str(k))  
        #        return None      
        #for k,v in v_campos_cad_validar.items():
        #    if v < 0:
        #        try:
        #            fd_arq_saida_resultado_Item_identicos.close()
        #        except:
        #            pass
        #        log("ERRO >> Posicao inválido [CADASTRO] : " + str(k)) 
        #        return None       
        for k,v in v_campos_item_validar.items():
            if v < 0:
                try:
                    fd_arq_saida_resultado_Item_identicos.close()
                except:
                    pass
                log("ERRO >> Posicao inválido [ITEM] : " + str(k))   
                return None     
        log("1 antes.....")
        nr_arquivo_conv39 = 0
        ob_encoding_arquivo_conv39 = encodingDoArquivo( v_arquivo_conv39 )
        fd_arquivo_conv39 = open(v_arquivo_conv39, 'r', encoding=ob_encoding_arquivo_conv39)
        lista_arquivo_conv39 = []
        for linha in fd_arquivo_conv39:
            if linha.startswith("2"):
                row = quebraRegistro(linha, v_layout_arq_conv39)
                dic1 = {}
                for key in v_dic_campos[v_layout_arq_conv39].keys():
                    dic1[key] = row[v_dic_campos[v_layout_arq_conv39][key]-1]
                lista_arquivo_conv39.append(dic1)
        fd_arquivo_conv39.close()
        for i in range(20):
            try:    
                log(lista_arquivo_conv39[i])
            except:
                pass    
        log("2 antes.....")
        
        lista_arquivo_conv39_ordenada  = sorted(lista_arquivo_conv39, key=lambda row:(\
            row['NUMERO_NF']
            ,row['SERIE']
            ,row['DATA_EMISSAO']),reverse=False)
        
        log("3 ordenada.....")
        
        
        # colocar os campos antes
        #--empresa [dados serie]
        #--uf [dados serie]       
        #numero_nota
        #data_emissao
        #serie
        #numero_item
        #-todos os campos do item
        #-todos os campos do 39  
        
        v_fl_cabecalho = False
        #abre o arq_saida_resultado_defesa tipo DEFESA para escrita no diretorio_saida
        if os.path.isfile(v_arq_saida_resultado_defesa_geral):
            v_fl_cabecalho = True

        fd_arq_saida_resultado_defesa_geral = open(v_arq_saida_resultado_defesa_geral, 'a', newline='')
        fd_arq_saida_resultado_defesa = open(v_arq_saida_resultado_defesa, 'w', newline='')
            
        v_DADO_EMPRESA = gv_ob_dados_serie.get("empresa","")
        v_DADO_UF = gv_ob_dados_serie.get("uf","")
        
        nr_linha=0
        #Para cada linha no arquivo arq_saida_resultado_Item_identicos faça
        while True:
            #Inico 1 True
            #Enquanto não v_achou_nota e linha_conv39 faça
            v_dic1_registro_conv39 = {}
            v_DADO_NUMERO_NF = ''
            v_DADO_DATA_EMISSAO = ''
            v_DADO_SERIE = ''
            v_DADO_NUMERO_ITEM = ''
            VALOR_ICMS_ITEM = ''
            v_achou_nota = False  
            lista_item = [str(campo).replace(chr(13), "")\
                                .replace(chr(10), "")\
                                .replace('\n', '')\
                                .replace('\r', '')\
                                .replace(";", "")\
                                .replace('"', '')\
                                .replace("'", "")\
                                .strip()                
                    for campo in v_ds_linha.split(";")]
          
            log('NUMERO_NF', str(lista_item[v_campos_item_validar['NUMERO_NF']]))

            for registro_conv39 in lista_arquivo_conv39_ordenada:
                if  (str(registro_conv39['DATA_EMISSAO']).strip()\
                ==  str(lista_item[v_campos_item_validar['DATA_EMISSAO']]).strip()\
                and str(registro_conv39['SERIE']).strip()\
                ==  str(lista_item[v_campos_item_validar['SERIE']]).strip()\
                and str(registro_conv39['NUMERO_NF']).strip()\
                == str(lista_item[v_campos_item_validar['NUMERO_NF']]).strip()\
                and str(registro_conv39['MODELO']).strip()\
                ==  str(lista_item[v_campos_item_validar['MODELO']]).strip()\
                and str(registro_conv39['NUM_ITEM']).strip()\
                == str(lista_item[v_campos_item_validar['NUM_ITEM']]).strip()\
                ):# or (1==1):
                    v_nr_compara_01 += 1
                    #if nr_linha < 101:
                    v_achou_nota = True
                    v_DADO_NUMERO_NF = str(lista_item[v_campos_item_validar['NUMERO_NF']]).strip()
                    v_DADO_DATA_EMISSAO = str(lista_item[v_campos_item_validar['DATA_EMISSAO']]).strip()
                    v_DADO_SERIE = str(lista_item[v_campos_item_validar['SERIE']]).strip()
                    v_DADO_NUMERO_ITEM = str(lista_item[v_campos_item_validar['NUM_ITEM']]).strip()            
                    VALOR_ICMS_ITEM = str(registro_conv39['VALOR_ICMS_ITEM']).strip()
                    for k,v in registro_conv39.items():
                        v_dic1_registro_conv39[k] = str(v)\
                                    .replace(chr(13), "")\
                                    .replace(chr(10), "")\
                                    .replace('\n', '')\
                                    .replace('\r', '')\
                                    .replace(";", "")\
                                    .strip()
                    pass
               
                if v_achou_nota:
                    log('achou...NUMERO_NF: ',registro_conv39['NUMERO_NF'])
                    break
                else:
                    if str(registro_conv39['NUMERO_NF']).strip() > str(lista_item[v_campos_item_validar['NUMERO_NF']]).strip():
                        log('break..NUMERO_NF: ',registro_conv39['NUMERO_NF'])
                        break
                    
            #Se v_achou_nota for Verdadeiro faça
            #   escreve no arq_saida_resultado_defesa os campos de registro_mestre, registro_cad,  registro_item e registro_conv39 conforme layout de modelo.
            if v_achou_nota:
                v_nr_quantidade_autenticos += 1
                log('nr_quantidade_autenticos', str(v_nr_quantidade_autenticos))
                if v_nr_quantidade_autenticos == 1:
                    v_principal_cabecalho  = ""
                    v_principal_cabecalho +=  '"EMPRESA"'+ ';'
                    v_principal_cabecalho +=  '"UF"'+ ';'
                    v_principal_cabecalho +=  '"NUMERO_NF"'+ ';'
                    v_principal_cabecalho +=  '"DATA_EMISSAO"'+ ';'
                    v_principal_cabecalho +=  '"SERIE"'+ ';'
                    v_principal_cabecalho +=  '"NUMERO_ITEM"'+ ';'
                    v_principal_cabecalho += str(v_ds_cabecalho)\
                                .replace(chr(13), '')\
                                .replace(chr(10), '')\
                                .replace('\n', '')\
                                .replace('\r', '')
                    
                    for k,v in v_dic1_registro_conv39.items():
                        v_principal_cabecalho += '"' + str(k) + "_CONV39" + '"' + ';'
                    
                    fd_arq_saida_resultado_defesa.write(v_principal_cabecalho + '\n')
                    if not v_fl_cabecalho:
                        fd_arq_saida_resultado_defesa_geral.write(v_principal_cabecalho + '\n')
                        v_fl_cabecalho = True

                v_principal_linha  = ""
                v_principal_linha += '"' + (str(v_DADO_EMPRESA)\
                                                .replace(chr(13), "")\
                                                .replace(chr(10), "")\
                                                .replace('\n', '')\
                                                .replace('\r', '')\
                                                .replace(";", "")\
                                                .strip()\
                                                ) + '"' + ';'
                v_principal_linha += '"' + (str(v_DADO_UF)\
                                                .replace(chr(13), "")\
                                                .replace(chr(10), "")\
                                                .replace('\n', '')\
                                                .replace('\r', '')\
                                                .replace(";", "")\
                                                .strip()\
                                                ) + '"' + ';'
                v_principal_linha += '"' + (str(v_DADO_NUMERO_NF)\
                                                .replace(chr(13), "")\
                                                .replace(chr(10), "")\
                                                .replace('\n', '')\
                                                .replace('\r', '')\
                                                .replace(";", "")\
                                                .strip()\
                                                ) + '"' + ';'
                v_principal_linha += '"' + (str(v_DADO_DATA_EMISSAO)\
                                                .replace(chr(13), "")\
                                                .replace(chr(10), "")\
                                                .replace('\n', '')\
                                                .replace('\r', '')\
                                                .replace(";", "")\
                                                .strip()\
                                                ) + '"' + ';'
                v_principal_linha += '"' + (str(v_DADO_SERIE)\
                                                .replace(chr(13), "")\
                                                .replace(chr(10), "")\
                                                .replace('\n', '')\
                                                .replace('\r', '')\
                                                .replace(";", "")\
                                                .strip()\
                                                ) + '"' + ';'
                v_principal_linha += '"' + (str(v_DADO_NUMERO_ITEM)\
                                                .replace(chr(13), "")\
                                                .replace(chr(10), "")\
                                                .replace('\n', '')\
                                                .replace('\r', '')\
                                                .replace(";", "")\
                                                .strip()\
                                                ) + '"' + ';'
                                
                v_principal_linha += str(v_ds_linha)\
                                .replace(chr(13), '')\
                                .replace(chr(10), '')\
                                .replace('\n', '')\
                                .replace('\r', '')

                for k,v in v_dic1_registro_conv39.items():
                    v_principal_linha += '"' + str(v) + '"' + ';'
                
                fd_arq_saida_resultado_defesa.write(v_principal_linha + '\n')
                fd_arq_saida_resultado_defesa_geral.write(v_principal_linha + '\n')
                
                pass
            
            #PROXIMA LINHA: Verifica final do arquivo    
            linha_arq_saida_resultado_Item_identicos = fd_arq_saida_resultado_Item_identicos.readline()  
            if not linha_arq_saida_resultado_Item_identicos:
                break
            v_ds_linha = linha_arq_saida_resultado_Item_identicos    
            #Fim 1 True    
        # Fecha os arquivos
        try:
            fd_arq_saida_resultado_Item_identicos.close()
        except:
            pass
        try:
            fd_arquivo_conv39.close()
        except:
            pass
        try:
            fd_arq_saida_resultado_defesa.close()
        except:
            pass     
        try:
            fd_arq_saida_resultado_defesa_geral.close()
        except:
            pass     
           
        log("*"*150)
        log("")
        log(" - Quantidade de registros autenticos >> " + str(v_nr_quantidade_autenticos ))
        log(" - 01 ) Numero auxiliar >> " + str(v_nr_compara_01 ))
        log(" - 02 ) Numero auxiliar >> " + str(v_nr_compara_02 ))
        log(" - 03 ) Numero auxiliar >> " + str(v_nr_compara_03 ))
        log(" - 04 ) Numero auxiliar >> " + str(v_nr_compara_04 ))
        log(" - 05 ) Numero auxiliar >> " + str(v_nr_compara_05 ))
        log(" - Arquivo Saida defesa >> " + str(v_arq_saida_resultado_defesa ))
        log("")
        log("*"*150)
        return v_nr_quantidade_autenticos

    except Exception as err:
        err_desc_trace = traceback.format_exc()
        try:
            log("*"*150)    
            log("ERRO!")
            log(" - Quantidade de registros autenticos >> " + str(v_nr_quantidade_autenticos ))
            log(" - 01 ) Numero auxiliar >> " + str(v_nr_compara_01 ))
            log(" - 02 ) Numero auxiliar >> " + str(v_nr_compara_02 ))
            log(" - 03 ) Numero auxiliar >> " + str(v_nr_compara_03 ))
            log(" - 04 ) Numero auxiliar >> " + str(v_nr_compara_04 ))
            log(" - 05 ) Numero auxiliar >> " + str(v_nr_compara_05 ))
            log(" - ERRO >>Arquivo Saida defesa >> " + str(v_arq_saida_resultado_defesa ))
            log("")
            log("*"*150)
        except:
            pass

        log("ERRO >> COMPARAR ARQUIVOS >> "\
        + str(err) + " - TRACE - " + err_desc_trace + " >> ")
        # TODO : Tratar os arquivos de saidas
        try:
            fd_arq_saida_resultado_Item_identicos.close()
        except:
            pass        
        try:
            fd_arquivo_conv39.close()
        except:
            pass
        try:
            fd_arq_saida_resultado_defesa.close()
        except:
            pass
        try:
            fd_arq_saida_resultado_defesa_geral.close()
        except:
            pass                   
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
        log("v_id_serie: " + str(v_cc_pleito))        
        v_cc_retorno = comparaConv39(v_id_serie,v_cc_pleito)
        log(v_cc_retorno)
        if v_cc_retorno is None:
            sys.exit(99)
        else:
            sys.exit(0)    