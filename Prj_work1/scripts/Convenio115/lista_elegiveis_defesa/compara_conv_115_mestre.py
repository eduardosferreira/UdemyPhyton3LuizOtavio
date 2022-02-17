#!/usr/local/bin/python3.7
# -*- coding: utf-8 -*-
"""
------------------------------------------------------------------------------
MODULO ...: TESHUVA
SCRIPT ...: compara_conv_115_mestre.py
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

gv_ob_dados_serie = {}


def listaArquivosComparar(p_nm_diretorio):
    """
    Lista de Arquivos do Diretório -> lista
    """
    global gv_ob_dados_serie
    global gv_cc_sep_dir
    v_ob_lista_processar = []
    v_nm_diretorio = p_nm_diretorio + str(gv_cc_sep_dir) + "*.*"
    log("Diretorio de busca....: " + str(v_nm_diretorio) + " >> ")

    if  not os.path.exists(p_nm_diretorio):
        log("ERRO >> Diretorio Inexistente ! " + str(p_nm_diretorio) + " >> ")
        return None

    if not gv_ob_dados_serie:
        log("ERRO >> Não existe dados da serie! >> ")
        return None

    try:
        v_cc_uf = str(gv_ob_dados_serie.get("uf","")).strip().upper()
        v_nr_pos = 28\
        if int(gv_ob_dados_serie.get("ano",0)) >= 2017 else 10        

        v_ob_lista_arquivos = [nm_arquivo for nm_arquivo in glob(v_nm_diretorio, recursive=False)]  

        #v_ob_lista_arquivos = [nm_arquivo\
        #                      for nm_arquivo in os.listdir(p_nm_diretorio)]
        if not v_ob_lista_arquivos:
            log("ERRO >> Arquivos Inexistente ! " + str(p_nm_diretorio) + " >> ")
            return None            

        for cc_arquivo in v_ob_lista_arquivos:
            v_nm_arquivo = ""
         
            try:
                v_nm_arquivo = cc_arquivo.split(gv_cc_sep_dir)[-1]
            except:
                v_nm_arquivo = ""
            
            if not v_nm_arquivo:
                log('Ignorando arquivo com nome fora do padrao', cc_arquivo + " ...")
                continue

            if  not os.path.isfile(cc_arquivo):
                log('Ignorando arquivo com nome fora do padrao', cc_arquivo + " ...")
                continue

            try:
                if v_nm_arquivo.strip().upper().startswith(v_cc_uf):
                    if v_nm_arquivo[v_nr_pos] in ("M", "D"):
                        v_ob_lista_processar.append(v_nm_arquivo)
                     
            except:
                pass    

        try:        
            v_ob_lista_processar.sort(reverse=True)        
        except:
            pass
        return v_ob_lista_processar

    except Exception as err:
        err_desc_trace = traceback.format_exc()
        log("ERRO >> BUSCAR ARQUIVOS >> "\
        + str(err) + " - TRACE - " + err_desc_trace + " >> ")
        return None


def comparaMestre(p_id_serie):
    """
    Compara dados de Mestre e Item
    """
    v_nr_quantidade_autenticos = 0
    global gv_ob_dados_serie
    global gv_cc_sep_dir
    try:
        if not p_id_serie:
            log("ERRO >> ID SERIE Inexistente ! " + str(p_id_serie) + " >> ")
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

        v_nm_diretorio_atual_ti =\
        gv_ob_dados_serie.get("dir_serie","")\
        + str(gv_cc_sep_dir)\
        + 'OBRIGACAO'
        v_nm_diretorio_ultima_entrega =\
        gv_ob_dados_serie.get("dir_serie","")\
        + str(gv_cc_sep_dir)\
        + 'ULTIMA_ENTREGA'

        log("v_nm_diretorio_atual_ti >> " + str(v_nm_diretorio_atual_ti))
        log("v_nm_diretorio_ultima_entrega >> " + str(v_nm_diretorio_ultima_entrega))

        if  not os.path.exists(v_nm_diretorio_atual_ti):
            log("ERRO >> Diretorio Inexistente ! " + str(v_nm_diretorio_atual_ti) + " >> ")
            return None

        if  not os.path.exists(v_nm_diretorio_ultima_entrega):
            log("ERRO >> Diretorio Inexistente ! " + str(v_nm_diretorio_ultima_entrega) + " >> ")
            return None

        v_ob_lista_arquivos_diretorio_atual_ti =\
        listaArquivosComparar(v_nm_diretorio_atual_ti)

        v_ob_lista_arquivos_diretorio_ultima_entrega =\
        listaArquivosComparar(v_nm_diretorio_ultima_entrega)

        log("v_ob_lista_arquivos_diretorio_atual_ti >> " + str(v_ob_lista_arquivos_diretorio_atual_ti))
        log("v_ob_lista_arquivos_diretorio_ultima_entrega >> " + str(v_ob_lista_arquivos_diretorio_ultima_entrega))

        if  not v_ob_lista_arquivos_diretorio_atual_ti:
            log("ERRO >> Arquivo Inexistente ! " + str(v_nm_diretorio_atual_ti) + " >> ")
            return None

        if  not v_ob_lista_arquivos_diretorio_ultima_entrega:
            log("ERRO >> Arquivo Inexistente ! " + str(v_nm_diretorio_ultima_entrega) + " >> ")
            return None

        v_LayoutMestre         = 'LayoutMestre'
        v_LayoutCadastro       = 'LayoutCadastro'
        v_dic_campos = dict()

        try:
            if int(gv_ob_dados_serie.get("ano",0)) < 2017: 
                v_LayoutMestre         += '_Antigo'
                v_LayoutCadastro       += '_Antigo'
        except:
            log("ERRO >> Busca Layout_Antigo ! " + str(gv_ob_dados_serie.get("ano",0)) + " >> ")
            return None
        
        log("LayoutMestre >> " + str(v_LayoutMestre ))
        log("LayoutCadastro >> " + str(v_LayoutCadastro ))
        
        try:
            v_dic_campos[v_LayoutMestre] =\
            v_dic_campos.get(v_LayoutMestre, carregaLayout.dic_layouts[v_LayoutMestre]['dic_campos'])
        except:
            log("ERRO >> Carregamento LayoutMestre ! " + str(gv_ob_dados_serie.get("ano",0)) + " >> ")
            return None

        try:
            v_dic_campos[v_LayoutCadastro] =\
            v_dic_campos.get(v_LayoutCadastro, carregaLayout.dic_layouts[v_LayoutCadastro]['dic_campos'])        
        except:
            log("ERRO >> Carregamento LayoutCadastro ! " + str(gv_ob_dados_serie.get("ano",0)) + " >> ")
            return None
        
        log("dic_campos >> " + str(v_dic_campos)) 

        v_diretorio_saida = getattr(configuracoes, 'diretorio_saida',"")
        v_diretorio_saida = str(v_diretorio_saida).replace("<<id_serie>>",p_id_serie).strip()
        
        log("diretorio_saida >> " + str(v_diretorio_saida ))
        
        if  not os.path.exists(v_diretorio_saida):
            log("Diretorio Inexistente ! " + str(v_diretorio_saida) + " >> ")
            os.makedirs(v_diretorio_saida)
        
        v_arq_saida = getattr(configuracoes, 'arq_saida',"")
        v_arq_saida = str(v_arq_saida).replace("<<TIPO>>","M").strip()
        if not v_arq_saida:
            log("ERRO >> Arquivo Inexistente [SAIDA] ! " + str(v_arq_saida) + " >> ")
            return None

        v_arquivo_saida = os.path.join( v_diretorio_saida, v_arq_saida )
        log("v_arquivo_saida >> " + str(v_arquivo_saida ))
        nr_contador_linhas = 0
        v_cc_uf = str(gv_ob_dados_serie.get("uf","")).strip().upper()
        #pos = 28 se int(dados_serie[‘ano’]) >= 2017 senão 10
        v_nr_pos = 28\
        if int(gv_ob_dados_serie.get("ano",0)) >= 2017 else 10        

        # abre o arq_saida_resultado_identicos tipo M para escrita no diretorio_saida
        # 1. cria o arquivo
        #ob_encoding_arquivo_saida = encodingDoArquivoW( v_arquivo_saida )
        fd_arquivo_saida = open(v_arquivo_saida, 'w', newline='')#encoding=ob_encoding_arquivo_saida)
        
        # 2. cria o objeto de gravação
        # w = csv.writer(fd_arquivo_saida)
        
        #Enquanto tiver arquivo em v_lista_atual_ti faça
        while v_ob_lista_arquivos_diretorio_atual_ti:
            # arquivo = v_lista_atual_ti.pop(0)
            # arq_mestre_atual_ti = arquivo
            # arq_cad_atual_ti = arquivo
            v_nm_arquivo = v_ob_lista_arquivos_diretorio_atual_ti.pop(0)
            log("v_nm_arquivo: " + str(v_nm_arquivo))
            #log("v_nr_pos: " + str(v_nr_pos))
            v_nm_arq_mestre_atual_ti = str(v_nm_arquivo)
            v_nm_arq_cad_atual_ti = str(v_nm_arquivo)
            # se arquivo[pos] for igual a ‘M’ faça
            if v_nm_arquivo[v_nr_pos] == "M":
                #log("v_nm_arq_cad_atual_ti: " + str(v_nm_arq_cad_atual_ti))
                v_ob_troca = list(v_nm_arq_cad_atual_ti) 
                v_ob_troca[v_nr_pos] = "D"
                v_nm_arq_cad_atual_ti = ''.join(v_ob_troca)
                #log("v_nm_arq_cad_atual_ti: " + str(v_nm_arq_cad_atual_ti))
                #arq_cad_atual_ti[pos] = ‘D’
                #se arq_cad_atual_ti em v_lista_atual_ti faça
                #   remove da v_lista_atual_ti o arq_cad_atual_ti
                #senão
                #   volte para o inicio do Enquanto ...                
                if v_nm_arq_cad_atual_ti in v_ob_lista_arquivos_diretorio_atual_ti:
                    v_ob_lista_arquivos_diretorio_atual_ti.remove(v_nm_arq_cad_atual_ti)
                else:
                    continue
            elif v_nm_arquivo[v_nr_pos] == "D":#senão se arquivo[pos] for igual a ‘D’ faça
                log("v_nm_arq_mestre_atual_ti: " + str(v_nm_arq_mestre_atual_ti))
                v_ob_troca = list(v_nm_arq_mestre_atual_ti) 
                v_ob_troca[v_nr_pos] = "M"
                v_nm_arq_mestre_atual_ti = ''.join(v_ob_troca)
                log("v_nm_arq_mestre_atual_ti: " + str(v_nm_arq_mestre_atual_ti))
                #arq_mestre_atual_ti[pos] = ‘M’
                #se arq_mestre_atual_ti em v_lista_atual_ti faça
                #    remove da v_lista_atual_ti o arq_mestre_atual_ti
                #senão
                #    volte para o inicio do Enquanto …
                if v_nm_arq_mestre_atual_ti in v_ob_lista_arquivos_diretorio_atual_ti:
                    v_ob_lista_arquivos_diretorio_atual_ti.remove(v_nm_arq_mestre_atual_ti)
                else:
                    continue
            else:#senão faça
                continue#volte para o inicio do Enquanto ..          


            ### Busca o nome do arquivo ultima entrega caso a versão do arquivo seja > 2017
            ##Se int(dados_serie[‘ano’]) >= 2017 faça
            v_nm_arq_mestre_ultima_entrega = ''
            v_nm_arq_cad_ultima_entrega = ''
            if int(gv_ob_dados_serie.get("ano",0)) >= 2017:
                v_mascara_arq_mestre = v_nm_arq_mestre_atual_ti[:25] + '*' + v_nm_arq_mestre_atual_ti[28:] 
                v_mascara_arq_cad = v_nm_arq_cad_atual_ti[:25] + '*' + v_nm_arq_cad_atual_ti[28:] 
                v_nm_arq_mestre_ultima_entrega = ''
                v_nm_arq_cad_ultima_entrega = ''
                for arquivo_ultima_entrega in v_ob_lista_arquivos_diretorio_ultima_entrega:
                    if  fnmatch.fnmatch(arquivo_ultima_entrega,v_mascara_arq_mestre):
                        v_nm_arq_mestre_ultima_entrega = arquivo_ultima_entrega
                
                    if  fnmatch.fnmatch(arquivo_ultima_entrega,v_mascara_arq_cad):
                        v_nm_arq_cad_ultima_entrega = arquivo_ultima_entrega
            #Senão faça
            else:
                if v_nm_arq_mestre_atual_ti in v_ob_lista_arquivos_diretorio_ultima_entrega\
                and v_nm_arq_cad_atual_ti in  v_ob_lista_arquivos_diretorio_ultima_entrega:
                    v_nm_arq_mestre_ultima_entrega = v_nm_arq_mestre_atual_ti
                    v_nm_arq_cad_ultima_entrega = v_nm_arq_cad_atual_ti

            #se arq_mestre_atual_ti em v_lista_ultima_entrega 
            #e arq_cad_atual_ti em v_lista_ultima_entrega faça        
            if v_nm_arq_mestre_ultima_entrega and v_nm_arq_cad_ultima_entrega:
                try:
                    log("****COMPARA****")
                    log("v_nm_arq_mestre_atual_ti",v_nm_arq_mestre_atual_ti)
                    log("v_nm_arq_cad_atual_ti",v_nm_arq_cad_atual_ti)                    
                    
                    log("v_nm_arq_mestre_ultima_entrega",v_nm_arq_mestre_ultima_entrega)
                    log("v_nm_arq_cad_ultima_entrega",v_nm_arq_cad_ultima_entrega)                    
                    
                    #arq_mestre_ultima_entrega = arq_mestre_atual_ti
                    #arq_cad_ultima_entrega = arq_cad_atual_ti
                    # Abri os arquivos    
                    v_arq_mestre_atual_ti = os.path.join( v_nm_diretorio_atual_ti, v_nm_arq_mestre_atual_ti )
                    v_arq_mestre_ultima_entrega = os.path.join( v_nm_diretorio_ultima_entrega, v_nm_arq_mestre_ultima_entrega )
                    v_arq_cad_atual_ti = os.path.join( v_nm_diretorio_atual_ti, v_nm_arq_cad_atual_ti )
                    v_arq_cad_ultima_entrega = os.path.join( v_nm_diretorio_ultima_entrega, v_nm_arq_cad_ultima_entrega )
                    ob_encoding_arq_mestre_atual_ti = encodingDoArquivo( v_arq_mestre_atual_ti )
                    ob_encoding_arq_mestre_ultima_entrega = encodingDoArquivo( v_arq_mestre_ultima_entrega )
                    ob_encoding_arq_cad_atual_ti = encodingDoArquivo( v_arq_cad_atual_ti )
                    ob_encoding_arq_cad_ultima_entrega = encodingDoArquivo( v_arq_cad_ultima_entrega )
                    #abre o v_nm_arq_mestre_atual_ti do diretorio v_diretorio_atual_ti em modo leitura (‘r’)
                    #abre o v_nm_arq_mestre_ultima_entrega do diretorio v_diretorio_ultima_entrega em modo leitura (‘r’)
                    #abre o v_nm_arq_cad_atual_ti do diretorio v_diretorio_atual_ti em modo leitura (‘r’)
                    #abre o v_nm_arq_cad_ultima_entrega do diretorio v_diretorio_ultima_entrega em modo leitura (‘r’)
                    fd_arq_mestre_atual_ti = open(v_arq_mestre_atual_ti, 'r', encoding=ob_encoding_arq_mestre_atual_ti)
                    fd_arq_mestre_ultima_entrega = open(v_arq_mestre_ultima_entrega, 'r', encoding=ob_encoding_arq_mestre_ultima_entrega)
                    fd_arq_cad_atual_ti = open(v_arq_cad_atual_ti, 'r', encoding=ob_encoding_arq_cad_atual_ti)
                    fd_arq_cad_ultima_entrega = open(v_arq_cad_ultima_entrega, 'r', encoding=ob_encoding_arq_cad_ultima_entrega)
                    
                    log("v_nm_arq_cad_atual_ti: " + str(v_nm_arq_cad_atual_ti))
                
                    
                    #contador_linhas = 0
                    #enquanto existir linha faça
                    nr_contador_linhas = 0                    
                    while True:
                        v_linhas_arq_mestre_atual_ti = fd_arq_mestre_atual_ti.readline()    
                        # inicio 
                        # contador_linhas = contador_linhas + 1
                        # linha_mestre_ultima_entrega = próxima linha do arquivo_mestre_ultima_entrega
                        # linha_cad_atual_ti = próxima linha do arquivo_cad_atual_ti
                        # linha_cad_ultima_entrega = próxima linha do arquivo_cad_ultima_entrega
                        # registro_mestre_atual_ti = layout.quebraRegistro(linha, v_layout_arq_mestre)
                        # registro_mestre_ultima_entrega = layout.quebraRegistro(linha_mestre_ultima_entrega, v_layout_arq_mestre)
                        # flag_identico = True                        
                        if not v_linhas_arq_mestre_atual_ti:
                            if not nr_contador_linhas:
                                log("Não foi encontrado dados no arquivo para validação! " + str(v_arq_mestre_atual_ti))
                            break# warning -- not idiomatic Python! See below...
                        
                        v_linhas_arq_mestre_ultima_entrega = fd_arq_mestre_ultima_entrega.readline()
                        v_linhas_arq_cad_atual_ti = fd_arq_cad_atual_ti.readline()
                        v_linhas_arq_cad_ultima_entrega = fd_arq_cad_ultima_entrega.readline() 
                        
                        if not v_linhas_arq_mestre_ultima_entrega:
                            log("Não foi encontrado dados no arquivo para validação! " + str(v_arq_mestre_ultima_entrega))
                            continue
                        if not v_linhas_arq_cad_atual_ti:
                            log("Não foi encontrado dados no arquivo para validação! " + str(v_arq_cad_atual_ti))
                            continue                        
                        if not v_linhas_arq_cad_ultima_entrega:
                            log("Não foi encontrado dados no arquivo para validação! " + str(v_arq_cad_ultima_entrega))
                            continue                                                 
                        
                        nr_contador_linhas += 1
                        registro_arq_mestre_atual_ti =\
                        quebraRegistro(v_linhas_arq_mestre_atual_ti, v_LayoutMestre)                                         
                        registro_arq_mestre_ultima_entrega =\
                        quebraRegistro(v_linhas_arq_mestre_ultima_entrega, v_LayoutMestre)                                         
                        v_fl_flag_identico = True                        
                        #para campo em v_campos_mestre_validar faça
                        #se registro_mestre_atual_ti[campo] diferente de registro_mestre_ultima_entrega[campo] faça
                        #   flag_identico = False
                        v_campos_arq_mestre_atual_ti_validar =\
                        str(registro_arq_mestre_atual_ti[v_dic_campos[v_LayoutMestre]['HASH_CODE_ARQ']-1])
                        v_campos_arq_mestre_ultima_entrega_validar =\
                        str(registro_arq_mestre_ultima_entrega[v_dic_campos[v_LayoutMestre]['HASH_CODE_ARQ']-1])
                        
                        if nr_contador_linhas < 5:
                            log("HASH_CODE_ARQ",v_campos_arq_mestre_atual_ti_validar,v_campos_arq_mestre_ultima_entrega_validar)
                        
                        if v_campos_arq_mestre_atual_ti_validar != v_campos_arq_mestre_ultima_entrega_validar: 
                           v_fl_flag_identico = False

                        # se flag_identico for verdadeiro faça
                        #   registro_cad_atual_ti = layout.quebraRegistro(linha_cad_atual_ti, v_layout_arq_cad)
                        #   registro_cad_ultima_entrega = layout.quebraRegistro(linha_cad_ultima_entrega, v_layout_arq_cad)
                        #   para campo em v_campos_cad_validar faça
                        if v_fl_flag_identico:
                            registro_arq_cad_atual_ti =\
                            quebraRegistro(v_linhas_arq_cad_atual_ti, v_LayoutCadastro)                                         
                            registro_arq_cad_ultima_entrega =\
                            quebraRegistro(v_linhas_arq_cad_ultima_entrega, v_LayoutCadastro)                                         
                            
                            v_campos_arq_cad_atual_ti_validar =\
                            str(registro_arq_cad_atual_ti[v_dic_campos[v_LayoutCadastro]['CodigoAutentRegistro']-1])
                            v_campos_arq_cad_ultima_entrega_validar =\
                            str(registro_arq_cad_ultima_entrega[v_dic_campos[v_LayoutCadastro]['CodigoAutentRegistro']-1])
                        
                            #se registro_cad_atual_ti[campo] diferente de registro_cad_ultima_entrega[campo] faça
                            #   flag_identico = False
                            if v_campos_arq_cad_atual_ti_validar != v_campos_arq_cad_ultima_entrega_validar: 
                                v_fl_flag_identico = False
                            
                            #se flag_identico for verdadeiro faça
                            #   escreve no arq_saida_resultado_identicos 
                            #   os campos de registro_mestre_atual_ti 
                            #   e registro_cad_atual_ti 
                            #   conforme layout de modelo.
                            if v_fl_flag_identico:
                                v_nr_quantidade_autenticos += 1
                                v_ds_cabecalho = ""
                                v_ds_linha     = ""                                
                                
                                for i,(k,v) in enumerate(v_dic_campos[v_LayoutMestre].items()):    
                                    if v_nr_quantidade_autenticos==1:
                                        if not i:
                                            v_ds_cabecalho += '"NOME_ARQUIVO_MESTRE_M_ATUAL"' + ';'
                                            v_ds_cabecalho += '"NOME_ARQUIVO_MESTRE_M_ENTREGUE"' + ';'
                                        v_ds_cabecalho += '"' + str(k) + '_M_ATUAL"' + ';'                                    
                                        v_ds_cabecalho += '"' + str(k) + '_M_ENTREGUE"' + ';'                                    
                                    
                                    if not i:
                                        v_ds_linha += '"' + str(v_nm_arq_mestre_atual_ti) + '"' + ';' 
                                        v_ds_linha += '"' + str(v_nm_arq_mestre_ultima_entrega) + '"' + ';' 
            
                                    v_ds_linha += \
                                        '"' + (str(registro_arq_mestre_atual_ti[int(v)-1])\
                                        .replace(chr(13), "")\
                                        .replace(chr(10), "")\
                                        .replace('\n', '')\
                                        .replace('\r', '')\
                                        .replace(";", "")\
                                        .strip()\
                                        ) + '"' + ';'

                                    v_ds_linha += \
                                        '"' + (str(registro_arq_mestre_ultima_entrega[int(v)-1])\
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
                                        v_ds_linha += '"' + str(v_nm_arq_cad_atual_ti) + '"' + ';' 
                                        v_ds_linha += '"' + str(v_nm_arq_cad_ultima_entrega) + '"' + ';' 

                                    v_ds_linha += \
                                        '"' + (str(registro_arq_cad_atual_ti[int(v)-1])\
                                        .replace(chr(13), "")\
                                        .replace(chr(10), "")\
                                        .replace('\n', '')\
                                        .replace('\r', '')\
                                        .replace(";", "")\
                                        .strip()\
                                        ) + '"' + ';'

                                    v_ds_linha += \
                                        '"' + (str(registro_arq_cad_ultima_entrega[int(v)-1])\
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
                                if v_ds_linha:
                                    fd_arquivo_saida.write(v_ds_linha + '\n')

                        # fim
                  
                    # Feche todos os arquivos abertos
                    fd_arq_mestre_atual_ti.close()
                    fd_arq_mestre_ultima_entrega.close()
                    fd_arq_cad_atual_ti.close()
                    fd_arq_cad_ultima_entrega.close()

                except Exception as err:
                    err_desc_trace = traceback.format_exc()
                    log("ERRO >> TRATAR OS ARQUIVOS >> "\
                    + str(err) + " - TRACE - " + err_desc_trace + " >> ")
                    try:
                        fd_arq_mestre_atual_ti.close()
                    except:
                        pass            
                    try:
                        fd_arq_mestre_ultima_entrega.close()
                    except:
                        pass        
                    try:    
                        fd_arq_cad_atual_ti.close()
                    except:
                        pass        
                    try:    
                        fd_arq_cad_ultima_entrega.close()
                    except:
                        pass
                    try:
                        fd_arquivo_saida.close()
                    except:
                        pass                    
                    return None
            else:
                pass            

        # Recomendado: feche o arquivo
        fd_arquivo_saida.close()
        log("*"*150)
        log("")
        log(" - Quantidade de registros autenticos >> " + str(v_nr_quantidade_autenticos ))
        log(" - Arquivo de saida >> " + str(v_arquivo_saida ))
        log("")
        log("*"*150)
        return v_nr_quantidade_autenticos

    except Exception as err:
        err_desc_trace = traceback.format_exc()
        log("ERRO >> COMPARAR ARQUIVOS >> "\
        + str(err) + " - TRACE - " + err_desc_trace + " >> ")
        try:
           fd_arquivo_saida.close()
        except:
            pass
        try:
            fd_arq_mestre_atual_ti.close()
        except:
            pass            
        try:
            fd_arq_mestre_ultima_entrega.close()
        except:
            pass        
        try:    
            fd_arq_cad_atual_ti.close()
        except:
            pass        
        try:    
            fd_arq_cad_ultima_entrega.close()
        except:
            pass
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        log('#### ERRO ') 
        log('-'* 100)
        log('QUANTIDADE DE PARAMETROS INVALIDA')
        log('-'* 100)
        log('EXEMPLO')
        log('-'* 100)
        log( '%s <ID SERIE LEVANTAMENTO>'%( sys.argv[0] ) )
        log('-'* 100)
        sys.exit(99)
    else:
        v_id_serie = sys.argv[1]
        log("v_id_serie: " + str(v_id_serie))
        v_cc_retorno = comparaMestre(v_id_serie)
        log(v_cc_retorno)
        if v_cc_retorno is None:
            sys.exit(99)
        else:
            sys.exit(0)    