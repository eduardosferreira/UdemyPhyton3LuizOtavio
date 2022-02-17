#!/bin/bash
clear
echo ""
############################ PARAMETROS DE ENTRADA ###################################################
export PROCESSO="${1:-NOME_PROCESSO_99999999}" 
##-------------------------------------------
## Caso o parametro PROCESSO inicie com M2_PORTAL_ o script ira receber os parametros em uma ordem
## diferente (else)
echo $PROCESSO | grep -q -e ^M2_PORTAL_ -e ^NOVO_MAPA_ ;processoPortal=$?
if [ ${processoPortal} -ne 0 ]
then
    export DATA_INICIO="${2:-01/01/2049}"
    export DATA_FIM="${3:-02/01/2049}"
    export FILTRO="${4:-nf.emps_cod= 'TBRA' AND nf.fili_cod = '0000'}" # Filtro de dados SQL.  Exemplo nf.emps_cod= 'TBRA' AND nf.fili_cod = '0001'
    export BASE="${5:-Clone 17}" # Base de dados
    export COMMIT="${6:-ROLLBACK}" # Transacao COMMIT ou ROLLBACK
    export REGRAS_HISTORIAS="${7:-R2015_1,R2015_2,R2015_4,R2015_7,R2015_9,R2015_19,R2015_14,R2015_34,CFOP_0000,R2015_6, ERRO_122,R2015_31}" # Regras de execucao. Exemplo: R2015_1,R2015_2,R2015_4,R2015_7,R2015_9,R2015_19,R2015_14,R2015_34,CFOP_0000,R2015_6, ERRO_122,R2015_31
    export PERIODO="${DATA_INICIO:3}"
    export STATUS_PROCESSO="${8:-Erro,Aguardando,Reprocessar,Em Processamento}" # Status para reprocessamento: Erro,Aguardando,Reprocessar,Em Processamento
    export TABELA_NF="${9:-MESTRE_NFTL_SERV}"
    export TABELA_INF="${10:-ITEM_NFTL_SERV}"
    export SCRIPT_REGRA_BEFORE_01="${11:-}"
    export SCRIPT_REGRA_AFTER_01="${12:-}"
    export DBLINK1="${13:-}"
else 
    ## Parametros passados pelo "Portal de Execuções"
    export REGRAS_HISTORIAS="${2:-R2015_1,R2015_2,R2015_4,R2015_7,R2015_9,R2015_19,R2015_14,R2015_34,CFOP_0000,R2015_6, ERRO_122,R2015_31}" # Regras de execucao. Exemplo: R2015_1,R2015_2,R2015_4,R2015_7,R2015_9,R2015_19,R2015_14,R2015_34,CFOP_0000,R2015_6, ERRO_122,R2015_31
    export COMMIT="${3:-ROLLBACK}" # Transacao COMMIT ou ROLLBACK
    empresa=${4// /}
    filial=${5// /}
    serie=${6//, /,}
    serie=${serie// ,/,}
    modelo=${7// /}
    nota=${8// /}
    sql=${9}

    export PERIODO="${10:-01/2049}"

    ###################### MONTANDO O CAMPO FILTRO.
    if [ "${sql}" == '' ]; then
      ### IF((ISBLANK(L4));"1=1";L4);
      FILTRO='1=1'
    else
      FILTRO=${sql}
    fi
    
    if [ "${serie}" != '' ]; then
      ### IF((ISBLANK(I4));"";" AND nf.mnfst_serie IN ('"&(SUBSTITUTE(I4;",";"','"))&"')");
      FILTRO="${FILTRO} AND nf.mnfst_serie IN ('${serie//,/','}')"
    fi

    if [ "${nota}" != '' ]; then
      ### IF((ISBLANK(K4));"";" AND nf.mnfst_num IN ('"&SUBSTITUTE(SUBSTITUTE(K4;",";"','");" ";"")&"')");
      FILTRO="${FILTRO} AND nf.mnfst_num IN ('${nota//,/','}')"
    fi
  
    if [ "${empresa}" != '' ]; then
      ### IF((ISBLANK(G4));"";" AND nf.emps_cod IN ('"&SUBSTITUTE(SUBSTITUTE(G4;",";"','");" ";"")&"')");
      FILTRO="${FILTRO} AND nf.emps_cod IN ('${empresa//,/','}')"
    fi

    if [ "${modelo}" != '' ]; then
      ### IF((ISBLANK(J4));"";" AND nf.mdoc_cod IN ('"&SUBSTITUTE(SUBSTITUTE(J4;",";"','");" ";"")&"')");
      FILTRO="${FILTRO} AND nf.mdoc_cod IN ('${modelo//,/','}')"
    fi
    
    if [ "${filial}" != '' ]; then
      ### IF((ISBLANK(H4));""&""&""&CHAR(34);" AND nf.fili_cod IN ('"&SUBSTITUTE(SUBSTITUTE(H4;",";"','");" ";"")&"')"&CHAR(34));
      FILTRO="${FILTRO} AND nf.fili_cod IN ('${filial//,/','}')"
    fi

    export FILTRO
    # export FILTRO="${5:-nf.emps_cod= 'TBRA' AND nf.fili_cod = '0000'}" # Filtro de dados SQL.  Exemplo nf.emps_cod= 'TBRA' AND nf.fili_cod = '0001'
    #################################################################################################

    export DATA_INICIO="01/${PERIODO}"
    mes=$(echo ${PERIODO} | cut -d'/' -f1)
    ano=$(echo ${PERIODO} | cut -d'/' -f2)
    dia=$(echo $(cal ${mes} ${ano} ) | awk '{ printf($NF)} ')
    export DATA_FIM="${dia}/${PERIODO}"

    ip=$( ifconfig | grep 10.238.10 | awk -F '10.238.10.' '{print($2)}' | cut -d' ' -f1 | head -1 )
    if [[ ${ip} -eq 208 ]]
    then
      export BASE="CLONE1" # Base de dados
    fi
    if [[ ${ip} -eq 209 ]]
    then
      export BASE="CLONE2" # Base de dados
    fi
    if [[ ${ip} -eq 174 ]]
    then
      export BASE="CLONE5" # Base de dados
    fi
    if [[ ${ip} -eq 210 ]]
    then
      export BASE="CLONE6" # Base de dados
    fi
    if [[ ${ip} -eq 109 ]]
    then
      export BASE="CLONE7" # Base de dados
    fi

    export STATUS_PROCESSO="${11:-Erro,Aguardando,Reprocessar,Em Processamento}" # Status para reprocessamento: Erro,Aguardando,Reprocessar,Em Processamento
    export TABELA_NF="${12:-MESTRE_NFTL_SERV}"
    export TABELA_INF="${13:-ITEM_NFTL_SERV}"
    export SCRIPT_REGRA_BEFORE_01="${14:-}"
    export SCRIPT_REGRA_AFTER_01="${15:-}"
    export DBLINK1="${16:-}"
fi

export REGRAS_HISTORIAS="${REGRAS_HISTORIAS^^}" 
export REGRAS_HISTORIAS="${REGRAS_HISTORIAS##*( )}" 
export REGRAS_HISTORIAS="${REGRAS_HISTORIAS%%*( )}"
# echo "${REGRAS_HISTORIAS}"
case "${REGRAS_HISTORIAS}" in
	*"NOVO_MAPA"*|*"novo_mapa"*|*"ABERTURA_MES"*|*"abertura_mes"*)
	    # echo "1"
		export SCRIPT_REGRA_BEFORE_01="185/MAP_2_REGRA_ISOLADAS/MAP_2_REGRA_46"
		# export SCRIPT_REGRA_AFTER_01="185/MAP_2_REGRA_ISOLADAS/MAP_2_REGRA_31_AJUSTE_TP_UTILIZACAO_MESTRE"
		export PARAMETRO_SANEA_PARTICAO="185/MAP_2_REGRA_UNIFICADO/MAP_2_REGRA_UNICO"
	;;
	*"ISOLADAS/"*|*"isoladas/"*)
		# echo "2"
		export PARAMETRO_SANEA_PARTICAO="${REGRAS_HISTORIAS}"
	;;
	*)	
		# echo "3"
		export REGRAS_HISTORIAS="${REGRAS_HISTORIAS^^}" 
	;;
esac
############################ PARAMETROS DIVERSOS ###################################################
export SCRIPT="sanea_particao"
export TABELA_CONTROLE="gfcadastro.CONTROLE_PROCESSAMENTO"
export FIND="'"
export REPLACE="''"
export DIRETORIO_PRINCIPAL="185/MAP_2_REGRA_UNIFICADO/"
export DIRETORIO_LOG="log/${PROCESSO}"
mkdir -p ${DIRETORIO_LOG} 2> /dev/null
# Variavel para tornar padrao o diretorio RAIZ, mesmo nos ambientes dos fornecedores
DIRNAME_0=`dirname $0`
DATA=`date +%Y%m%d%H%M%S`
BASENAME_0=`basename $0 | awk -F. '{print $1}'`
TIPO_ARQ="SQL"
DIR_SISTEMA=`dirname $0`
DIR_LOGS=`dirname $0`
# Define o arquivo de log
ARQ_LOG="${DIRETORIO_LOG}/${BASENAME_0}_${DATA}_${PROCESSO}.log"

#
# Programa para gravar mensagens no arquivo de LOG
#
Log ()
####################################################################################################
{
echo " $(date +'%Y-%m-%d %H:%M:%S')-> $1" | tee -a ${ARQ_LOG}
}

#
# Funcao para gravar de inicio de processamento
#
fc_inicio()
{
Log "**************************************************************************************************************************"
Log " Inicio de Processamento - ${BASENAME_0}.sh"
Log " Diretorio principal: ${DIR_SISTEMA}"
Log "**************************************************************************************************************************"
Log " "
}

#
# Funcao para sair do shell script
#
fc_saida()
{

COD_ERRO=$1
if [[ ${COD_ERRO} -eq 0 ]]
then
  sts='SUCESSO'
else
  sts='ERRO'
fi

Log " "
Log "**************************************************************************************************************************"
Log " Fim de Processamento - ${BASENAME_0}.sh"
Log " Status final.: ${sts}"
Log " Return Code .: ${COD_ERRO} "
Log "**************************************************************************************************************************"
Log " "

exit ${COD_ERRO}
}

#
# Verificacao de ocorrencia de erro durante a execucao do comando no banco de dados
#
fc_verifica_execucao_sql()
{
if [ ! -r $1 ]
then
   Log " ERRO: Nao existe LOG de comando SQL para ser verificado"
   return 102
fi
if test $(grep -c "CONEXAO BANCO DE DADOS - OK" $1) -ne 0
then
   Log " Conexao com Banco de Dados realizada com sucesso"
   return 0
fi
if test $(grep -c "invalid username/password" $1) -ne 0
then
   Log " ERRO: PCP favor verificar usuario / senha de conexao de banco de dados"
   return 201
fi
if test $(grep -c "no listener " $1) -ne 0
then
   Log " ERRO: PCP verificar se o BD esta ativo - direcionar para equipe de DBAs"
   return 202
fi
if test $(grep -c "ORA-20999" $1) -ne 0
then
   Log " ERRO: enviar LOG para equipe de DESENVOLVIMENTO"
   return 901
fi
if test $(grep -c "SQL\*Loader-" $1) -ne 0
then
   Log " ERRO: na execucao do comando SQLLDR. Favor acionar o Analista da equipe de DESENVOLVIMENTO"
   return 203
fi
if test $(grep -c "ORA-" $1) -ne 0
then
   Log " ERRO no comando de Banco de Dados (nao catalogado). Favor acionar o Analista da equipe de DESENVOLVIMENTO"
   return 203
fi
Log " Verificacao de LOG SQL nao encontrou erros no arquivo: $1"
return 0
}

#
# Teste de Conexao - Banco de Dados
#
fc_test_conn_DB()
{
sleep 1
DATA_LOG_SQL=`date +%Y%m%d%H%M%S`
ARQ_LOG_SQL="${DIR_LOGS}/${BASENAME_0}_${TIPO_ARQ}_${DATA_LOG_SQL}_sql_${PROCESSO}.log"

sqlplus -silent << FIM >> $ARQ_LOG_SQL
${BD_CONN}

set serveroutput on size 1000000
declare
   --
   v_erro varchar2(200);
   --
begin
   --
   select 'CONEXAO BANCO DE DADOS - OK'
     into  v_erro
     from  dual;
   dbms_output.put_line(v_erro);
   --
end;
/
FIM

# Verifica erro na execucao do comando sql
fc_verifica_execucao_sql $ARQ_LOG_SQL
CD_ERRO=$?

if [ ${CD_ERRO} -ne 0 ]
then
   fc_saida ${CD_ERRO}
fi
}


#
# Executa como nohup
#
fc_execute_nohup()
{
ARQ_EXE=$1
# Verifica se parametro de entrada foi passado
if [ $# -lt 1 ]
then
    Log " "
    Log " ERRO - Parametros nao foi informado corretamente ! ${ARQ_EXE}"
    Log " "
    fc_saida 101
fi    
sleep 1
DATA_LOG_SQL=`date +%Y%m%d%H%M%S`
ARQ_LOG_SQL="${DIR_LOGS}/${BASENAME_0}_${TIPO_ARQ}_${DATA_LOG_SQL}_sql_${PROCESSO}.log"
ARQ_LOG_ERR="${DIR_LOGS}/${BASENAME_0}_${TIPO_ARQ}_${DATA_LOG_SQL}_err_${PROCESSO}.err"
nohup sqlplus ${BD_CONN} @${ARQ_EXE} > ${ARQ_LOG_SQL} 2> ${ARQ_LOG_ERR} &
CD_ERRO=$?
if [ ${CD_ERRO} -ne 0 ]
then
    Log " "
    Log " ERRO durante acionamento SQL ! ${ARQ_EXE}"
    Log " "
    fc_saida 101
fi
sleep 10
# Verifica erro na execucao do comando sql
fc_verifica_execucao_sql $ARQ_LOG_SQL
CD_ERRO=$?

if [ ${CD_ERRO} -ne 0 ]
then
   fc_saida ${CD_ERRO}
fi
}

#
# Executa normalmente
#
fc_execute()
{
ARQ_EXE=$1
# Verifica se parametro de entrada foi passado
if [ $# -lt 1 ]
then
    Log " "
    Log " ERRO - Parametros nao foi informado corretamente ! ${ARQ_EXE}"
    Log " "
    fc_saida 101
fi    
sleep 1
DATA_LOG_SQL=`date +%Y%m%d%H%M%S`
ARQ_LOG_SQL="${DIR_LOGS}/${BASENAME_0}_${TIPO_ARQ}_${DATA_LOG_SQL}_sql_${PROCESSO}.log"
ARQ_LOG_ERR="${DIR_LOGS}/${BASENAME_0}_${TIPO_ARQ}_${DATA_LOG_SQL}_err_${PROCESSO}.err"
sqlplus -silent << FIM >> $ARQ_LOG_SQL
${BD_CONN}

@$ARQ_EXE
FIM
# Verifica erro na execucao do comando sql
fc_verifica_execucao_sql $ARQ_LOG_SQL
CD_ERRO=$?

if [ ${CD_ERRO} -ne 0 ]
then
   fc_saida ${CD_ERRO}
fi
}

trap '

if [ ${processoPortal} -ne 0 ]
then
  rm -f ./.alive.pid
fi

echo ""
echo "*****************************************************************************************************************************************************"
echo "---> DISPLAY DOS LOGS GERADOS PELA EXECUCAO !!!!! "
echo ""
mkdir -p ${DIRETORIO_LOG} 2> /dev/null
diretorios=( "./" "${DIRETORIO_PRINCIPAL}" "185/MAP_2_REGRA_UNIFICADO/" "185/MAP_2_REGRA_ISOLADAS/" )
IFS="
"
for i in $( seq ${#diretorios[@]})
do
    for arq in $(ls ${diretorios[i-1]}*${PROCESSO}.* 2> /dev/null | grep -e .log$ -e .err$ -e .spool$ )
    do
        echo "=========================================================================================================================="
        echo "## Conteudo do arquivo : ${arq}"
        for linha in $(cat ${arq})
        do
            echo "# >> ${linha}"
        done
        echo "##########################################################################################################################"
        echo ""
        mv -f ${arq} ${DIRETORIO_LOG} 2> /dev/null
    done
done
echo ""
echo "---> Fim do(s) log(s) ... "
echo "*****************************************************************************************************************************************************"

echo "*****************************************************************************************************"
echo "-- Resultado final :"
IFS="
"
for lin in ${RES_FINAL}
do
    echo "${lin}"
done

echo "*****************************************************************************************************"

' EXIT ERR INT TERM

# Grava log de inicio de execucao
fc_inicio

#### Codigo abaixo caso a execucao nao seja feita pelo portal 
#### ele valida se ja existe execucoes partindo do mesmo diretorio
#### caso sim o processo da um kill no processo executado anteriormente.
if [ ${processoPortal} -ne 0 ]
then

  if [ -f ./.alive.pid ]; then
    PID_PROCESSO=$( cat ./.alive.pid )
    kill -0 ${PID_PROCESSO} 2> /dev/null
    sleep 10
    if [ $? -eq 0 ]; then
      Log " ***** Processo ja em execucao com PID [{$PID_PROCESSO}] ************** " 
      # fc_saida 1
    fi
    rm -f ./.alive.pid
  fi
  echo $$ > ./.alive.pid
fi
##########################################################################################
case ${BASE} in
    "GFCLONE7"|"GF_CLONE_DEV7"|"GFPRODC7"|"7"|"10.238.45.228"|"svc_gfprodc7"|"Clone 7"|"Clone7"|"CLONE 7"|"CLONE7")
        export STRING_CONEXAO="gfcadastro/vivo2019@10.238.45.230/svc_gfprodc7"
        export STRING_HOST="10.238.10.109"
        export NRO_BASE="7"
    ;;
    "GFCLONE6"|"GF_CLONE_DEV6"|"GFPRODC6"|"6"|"10.238.45.227"|"svc_gfprodc6"|"Clone 6"|"Clone6"|"CLONE 6"|"CLONE6")
        export STRING_CONEXAO="gfcadastro/vivo2019@10.238.45.230/svc_gfprodc6"
        export STRING_HOST="10.238.10.210"
        export NRO_BASE="6"
    ;;
    "GFREAD"|"GF_CLONE_GFREAD"|"READ")
        export STRING_CONEXAO="GFREAD/vivo2019@10.238.10.173/gfread"
        export STRING_HOST="10.238.10.174"
        export NRO_BASE="5"
    ;;    
    "GFREAD_OPENRISOW"|"GF_CLONE_GFREAD_OPENRISOW"|"READ_OPENRISOW")
        export STRING_CONEXAO="OPENRISOW/OPENRISOW@10.238.10.173/gfread"
        export STRING_HOST="10.238.10.174"
        export NRO_BASE="5"
    ;;
    "GFCLONE5"|"GF_CLONE_DEV5"|"DEV5"|"5"|"10.238.10.173"|"Clone 5"|"Clone5"|"CLONE 5"|"CLONE5")
        export STRING_CONEXAO="gfcadastro/vivo2019@10.238.10.173/gfprod"
        export STRING_HOST="10.238.10.174"
        export NRO_BASE="5"
    ;;
    "GF_CLONE_DEV2"|"DEV2"|"2"|"10.238.10.207"|"GFCLONEPREPROD"|"Clone 2"|"Clone2"|"CLONE 2"|"CLONE2")
        export STRING_CONEXAO="gfcadastro/vivo2019@10.238.10.207/gfprod"
        export STRING_HOST="10.238.10.209"
        export NRO_BASE="2"
    ;;
    "GF_CLONE_DEV"|"DEV"|"1"|"10.238.10.106"|"GFCLONEDEV"|"Clone 1"|"Clone1"|"CLONE 1"|"CLONE1")
        export STRING_CONEXAO="gfcadastro/vivo2019@10.238.10.106/gfprod"
        export STRING_HOST="10.238.10.208"
        export NRO_BASE="1"
    ;;
    *)
        export STRING_CONEXAO="gfcadastro/vivo2019@${BASE}/gfprod"
        export STRING_HOST="${BASE}"
        export NRO_BASE="0"
    ;;
esac
export BD_CONN="${STRING_CONEXAO}"

Log " -------------------------------------------------------------------------------- "
Log "PROCESSO ....................: ${PROCESSO} "
Log "PERIODO DE PROCESSAMENTO ....: ${PERIODO} "
Log "DATA_INICIO .................: ${DATA_INICIO} "
Log "DATA_FIM ....................: ${DATA_FIM} "
Log "FILTRO ......................: ${FILTRO} "
Log "BASE ........................: ${BASE} "
Log "COMMIT ......................: ${COMMIT} "
Log "REGRAS_HISTORIAS ............: ${REGRAS_HISTORIAS} "
Log "STATUS_PROCESSO .............: ${STATUS_PROCESSO} "
Log "TABELA_NF ...................: ${TABELA_NF} "
Log "TABELA_INF ..................: ${TABELA_INF} "
Log "SCRIPT_REGRA_BEFORE_01 ......: ${SCRIPT_REGRA_BEFORE_01} "
Log "SCRIPT_REGRA_AFTER_01 .......: ${SCRIPT_REGRA_AFTER_01} "
Log "DBLINK1 .....................: ${DBLINK1} "
Log "PARAMETRO_SANEA_PARTICAO ....: ${PARAMETRO_SANEA_PARTICAO} "
Log "QTDE_MAX_PROCESSOS ..........: ${QTDE_MAX_PROCESSOS} "
Log "SCRIPT ......................: ${SCRIPT} "
Log "TABELA_CONTROLE .............: ${TABELA_CONTROLE} "
Log "FIND ........................: ${FIND} "
Log "REPLACE .....................: ${REPLACE} "
Log "DIRETORIO_PRINCIPAL .........: ${DIRETORIO_PRINCIPAL} "
Log "DIRETORIO_LOG ...............: ${DIRETORIO_LOG} "
user=$( echo ${STRING_CONEXAO} | cut -d'/' -f1 )
base=$( echo ${STRING_CONEXAO} | cut -d'@' -f2 )
Log "STRING_CONEXAO ..............: ${user}/********@${base} "
Log "STRING_HOST .................: ${STRING_HOST} "
Log "NRO_BASE ....................: ${NRO_BASE} "
Log " -------------------------------------------------------------------------------- "

if [[ ! ${BASE} ]]
then
  Log "#####################################################################"
  Log "### BASE de dados não encontrada, verifique ..."
  Log "#####################################################################"
  fc_saida 1
fi

Log " ---- INICIO TESTE CONEXAO COM BD --- "
fc_test_conn_DB
Log " ---- FIM    TESTE CONEXAO COM BD --- "
Log " -------------------------------------------------------------------------------- "

Log " ---- INICIO : BUSCA ID DO CONTROLE DE EXECUCAO --- "
export SEQUENCE_CONTROLE=$( 
sqlplus -S -m 'csv on delimiter ; QUOTE OFF' /nolog <<@EOF
CONNECT ${STRING_CONEXAO}
SET SERVEROUTPUT ON SIZE 1000000;
set heading off
set feedback off
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
select GFCADASTRO.tcsq_kyros_process_log.nextval as sq_kyros_process_log from dual; 
@EOF
exit $?)
RETORNO=$?

#### PARA FIM DE TESTE DE EXECUCAO ... SO EXECUTA ATE ESTA LINHA ..
# fc_saida ${RETORNO}

if [ ${RETORNO} -eq 0 ]
then
    echo ${SEQUENCE_CONTROLE} | grep -q -e 'ERROR' -e 'ORA-'; res=$?
    if [ ${res} -eq 0 ]
    then
        echo ""
        echo "########################################################################"
        echo '###  Problemas com a conexão do banco de dados :'
        echo '###'
        user=$( echo ${STRING_CONEXAO} | cut -d'/' -f1 )
        base=$( echo ${STRING_CONEXAO} | cut -d'@' -f2 )
        echo "###  String de conexao ..: ${user}/********@${base}"
        echo "###  Favor validar esses dados ..."
        echo "########################################################################"
        echo "Erro retornado :"
        echo "${SEQUENCE_CONTROLE}"
        fc_saida 1    
    fi
fi

if [ ${RETORNO} -ne 0 ]; then
    printf "Execucao interrompida!\nFavor olhar a sequence : ${TABELA_CONTROLE}!\n"
    printf "${SEQUENCE_CONTROLE}!\n"
    fc_saida 1
elif [ ${SEQUENCE_CONTROLE} -eq 0 ]; then
    Log "***ATENCAO: Favor olhar : ${SEQUENCE_CONTROLE}! **** "
    fc_saida 1
fi
Log " ---- FIM [${RETORNO}]: BUSCA ID DO CONTROLE DE EXECUCAO >> ${SEQUENCE_CONTROLE} --- "
Log " -------------------------------------------------------------------------------- "



Log " ---- INICIO : GERACAO DA CARGA DOS REGISTROS DE CONTROLE DE EXECUCAO --- "

export SPOOL_FILE=./${SCRIPT}_CONTROLE_PROCESSAMENTO_${PROCESSO}.spool
sqlplus -S -m 'csv on delimiter ; QUOTE OFF' /nolog <<@EOF
CONNECT ${STRING_CONEXAO}
SET define OFF;
SET serveroutput ON size 1000000;
SET timing ON;
SPOOL  ${SPOOL_FILE} 
var exit_code NUMBER = 0
whenever oserror EXIT 1;
whenever sqlerror EXIT 2;
BEGIN

    IF NVL(LENGTH(TRIM('${STATUS_PROCESSO}')),0) > 0  
    THEN
    
        UPDATE gfcadastro.CONTROLE_PROCESSAMENTO 
        SET    ST_PROCESSAMENTO                = 'Reprocessar' 
            ,  ID_PROCESS                      = ${SEQUENCE_CONTROLE} 
            ,  DT_INI_PROC                     = NULL
            ,  DT_FIM_PROC                     = NULL
            ,  QT_ATUALIZADOS_CLI              = 0
            ,  QT_ATUALIZADOS_COMP             = 0
            ,  QT_ATUALIZADOS_NF               = 0
            ,  QT_ATUALIZADOS_INF              = 0
            ,  QT_CAD_NAO_ENCONTRADO           = 0
            ,  DS_MSG_ERRO                     = NULL 
            ,  DS_FILTRO                       = SUBSTR('${FILTRO//$FIND/$REPLACE}',1,4000)
            ,  DS_TRANSACAO                    = SUBSTR('${COMMIT//$FIND/$REPLACE}',1,100)
            ,  DS_REGRAS                       = SUBSTR('${REGRAS_HISTORIAS//$FIND/$REPLACE}',1,4000)
            ,  DS_OUTROS_PARAMETROS            = SUBSTR('STATUS_PROCESSO:${STATUS_PROCESSO//$FIND/$REPLACE}|TABELA_NF:${TABELA_NF//$FIND/$REPLACE}|TABELA_INF:${TABELA_INF//$FIND/$REPLACE}|SCRIPT_REGRA_BEFORE_01:${SCRIPT_REGRA_BEFORE_01//$FIND/$REPLACE}|SCRIPT_REGRA_AFTER_01:${SCRIPT_REGRA_AFTER_01//$FIND/$REPLACE}',1,4000)
        WHERE  DT_LIMITE_INF_NF BETWEEN TO_DATE('${DATA_INICIO}','DD/MM/YYYY') AND TO_DATE('${DATA_FIM}','DD/MM/YYYY') 
        AND    UPPER(TRIM(NM_PROCESSO)) = UPPER(TRIM('${PROCESSO}'))
        AND    UPPER(TRIM(TRANSLATE('${STATUS_PROCESSO}','^ ','^'))) LIKE '%' || UPPER(TRIM(TRANSLATE(ST_PROCESSAMENTO,'^ ','^'))) || '%';
    
    ELSE
    
        DELETE FROM gfcadastro.CONTROLE_PROCESSAMENTO 
        WHERE dt_limite_inf_nf BETWEEN TO_DATE('${DATA_INICIO}','DD/MM/YYYY') AND TO_DATE('${DATA_FIM}','DD/MM/YYYY') 
        AND UPPER(TRIM(NM_PROCESSO)) = UPPER(TRIM('${PROCESSO}'));
    
    END IF;

    INSERT INTO gfcadastro.CONTROLE_PROCESSAMENTO
        (
          NM_PROCESSO,
          NM_PARTICAO_NF ,
          DT_LIMITE_INF_NF ,
          DT_LIMITE_SUP_NF ,
          QT_REGISTROS_NF ,
          NM_PARTICAO_INF ,
          DT_LIMITE_INF_INF ,
          DT_LIMITE_SUP_INF ,
          QT_REGISTROS_INF ,
          ST_PROCESSAMENTO,
          DT_INI_PROC,
          DT_FIM_PROC,
          QT_ATUALIZADOS_CLI,
          QT_ATUALIZADOS_COMP,
          QT_ATUALIZADOS_NF,
          QT_ATUALIZADOS_INF,
          DS_MSG_ERRO,
          DS_FILTRO,
          DS_TRANSACAO,
          DS_REGRAS,
          DS_OUTROS_PARAMETROS,
          ID_PROCESS
        )
WITH partition_data AS
  (SELECT TRIM(upper(tmp_xml.base_data)) base_data ,
    TRIM(upper(tmp_xml.table_owner)) table_owner ,
    TRIM(upper(tmp_xml.table_name)) table_name ,
    to_date(SUBSTR(TRIM(regexp_replace(tmp_xml.high_value, '[a-zA-Z|_|( |)|'']')), 1, 10), 'YYYY-MM-DD') - 1 date_partition_data_value ,
    tmp_xml.partition_name
  FROM
    (SELECT dbms_xmlgen.getxmltype(q'[SELECT 'CURRENT' base_data, p.table_owner, p.table_name, p.high_value, p.partition_name  FROM   all_part_key_columns k,all_tab_cols c, all_tab_partitions p WHERE  k.owner = c.owner  AND    k.column_name = c.column_name   AND    k.name = c.table_name  AND    k.owner = p.table_owner AND    k.name = p.table_name AND    (c.data_type = 'DATE' or c.data_type like 'TIMESTAMP%') AND TRIM(upper(k.owner)) = 'OPENRISOW' AND TRIM(upper(k.name)) IN ('${TABELA_INF}','${TABELA_NF}')]' ) AS xml
    FROM dual
    UNION ALL
    SELECT dbms_xmlgen.getxmltype(q'[SELECT  null base_data, null as table_owner, null as table_name, null as high_value,null as partition_name  FROM   dual]' ) AS xml
    FROM dual
      -- UNION ALL
      -- SELECT dbms_xmlgen.getxmltype(q'[SELECT  /*+ DRIVING_SITE(p1) OTHER */ 'OTHER' base_data, p.table_owner, p.table_name, p.high_value, p.partition_name
      -- FROM   all_part_key_columns@gfread k, all_tab_cols@gfread c, all_tab_partitions@gfread p , dual p1
      -- WHERE  k.owner = c.owner AND    k.column_name = c.column_name AND    k.name = c.table_name AND    k.owner = p.table_owner AND    k.name = p.table_name AND    (c.data_type = 'DATE' or c.data_type like 'TIMESTAMP%')
      -- AND TRIM(upper(k.owner)) = 'OPENRISOW' AND TRIM(upper(k.name)) IN ('${TABELA_INF}','${TABELA_NF}')
      -- ]') AS xml
      --            FROM  dual
    ) tmp,
    XMLTABLE ( '/ROWSET/ROW' PASSING tmp.xml COLUMNS base_data VARCHAR2(30) PATH '/ROW/BASE_DATA', table_owner VARCHAR2(30) PATH '/ROW/TABLE_OWNER', table_name VARCHAR2(30) PATH '/ROW/TABLE_NAME', high_value VARCHAR2(30) PATH '/ROW/HIGH_VALUE', partition_name VARCHAR2(30) PATH '/ROW/PARTITION_NAME' ) tmp_xml
  WHERE ( LENGTH(TRIM(regexp_replace(tmp_xml.high_value, '[a-zA-Z|_|( |)|'']'))) = 18
  OR LENGTH(TRIM(regexp_replace(tmp_xml.high_value, '[a-zA-Z|_|( |)|'']')))      = 10 )
  ) ,
  tmp_data AS
  (SELECT A.base_data,
    TRUNC(A.date_partition_data_value) dt_limite_inf_nf,
    TO_CHAR(A.date_partition_data_value, 'YYYY-MM-DD') date_partition_data_value,
    A.partition_name,
    A.table_name
  FROM partition_data A
  WHERE A.base_data               IS NOT NULL
  AND A.date_partition_data_value >= TO_DATE('${DATA_INICIO}','DD/MM/YYYY')
  AND A.date_partition_data_value  < TO_DATE('${DATA_FIM}','DD/MM/YYYY')+1
  ) ,
  tmp_pivot1 AS
  (SELECT                                                          *
  FROM tmp_data PIVOT ( MAX ( partition_name ) FOR ( table_name ) IN ( '${TABELA_NF}' nm_particao_nf, '${TABELA_INF}' nm_particao_inf ) )
  ORDER BY dt_limite_inf_nf --,    base_data;
  ) ,
  tmp_pivot2 AS
  (SELECT                                                                                                             *
  FROM tmp_pivot1 PIVOT ( MAX ( nm_particao_nf ) particao_nf, MAX ( nm_particao_inf ) particao_inf FOR ( base_data ) IN ( 'OTHER' other, 'CURRENT' actual ) )
  WHERE other_particao_nf                                                                                            IS NULL
  ORDER BY dt_limite_inf_nf
    -- DT_LIMITE_INF_NF, OTHER_PARTICAO_NF, OTHER_PARTICAO_INF, ACTUAL_PARTICAO_NF, ACTUAL_PARTICAO_INF
  ),
  datas AS
  (SELECT TO_DATE('${DATA_INICIO}','DD/MM/YYYY')+(ROWNUM-1) DATA
  FROM DUAL
    CONNECT BY level <= (TO_DATE('${DATA_FIM}','DD/MM/YYYY')-TO_DATE('${DATA_INICIO}','DD/MM/YYYY'))+1
  ORDER BY 1
  )
SELECT DISTINCT UPPER(TRIM('${PROCESSO}')),
        A.ACTUAL_PARTICAO_NF AS NM_PARTICAO_NF ,
        B.DATA AS DT_LIMITE_INF_NF ,
        B.DATA AS DT_LIMITE_SUP_NF ,
        1 AS QT_REGISTROS_NF ,
        A.ACTUAL_PARTICAO_INF AS NM_PARTICAO_INF ,
        B.DATA AS DT_LIMITE_INF_INF ,
        B.DATA AS DT_LIMITE_SUP_INF ,
        1 AS ,
        'Aguardando'  AS ST_PROCESSAMENTO,
        NULL          AS DT_INI_PROC,
        NULL          AS DT_FIM_PROC,
        0             AS QT_ATUALIZADOS_CLI,
        0             AS QT_ATUALIZADOS_COMP,
        0             AS QT_ATUALIZADOS_NF,
        0             AS QT_ATUALIZADOS_INF,
        NULL          AS DS_MSG_ERRO,
        SUBSTR('${FILTRO//$FIND/$REPLACE}',1,4000)                AS DS_FILTRO,
        SUBSTR('${COMMIT//$FIND/$REPLACE}',1,100)                 AS DS_TRANSACAO,
        SUBSTR('${REGRAS_HISTORIAS//$FIND/$REPLACE}',1,4000)      AS DS_REGRAS,
        SUBSTR('STATUS_PROCESSO:${STATUS_PROCESSO//$FIND/$REPLACE}|TABELA_NF:${TABELA_NF//$FIND/$REPLACE}|TABELA_INF:${TABELA_INF//$FIND/$REPLACE}|SCRIPT_REGRA_BEFORE_01:${SCRIPT_REGRA_BEFORE_01//$FIND/$REPLACE}|SCRIPT_REGRA_AFTER_01:${SCRIPT_REGRA_AFTER_01//$FIND/$REPLACE}',1,4000) AS DS_OUTROS_PARAMETROS
      , ${SEQUENCE_CONTROLE} AS SEQUENCE_CONTROLE
FROM datas B
LEFT JOIN tmp_pivot2 A
ON (B.DATA                 = A.DT_LIMITE_INF_NF
AND (A.ACTUAL_PARTICAO_NF IS NOT NULL
AND A.ACTUAL_PARTICAO_INF IS NOT NULL))
WHERE NOT EXISTS (SELECT 1 
FROM gfcadastro.CONTROLE_PROCESSAMENTO  B1 
WHERE B1.NM_PROCESSO = '${PROCESSO}' 
AND B1.DT_LIMITE_INF_NF = B.DATA )
ORDER BY B.DATA
;
        
  INSERT INTO GFCADASTRO.TCTB_KYROS_PROCESS (
      ID_IDENTIFY         
    , CC_PROCESS             
    , DT_VAR01            
    , DT_VAR02
    , DS_VAR01              
    , DS_VAR02              
    , DS_VAR03              
    , DS_VAR04)
  VALUES (${SEQUENCE_CONTROLE} 
       , UPPER(TRIM(Q'[${PROCESSO}]'))
       , TO_DATE('${DATA_INICIO}','DD/MM/YYYY')
       , TO_DATE('${DATA_FIM}','DD/MM/YYYY')
       , SUBSTR('${BASE//$FIND/$REPLACE}',1,4000)
       , SUBSTR('${FILTRO//$FIND/$REPLACE}',1,4000) 
       , SUBSTR('${REGRAS_HISTORIAS//$FIND/$REPLACE}',1,4000)
       , SUBSTR('${COMMIT//$FIND/$REPLACE}',1,100)
       );

  COMMIT;
  
EXCEPTION
WHEN OTHERS THEN
  ROLLBACK;
  :exit_code := 1;
END;
/
@EOF
RETORNO=$?
Log " ---- FIM [${RETORNO}] : GERACAO DA CARGA DOS REGISTROS DE CONTROLE DE EXECUCAO --- "
if [ ${RETORNO} -ne 0 ]; then
        Log "Execucao interrompida! Favor olhar tabela de controle: gfcadastro.CONTROLE_PROCESSAMENTO!"
        fc_saida 1
fi

if [ "${SCRIPT_REGRA_BEFORE_01}" != "" ]; then
    Log "Execucao 1 ! : ${SCRIPT_REGRA_BEFORE_01}!"
    export SPOOL_FILE=./SCRIPT_REGRA_BEFORE_01_${PROCESSO}.spool
    ./${SCRIPT_REGRA_BEFORE_01}.sh > SCRIPT_REGRA_BEFORE_01_${PROCESSO}.log 2> SCRIPT_REGRA_BEFORE_01_${PROCESSO}.err
    RETORNO=$?
    if [ ${RETORNO} -ne 0 ]; then
        Log "Execucao interrompida!: ${SCRIPT_REGRA_BEFORE_01}!"
        fc_saida 1
    fi
fi

Log " ---- INICIO ${QTDE_MAX_PROCESSOS}: BUSCAR QTDE PROCESSOS E PARTICOES --- "
if [ "${QTDE_MAX_PROCESSOS}" == "" ]; then
	export MAX_PROCESSOS=$( cat PARAMETRO.CONF )
else
	export MAX_PROCESSOS=${QTDE_MAX_PROCESSOS}
fi
PARTICOES=$( ./pega_particoes.sh ${MAX_PROCESSOS} )
RETORNO=$?
Log " ---- FIM [${RETORNO}] : BUSCAR QTDE PROCESSOS [${MAX_PROCESSOS}] E PARTICOES [${PARTICOES}] --- "
if [ ${RETORNO} -ne 0 ]; then
    Log "Execucao interrompida!Favor olhar tabela de controle e particao : gfcadastro.CONTROLE_PROCESSAMENTO!"
    fc_saida 1
fi
wait
Log " ---- INICIO : PARALELISMO --- "
while [ -n "$PARTICOES" ]; do
  for PARTICAO in ${PARTICOES}; do
    ROWID_CTL=$( echo ${PARTICAO} | cut -d";" -f1 )
    PARTICAO_NF=$( echo ${PARTICAO} | cut -d";" -f2 )
    PARTICAO_INF=$( echo ${PARTICAO} | cut -d";" -f3 )
    Log "ROWID=${ROWID_CTL} NF=$PARTICAO_NF INF=$PARTICAO_INF"
    export SPOOL_FILE=./${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.spool
    Log "./${SCRIPT}.sh ${PARTICAO_NF} ${PARTICAO_INF} ${ROWID_CTL} > ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.log 2> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.err &"
    ./${SCRIPT}.sh ${PARTICAO_NF} ${PARTICAO_INF} ${ROWID_CTL} > ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.log 2> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.err &
  done
  sleep 5
  QTD_PROCESSOS=$(( ${MAX_PROCESSOS} - $( jobs | wc -l )))
  while [ ${QTD_PROCESSOS} -eq 0 ]; do
    sleep 5
    if [ "${QTDE_MAX_PROCESSOS}" == "" ]; then
        export MAX_PROCESSOS=$( cat PARAMETRO.CONF )
    else
        export MAX_PROCESSOS=${QTDE_MAX_PROCESSOS}
    fi	 
    QTD_PROCESSOS=$(( ${MAX_PROCESSOS} - $( jobs | wc -l )))
    Log " ---- QTDE PROCESSOS [${QTD_PROCESSOS} >> ${MAX_PROCESSOS}] "
  done
  sleep 15
  if [ -f .${SCRIPT}_${PROCESSO}.stop ]; then
     Log "Execucao interrompida por erro fatal! Favor olhar tabela de controle: gfcadastro.CONTROLE_PROCESSAMENTO!"
     wait
     rm -f .${SCRIPT}_${PROCESSO}.stop
     fc_saida 1
  fi    
  PARTICOES=$( ./pega_particoes.sh ${QTD_PROCESSOS} )
  Log " ---- PARTICOES [${PARTICOES}] --- "
done
wait
if [ "${SCRIPT_REGRA_AFTER_01}" != "" ]; then
    Log "Execucao 2 ! : ${SCRIPT_REGRA_AFTER_01}!"
	export SPOOL_FILE=./SCRIPT_REGRA_AFTER_01_${PROCESSO}.spool
	./${SCRIPT_REGRA_AFTER_01}.sh > SCRIPT_REGRA_AFTER_01_${PROCESSO}.log 2> SCRIPT_REGRA_AFTER_01_${PROCESSO}.err
	RETORNO=$?
	if [ ${RETORNO} -ne 0 ]; then
		Log "Execucao interrompida!: ${SCRIPT_REGRA_AFTER_01}!"
		fc_saida 1
	fi
fi
wait
rm -f ./.alive.pid 2> /dev/null
Log " ---- FIM : PARALELISMO --- "

RES_FINAL=$( 
sqlplus -S -m 'csv on delimiter ; QUOTE OFF' /nolog <<@EOF
CONNECT ${STRING_CONEXAO}
SET SERVEROUTPUT ON SIZE 1000000;
set heading off
set feedback off
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
SELECT ' O processo ' || NM_PROCESSO || 
        ' executado para o periodo ' || TO_CHAR(TRUNC(DT_LIMITE_INF_NF,'MM'),'MM/YYYY') || 
        ' teve seguintes atualizações : ' || chr(13) ||
        ' - NOTA FISCAL (NF): ' || SUM(NVL(QT_ATUALIZADOS_NF,0)) || chr(13) ||
        ' - ITEM (INF): ' || SUM(NVL(QT_ATUALIZADOS_INF,0)) || chr(13) ||
        ' - CLIENTE (CLI): ' || SUM(NVL(QT_ATUALIZADOS_CLI,0)) || chr(13) ||
        ' - COMPLEMENTO DO CLI.(COMP): ' || SUM(NVL(QT_ATUALIZADOS_COMP,0)) || 
        ' - SERVICO (ST): ' || SUM(NVL(QT_CAD_NAO_ENCONTRADO,0)) 
        AS MSG,        
        NM_PROCESSO,  
        TO_CHAR(TRUNC(DT_LIMITE_INF_NF,'MM'),'MM/YYYY') PERIODO, 
        ST_PROCESSAMENTO, 
        to_char(min(dt_ini_proc),'dd/mm/yyyy hh24:mi:ss') dt_inicio , 
        to_char(max(dt_fim_proc),'dd/mm/yyyy hh24:mi:ss') dt_fim ,
        MAX(ID_PROCESS) ID_PROCESS,
        MAX(DS_REGRAS) REGRAS,
        ROUND((TO_NUMBER( MAX(DT_FIM_PROC) -
                         MIN(DT_INI_PROC)) * 1440) -0) TEMPO_MINUTOS,
        COUNT(1) QT_PARTICOES,
        SUM(NVL(QT_ATUALIZADOS_INF,0)) QT_ATUALIZADOS_INF,
        SUM(NVL(QT_ATUALIZADOS_NF,0)) QT_ATUALIZADOS_NF,
        SUM(NVL(QT_ATUALIZADOS_CLI,0)) QT_ATUALIZADOS_CLI,
        SUM(NVL(QT_ATUALIZADOS_COMP,0)) QT_ATUALIZADOS_COMP,
        SUM(NVL(QT_CAD_NAO_ENCONTRADO,0)) QT_ATUALIZADOS_ST
  FROM GFCADASTRO.CONTROLE_PROCESSAMENTO a
WHERE NM_PROCESSO in ('${PROCESSO}') 
GROUP BY TRUNC(DT_LIMITE_INF_NF,'MM'), ST_PROCESSAMENTO, NM_PROCESSO
order by 1, 2,3,4; 
@EOF
exit $?)
RETORNO=$?

fc_saida 0

