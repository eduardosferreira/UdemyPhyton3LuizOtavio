#!/bin/bash
clear
# Parametros de entrada
export PROCESSO="${1:-NR_PROCESSO_01}" 
export DATA_INICIO="${2:-01/01/2015}"
export DATA_FIM="${3:-01/01/2015}"
export FILTRO="${4:-nf.emps_cod='TBRA' and nf.fili_cod='0001' and nf.mdoc_cod = 22 and nf.mnfst_serie = '0  6' and nf.mnfst_num = '000086694'}"
export BASE="${5:-Clone 5}"
export COMMIT="${6:-COMMIT}"
export REGRAS_HISTORIAS="${7:-NOVO_MAPA}"
export STATUS_PROCESSO="${8:-Erro,Falha,Aguardando,Reprocessar,Em Processamento,Inicializado,Em Execucao,ERRO_SALTO_DUPLICIDADE_NF,R2015_46}"
export MAX_PROCESSOS="${9:-30}"
export REGRAS_BEFORE="${10:-OK}"
export REGRAS_AFTER="${11:-OK}"
export SQLPLUS="${12:-sqlplus}"

#---------------------------------------------------------------------------------------------
# Variavel para tornar padrao o diretorio RAIZ, mesmo nos ambientes dos fornecedores
export DIRETORIO_ATUAL="$(pwd)"
export PID_SANEA=""
PIDS_SANEA=()
DIRNAME_0=`dirname $0`
DATA=`date +%Y%m%d%H%M%S`
BASENAME_0=`basename $0 | awk -F. '{print $1}'`
DIR_SISTEMA=`dirname $0`
DIR_LOGS="${DIRETORIO_ATUAL}"
# Define o arquivo de log
ARQ_LOG="${DIR_LOGS}/${PROCESSO}_${BASENAME_0}_${DATA}.log"

#
# Programa para gravar mensagens no arquivo de LOG
#
Log ()
####################################################################################################
{
echo " $(date +'%Y-%m-%d %H:%M:%S')-> $1" | tee -a ${ARQ_LOG}
}

#
# Funcao para sair do shell script
#
fc_saida()
{

COD_ERRO=$1

Log " "
Log "**************************************************************************************************************************"
Log " Fim de Processamento - ${BASENAME_0}.sh - RC: ${COD_ERRO} "
Log "**************************************************************************************************************************"
Log " "
wait
DIRETORIO_LOG="${DIR_LOGS}/log/${PROCESSO}"
rm -f .sanea_${PROCESSO}.stop
rm -f ./.alive.pid
mkdir -p ${DIRETORIO_LOG} 2> /dev/null
mv -f *${PROCESSO}_*.log ${DIRETORIO_LOG} 2> /dev/null
mv -f *${PROCESSO}_*.err ${DIRETORIO_LOG} 2> /dev/null
mv -f *${PROCESSO}_*.spool ${DIRETORIO_LOG} 2> /dev/null
mv -f ${DIR_SISTEMA}/*${PROCESSO}_*.log ${DIRETORIO_LOG} 2> /dev/null
mv -f ${DIR_SISTEMA}/*${PROCESSO}_*.err ${DIRETORIO_LOG} 2> /dev/null
mv -f ${DIR_SISTEMA}/*${PROCESSO}_*.spool ${DIRETORIO_LOG} 2> /dev/null
mv -f *_${PROCESSO}.log ${DIRETORIO_LOG} 2> /dev/null
mv -f *_${PROCESSO}.err ${DIRETORIO_LOG} 2> /dev/null
mv -f *_${PROCESSO}.spool ${DIRETORIO_LOG} 2> /dev/null
mv -f ${DIR_SISTEMA}/*_${PROCESSO}.log ${DIRETORIO_LOG} 2> /dev/null
mv -f ${DIR_SISTEMA}/*_${PROCESSO}.err ${DIRETORIO_LOG} 2> /dev/null
mv -f ${DIR_SISTEMA}/*_${PROCESSO}.spool ${DIRETORIO_LOG} 2> /dev/null

exit ${COD_ERRO}

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
if [ -f ./.alive.pid ]; then
  export PID_PROCESSO=$( cat ./.alive.pid )
  kill -0 ${PID_PROCESSO} 2> /dev/null
  if [ $? -eq 0 ]; then
     Log "Processo ja em execucao com  PID%s\n\n" {$PID_PROCESSO}
     fc_saida 101
  fi
  rm -f ./.alive.pid
fi
echo $$ > ./.alive.pid
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
ARQ_LOG_SQL="${DIR_LOGS}/${PROCESSO}_${BASENAME_0}_${DATA_LOG_SQL}_sql.log"

$SQLPLUS -silent << FIM >> $ARQ_LOG_SQL
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
# Busca PARTICOES
#
fc_busca_particoes()
{
sleep 1
QT_LINHA=$1
Log " "
Log " Buscando particoes "
Log " "
# Verifica se parametro de entrada foi passado
if [ $# -lt 1 ]
then
   
    Log " "
    Log " ERRO: Parâmetros insuficientes!"
    Log " `basename $0` [QT_LINHA ${QT_LINHA}]"   
    Log " "
    fc_saida 101
fi
Log " "
fc_test_conn_DB
Log " "
export PARTICOES=$( 
$SQLPLUS -S -m 'csv on delimiter ; QUOTE OFF' /nolog <<@EOF
CONNECT ${BD_CONN}
SET SERVEROUTPUT ON SIZE 1000000;
SET HEADING OFF
SET FEEDBACK OFF
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
SELECT A.ROW_ID, 
	   A.cc_control AS DT_LIMITE_INF_NF,
	   A.nm_var02   AS PARTICAO_NF,
	   A.nm_var03	AS PARTICAO_INF,
       A.id_process AS ID 
  FROM (
        SELECT b.rowid AS ROW_ID, 
			   b.cc_control,
			   b.nm_var02,
			   b.nm_var03,
			   b.id_process,
			   ROW_NUMBER() OVER (ORDER BY DECODE(TRIM(UPPER(b.nm_var01)),'REPROCESSAR',1,2), b.id_process, b.cc_control) num_linha
          FROM gfcadastro.tctb_kyros_process a, gfcadastro.tctb_kyros_control_process b 
		 WHERE a.id_identify = ${SEQUENCE_CONTROLE}
		   AND b.id_process  = a.id_identify
		   AND TRIM(UPPER(b.nm_var01))   IN ('AGUARDANDO','ABERTO','REPROCESSAR')
           AND LENGTH(TRIM(b.cc_control)) = 10
           AND b.cc_control NOT LIKE '%|%'
           AND b.dt_var01 IS NOT NULL
		 ORDER BY b.id_process, b.cc_control  
      ) A
 WHERE A.num_linha <= ${QT_LINHA}
 ORDER BY A.id_process, A.cc_control  
 ;
@EOF
exit $?)
RETORNO=$?
if [ ${RETORNO} -ne 0 ]; then
	Log " "
    Log "Execucao interrompida! Favor olhar a tabela de controle de particoes! ${SEQUENCE_CONTROLE}"
	Log " "	
    fc_saida 101
fi
Log " "
Log " PARTICOES ${PARTICOES} "
Log " "
}

#
# Altera PARTICOES
#
fc_alterar_particoes()
{
sleep 1
CC_ROWID=$1
CC_ST_PROCESSAMENTO=$2
CC_PID="${3:-0}"
CC_MSG_ERRO=$4
# Verifica se parametro de entrada foi passado
if [ $# -lt 2 ]
then
   
    Log " "
    Log " ERRO: Parâmetros insuficientes!"
    Log " `basename $0` [CC_ROWID ${CC_ROWID}]  [CC_ST_PROCESSAMENTO  ${CC_ST_PROCESSAMENTO}]"   
    Log " "
    fc_saida 101
fi
DATA_LOG_SQL=`date +%Y%m%d%H%M%S`
ARQ_LOG_SQL="${DIR_LOGS}/${PROCESSO}_${BASENAME_0}_${DATA_LOG_SQL}.log"
$SQLPLUS -silent << FIM >> $ARQ_LOG_SQL
${BD_CONN}
SET DEFINE OFF;
SET SERVEROUTPUT ON SIZE 1000000;
SET TIMING ON;
VAR EXIT_CODE NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
BEGIN
	UPDATE gfcadastro.tctb_kyros_control_process cp
	SET cp.nm_var01    		   = TRIM('${CC_ST_PROCESSAMENTO}'),
		cp.DS_VAR02    		   = UPPER(TRIM('${CC_PID}')),
		cp.dt_var04 		   = NVL(cp.dt_var04,SYSDATE),
		cp.dt_var05 		   = NVL(cp.dt_var05,DECODE(UPPER(TRIM('${CC_ST_PROCESSAMENTO}')),'ERRO',SYSDATE,DECODE(UPPER(TRIM('${CC_ST_PROCESSAMENTO}')),'PROCESSADO',SYSDATE,cp.dt_var05))),
		cp.ds_var03            = DECODE(NVL(UPPER(TRIM('${CC_MSG_ERRO}')),'N/A'),'N/A',cp.ds_var03,(SUBSTR(UPPER(TRIM('${CC_MSG_ERRO}')),1,200) || ' > ' || SUBSTR(TRIM(cp.ds_var03),1,3700))) 
	WHERE cp.ROWID = '${CC_ROWID}'
	AND   NVL(UPPER(TRIM(cp.nm_var01)),'N/A')  NOT IN ('ERRO','PROCESSADO');
	COMMIT;
EXCEPTION
WHEN OTHERS THEN
	BEGIN
		DBMS_OUTPUT.PUT_LINE(SUBSTR(SQLERRM,1,2000));
	EXCEPTION
	WHEN OTHERS THEN
		NULL;
	END;
	:EXIT_CODE := 1;
	ROLLBACK;	
END;
/
PROMPT Processado 
exit :EXIT_CODE;
FIM
RETORNO=$?
# Verifica erro na execucao do comando sql
fc_verifica_execucao_sql $ARQ_LOG_SQL
CD_ERRO=$?
if [ ${CD_ERRO} -ne 0 ]
then
   fc_saida ${CD_ERRO}
fi
if [ ${RETORNO} -ne 0 ]; then
	Log " "
    Log "Execucao interrompida! Favor olhar a tabela de controle! ${SEQUENCE_CONTROLE}"
	Log " "	
    fc_saida 101
fi
}


#
# Tratar ERRO GERAL
#
fc_tratar_erro_geral()
{
sleep 1
CC_ID=$1
CC_ST_PROCESSAMENTO=$2
CC_PID="${3:-0}"
CC_MSG_ERRO=$4
# Verifica se parametro de entrada foi passado
if [ $# -lt 2 ]
then
   
    Log " "
    Log " ERRO: Parâmetros insuficientes!"
    Log " `basename $0` [CC_ID ${CC_ID}]  [CC_ST_PROCESSAMENTO  ${CC_ST_PROCESSAMENTO}]"   
    Log " "
    fc_saida 101
fi
DATA_LOG_SQL=`date +%Y%m%d%H%M%S`
ARQ_LOG_SQL="${DIR_LOGS}/${PROCESSO}_${BASENAME_0}_${DATA_LOG_SQL}.log"
$SQLPLUS -silent << FIM >> $ARQ_LOG_SQL
${BD_CONN}
SET DEFINE OFF;
SET SERVEROUTPUT ON SIZE 1000000;
SET TIMING ON;
VAR EXIT_CODE NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
BEGIN
	UPDATE gfcadastro.tctb_kyros_control_process cp
	SET cp.nm_var01    		   = '${CC_ST_PROCESSAMENTO}',
		cp.DS_VAR02    		   = NVL(TRIM(cp.DS_VAR02),UPPER(TRIM('${CC_PID}'))),
		cp.dt_var04 		   = NVL(cp.dt_var04,SYSDATE),
		cp.dt_var05 		   = NVL(cp.dt_var05,DECODE(UPPER(TRIM('${CC_ST_PROCESSAMENTO}')),'ERRO',SYSDATE,DECODE(UPPER(TRIM('${CC_ST_PROCESSAMENTO}')),'PROCESSADO',SYSDATE,cp.dt_var05))),
		cp.ds_var03            = DECODE(NVL(UPPER(TRIM('${CC_MSG_ERRO}')),'N/A'),'N/A',cp.ds_var03,(SUBSTR(UPPER(TRIM('${CC_MSG_ERRO}')),1,200) || ' > ' || SUBSTR(TRIM(cp.ds_var03),1,3700))) 
	WHERE cp.id_process = ${CC_ID}	
	AND   NVL(UPPER(TRIM(cp.nm_var01)),'N/A')  NOT IN ('ERRO','PROCESSADO');
	COMMIT;
EXCEPTION
WHEN OTHERS THEN
	BEGIN
		DBMS_OUTPUT.PUT_LINE(SUBSTR(SQLERRM,1,2000));
	EXCEPTION
	WHEN OTHERS THEN
		NULL;
	END;
	:EXIT_CODE := 1;
	ROLLBACK;	
END;
/
PROMPT Processado 
exit :EXIT_CODE;
FIM
RETORNO=$?
# Verifica erro na execucao do comando sql
fc_verifica_execucao_sql $ARQ_LOG_SQL
CD_ERRO=$?
if [ ${CD_ERRO} -ne 0 ]
then
   fc_saida ${CD_ERRO}
fi
if [ ${RETORNO} -ne 0 ]; then
	Log " "
    Log "Execucao interrompida! Favor olhar a tabela de controle! ${SEQUENCE_CONTROLE}"
	Log " "	
    fc_saida 101
fi
}


#
# Alterar o status da execucao por PID
#
fc_alterar_por_PID()
{
sleep 1
CC_ID=$1
CC_ST_PROCESSAMENTO=$2
CC_PID=$3
# Verifica se parametro de entrada foi passado
if [ $# -lt 3 ]
then
   
    Log " "
    Log " ERRO: Parâmetros insuficientes!"
    Log " `basename $0` [CC_ID ${CC_ID}]  [CC_ST_PROCESSAMENTO  ${CC_ST_PROCESSAMENTO}]  [CC_PID  ${CC_PID}]"   
    Log " "
	fc_saida 101
else

	DATA_LOG_SQL=`date +%Y%m%d%H%M%S`
	ARQ_LOG_SQL="${DIR_LOGS}/${PROCESSO}_${BASENAME_0}_${DATA_LOG_SQL}.log"
$SQLPLUS -silent << FIM >> $ARQ_LOG_SQL
${BD_CONN}
SET DEFINE OFF;
SET SERVEROUTPUT ON SIZE 1000000;
SET TIMING ON;
VAR EXIT_CODE NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
BEGIN
	UPDATE gfcadastro.tctb_kyros_control_process cp
	SET cp.nm_var01    		   = '${CC_ST_PROCESSAMENTO}',
		cp.dt_var04 		   = NVL(cp.dt_var04,SYSDATE),
		cp.dt_var05 		   = NVL(cp.dt_var05,SYSDATE)
	WHERE cp.id_process = ${CC_ID}	
	AND   NVL(UPPER(TRIM(cp.nm_var01)),'N/A')  != 'ERRO'
	AND   UPPER(TRIM(cp.DS_VAR02)) = UPPER(TRIM('${CC_PID}'));
	COMMIT;
EXCEPTION
WHEN OTHERS THEN
	BEGIN
		DBMS_OUTPUT.PUT_LINE(SUBSTR(SQLERRM,1,2000));
	EXCEPTION
	WHEN OTHERS THEN
		NULL;
	END;
	:EXIT_CODE := 1;
	ROLLBACK;	
END;
/
PROMPT Processado 
exit :EXIT_CODE;
FIM
RETORNO=$?
	# Verifica erro na execucao do comando sql
	fc_verifica_execucao_sql $ARQ_LOG_SQL
	CD_ERRO=$?
	if [ ${CD_ERRO} -ne 0 ]
	then
		fc_saida ${CD_ERRO}
	fi
	if [ ${RETORNO} -ne 0 ]; then
		Log " "
		Log "Execucao interrompida! Favor olhar a tabela de controle! ${SEQUENCE_CONTROLE}"
		Log " "	
		fc_saida 101
	fi

fi
}


#
# SANEAMENTO.
#
fc_sanea()
{
sleep 1
ROWID=$1
DT_LIMITE_INF_NF=$2
PARTICAO_NF=$3
PARTICAO_INF=$4
SANEA_PARTICAO="sanea"
# Verifica se parametro de entrada foi passado
if [ "${ROWID}" == "" ]; then    
	Log " "		
	Log " ROWID ${ROWID} nao eh valido!"
	Log " "
	fc_saida 101
fi
if [ "${DT_LIMITE_INF_NF}" == "" ]; then    
	Log " "		
	Log " DT_LIMITE_INF_NF ${DT_LIMITE_INF_NF} nao eh valido!"
	Log " "
	fc_saida 101
fi
fc_alterar_particoes ${ROWID} "Inicializado"

DATA_LOG_SQL_SANEA=`date +%Y%m%d%H%M%S`
ARQ_LOG_SQL_SANEA="${DIR_LOGS}/${PROCESSO}_${SANEA_PARTICAO}_${DT_LIMITE_INF_NF}_${DATA_LOG_SQL_SANEA}.log"
ARQ_LOG_ERR_SANEA="${DIR_LOGS}/${PROCESSO}_${SANEA_PARTICAO}_${DT_LIMITE_INF_NF}_${DATA_LOG_SQL_SANEA}.err"

Log " --------SANEAMENTO ${DATA_LOG_SQL_SANEA} ------------------------ "
Log " - ROWID............: ${ROWID}" 
Log " - DT_LIMITE_INF_NF.: ${DT_LIMITE_INF_NF}"
Log " - PARTICAO_NF......: ${PARTICAO_NF}"
Log " - PARTICAO_INF.....: ${PARTICAO_INF}"
Log " - LOG..............: ${ARQ_LOG_SQL_SANEA}"
Log " - ERR..............: ${ARQ_LOG_ERR_SANEA}"
Log " -------------------------------- "
Log "./${SANEA_PARTICAO}.sh ${ROWID} ${DT_LIMITE_INF_NF} ${PARTICAO_NF} ${PARTICAO_INF} > ${ARQ_LOG_SQL_SANEA} 2> ${ARQ_LOG_ERR_SANEA} &"	

if [ -e ./${SANEA_PARTICAO}.sh ]
then
   Log " "	
else
	wait
	fc_alterar_particoes ${ROWID_CTL_SANEA} "Erro"
	Log " "
	Log " ERRO durante acionamento script saneamento ! ${DS_VAR04_CTL_SANEA}"
	Log " "
	fc_tratar_erro_geral ${SEQUENCE_CONTROLE} "Erro" "0" "Nao Localizado Script ${SANEA_PARTICAO}.sh"
	fc_saida 101
fi
./${SANEA_PARTICAO}.sh ${ROWID} ${DT_LIMITE_INF_NF} ${PARTICAO_NF} ${PARTICAO_INF} > ${ARQ_LOG_SQL_SANEA} 2> ${ARQ_LOG_ERR_SANEA} &
CD_ERRO=$?
export PID_SANEA=$!
Log "Iniciado (#${PID_SANEA}): ${ROWID} ${DT_LIMITE_INF_NF}"
if [ ${CD_ERRO} -ne 0 ]
then
	wait
	fc_alterar_particoes ${ROWID} "Erro" "${PID_SANEA}"
	Log " "
	Log " ERRO durante acionamento script saneamento !"
	Log " "
	fc_tratar_erro_geral ${SEQUENCE_CONTROLE} "Erro" "${PID_SANEA}" " ERRO durante acionamento script saneamento !"
	fc_saida 101
else
	DATA_LOG_SQL=`date +%Y%m%d%H%M%S`
	ARQ_LOG_SQL="${DIR_LOGS}/${PROCESSO}_${BASENAME_0}_${DATA_LOG_SQL}.log"
$SQLPLUS -silent << FIM >> $ARQ_LOG_SQL
${BD_CONN}
SET DEFINE OFF;
SET SERVEROUTPUT ON SIZE 1000000;
SET TIMING ON;
VAR EXIT_CODE NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
BEGIN
	UPDATE 	gfcadastro.tctb_kyros_control_process cp
	SET 	cp.DS_VAR02 = UPPER(TRIM('${PID_SANEA}'))
	WHERE 	cp.ROWID = '${ROWID}';
	COMMIT;
EXCEPTION
WHEN OTHERS THEN
	BEGIN
		DBMS_OUTPUT.PUT_LINE(SUBSTR(SQLERRM,1,2000));
	EXCEPTION
	WHEN OTHERS THEN
		NULL;
	END;
	:EXIT_CODE := 1;
	ROLLBACK;	
END;
/
PROMPT Processado 
exit :EXIT_CODE;
FIM
RETORNO=$?
	# Verifica erro na execucao do comando sql
	fc_verifica_execucao_sql $ARQ_LOG_SQL
	CD_ERRO=$?
	if [ ${CD_ERRO} -ne 0 ]
	then
		wait
		Log " "
		Log "Execucao interrompida! Favor olhar a tabela de controle! Problemas na alteracao PID: ${ROWID} ${SEQUENCE_CONTROLE} ${PID_SANEA}"
		Log " "		
		fc_tratar_erro_geral ${SEQUENCE_CONTROLE} "Erro" "${PID_SANEA}" "Execucao interrompida! Favor olhar a tabela de controle! Problemas na alteracao PID: ${ROWID} ${SEQUENCE_CONTROLE} ${PID_SANEA}"	
		fc_saida 101
	fi
	if [ ${RETORNO} -ne 0 ]; then
		wait
		Log " "
		Log "Execucao interrompida! Favor olhar a tabela de controle! ${ROWID} ${SEQUENCE_CONTROLE} ${PID_SANEA}"
		Log " "	
		fc_tratar_erro_geral ${SEQUENCE_CONTROLE} "Erro" "${PID_SANEA}" "Execucao interrompida! Favor olhar a tabela de controle! ${ROWID} ${SEQUENCE_CONTROLE} ${PID_SANEA}"	
		fc_saida 101
	fi
fi

}

# =================================================================
# Inicio
# =================================================================
Log " -------------------------------- "
Log " - PROCESSO.........: ${PROCESSO}" 
Log " - DATA_INICIO......: ${DATA_INICIO}"
Log " - DATA_FIM.........: ${DATA_FIM}"
Log " - FILTRO...........: ${FILTRO}"
Log " - BASE.............: ${BASE}"
Log " - COMMIT...........: ${COMMIT}"
Log " - REGRAS_HISTORIAS.: ${REGRAS_HISTORIAS}"
Log " - STATUS_PROCESSO..: ${STATUS_PROCESSO}"
Log " - MAX_PROCESSOS....: ${MAX_PROCESSOS}"
Log " - REGRAS_BEFORE....: ${REGRAS_BEFORE}"
Log " - REGRAS_AFTER.....: ${REGRAS_AFTER}"
Log " -------------------------------- "

if [ "${PROCESSO}" == "" ]; then    
	Log " "		
	Log " PROCESSO ${PROCESSO} nao eh valido!"
	Log " "
	fc_saida 101
fi

if [ "${DATA_INICIO}" == "" ]; then    
	Log " "		
	Log " DATA_INICIO ${DATA_INICIO} nao eh valido!"
	Log " "
	fc_saida 101
fi

if [ "${DATA_FIM}" == "" ]; then    
	Log " "		
	Log " DATA_FIM ${DATA_FIM} nao eh valido!"
	Log " "
	fc_saida 101
fi

if [ "${FILTRO}" == "" ]; then    
	Log " "		
	Log " FILTRO ${FILTRO} nao eh valido!"
	Log " "
	fc_saida 101
fi

if [ "${COMMIT}" == "COMMIT" ] || [ "${COMMIT}" == "ROLLBACK" ] ; then    
	Log " "		
else
	Log " "		
	Log " TRANSACAO ${COMMIT} nao eh valido!"
	Log " "
	fc_saida 101
fi

if [ "${MAX_PROCESSOS}" == "" ]; then    
	Log " "		
	Log " MAX_PROCESSOS ${MAX_PROCESSOS} nao eh valido!"
	Log " "
	fc_saida 101
fi

case ${BASE} in
	"GFCLONE7"|"GF_CLONE_DEV7"|"GFPRODC7"|"7"|"10.238.45.228"|"svc_gfprodc7"|"Clone 7"|"Clone7"|"CLONE 7"|"CLONE7")
		export BD_CONN="gfcadastro/vivo2019@10.238.45.230/svc_gfprodc7"
		export STRING_HOST="10.238.10.109"
		export NRO_BASE="7"
	;;
	"GFCLONE6"|"GF_CLONE_DEV6"|"GFPRODC6"|"6"|"10.238.45.227"|"svc_gfprodc6"|"Clone 6"|"Clone6"|"CLONE 6"|"CLONE6")
		export BD_CONN="gfcadastro/vivo2019@10.238.45.230/svc_gfprodc6"
		export STRING_HOST="10.238.10.210"
		export NRO_BASE="6"
	;;
	"GFREAD"|"GF_CLONE_GFREAD"|"READ")
		export BD_CONN="GFREAD/vivo2019@10.238.10.173/gfread"
		export STRING_HOST="10.238.10.174"
		export NRO_BASE="5"
	;;	
	"GFREAD_OPENRISOW"|"GF_CLONE_GFREAD_OPENRISOW"|"READ_OPENRISOW")
		export BD_CONN="OPENRISOW/OPENRISOW@10.238.10.173/gfread"
		export STRING_HOST="10.238.10.174"
		export NRO_BASE="5"
	;;
	"GFCLONE5"|"GF_CLONE_DEV5"|"DEV5"|"5"|"10.238.10.173"|"Clone 5"|"Clone5"|"CLONE 5"|"CLONE5")
		export BD_CONN="gfcadastro/vivo2019@10.238.10.173/gfprod"
		export STRING_HOST="10.238.10.174"
		export NRO_BASE="5"
	;;
	"GF_CLONE_DEV2"|"DEV2"|"2"|"10.238.10.207"|"GFCLONEPREPROD"|"Clone 2"|"Clone2"|"CLONE 2"|"CLONE2")
		export BD_CONN="gfcadastro/vivo2019@10.238.10.207/gfprod"
		export STRING_HOST="10.238.10.209"
		export NRO_BASE="2"
	;;
	"GF_CLONE_DEV"|"DEV"|"1"|"10.238.10.106"|"GFCLONEDEV"|"Clone 1"|"Clone1"|"CLONE 1"|"CLONE1")
		export BD_CONN="gfcadastro/vivo2019@10.238.10.106/gfprod"
		export STRING_HOST="10.238.10.208"
		export NRO_BASE="1"
	;;
	*)
		Log " "		
		Log " Conexao ${BASE} nao eh valido!"
		Log " "
		fc_saida 101			
	;;
esac
export STRING_CONEXAO="${BD_CONN}"
Log " "
Log " String de Conexao: ${STRING_CONEXAO}"
Log " Host: ${STRING_HOST}"
Log " Number: ${NRO_BASE}"
Log " "
fc_test_conn_DB
Log " "

# Grava log de inicio de execucao
fc_inicio

Log " "
Log " Processando sequence"
Log " "
export SEQUENCE_CONTROLE=$( 
$SQLPLUS -S -m 'csv on delimiter ; QUOTE OFF' /nolog <<@EOF
CONNECT ${BD_CONN}
SET SERVEROUTPUT ON SIZE 1000000;
SET HEADING OFF
SET FEEDBACK OFF
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
SELECT   
	NVL((SELECT MAX(ID_IDENTIFY) AS ID_IDENTIFY
		FROM  GFCADASTRO.TCTB_KYROS_PROCESS
		WHERE CC_PROCESS  = UPPER(TRIM(q'[${PROCESSO}]'))
		AND DT_VAR01    = TO_DATE(TRIM('${DATA_INICIO}'),'DD/MM/YYYY')
		AND DT_VAR02    = TO_DATE(TRIM('${DATA_FIM}'),'DD/MM/YYYY')
		AND DS_VAR01    = UPPER(TRIM(q'[${BASE}]')) 
		AND DS_VAR02    = UPPER(TRIM(q'[${FILTRO}]'))
		AND DS_VAR04    = UPPER(TRIM(q'[${REGRAS_HISTORIAS}]')))	
	,GFCADASTRO.TCSQ_KYROS_PROCESS_LOG.NEXTVAL) AS SQ_KYROS_PROCESS_LOG
FROM DUAL; 
@EOF
exit $?)
RETORNO=$?
Log "Status do retorno da execucao da sequence: ${RETORNO}"
if [ ${RETORNO} -ne 0 ]; then
	Log " "
    Log "Execucao interrompida! Favor olhar a sequence ! ${SEQUENCE_CONTROLE}"
	Log " "	
    fc_saida 101
elif [ ${SEQUENCE_CONTROLE} -eq 0 ]; then
	Log " "
	Log "ATENCAO: Favor olhar : ${SEQUENCE_CONTROLE}!"
	Log " "	
    fc_saida 101
fi
Log " "
Log "ATENCAO: SEQUENCE DE EXECUCAO : ${SEQUENCE_CONTROLE}!"
Log " "	
#
Log " "
Log " Criando particoes "
Log " "
DATA_LOG_SQL=`date +%Y%m%d%H%M%S`
ARQ_LOG_SQL="${DIR_LOGS}/${PROCESSO}_${BASENAME_0}_${DATA_LOG_SQL}_sql.log"
SPOOL_FILE="${DIR_LOGS}/${PROCESSO}_CRIAR_PARAMETROS_PARALELISMO_${DATA_LOG_SQL}_spool.spool"
$SQLPLUS -silent << FIM >> $ARQ_LOG_SQL
${BD_CONN}
SET DEFINE OFF;
SET SERVEROUTPUT ON SIZE 1000000;
SET TIMING ON;
SPOOL  ${SPOOL_FILE} 
VAR EXIT_CODE NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
DECLARE

	l_tctb_kyros_process gfcadastro.tctb_kyros_process%rowtype;
		
BEGIN
	
	l_tctb_kyros_process.ID_IDENTIFY := ${SEQUENCE_CONTROLE};
	l_tctb_kyros_process.CC_PROCESS  := UPPER(TRIM(q'[${PROCESSO}]')); 
	l_tctb_kyros_process.DT_VAR01    := TO_DATE(TRIM('${DATA_INICIO}'),'DD/MM/YYYY'); 
	l_tctb_kyros_process.DT_VAR02    := TO_DATE(TRIM('${DATA_FIM}'),'DD/MM/YYYY');
	l_tctb_kyros_process.DT_VAR03    := SYSDATE;
	l_tctb_kyros_process.DS_VAR01    := UPPER(TRIM(q'[${BASE}]')); 
	l_tctb_kyros_process.DS_VAR02    := UPPER(TRIM(q'[${FILTRO}]'));
	l_tctb_kyros_process.DS_VAR03    := UPPER(TRIM('${MAX_PROCESSOS}'));
    l_tctb_kyros_process.DS_VAR04    := UPPER(TRIM(q'[${REGRAS_HISTORIAS}]'));	
    l_tctb_kyros_process.DS_VAR05    := TRIM(q'[${STATUS_PROCESSO}]');	
    l_tctb_kyros_process.DS_VAR06    := TRIM(q'[${REGRAS_BEFORE}]');	
    l_tctb_kyros_process.DS_VAR07    := TRIM(q'[${REGRAS_AFTER}]');	
    l_tctb_kyros_process.DS_VAR08    := TRIM(q'[${COMMIT}]');	
	l_tctb_kyros_process.DT_CREATED  := SYSDATE; 

	:EXIT_CODE := PKGMAP_SANEA_GF.fccts_criar_process(p_tctb_kyros_process => l_tctb_kyros_process);
EXCEPTION
WHEN OTHERS THEN
	BEGIN
		DBMS_OUTPUT.PUT_LINE(SUBSTR(SQLERRM,1,2000));
	EXCEPTION
	WHEN OTHERS THEN
		NULL;
	END;
	:EXIT_CODE := 1;
	ROLLBACK;	
END;
/
PROMPT Processado 
exit :EXIT_CODE;
FIM
RETORNO=$?
# Verifica erro na execucao do comando sql
fc_verifica_execucao_sql $ARQ_LOG_SQL
CD_ERRO=$?
if [ ${CD_ERRO} -ne 0 ]
then
   fc_saida ${CD_ERRO}
fi
if [ ${RETORNO} -ne 0 ]; then
	Log " "
    Log "Execucao interrompida! Favor olhar a tabela de controle! ${SEQUENCE_CONTROLE}"
	Log " "	
    fc_saida 101
fi

#
if [ "${REGRAS_BEFORE}" == "OK" ]; then    
	Log " "
	Log " Executando regras BEFORE "
	Log " "
	DATA_LOG_SQL=`date +%Y%m%d%H%M%S`
	ARQ_LOG_SQL="${DIR_LOGS}/${PROCESSO}_${BASENAME_0}_${DATA_LOG_SQL}_sql.log"
	SPOOL_FILE="${DIR_LOGS}/${PROCESSO}_${BASENAME_0}_BEFORE_spool.spool"
$SQLPLUS -silent << FIM >> $ARQ_LOG_SQL
${BD_CONN}
SET DEFINE OFF;
SET SERVEROUTPUT ON SIZE 1000000;
SET TIMING ON;
SPOOL  ${SPOOL_FILE} 
VAR EXIT_CODE NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
BEGIN
	  COMMIT;
EXCEPTION
WHEN OTHERS THEN
	BEGIN
		DBMS_OUTPUT.PUT_LINE(SUBSTR(SQLERRM,1,2000));
	EXCEPTION
	WHEN OTHERS THEN
		NULL;
	END;
	:EXIT_CODE := 1;
	ROLLBACK;	
END;
/
PROMPT Processado 
exit :EXIT_CODE;
FIM
RETORNO=$?
	# Verifica erro na execucao do comando sql
	fc_verifica_execucao_sql $ARQ_LOG_SQL
	CD_ERRO=$?
	if [ ${CD_ERRO} -ne 0 ]
	then
		fc_saida ${CD_ERRO}
	fi
	if [ ${RETORNO} -ne 0 ]; then
		Log " "
		Log "Execucao interrompida! Favor olhar a tabela de controle! ${SEQUENCE_CONTROLE}"
		Log " "	
		fc_saida 101
	fi
	
fi

wait

# Busca particoes
fc_busca_particoes ${MAX_PROCESSOS}

wait

while [ -n "$PARTICOES" ]; do

  for PARTICAO in ${PARTICOES}; do  
	ROWID=$( echo ${PARTICAO} | cut -d";" -f1 )
	DT_LIMITE_INF_NF=$( echo ${PARTICAO} | cut -d";" -f2 )
	PARTICAO_NF=$( echo ${PARTICAO} | cut -d";" -f3 )
	PARTICAO_INF=$( echo ${PARTICAO} | cut -d";" -f4 )
    fc_sanea ${ROWID} ${DT_LIMITE_INF_NF} ${PARTICAO_NF} ${PARTICAO_INF}   
	PIDS_SANEA+=(${PID_SANEA})
	finalizados=( $( kill -0 ${PIDS_SANEA[@]} 2>&1 >/dev/null | awk -F'[()]' '{ print $2 }' ) )
  done
 
  sleep 5 	
  QTD_PROCESSOS=$(( ${MAX_PROCESSOS} - $( jobs | wc -l )))
  while [ ${QTD_PROCESSOS} -eq 0 ]; do
     sleep 5
     QTD_PROCESSOS=$(( ${MAX_PROCESSOS} - $( jobs | wc -l )))
  done
  
  sleep 5
  if [ -f .sanea_${PROCESSO}.stop ]; then
     Log " "
	 Log "Execucao interrompida por erro fatal! Favor olhar o erro no script de saneamento!"
     Log " "	 
	 fc_tratar_erro_geral ${SEQUENCE_CONTROLE} "Erro" "0" "Execucao interrompida por erro fatal! Favor olhar o erro no script de saneamento!"	
	 wait
	 fc_saida 101
  fi    
  
  for finalizado in "${finalizados[@]}"; do
	for i in "${!PIDS_SANEA[@]}"; do
		if [[ ${PIDS_SANEA[i]} = $finalizado ]]; then
			# fc_alterar_por_PID ${SEQUENCE_CONTROLE} "ERRO" ${finalizado}
			Log " ${SEQUENCE_CONTROLE} Terminado (#${finalizado})"
			unset 'PIDS_SANEA[i]'
		fi
	done
  done  
  
  fc_busca_particoes ${QTD_PROCESSOS}

done

wait

#
if [ "${REGRAS_AFTER}" == "OK" ]; then    
	Log " "
	Log " Executando regras AFTER "
	Log " "
	DATA_LOG_SQL=`date +%Y%m%d%H%M%S`
	ARQ_LOG_SQL="${DIR_LOGS}/${PROCESSO}_${BASENAME_0}_${DATA_LOG_SQL}_sql.log"
	SPOOL_FILE="${DIR_LOGS}/${PROCESSO}_${BASENAME_0}_AFTER_spool.spool"
$SQLPLUS -silent << FIM >> $ARQ_LOG_SQL
${BD_CONN}
SET DEFINE OFF;
SET SERVEROUTPUT ON SIZE 1000000;
SET TIMING ON;
SPOOL  ${SPOOL_FILE} 
VAR EXIT_CODE NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
BEGIN
	  COMMIT;
EXCEPTION
WHEN OTHERS THEN
	BEGIN
		DBMS_OUTPUT.PUT_LINE(SUBSTR(SQLERRM,1,2000));
	EXCEPTION
	WHEN OTHERS THEN
		NULL;
	END;
	:EXIT_CODE := 1;
	ROLLBACK;	
END;
/
PROMPT Processado 
exit :EXIT_CODE;
FIM
RETORNO=$?
	# Verifica erro na execucao do comando sql
	fc_verifica_execucao_sql $ARQ_LOG_SQL
	CD_ERRO=$?
	if [ ${CD_ERRO} -ne 0 ]
	then
		fc_saida ${CD_ERRO}
	fi
	if [ ${RETORNO} -ne 0 ]; then
		Log " "
		Log "Execucao interrompida! Favor olhar a tabela de controle! ${SEQUENCE_CONTROLE}"
		Log " "	
		fc_saida 101
	fi
fi

wait

fc_saida 0

