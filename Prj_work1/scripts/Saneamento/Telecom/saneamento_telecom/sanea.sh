#!/bin/bash
ROWID=$1
DT_LIMITE_INF_NF=$2
PARTICAO_NF=$3
PARTICAO_INF=$4

#---------------------------------------------------------------------------------------------
# Variavel para tornar padrao o diretorio RAIZ, mesmo nos ambientes dos fornecedores
export DIRETORIO_ATUAL="$(pwd)"
DIRNAME_0=`dirname $0`
DATA=`date +%Y%m%d%H%M%S`
BASENAME_0=`basename $0 | awk -F. '{print $1}'`
DIR_SISTEMA=`dirname $0`
DIR_LOGS="${DIRETORIO_ATUAL}"
# Define o arquivo de log
ARQ_LOG="${DIR_LOGS}/${PROCESSO}_${BASENAME_0}_${DATA}.log"
# Outras variaveis
export DATA_LOG_SQL_SANEA=`date +%Y%m%d%H%M%S`
export ARQ_LOG_SQL_SANEA="${DIR_LOGS}/${PROCESSO}_${BASENAME_0}_${DT_LIMITE_INF_NF}_${DATA_LOG_SQL_SANEA}.log"
export ARQ_LOG_SPOOL_SANEA="${DIR_LOGS}/${PROCESSO}_${BASENAME_0}_${DT_LIMITE_INF_NF}_${DATA_LOG_SQL_SANEA}.spool"
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
Log " Fim de Processamento  - RC: ${COD_ERRO} "
Log "**************************************************************************************************************************"
Log " "

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
ARQ_LOG_SQL="${DIR_LOGS}/${BASENAME_0}_${PROCESSO}_${DATA_LOG_SQL}_sql.log"

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
fc_alterar_particoes()
{
sleep 1
CC_ROWID=$1
CC_ST_PROCESSAMENTO=$2
CC_MSG_ERRO=$3
# Verifica se parametro de entrada foi passado
if [ $# -lt 2 ]
then
   
    Log " "
    Log " ERRO: Parâmetros insuficientes!"
    Log " `basename $0` [CC_ROWID ${CC_ROWID}]  [CC_ST_PROCESSAMENTO  ${CC_ST_PROCESSAMENTO}]"   
    Log " "
    fc_saida 101
fi

$SQLPLUS -S -m 'csv on delimiter ; QUOTE OFF' /nolog <<@EOF
CONNECT ${BD_CONN}
SET DEFINE OFF;
SET SERVEROUTPUT ON SIZE 1000000;
SET TIMING ON;
VAR EXIT_CODE NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
BEGIN
	
	UPDATE gfcadastro.tctb_kyros_control_process cp
	SET cp.nm_var01    		   = TRIM('${CC_ST_PROCESSAMENTO}'),
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
@EOF
RETORNO=$?
if [ ${RETORNO} -ne 0 ]; then
	Log " "
    Log "Execucao interrompida! Favor olhar a tabela de controle! ${SEQUENCE_CONTROLE}"
	Log " "	
    fc_saida 101
fi
}


fc_inicio



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

Log " --------EXECUCAO ${DATA_LOG_SQL_SANEA} ------------------------ "
Log " - ROWID............: ${ROWID}" 
Log " - DT_LIMITE_INF_NF.: ${DT_LIMITE_INF_NF}"
Log " - PARTICAO_NF......: ${PARTICAO_NF}"
Log " - PARTICAO_INF.....: ${PARTICAO_INF}"
Log " - LOG..............: ${ARQ_LOG_SQL_SANEA}"
Log " - SPOOL............: ${ARQ_LOG_SPOOL_SANEA}"
Log " -------------------------------- "

$SQLPLUS -silent << FIM >> $ARQ_LOG_SQL_SANEA
${BD_CONN}
SET DEFINE OFF;
SET SERVEROUTPUT ON SIZE 1000000;
SET TIMING ON;
SPOOL  ${ARQ_LOG_SPOOL_SANEA} 
VAR EXIT_CODE NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
--ALTER SYSTEM FLUSH SHARED_POOL;
--ALTER SYSTEM SET cursor_sharing='FORCE' SCOPE=BOTH;

BEGIN
	
	:EXIT_CODE := PKGMAP_SANEA_GF.fccts_sanea_rowid_process('${ROWID}');
	ROLLBACK;

EXCEPTION
WHEN OTHERS THEN
	
	DECLARE
	
		l_ERRO VARCHAR2(4000) := 'ERRO';
	
	BEGIN
	
		BEGIN
			l_ERRO := SUBSTR(SQLERRM,1,4000);
		EXCEPTION
		WHEN OTHERS THEN
			NULL;
		END;		
	
		BEGIN
			DBMS_OUTPUT.PUT_LINE(SUBSTR(l_ERRO,1,2000));
		EXCEPTION
		WHEN OTHERS THEN
			NULL;
		END;
		
		:EXIT_CODE := 1;
		ROLLBACK;	
		
	END;
	
END;
/
PROMPT Processado 
exit :EXIT_CODE;
FIM
RETORNO=$?
# Verifica erro na execucao do comando sql
fc_verifica_execucao_sql $ARQ_LOG_SQL_SANEA
CD_ERRO=$?
if [ ${CD_ERRO} -ne 0 ]
then
   MSG_ERRO=$( grep -e 'ORA-' -e 'PLS-' -e 'ERROR' ${ARQ_LOG_SQL_SANEA} )
   MSG_ERRO_SPOOL=$( grep -e 'ORA-' -e 'PLS-' -e 'ERROR' ${ARQ_LOG_SPOOL_SANEA})
   DS_MSG_ERRO="${MSG_ERRO} ${MSG_ERRO_SPOOL}"
   printf " ${DS_MSG_ERRO} "
		  > ./.sanea_${PROCESSO}.stop
   Log "${DT_LIMITE_INF_NF} : ${ROWID} >> Erro: ${DS_MSG_ERRO}"  	  
   fc_alterar_particoes ${ROWID} "Erro" "${DS_MSG_ERRO}"
   fc_saida ${CD_ERRO}
fi
if [ ${RETORNO} -ne 0 ]; then
   MSG_ERRO=$( grep -e 'ORA-' -e 'PLS-' -e 'ERROR' ${ARQ_LOG_SQL_SANEA} )
   MSG_ERRO_SPOOL=$( grep -e 'ORA-' -e 'PLS-' -e 'ERROR' ${ARQ_LOG_SPOOL_SANEA})
   DS_MSG_ERRO="${MSG_ERRO} ${MSG_ERRO_SPOOL}"
   printf " ${DS_MSG_ERRO} "
		  > ./.sanea_${PROCESSO}.stop
   Log "${DT_LIMITE_INF_NF}: ${ROWID} >> Erro: ${DS_MSG_ERRO}"
   fc_alterar_particoes ${ROWID} "Erro" "${DS_MSG_ERRO}"
   Log " "
   Log "Execucao interrompida! Favor olhar a tabela de controle! ${SEQUENCE_CONTROLE}"
   Log " "	
   fc_saida 101
fi
fc_saida 0