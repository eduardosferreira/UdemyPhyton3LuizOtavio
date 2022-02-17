#!/bin/bash
clear
############################ PARAMETROS DE ENTRADA ###################################################
PARTICAO_NF=${1}
PARTICAO_INF=${2}
ROWID_CP=${3}
############################ PARAMETROS PADROES ###################################################
DIRNAME_0=`dirname $0`
DATA=`date +%Y%m%d%H%M%S`
BASENAME_0=`basename $0 | awk -F. '{print $1}'`
TIPO_ARQ="SQL"
DIR_SISTEMA=`dirname $0`
DIR_LOGS=`dirname $0`
############################ PARAMETROS DIVERSOS ###################################################
FILTRO_SCRIPT="${FILTRO}"
REGRAS_SCRIPT="${REGRAS_HISTORIAS}"
PROCESSO_SCRIPT="${PROCESSO}"
NOME_SCRIPT="${SCRIPT}"
SPOOL_FILE_SCRIPT="${SPOOL_FILE}"
STRING_CONEXAO_SCRIPT="${STRING_CONEXAO}"
DIRETORIO_SCRIPT="${DIR_SISTEMA}/"
COMMIT_SCRIPT="${COMMIT}"
SEQUENCE_CONTROLE_SCRIPT="${SEQUENCE_CONTROLE}"
############################ FUNCOES PADROES ###################################################
#
# Programa para gravar mensagens no arquivo de LOG
#
# Define o arquivo de log
ARQ_LOG="${DIRNAME_0}/${BASENAME_0}_${DATA}_${PROCESSO_SCRIPT}.log"

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

Log " "
Log "**************************************************************************************************************************"
Log " Fim de Processamento - ${BASENAME_0}.sh - RC: ${COD_ERRO} "
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
ARQ_LOG_SQL="${DIR_LOGS}/${BASENAME_0}_${TIPO_ARQ}_${DATA_LOG_SQL}_sql_${PROCESSO_SCRIPT}.log"

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

############################ TRATAMENTOS DOS PARAMETROS ###################################################
# Grava log de inicio de execucao
fc_inicio
Log " - > PARTICAO_NF  : ${PARTICAO_NF}     	"
Log " - > PARTICAO_INF : ${PARTICAO_INF}     	"
Log " - > ROWID_CP     : ${ROWID_CP}     		"

Log " ***** TRATAMENTOS DOS PARAMETROS ******   	"

FILTRO_SCRIPT="${FILTRO_SCRIPT^^}" 
FILTRO_SCRIPT="${FILTRO_SCRIPT##*( )}" 
FILTRO_SCRIPT="${FILTRO_SCRIPT%%*( )}"

if [ "${TABELA_NF}" == *"_NFEN_"* -o "${TABELA_INF}" ==  *"_NFEM_"* ]; then
	FINDUNICO="MNFST_"
	REPLACEUNICO="MNFEM_"
	FILTRO_SCRIPT="${FILTRO_SCRIPT//$FINDUNICO/$REPLACEUNICO}"	
	FINDUNICO="INFST_"
	REPLACEUNICO="INFEM_"
	FILTRO_SCRIPT="${FILTRO_SCRIPT//$FINDUNICO/$REPLACEUNICO}"		
	FINDUNICO="DTEMISS"
	REPLACEUNICO="DTEMIS"
	FILTRO_SCRIPT="${FILTRO_SCRIPT//$FINDUNICO/$REPLACEUNICO}"
elif [ "${TABELA_NF}" == *"_NFSD_"* -o "${TABELA_INF}" ==  *"_NFSD_"* ]; then
	FINDUNICO="MNFST_"
	REPLACEUNICO="MNFSM_"
	FILTRO_SCRIPT="${FILTRO_SCRIPT//$FINDUNICO/$REPLACEUNICO}"	
	FINDUNICO="INFST_"
	REPLACEUNICO="INFSM_"
	FILTRO_SCRIPT="${FILTRO_SCRIPT//$FINDUNICO/$REPLACEUNICO}"		
fi

if [ "${FILTRO_SCRIPT}" == "" ]; then
	FILTRO_SCRIPT="1=1"
fi

Log " - > FILTRO_SCRIPT : ${FILTRO_SCRIPT}     	"
Log " - > PARTICAO_NF   : ${PARTICAO_NF}   		"
Log " - > PARTICAO_INF  : ${PARTICAO_INF}    	"

if [[ $PARTICAO_NF == *"PARTICAO"* ]]; then

	FINDUNICO="PARTICAO"
	REPLACEUNICO=""
	PARTITION_NF="${PARTICAO_NF//$FINDUNICO/$REPLACEUNICO}"	
	PARTITION_INF="${PARTICAO_INF//$FINDUNICO/$REPLACEUNICO}"
	
	if [ "${TABELA_NF}" == *"_NFEN_"* -o "${TABELA_INF}" ==  *"_NFEM_"* ]; then
		FILTRO_SCRIPT="${FILTRO_SCRIPT^^} AND NF.MNFEM_DTENTR = TO_DATE('${PARTITION_NF}','DDMMYYYY')"	
	elif [ "${TABELA_NF}" == *"_NFSD_"* -o "${TABELA_INF}" ==  *"_NFSD_"* ]; then
		FILTRO_SCRIPT="${FILTRO_SCRIPT^^} AND NF.MNFSM_DTEMISS = TO_DATE('${PARTITION_NF}','DDMMYYYY')"		
	else
		FILTRO_SCRIPT="${FILTRO_SCRIPT^^} AND NF.MNFST_DTEMISS = TO_DATE('${PARTITION_NF}','DDMMYYYY')"	
	fi
	
	PARTITION_NF=""		
	PARTITION_INF=""

elif [[ $PARTICAO_INF == *"PARTICAO"* ]]; then

	FINDUNICO="PARTICAO"
	REPLACEUNICO=""
	PARTITION_NF="${PARTICAO_NF//$FINDUNICO/$REPLACEUNICO}"	
	PARTITION_INF="${PARTICAO_INF//$FINDUNICO/$REPLACEUNICO}"

	if [ "${TABELA_NF}" == *"_NFEN_"* -o "${TABELA_INF}" ==  *"_NFEM_"* ]; then
		FILTRO_SCRIPT="${FILTRO_SCRIPT^^} AND NF.MNFEM_DTENTR = TO_DATE('${PARTITION_INF}','DDMMYYYY')"	
	elif [ "${TABELA_NF}" == *"_NFSD_"* -o "${TABELA_INF}" ==  *"_NFSD_"* ]; then
		FILTRO_SCRIPT="${FILTRO_SCRIPT^^} AND NF.MNFSM_DTEMISS = TO_DATE('${PARTITION_INF}','DDMMYYYY')"		
	else
		FILTRO_SCRIPT="${FILTRO_SCRIPT^^} AND NF.MNFST_DTEMISS = TO_DATE('${PARTITION_INF}','DDMMYYYY')"	
	fi	
	
	PARTITION_NF=""		
	PARTITION_INF=""

else 

	PARTITION_NF=" PARTITION (${PARTICAO_NF})"	
	PARTITION_INF=" PARTITION (${PARTICAO_INF})"

fi
Log " - > PARTITION_NF  : ${PARTITION_NF}   		"
Log " - > PARTITION_INF : ${PARTITION_INF}    	"
Log " - > FILTRO_SCRIPT : ${FILTRO_SCRIPT}     	"
Log " - > PARTICAO_NF   : ${PARTICAO_NF}   		"
Log " - > PARTICAO_INF  : ${PARTICAO_INF}    	"


REGRAS_SCRIPT="${REGRAS_SCRIPT^^}" 
REGRAS_SCRIPT="${REGRAS_SCRIPT##*( )}" 
REGRAS_SCRIPT="${REGRAS_SCRIPT%%*( )}"

Log " - > REGRAS_SCRIPT  : ${REGRAS_SCRIPT}    	"

#turn it on
shopt -s extglob

REPLACEVAZIO=""
REGRA=""
PRCTS_STOP=""


IFS=',' # colon (,) is set as delimiter
read -ra ADDR <<< "$REGRAS_SCRIPT" # str is read into an array as tokens separated by IFS

for i in "${ADDR[@]}"; do # access each element of array

    REGRA="$i"
	### Trim leading whitespaces ###
	REGRA="${REGRA##*( )}" 
	### trim trailing whitespaces  ##
	REGRA="${REGRA%%*( )}"
	REGRA="${REGRA^^}" 	
	
	Log " - > REGRA  : ${REGRA}    	"

done


IFS=' ' # reset to default value after usage

# turn it off
shopt -u extglob

FINDUNICO="|"
REPLACEUNICO=","
PRCTS_STOP="${PRCTS_STOP//$FINDUNICO/$REPLACEUNICO}"
Log " - > PRCTS_STOP  : ${PRCTS_STOP}    	"


Log " ***** EXECUCAO DO SCRIPT ******   	"

sqlplus -S /nolog <<@EOF >> ${NOME_SCRIPT}_${PARTICAO_NF}_${PROCESSO_SCRIPT}.log 2>> ${NOME_SCRIPT}_${PARTICAO_NF}_${PROCESSO_SCRIPT}.err
CONNECT ${STRING_CONEXAO_SCRIPT}
set define off;
SET SERVEROUTPUT ON SIZE 1000000;
set timing on;
SPOOL  ${SPOOL_FILE_SCRIPT} 
var v_st_processamento    VARCHAR2(50) = 'Finalizado'
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_CURSOR_PRINCIPAL'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0

var v_qtd_atu_nf          NUMBER         = 0
var v_qtd_atu_inf         NUMBER         = 0
var v_qtd_atu_cli         NUMBER         = 0
var v_qtd_atu_comp        NUMBER         = 0
var v_qtd_atu_st          NUMBER         = 0

WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_CURSOR_PRINCIPAL
PROMPT ### Inicio do processo ###
PROMPT

<<PRINCIPAL>>
DECLARE

	v_action_name VARCHAR2(32) := substr('MAP_2_CURSOR_PRINCIPAL',1,32);
	v_module_name VARCHAR2(32) := substr('${PROCESSO_SCRIPT}',1,32);
	
	CONSTANTE_LIMIT PLS_INTEGER := 5000; 

	-- Variaveis locais
	v_ds_etapa            VARCHAR2(4000);	
	
	PROCEDURE prcts_etapa(p_ds_ddo IN VARCHAR2) AS 
		
	BEGIN
		BEGIN			
			v_ds_etapa := substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' : ' || p_ds_ddo || ' >> ' || v_ds_etapa,1,4000); 
		EXCEPTION
		WHEN OTHERS THEN
			NULL;
		END;
		BEGIN
			DBMS_APPLICATION_INFO.set_client_info(substr(v_ds_etapa ,1,62));				 	
		EXCEPTION
		WHEN OTHERS THEN
			NULL;
		END;
		BEGIN
			DBMS_OUTPUT.PUT_LINE(substr(v_ds_etapa ,1,62));				 	
		EXCEPTION
		WHEN OTHERS THEN
			NULL;
		END;
	END;
		
BEGIN

	-----------------------------------------------------------------------------
	--> Nomeando o processo
	-----------------------------------------------------------------------------	
	DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,null);
	DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,v_action_name);			
	
	-- Inicializacao
	prcts_etapa('Inicializacao');

	IF TRIM(UPPER('${TABELA_NF}')) LIKE '%NFEN%' OR TRIM(UPPER('${TABELA_INF}')) LIKE '%NFEM%' THEN
		GOTO ENTRADA;
	ELSIF TRIM(UPPER('${TABELA_NF}')) LIKE '%NFSD%' OR TRIM(UPPER('${TABELA_INF}')) LIKE '%NFSD%' THEN
		GOTO SAIDA;
	ELSE
		GOTO TELCOM;
	END IF;	
	RETURN;
			
	<<ENTRADA>>
	BEGIN
		NULL;
		
		GOTO FIM;
		
		RETURN;		
	END;
		
	<<SAIDA>>
	BEGIN
		NULL;
		
		GOTO FIM;
		
		RETURN;		
	END;
		
	<<TELCOM>>
	DECLARE
   
		CURSOR c_sanea
		IS	
		WITH tmp_nf AS (
			SELECT /*+ parallel(15) index(nf MESTRE_NFTL_SERVP1) index(inf ITEM_NFTL_SERVP1) lead(f nf inf) CURSOR_SHARING_FORCE */
				f.rowid rowid_f,
				nf.rowid rowid_nf,
				inf.rowid rowid_inf,
				nf.emps_cod,
				nf.fili_cod,
				nf.mnfst_serie,
				nf.mnfst_num,			
				nf.mnfst_dtemiss,			
				nf.mdoc_cod,
				nf.catg_cod,
				nf.cadg_cod,
				inf.infst_num_seq,
				ROW_NUMBER() OVER (PARTITION BY nf.rowid ORDER BY inf.infst_num_seq) nu_nf	
			FROM 
				openrisow.item_nftl_serv   ${PARTITION_INF} inf, 
				openrisow.mestre_nftl_serv ${PARTITION_NF} nf, 
				openrisow.filial f
			WHERE ${FILTRO_SCRIPT}
			AND   nf.emps_cod		= f.emps_cod
			AND   nf.fili_cod   	= f.fili_cod
			AND   inf.emps_cod      (+) = nf.emps_cod
			AND   inf.fili_cod      (+) = nf.fili_cod
			AND   inf.infst_serie   (+) = nf.mnfst_serie
			AND   inf.infst_num     (+) = nf.mnfst_num
			AND   inf.infst_dtemiss (+) = nf.mnfst_dtemiss
		)
		, tmp_cli AS (			
			SELECT 
				/*+ index(comp COMPLVU_CLIFORNECP1)*/
				comp.rowid as rowid_comp,
				cli.*
			FROM 
				openrisow.complvu_clifornec comp,
				(
				SELECT /*+ index(cli CLI_FORNEC_TRANSPP1)*/
					nf.rowid_nf,
					cli.rowid as rowid_cli,
					cli.cadg_cod,
					cli.catg_cod,
					cli.cadg_dat_atua,
					ROW_NUMBER() OVER (PARTITION BY cli.cadg_cod, cli.catg_cod ORDER BY cli.cadg_dat_atua DESC) nu_cli	
				FROM 
					openrisow.cli_fornec_transp cli,
					tmp_nf nf
				WHERE nf.nu_nf = 1
				AND cli.cadg_cod       = nf.cadg_cod
				AND cli.catg_cod       = nf.catg_cod
				AND cli.cadg_dat_atua <= nf.mnfst_dtemiss
			) cli 
			WHERE cli.nu_cli = 1
			AND comp.cadg_cod       (+) = cli.cadg_cod
			AND comp.catg_cod       (+) = cli.catg_cod
			AND comp.cadg_dat_atua  (+) = cli.cadg_dat_atua
		)
		SELECT 
			/*+ PARALLEL (15) */
			CASE LEAD(nf.cadg_cod || '|' || nf.catg_cod , 1) OVER (ORDER BY nf.cadg_cod , nf.catg_cod )
				WHEN nf.cadg_cod || '|' || nf.catg_cod
				THEN 'N'
				ELSE 'S'
			END AS last_reg_cli,
			CASE LEAD(nf.rowid_nf, 1) OVER (ORDER BY nf.cadg_cod , nf.catg_cod, nf.rowid_nf, nf.infst_num_seq)
				WHEN nf.rowid_nf
				THEN 'N'
				ELSE 'S'
			END AS last_reg_nf,
			nf.*,
			cli.rowid_cli,
			cli.rowid_comp,
			cli.cadg_dat_atua
		FROM 
			tmp_nf nf,
			tmp_cli cli			
		WHERE cli.rowid_nf (+) = nf.rowid_nf
		ORDER BY nf.cadg_cod , nf.catg_cod, nf.rowid_nf, nf.infst_num_seq
		;
		TYPE t_sanea IS TABLE OF c_sanea%ROWTYPE INDEX BY PLS_INTEGER;
		v_bk_sanea t_sanea;
		v_sanea    c_sanea%ROWTYPE;		
   
	BEGIN
	
		prcts_etapa('OPEN c_sanea');
		OPEN c_sanea;
		LOOP
			FETCH c_sanea BULK COLLECT INTO v_bk_sanea LIMIT CONSTANTE_LIMIT;   
			:v_qtd_processados       := :v_qtd_processados + v_bk_sanea.COUNT;
			
			IF v_bk_sanea.COUNT > 0 THEN
				
				FOR i IN v_bk_sanea.FIRST .. v_bk_sanea.LAST 
				LOOP
					IF v_bk_sanea.EXISTS(i) THEN
						v_sanea := v_bk_sanea(i);						
						IF v_sanea.rowid_inf IS NOT NULL THEN
							:v_qtd_atu_inf := :v_qtd_atu_inf + 1;
						END IF;
						IF v_sanea.last_reg_cli = 'S' THEN
							IF v_sanea.rowid_cli IS NOT NULL THEN 
								:v_qtd_atu_cli := :v_qtd_atu_cli + 1;
							END IF;		
							IF v_sanea.rowid_comp IS NOT NULL THEN
								:v_qtd_atu_comp := :v_qtd_atu_comp + 1;		
							END IF;	
						END IF;						
						IF v_sanea.last_reg_nf = 'S' THEN 
							:v_qtd_atu_nf := :v_qtd_atu_nf + 1;
						END IF;	
					END IF;					
				END LOOP;	
				
			END IF;
			
			${COMMIT_SCRIPT};
			prcts_etapa('QT: ' || :v_qtd_processados);
			EXIT WHEN c_sanea%NOTFOUND;	  
		
		END LOOP;        
		CLOSE c_sanea; 
		prcts_etapa('CLOSE c_sanea : ' || :v_qtd_processados);
		
		GOTO FIM;		
		RETURN;		
	
	END;
	
	<<FIM>>		
	BEGIN
			
		${COMMIT_SCRIPT};
			
		prcts_etapa('FIM : ${COMMIT_SCRIPT} >> Processados  >>  : ' || :v_qtd_processados || ' | NF : ' || :v_qtd_atu_nf || ' | INF : ' || :v_qtd_atu_inf|| ' | CLI : ' || :v_qtd_atu_cli);
		:v_msg_erro :=   substr(substr(nvl(v_ds_etapa,' '),1,3000) || ' <||> ' || substr(:v_msg_erro,1,990) ,1,4000);
			
		-----------------------------------------------------------------------------
		--> Eliminando
		-----------------------------------------------------------------------------
		DBMS_APPLICATION_INFO.set_module(null,null);
		DBMS_APPLICATION_INFO.set_client_info (null);
		
		RETURN;
		
	END ;
	
EXCEPTION           
WHEN OTHERS THEN 
	ROLLBACK;     
	prcts_etapa('ERRO : ' || SUBSTR(SQLERRM,1,500));
	:v_msg_erro :=   substr(substr(nvl(v_ds_etapa,' '),1,3000) || ' <||> ' || substr(:v_msg_erro,1,990) ,1,4000);
	:v_st_processamento := 'Erro';
	:exit_code := 1;
	-----------------------------------------------------------------------------
	--> Eliminando
	-----------------------------------------------------------------------------
	DBMS_APPLICATION_INFO.set_module(null,null);
	DBMS_APPLICATION_INFO.set_client_info (null);	  
END;       
/                   
                    
PROMPT Processado   
ROLLBACK;           
UPDATE gfcadastro.CONTROLE_PROCESSAMENTO cp
   SET cp.dt_fim_proc            = SYSDATE,
       cp.st_processamento       = :v_st_processamento,
       cp.ds_msg_erro            = substr(substr(nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(cp.ds_msg_erro,1,990) ,1,4000),
       cp.qt_atualizados_nf      = NVL(cp.qt_atualizados_nf,0)       + :v_qtd_atu_nf,
       cp.qt_atualizados_inf     = NVL(cp.qt_atualizados_inf,0)      + :v_qtd_atu_inf,
       cp.qt_atualizados_cli     = NVL(cp.qt_atualizados_cli,0)      + :v_qtd_atu_cli,
       cp.qt_atualizados_comp    = NVL(cp.qt_atualizados_comp,0)     + :v_qtd_atu_comp,
	   cp.qt_cad_nao_encontrado  = NVL(cp.qt_cad_nao_encontrado,0)   + :v_qtd_atu_st
 WHERE cp.rowid = '${ROWID_CP}';
COMMIT;             
                    
PROMPT Processado   
                    
exit :exit_code;    
                    
@EOF

RETORNO=$?

${WAIT}

fc_saida ${RETORNO}

