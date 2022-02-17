#!/bin/bash
PARTICAO_NF=${1}
PARTICAO_INF=${2}
ROWID_CP=${3}
sqlplus -S /nolog <<@EOF >> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.log 2>> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.err
CONNECT ${STRING_CONEXAO}
set define off;
SET SERVEROUTPUT ON SIZE 1000000;
set timing on;
SPOOL  ${SPOOL_FILE} 
var v_st_processamento    VARCHAR2(50) = 'Em Processamento'
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_DELETAR_INF'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
var v_qtd_atu_nf          NUMBER = 0
var v_qtd_atu_inf         NUMBER = 0
var v_qtd_atu_cli         NUMBER = 0
var v_qtd_atu_comp        NUMBER = 0
var v_qtd_reg_paralizacao NUMBER = 0

WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_REGRA_DELETAR_INF
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT

DECLARE
   
   CONSTANTE_LIMIT PLS_INTEGER := 250000; 
   
   CURSOR c_inf
       IS
	SELECT  /*+ parallel(15) */
	   inf.rowid    rowid_inf  
    FROM openrisow.item_nftl_serv   PARTITION (${PARTICAO_INF}) inf
    WHERE ${FILTRO};
   TYPE t_inf IS TABLE OF c_inf%ROWTYPE INDEX BY PLS_INTEGER;
   v_bk_inf t_inf;
   
   v_rowid_nf            ROWID := NULL;
   v_ds_etapa            VARCHAR2(4000);
   PROCEDURE prc_tempo(p_ds_ddo IN VARCHAR2) AS 
   BEGIN
     v_ds_etapa := substr(p_ds_ddo || ' >> ' || v_ds_etapa,1,4000); 
     DBMS_OUTPUT.PUT_LINE(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' ||  p_ds_ddo ,1,2000));
   EXCEPTION
     WHEN OTHERS THEN
	   NULL;
   END;
   
BEGIN
   prc_tempo('inicio');  
   OPEN c_inf;
   LOOP
      FETCH c_inf BULK COLLECT INTO v_bk_inf LIMIT CONSTANTE_LIMIT;   
	  :v_qtd_atu_inf       := :v_qtd_atu_inf + v_bk_inf.COUNT;
      IF v_bk_inf.COUNT > 0 THEN
		FORALL i IN v_bk_inf.FIRST .. v_bk_inf.LAST
			DELETE FROM openrisow.item_nftl_serv PARTITION (${PARTICAO_INF}) inf WHERE ROWID = v_bk_inf(i).rowid_inf;
      END IF;
	  ${COMMIT};		
	  EXIT WHEN c_inf%NOTFOUND;
   END LOOP;
   CLOSE c_inf;
   ${COMMIT};		
   prc_tempo('Fim - Processados ${COMMIT}:      ' || :v_qtd_atu_inf);
EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK;
      prc_tempo('ERRO : ' || SUBSTR(SQLERRM,1,500));
      :v_msg_erro := SUBSTR(v_ds_etapa || ' >> ' || :v_msg_erro,1,4000);
      :v_st_processamento := 'Erro';
      :exit_code := 1;
END;
/

PROMPT Processado
ROLLBACK;
UPDATE ${TABELA_CONTROLE} cp
   SET cp.dt_fim_proc = SYSDATE,
       cp.st_processamento = :v_st_processamento,
       cp.ds_msg_erro = substr(substr(nvl(:v_msg_erro,' '),1,1000) || cp.ds_msg_erro ,1,4000),
       cp.qt_atualizados_nf = NVL(cp.qt_atualizados_nf,0) + :v_qtd_atu_nf,
       cp.qt_atualizados_inf = NVL(cp.qt_atualizados_inf,0) + :v_qtd_atu_inf,
       cp.qt_atualizados_cli = NVL(cp.qt_atualizados_cli,0) + :v_qtd_atu_cli,
       cp.qt_atualizados_comp = NVL(cp.qt_atualizados_comp,0) + :v_qtd_atu_comp
 WHERE cp.rowid = '${ROWID_CP}'
   AND cp.st_processamento = 'Em Processamento'
   AND cp.NM_PROCESSO = '${PROCESSO}';
COMMIT;


PROMPT Processado

exit :exit_code;

@EOF

RETORNO=$?

${WAIT}

exit ${RETORNO}

