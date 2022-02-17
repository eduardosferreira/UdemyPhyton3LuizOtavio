#!/bin/bash
PARTICAO_NF=${1}
PARTICAO_INF=${2}
ROWID_CP=${3}
sqlplus -S /nolog <<@EOF >> ${SCRIPT}_${PARTICAO_NF}.log 2>> ${SCRIPT}_${PARTICAO_NF}.err
CONNECT ${STRING_CONEXAO}
set define off;
SET SERVEROUTPUT ON SIZE 1000000;
set timing on;
SPOOL  ${SPOOL_FILE} 
var v_st_processamento    VARCHAR2(50) = 'Em Processamento'
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_TMP_MESTRE_REL_GRP'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
var v_qtd_atu_nf          NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_REGRA_TMP_MESTRE_REL_GRP
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT

DECLARE
/*

-- Create table
create table GFCADASTRO.TMP_MESTRE_REL_GRP
(
  emps_cod    VARCHAR2(9),
  fili_cod    VARCHAR2(9),
  mnfst_serie VARCHAR2(5),
  periodo     VARCHAR2(7),
  qtde        NUMBER
);


create index GFCADASTRO.TMP_MESTRE_REL_GRP_IND1 on TMP_MESTRE_REL_GRP (periodo);
create index GFCADASTRO.TMP_MESTRE_REL_GRP_IND2 on TMP_MESTRE_REL_GRP (MNFST_SERIE);
create index GFCADASTRO.TMP_MESTRE_REL_GRP_IND3 on TMP_MESTRE_REL_GRP (emps_cod, fili_cod, mnfst_serie, periodo);

*/
   
   CONSTANTE_LIMIT PLS_INTEGER := 250000; 
   
   v_mnfst_dtemiss  openrisow.mestre_nftl_serv.mnfst_dtemiss%type;
   
   CURSOR c_nf(p_mnfst_dtemiss in openrisow.mestre_nftl_serv.mnfst_dtemiss%type)
       IS
	SELECT  /*+ PARALLEL (nf,8,8) */ nf.emps_cod, nf.fili_cod, nf.mnfst_serie,  TO_CHAR(TRUNC(nf.MNFST_DTEMISS,'MM'),'MM/YYYY') periodo , count(1) qtde
    FROM  openrisow.mestre_nftl_serv nf
	WHERE ${FILTRO} AND TRUNC(nf.mnfst_dtemiss,'MM') = TRUNC(p_mnfst_dtemiss,'MM') 
	GROUP BY nf.emps_cod, nf.fili_cod, nf.mnfst_serie, TRUNC(nf.mnfst_dtemiss,'MM');
   TYPE t_nf IS TABLE OF c_nf%ROWTYPE INDEX BY PLS_INTEGER;
   v_bk_nf t_nf;
   
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
   
   SELECT cp.DT_LIMITE_INF_NF
   INTO   v_mnfst_dtemiss
   FROM   ${TABELA_CONTROLE} cp
   WHERE  cp.rowid = '${ROWID_CP}';
   
   IF v_mnfst_dtemiss = TRUNC(v_mnfst_dtemiss,'MM') THEN
	   DELETE FROM GFCADASTRO.TMP_MESTRE_REL_GRP WHERE 	periodo = TO_CHAR(TRUNC(v_mnfst_dtemiss,'MM'),'MM/YYYY');
	   ${COMMIT};
	   prc_tempo('cursor');
	   OPEN c_nf(p_mnfst_dtemiss => v_mnfst_dtemiss);
	   LOOP
		  FETCH c_nf BULK COLLECT INTO v_bk_nf LIMIT CONSTANTE_LIMIT;   
		  :v_qtd_atu_nf       := :v_qtd_atu_nf + v_bk_nf.COUNT;
		  IF v_bk_nf.COUNT > 0 THEN
			FORALL i IN v_bk_nf.FIRST .. v_bk_nf.LAST		  	
			  INSERT INTO GFCADASTRO.TMP_MESTRE_REL_GRP(emps_cod,fili_cod,mnfst_serie,periodo,qtde)  VALUES (v_bk_nf(i).emps_cod,v_bk_nf(i).fili_cod,v_bk_nf(i).mnfst_serie,v_bk_nf(i).periodo,v_bk_nf(i).qtde);
		  END IF;
		  ${COMMIT};	
		  EXIT WHEN c_nf%NOTFOUND;	  
	   END LOOP;
	   CLOSE c_nf;
   END IF;
   
   ${COMMIT};		
   prc_tempo('Fim - Processados ${COMMIT}:      ' || :v_qtd_atu_nf);
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
       cp.qt_atualizados_nf = NVL(cp.qt_atualizados_nf,0) + :v_qtd_atu_nf
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

