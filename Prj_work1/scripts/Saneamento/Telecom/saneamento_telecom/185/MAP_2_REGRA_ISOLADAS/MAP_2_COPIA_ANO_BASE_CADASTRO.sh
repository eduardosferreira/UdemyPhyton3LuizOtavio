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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_COPIA_ANO_BASE_CADASTRO'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
var v_qtd_atu_comp        NUMBER = 0
var v_qtd_atu_cli         NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_COPIA_ANO_BASE_CADASTRO
PROMPT ### Inicio do processo ${0}  ###
PROMPT
DECLARE

   v_action_name VARCHAR2(32) := substr('MAP_2_COPIA_ANO_BASE_CADASTRO',1,32);
   v_module_name VARCHAR2(32) := substr('${PROCESSO}',1,32);

   CONSTANTE_LIMIT PLS_INTEGER := 50000; 
  
   CURSOR c_sanea
    IS
		WITH tab1 AS
		  (SELECT			
            -- NF
			nf.rowid AS rowid_nf,
			nf.catg_cod AS mnfst_catg_cod,
			nf.cadg_cod AS mnfst_cadg_cod,
			nf.mnfst_dtemiss
		  FROM openrisow.mestre_nftl_serv PARTITION (${PARTICAO_NF}) nf
		 WHERE ${FILTRO}
	  ),
	    tab2 AS
		  (SELECT tmp.*,
			comp.rowid rowid_comp
		  FROM openrisow.complvu_clifornec comp,
			(SELECT nf.rowid_nf  AS rowid_nf_cli,
					cli.rowid    AS rowid_cli,
					cli.cadg_cod AS cadg_cod_cli,
					cli.catg_cod AS catg_cod_cli,
					cli.cadg_dat_atua,
					ROW_NUMBER() OVER (PARTITION BY nf.rowid_nf ORDER BY cli.cadg_dat_atua DESC) nu
				FROM openrisow.cli_fornec_transp cli,
					 tab1 nf
				WHERE   cli.cadg_cod       = nf.mnfst_cadg_cod
					AND cli.catg_cod       = nf.mnfst_catg_cod
					AND cli.cadg_dat_atua <= nf.mnfst_dtemiss
			) tmp
		  WHERE nu               = 1
		  AND comp.cadg_cod      = tmp.cadg_cod_cli
		  AND comp.catg_cod      = tmp.catg_cod_cli
		  AND comp.cadg_dat_atua = tmp.cadg_dat_atua
	  )
	SELECT /*+ PARALLEL (15) */
	  t2.rowid_cli,
	  t2.rowid_comp,
	  MIN(t2.rowid_nf_cli) AS DS_OBSERVACAO,
	  MIN(t2.cadg_dat_atua)  cadg_dat_atua
	FROM tab2 t2
	   , tab1 t1  
	WHERE t2.rowid_nf_cli     = t1.rowid_nf 
    GROUP BY t2.rowid_cli,
			 t2.rowid_comp;
		
   TYPE t_sanea IS TABLE OF c_sanea%ROWTYPE INDEX BY PLS_INTEGER;
   v_bk_sanea t_sanea;
   v_sanea    c_sanea%ROWTYPE;

   v_ds_etapa            VARCHAR2(4000);
   PROCEDURE prc_tempo(p_ds_ddo IN VARCHAR2) AS 
   BEGIN
     v_ds_etapa := substr(p_ds_ddo || ' >> ' || v_ds_etapa,1,4000); 
     DBMS_OUTPUT.PUT_LINE(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' ||  p_ds_ddo ,1,2000));
   EXCEPTION
     WHEN OTHERS THEN
	   NULL;
   END;

	PROCEDURE prcts_stop(p_ds_ddo IN VARCHAR2 := NULL) AS 		
	BEGIN
		BEGIN
			v_ds_etapa := substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' - > ' || p_ds_ddo || ' >> ' || v_ds_etapa,1,4000); 
		EXCEPTION
		 WHEN OTHERS THEN
		   NULL;
		END;
		RAISE_APPLICATION_ERROR (-20343, 'STOP! ' || SUBSTR(v_ds_etapa,1,1000));
	END;	 
  
BEGIN

   -- Inicializacao
   prc_tempo('Inicializacao');

   -----------------------------------------------------------------------------
   --> Nomeando o processo
   -----------------------------------------------------------------------------	
   DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,null);
   DBMS_APPLICATION_INFO.SET_MODULE(v_module_name,v_action_name);
   
   prc_tempo('SANEA');
   OPEN c_sanea;
   LOOP
	  FETCH c_sanea BULK COLLECT INTO v_bk_sanea LIMIT CONSTANTE_LIMIT;   
	  :v_qtd_processados       := :v_qtd_processados + v_bk_sanea.COUNT;
	  IF v_bk_sanea.COUNT > 0 THEN
		FOR i IN v_bk_sanea.FIRST .. v_bk_sanea.LAST LOOP
			BEGIN
				INSERT INTO GFCADASTRO.TMP_CADASTRO_CLI_COMP (ROWID_CLI,ROWID_COMP,DS_OBSERVACAO,DT_PERIODO) VALUES (v_bk_sanea(i).rowid_cli,v_bk_sanea(i).rowid_comp,v_bk_sanea(i).DS_OBSERVACAO,v_bk_sanea(i).cadg_dat_atua);
				:v_qtd_atu_comp := :v_qtd_atu_comp + 1;
				:v_qtd_atu_cli  := :v_qtd_atu_cli  + 1;
				${COMMIT};		
			EXCEPTION
			WHEN DUP_VAL_ON_INDEX THEN
				ROLLBACK;
			END;
		END LOOP;
	  END IF;       
	  ${COMMIT};	
	  EXIT WHEN c_sanea%NOTFOUND;	  
   END LOOP;        
   CLOSE c_sanea;   
   ${COMMIT};		
   prc_tempo('Fim - Processados ${COMMIT}:      ' || :v_qtd_processados);
   :v_msg_erro :=   substr(substr(nvl(v_ds_etapa,' '),1,3000) || ' <||> ' || substr(:v_msg_erro,1,990) ,1,4000);
   -----------------------------------------------------------------------------
   --> Eliminando a nomeação
   -----------------------------------------------------------------------------
   DBMS_APPLICATION_INFO.set_module(null,null);
   DBMS_APPLICATION_INFO.set_client_info (null);   
EXCEPTION           
   WHEN OTHERS THEN 
      ROLLBACK;     
      prc_tempo('ERRO : ' || SUBSTR(SQLERRM,1,500));
      :v_msg_erro := SUBSTR(v_ds_etapa || ' >> ' || :v_msg_erro,1,4000);
      :v_st_processamento := 'Erro';
      :exit_code := 1;
	  -----------------------------------------------------------------------------
	  --> Eliminando a nomeação
	  -----------------------------------------------------------------------------
	  DBMS_APPLICATION_INFO.set_module(null,null);
	  DBMS_APPLICATION_INFO.set_client_info (null);	 	  
END;                
/                   

PROMPT Processado   
ROLLBACK;           
UPDATE ${TABELA_CONTROLE} cp
   SET cp.dt_fim_proc          = SYSDATE,
       cp.st_processamento     = :v_st_processamento,
       cp.ds_msg_erro          = substr(substr(nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(cp.ds_msg_erro,1,990) ,1,4000),
       cp.qt_atualizados_nf    = NVL(cp.qt_atualizados_nf,0)   + :v_qtd_processados,
	   cp.qt_atualizados_cli   = NVL(cp.qt_atualizados_cli,0)  + :v_qtd_atu_cli,
	   cp.qt_atualizados_comp  = NVL(cp.qt_atualizados_comp,0) + :v_qtd_atu_comp
 WHERE cp.rowid = '${ROWID_CP}';
COMMIT;
PROMPT Processado   
                    
exit :exit_code;    
                    
@EOF                

RETORNO=$?

${WAIT}

exit ${RETORNO}

