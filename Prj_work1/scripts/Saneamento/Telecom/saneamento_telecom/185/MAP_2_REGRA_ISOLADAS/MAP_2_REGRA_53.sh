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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_53'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
var v_qtd_atu_nf          NUMBER         = 0
var v_qtd_atu_inf         NUMBER         = 0
var v_qtd_ins_inf         NUMBER         = 0
var v_qtd_atu_cli         NUMBER         = 0
var v_qtd_atu_comp        NUMBER         = 0
var v_qtd_atu_st          NUMBER         = 0


WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_REGRA_53
PROMPT ### Inicio do processo ${0}  ###
PROMPT
DECLARE

   v_action_name VARCHAR2(32) := substr('MAP_2_REGRA_53',1,32);
   v_module_name VARCHAR2(32) := substr('${PROCESSO}',1,32);
    
   CONSTANTE_LIMIT CONSTANT PLS_INTEGER := 2000; 
     
   CURSOR c_sanea
    IS	
	SELECT  /*+ parallel(15) */
		   nf.rowid rowid_nf 
		   , nf.emps_cod
           , nf.fili_cod
           , nf.mnfst_serie
           , nf.mnfst_num
           , nf.mnfst_dtemiss			
		   , inf.rowid rowid_inf
		   , inf.infst_val_cont 
	FROM    openrisow.item_nftl_serv      PARTITION (${PARTICAO_INF}) inf,  
			openrisow.mestre_nftl_serv    PARTITION (${PARTICAO_NF})  nf   
	WHERE   ${FILTRO}
	      AND nf.fili_cod in ('3506','3501') 
	      AND inf.emps_cod                                               = nf.emps_cod
		  AND inf.fili_cod                                               = nf.fili_cod
		  AND inf.infst_serie                                            = nf.mnfst_serie
		  AND inf.infst_num                                              = nf.mnfst_num
		  AND inf.infst_dtemiss                                          = nf.mnfst_dtemiss
		  AND NVL(inf.infst_outras_icms,0) < 0
		  AND NVL(inf.infst_val_cont,0) < 0
		  AND NVL(inf.infst_val_serv,0) < 0
		  AND NVL(inf.infst_base_icms,0) = 0
		  AND NVL(inf.infst_val_icms,0) = 0
		  AND NVL(inf.infst_aliq_icms,0) = 0 
		  AND EXISTS (SELECT /*+ first_rows(1) */1 FROM  openrisow.item_nftl_serv    PARTITION (${PARTICAO_INF}) inf1 
					  WHERE inf1.emps_cod                                               = nf.emps_cod
					  AND   inf1.fili_cod                                               = nf.fili_cod
					  AND   inf1.infst_serie                                            = nf.mnfst_serie
					  AND   inf1.infst_num                                              = nf.mnfst_num
					  AND   inf1.infst_dtemiss                                          = nf.mnfst_dtemiss
					  AND   NVL(inf1.infst_val_cont,0) > 0
					  AND   NVL(inf1.infst_val_cont,0) >= NVL(inf.infst_val_cont,0) *(-1)
					  )					     
    ORDER BY rowid_nf,  inf.infst_val_cont ;

		
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

				UPDATE openrisow.item_nftl_serv PARTITION (${PARTICAO_INF}) inf1
				SET   inf1.var05 = SUBSTR('R2015_53[' || v_bk_sanea(i).rowid_inf || ']:' || inf1.infst_val_cont || '|' || inf1.infst_val_desc || '>>'|| inf1.var05,1,150)
					, inf1.infst_val_cont = NVL(inf1.infst_val_cont,0) + v_bk_sanea(i).infst_val_cont
					, inf1.infst_val_desc = NVL(inf1.infst_val_desc,0) + (v_bk_sanea(i).infst_val_cont *(-1))   	
				WHERE inf1.emps_cod              = v_bk_sanea(i).emps_cod
				AND   inf1.fili_cod              = v_bk_sanea(i).fili_cod
				AND   inf1.infst_serie           = v_bk_sanea(i).mnfst_serie
				AND   inf1.infst_num             = v_bk_sanea(i).mnfst_num
				AND   inf1.infst_dtemiss         = v_bk_sanea(i).mnfst_dtemiss
				AND   NVL(inf1.infst_val_cont,0) > 0
				AND   NVL(inf1.infst_val_cont,0) >= NVL(v_bk_sanea(i).infst_val_cont,0) * (-1)
				AND   ROWNUM < 2;
				
				IF SQL%FOUND THEN
				
					:v_qtd_atu_inf             := :v_qtd_atu_inf + 2;  
					UPDATE openrisow.item_nftl_serv PARTITION (${PARTICAO_INF}) inf
					SET inf.var05 = SUBSTR('R2015_53u:' || inf.infst_outras_icms || '|' || inf.infst_val_cont || '|' || inf.infst_val_serv || '>>'|| inf.var05,1,150)
					  , inf.infst_outras_icms = 0
					  , inf.infst_val_cont    = 0 
					  , inf.infst_val_serv    = 0		
					WHERE inf.rowid = v_bk_sanea(i).rowid_inf;
					
					gfcadastro.pkgtc_kyros_process.prcts_process_log(p_id_process    =>  ${SEQUENCE_CONTROLE},    
																	 p_nm_var01      =>  v_bk_sanea(i).emps_cod,
																	 p_nm_var02      =>  v_bk_sanea(i).fili_cod,
																	 p_nm_var03      =>  v_bk_sanea(i).mnfst_serie,
																	 p_nm_var04      =>  TO_CHAR(v_bk_sanea(i).mnfst_dtemiss,'YYYY-MM-DD'),
																	 p_nm_var05      =>  '|R2015_53|',
																	 p_nr_var02      =>  2);
																	 
					gfcadastro.pkgtc_kyros_process.prcts_process_log(p_id_process    =>  ${SEQUENCE_CONTROLE},    
																	 p_nm_var01      =>  v_bk_sanea(i).emps_cod,
																	 p_nm_var02      =>  v_bk_sanea(i).fili_cod,
																	 p_nm_var03      =>  v_bk_sanea(i).mnfst_serie,
																	 p_nm_var04      =>  TO_CHAR(v_bk_sanea(i).mnfst_dtemiss,'YYYY-MM-DD'),
																	 p_nm_var05      =>  gfcadastro.pkgtc_kyros_process.CONSTANTE_AMOUNT,
																	 p_nr_var02      =>  2);																	 
				END IF; 				
			
			END LOOP;    
		  
		END IF;       
		
		${COMMIT};	
		
		gfcadastro.pkgtc_kyros_process.prcts_insere_process_log;
		
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
   SET cp.dt_fim_proc            = SYSDATE,
       cp.st_processamento       = :v_st_processamento,
       cp.ds_msg_erro            = substr(substr(nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(cp.ds_msg_erro,1,990) ,1,4000),
       cp.qt_atualizados_nf      = NVL(cp.qt_atualizados_nf,0)       + :v_qtd_atu_nf,
       cp.qt_atualizados_inf     = NVL(cp.qt_atualizados_inf,0)      + :v_qtd_atu_inf,
       cp.qt_atualizados_cli     = NVL(cp.qt_atualizados_cli,0)      + :v_qtd_atu_cli,
       cp.qt_atualizados_comp    = NVL(cp.qt_atualizados_comp,0)     + :v_qtd_atu_comp,
	   cp.qt_cad_nao_encontrado  = NVL(cp.qt_cad_nao_encontrado,0)   + :v_qtd_processados
 WHERE cp.rowid = '${ROWID_CP}';
COMMIT;
PROMPT Processado   
                    
exit :exit_code;    
                    
@EOF                

RETORNO=$?

${WAIT}

exit ${RETORNO}

