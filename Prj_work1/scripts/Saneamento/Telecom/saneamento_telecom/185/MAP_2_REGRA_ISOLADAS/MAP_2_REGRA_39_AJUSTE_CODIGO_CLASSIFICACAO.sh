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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_39_AJUSTE_CODIGO_CLASSIFICACAO'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
var v_qtd_atu_nf          NUMBER         = 0
var v_qtd_atu_inf         NUMBER         = 0
var v_qtd_ins_inf         NUMBER         = 0
var v_qtd_atu_cli         NUMBER         = 0
var v_qtd_atu_comp        NUMBER         = 0
var v_qtd_reg_paralizacao NUMBER         = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_REGRA_39_AJUSTE_CODIGO_CLASSIFICACAO
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT
DECLARE

    CONSTANTE_LIMIT PLS_INTEGER := 1000; 

	CURSOR c_inf IS
	   select SUBSTR(tab1.row_id_clasf_cod,1,18) as row_id_serv,
			  SUBSTR(tab1.row_id_clasf_cod,19,2) as clasf_cod, 
			  tab1.row_id_item,
			  replace(tab1.infst_serie, ' ','') as serie,
			  tab1.infst_dtemiss as data_emissao,
			  tab1.SERV_COD,
			  tab1.emps_cod,
			  tab1.fili_cod
		 from (  select /*+ parallel(15)*/
						( SELECT /*+ first_rows(1)*/
								s2.rowid||s2.clasfi_cod
						   FROM openrisow.SERVICO_TELCOM S2
						  WHERE S2.EMPS_COD        = item.EMPS_COD
							AND S2.FILI_COD        = item.FILI_COD
							AND S2.SERVTL_COD      = item.SERV_COD
							AND S2.SERVTL_DAT_ATUA =
											   (SELECT MAX(S3.SERVTL_DAT_ATUA)
												   FROM openrisow.SERVICO_TELCOM S3
												  WHERE S3.EMPS_COD = item.EMPS_COD
													AND S3.FILI_COD = item.FILI_COD
													AND S3.SERVTL_COD = item.SERV_COD
													AND S3.SERVTL_DAT_ATUA <= item.INFST_DTEMISS)) row_id_clasf_cod,
						 item.rowid as row_id_item,
						 item.infst_serie,
						 item.infst_dtemiss,
						 item.serv_cod,
						 item.emps_cod,
						 item.fili_cod
					from openrisow.mestre_nftl_serv PARTITION (${PARTICAO_NF})  nf,     
	                     openrisow.item_nftl_serv   PARTITION (${PARTICAO_INF}) item
				   where ${FILTRO} 
					AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ( 'AS1', 'AS2', 'AS3', 'T1') 	AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))
					AND item.emps_cod      = nf.emps_cod   
					AND item.fili_cod      = nf.fili_cod   
					AND item.infst_serie   = nf.mnfst_serie   
					AND item.infst_num     = nf.mnfst_num   
					AND item.infst_dtemiss = nf.mnfst_dtemiss 
					AND item.cfop          <> '0000') tab1
		where SUBSTR(tab1.row_id_clasf_cod,19,2)  IN ('08','09');
  
	v_rowid_item      VARCHAR2(20);
	v_rowid_serv      VARCHAR2(20);
	v_clasf_cod       CHAR(2);
	v_serie           VARCHAR2(5);
	v_data_emis       date;
	v_serv_cod        VARCHAR2(20);
	v_emps_cod        openrisow.item_nftl_serv.emps_cod%type;
	v_fili_cod        openrisow.item_nftl_serv.fili_cod%type;
	v_serv            openrisow.SERVICO_TELCOM%ROWTYPE;
	v_existe          CHAR(1);
	v_cod_serv        varchar2(80);
   

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

	OPEN c_inf;
	LOOP
	FETCH c_inf INTO v_rowid_serv, v_clasf_cod, v_rowid_item, v_serie, v_data_emis, v_serv_cod, v_emps_cod, v_fili_cod;
	EXIT WHEN c_inf%NOTFOUND;
	  
	  --dbms_output.put_line('ServCod :'||v_serv_cod);
	  
	  begin
		--dbms_output.put_line('1 - Verifica se existe servico');
		--dbms_output.put_line('Data emiss: '||v_data_emis);
		SELECT /*+ first_rows(1)*/
			   s2.*
		  INTO v_serv
		  FROM openrisow.SERVICO_TELCOM S2
		 WHERE 1 = 1
		   and s2.emps_cod        = v_emps_cod
		   and s2.fili_cod        = v_fili_cod
		   and (s2.servtl_cod = v_serie||'C08'||v_serv_cod  OR  s2.servtl_cod = v_serie||'C09'||v_serv_cod)
		   and s2.servtl_dat_atua = (SELECT MAX(S3.SERVTL_DAT_ATUA)
									   FROM openrisow.SERVICO_TELCOM S3
									  WHERE S3.EMPS_COD         = s2.EMPS_COD
										AND S3.FILI_COD         = s2.FILI_COD
										AND S3.SERVTL_COD       = s2.servtl_cod
										AND S3.SERVTL_DAT_ATUA  <= v_data_emis)
		   and (substr(s2.clasfi_cod,1,2) <> '08' OR substr(s2.clasfi_cod,1,2) <> '09')
		   and rownum = 1;
		
		v_existe := 'S'; 
		--dbms_output.put_line('-->'||v_existe);
		
	  exception
		when no_data_found then
			v_existe := 'N';
			--dbms_output.put_line('-->'||v_existe);
			SELECT /*+ first_rows(1)*/
				   s2.*
			  INTO v_serv
			  FROM openrisow.SERVICO_TELCOM S2
			 WHERE s2.rowid = v_rowid_serv;          
	  end;      
	  
	  if v_existe = 'S' then
			--dbms_output.put_line('Existe, entao apenas updata nda item.');
		  update OPENRISOW.item_nftl_serv item
			 set item.SERV_COD = v_serv.servtl_cod
		   where item.rowid = v_rowid_item;
		   :v_qtd_atu_inf := :v_qtd_atu_inf + 1;	
	  else
		 
		 --dbms_output.put_line('NAO Existe, entao cria servico nda item.');
		 --dbms_output.put_line('--->'||substr(v_serv.clasfi_cod,1,2));
		 v_cod_serv := case when v_clasf_cod = '08' then v_serie||'C08'||v_serv_cod
							when v_clasf_cod = '09' then v_serie||'C09'||v_serv_cod
							else null
						end;
		 --dbms_output.put_line('Servico a incluir: '||v_cod_serv);
		 
		 INSERT INTO openrisow.servico_telcom (
			  emps_cod,
			  fili_cod,
			  servtl_dat_atua,
			  servtl_cod,
			  clasfi_cod,
			  servtl_desc,
			  servtl_compl,
			  servtl_ind_tprec,
			  servtl_ind_tpserv,
			  servtl_cod_nat,
			  var01,
			  var02,
			  var03,
			  var04,
			  var05,
			  num01,
			  num02,
			  num03,
			  servtl_ind_rec,
			  servtl_tip_utiliz
			) 
		VALUES (
			  v_serv.emps_cod,
			  v_serv.fili_cod,
			  v_data_emis,
			  v_cod_serv,
			  '0199',
			  v_serv.servtl_desc,
			  v_serv.servtl_compl,
			  v_serv.servtl_ind_tprec,
			  v_serv.servtl_ind_tpserv,
			  v_serv.servtl_cod_nat,
			  v_serv.var01,
			  v_serv.var02,
			  v_serv.var03,
			  v_serv.var04,
			  v_serv.var05,
			  v_serv.num01,
			  v_serv.num02,
			  v_serv.num03,
			  v_serv.servtl_ind_rec,
			  '6' -- v_serv.servtl_tip_utiliz
			);
		  
		 -- dbms_output.put_line('Servico a updatar: '||v_cod_serv);
		  update OPENRISOW.item_nftl_serv item
			 set item.SERV_COD = v_cod_serv
		   where item.rowid = v_rowid_item;        
	  end if;

	END LOOP;

	CLOSE c_inf;
  

    ${COMMIT};		
    prc_tempo('Fim - Processados ${COMMIT}');
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
   SET cp.dt_fim_proc          = SYSDATE,
       cp.st_processamento     = :v_st_processamento,
       cp.ds_msg_erro          = substr(substr(nvl(:v_msg_erro,' '),1,1000) || ' >> ' || cp.ds_msg_erro ,1,4000),
       cp.qt_atualizados_nf    = NVL(cp.qt_atualizados_nf,0)   + :v_qtd_atu_nf,
       cp.qt_atualizados_inf   = NVL(cp.qt_atualizados_inf,0)  + :v_qtd_atu_inf,
       cp.qt_atualizados_cli   = NVL(cp.qt_atualizados_cli,0)  + :v_qtd_atu_cli,
       cp.qt_atualizados_comp  = NVL(cp.qt_atualizados_comp,0) + :v_qtd_atu_comp
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

