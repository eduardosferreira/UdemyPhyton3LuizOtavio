#!/bin/bash
PARTICAO_NF=${1}
PARTICAO_INF=${2}
ROWID_CP=${3}
FILTRO_SCRIPT="${4:-${FILTRO}}"


#turn it on
shopt -s extglob
CRITERIOS_SCRIPT="${${FILTRO_SCRIPT}}"
CRITERIOS_SCRIPT="${CRITERIOS_SCRIPT^^}"
### Trim leading whitespaces ###
CRITERIOS_SCRIPT="${CRITERIOS_SCRIPT##*( )}" 
### trim trailing whitespaces  ##
CRITERIOS_SCRIPT="${CRITERIOS_SCRIPT%%*( )}"
CRITERIOS_SCRIPT="${CRITERIOS_SCRIPT^^}" 	
CRITERIOS_SCRIPT="AND ${CRITERIOS_SCRIPT} AND " 	

REGRAS_SCRIPT="1=1"
REGRAS_SCRIPT_AUX="1=1"
CRITERIOS_AUX=""
FINDUNICO="AND"
REPLACEUNICO="|"
CRITERIOS_SCRIPT="${CRITERIOS_SCRIPT//$FINDUNICO/$REPLACEUNICO}"

echo "${CRITERIOS_SCRIPT}"

IFS='|' # colon (|) is set as delimiter
read -ra ADDR <<< "$CRITERIOS_SCRIPT" # str is read into an array as tokens separated by IFS

for i in "${ADDR[@]}"; do # access each element of array

    CRITERIOS_AUX="$i"
	### Trim leading whitespaces ###
	CRITERIOS_AUX="${CRITERIOS_AUX##*( )}" 
	### trim trailing whitespaces  ##
	CRITERIOS_AUX="${CRITERIOS_AUX%%*( )}"
	CRITERIOS_AUX="${CRITERIOS_AUX^^}" 	
	case ${CRITERIOS_AUX} in
    	   *"FILI_COD"*|*"EMPS_COD"*|*"MDOC_COD"*|*"SERIE"*|*"serie"*|*"MNFST_SERIE"*)
	    REGRAS_SCRIPT="${REGRAS_SCRIPT^^} AND ${CRITERIOS_AUX}" 
	;;
    	*)
        REGRAS_SCRIPT="${REGRAS_SCRIPT^^}" 
	;;
	esac
	
	case ${CRITERIOS_AUX} in
    	   *"FILI_COD"*|*"EMPS_COD"*)
	    REGRAS_SCRIPT_AUX="${REGRAS_SCRIPT_AUX^^} AND ${CRITERIOS_AUX}" 
	;;
    	*)
        REGRAS_SCRIPT_AUX="${REGRAS_SCRIPT_AUX^^}" 
	;;
	esac	

	echo "${CRITERIOS_AUX}"
done
echo "${REGRAS_SCRIPT}"
echo "${REGRAS_SCRIPT_AUX}"

IFS=' ' # reset to default value after usage

# turn it off
shopt -u extglob

FINDUNICO="|"
REPLACEUNICO=","


sqlplus -S /nolog <<@EOF >> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.log 2>> ${SCRIPT}_${PARTICAO_NF}_${PROCESSO}.err
CONNECT ${STRING_CONEXAO}
set define off;
SET SERVEROUTPUT ON SIZE 1000000;
set timing on;
SPOOL  ${SPOOL_FILE} 
var v_st_processamento    VARCHAR2(50) = 'Em Processamento'
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_RELATORIO_CFOP_NEGATIVO'
var exit_code             NUMBER = 0
var v_qtd_processados     NUMBER = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_RELATORIO_CFOP_NEGATIVO
PROMPT ### Inicio do processo ${0}  ###
PROMPT
BEGIN 
 UPDATE gfcadastro.CONTROLE_PROCESSAMENTO cp
   SET cp.ds_msg_erro            = substr(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(cp.ds_msg_erro,1,990) ,1,4000),
       cp.qt_cad_nao_encontrado  = NVL(cp.qt_cad_nao_encontrado,0) + :v_qtd_processados
 WHERE cp.rowid = '${ROWID_CP}';
 COMMIT;
 
EXCEPTION
WHEN OTHERS THEN
	BEGIN
		UPDATE gfcadastro.CONTROLE_PROCESSAMENTO  
		SET ds_msg_erro              = substr(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(ds_msg_erro,1,990) ,1,4000),
			qt_cad_nao_encontrado    = NVL(qt_cad_nao_encontrado,0) + :v_qtd_processados
		WHERE dt_limite_inf_nf       = TO_DATE('${DATA_INICIO}','DD/MM/YYYY') 
		AND UPPER(TRIM(NM_PROCESSO)) = UPPER(TRIM('${PROCESSO}'))
        AND qt_registros_inf > 0
        AND qt_registros_nf  > 0		
		AND ROWNUM < 2;
		COMMIT;
	EXCEPTION
		WHEN OTHERS THEN
			ROLLBACK;
	END;
END;
/
DECLARE
   v_st_processamento    VARCHAR2(50)   := 'Em Processamento';
   v_msg_erro            VARCHAR2(4000) := 'MAP_2_RELATORIO_CFOP_NEGATIVO';
   exit_code             NUMBER         := 0;
   v_qtd_processados     NUMBER         := 0;
   
   v_COMMIT              VARCHAR2(100)  := upper(trim('${COMMIT}'));
   v_PROCESSO            VARCHAR2(1000) := upper(trim('${PROCESSO}'));
   v_data_inicio         DATE           := TO_DATE('${DATA_INICIO}','DD/MM/YYYY');
   v_data_fim            DATE           := TO_DATE('${DATA_FIM}','DD/MM/YYYY');
   v_REGRAS_SCRIPT_AUX   VARCHAR2(4000) := upper(trim('${REGRAS_SCRIPT_AUX}'));
   v_REGRAS_SCRIPT       VARCHAR2(4000) := upper(trim('${REGRAS_SCRIPT}'));   
   v_data_controle       DATE           := v_data_inicio;
   

--   v_COMMIT              VARCHAR2(100)  := upper(trim('COMMIT'));
--   v_PROCESSO            VARCHAR2(1000) := upper(trim('${PROCESSO}'));
--   v_data_inicio         DATE           := TO_DATE('01/01/2015','DD/MM/YYYY');
--   v_data_fim            DATE           := TO_DATE('31/01/2015','DD/MM/YYYY');
--   v_REGRAS_SCRIPT_AUX   VARCHAR2(4000) := upper(trim(q'[nf.emps_cod = 'TBRA' and nf.fili_cod = '0001' and nf.ctr_ser_ori = 'U  T' ]'));
--   v_REGRAS_SCRIPT       VARCHAR2(4000) := upper(trim('1=1'));   
--   v_data_controle       DATE           := v_data_inicio;

BEGIN


	BEGIN
	    SELECT cp.DT_LIMITE_INF_NF INTO   v_data_controle FROM   gfcadastro.CONTROLE_PROCESSAMENTO cp	 WHERE  cp.rowid = '${ROWID_CP}';   
	EXCEPTION
		WHEN OTHERS THEN
			v_data_controle  := v_data_inicio;
	END;

	IF v_data_inicio  != v_data_controle THEN
		RETURN;
	END IF;

DECLARE
/*
DROP TABLE GFCADASTRO.TB_REL_CFOP_NEGATIVO_AGRUPADO;
CREATE TABLE GFCADASTRO.TB_REL_CFOP_NEGATIVO_AGRUPADO
( 
    NM_PROCESSO	       VARCHAR2(4000) NOT NULL
  , DT_PROCESSSO       DATE DEFAULT SYSDATE NOT NULL
  , VOLUME             VARCHAR2(10)
  , MES                VARCHAR2(10)
  , EMPS_COD           VARCHAR2(9)
  , FILI_COD           VARCHAR2(9)
  , MDOC_COD           VARCHAR2(3)
  , INFST_SERIE        VARCHAR2(5)
  , ESTB_COD           VARCHAR2(2)
  , FL_ALIQ_ICMS       NUMBER
  , CFOP               VARCHAR2(6)
  , UNFE_SIG           VARCHAR2(2)
  , INFST_IND_CANC     VARCHAR2(1)
  , NUM_MAX            VARCHAR2(15)
  , NUM_MIN            VARCHAR2(15)
  , DTEMISS_MAX        DATE
  , DTEMISS_MIN        DATE 
  , VAL_CONT           NUMBER 
  , VAL_SERV           NUMBER
  , VAL_DESC           NUMBER
  , BASE_ICMS          NUMBER 
  , VAL_ICMS           NUMBER
  , ISENTA_ICMS        NUMBER
  , OUTRAS_ICMS        NUMBER
  , QTD_ITENS          NUMBER
  , ROWID_INF          ROWID
  , INFST_NUM          VARCHAR2(15)
  , INFST_DTEMISS      DATE
  , INFST_NUM_SEQ      NUMBER
  , INFST_VAL_CONT     NUMBER
  , INFST_VAL_SERV     NUMBER
  , INFST_VAL_DESC     NUMBER
  , INFST_BASE_ICMS    NUMBER
  , INFST_VAL_ICMS     NUMBER
  , INFST_ISENTA_ICMS  NUMBER
  , INFST_OUTRAS_ICMS  NUMBER
);
CREATE INDEX GFCADASTRO.IN1_REL_CFOP_NEGATIVO_AGRUPADO ON GFCADASTRO.TB_REL_CFOP_NEGATIVO_AGRUPADO(UPPER(TRIM(NM_PROCESSO)));
CREATE INDEX GFCADASTRO.IN2_REL_CFOP_NEGATIVO_AGRUPADO ON GFCADASTRO.TB_REL_CFOP_NEGATIVO_AGRUPADO(NM_PROCESSO);

SELECT
/+ parallel(15) /
A.*
FROM (SELECT /+ INDEX(A IN2_REL_CFOP_NEGATIVO_AGRUPADO) /
A.*, 
ROW_NUMBER () OVER (PARTITION BY A.NM_PROCESSO, 
A.VOLUME, 
A.MES, 
A.EMPS_COD, 
A.FILI_COD, 
A.MDOC_COD, 
A.INFST_SERIE, 
A.ESTB_COD, 
A.FL_ALIQ_ICMS, 
A.CFOP, 
A.UNFE_SIG, 
A.INFST_IND_CANC ORDER BY  'X' ) rnk
FROM GFCADASTRO.TB_REL_CFOP_NEGATIVO_AGRUPADO A
) A 
WHERE A.NM_PROCESSO = '${PROCESSO}'
AND A.RNK = 1
ORDER BY A.NM_PROCESSO, 
A.VOLUME, 
A.MES, 
A.EMPS_COD, 
A.FILI_COD, 
A.UNFE_SIG,
A.MDOC_COD, 
A.INFST_SERIE, 
A.ESTB_COD, 
A.FL_ALIQ_ICMS, 
A.CFOP, 
A.INFST_IND_CANC,
A.INFST_DTEMISS,
A.INFST_NUM,  
A.INFST_NUM_SEQ;
*/
   v_action_name VARCHAR2(32) := substr('MAP_2_RELATORIO_CFOP_NEGATIVO',1,32);
   v_module_name VARCHAR2(32) := upper(trim(substr(v_PROCESSO,1,32)));
    
   l_error_count  NUMBER;    
   ex_dml_errors  EXCEPTION;
   PRAGMA EXCEPTION_INIT(ex_dml_errors, -24381);
   v_error_bk     VARCHAR2(4000);
   v_idx_error    PLS_INTEGER :=0; 
	
   CONSTANTE_LIMIT PLS_INTEGER := 50000; 
 
 	TYPE r_main IS RECORD (emps_cod 		openrisow.ctr_ident_cnv115.emps_cod%TYPE, 
			fili_cod 		openrisow.ctr_ident_cnv115.fili_cod%TYPE, 
			ctr_modelo 		openrisow.ctr_ident_cnv115.ctr_modelo%TYPE,
			mdoc_cod 	    openrisow.ctr_ident_cnv115.ctr_modelo%TYPE,
            ctr_volume		openrisow.ctr_ident_cnv115.ctr_volume%TYPE, 
			ctr_serie		openrisow.ctr_ident_cnv115.ctr_serie%TYPE, 
			mnfst_serie		openrisow.ctr_ident_cnv115.ctr_ser_ori%TYPE,
			ctr_ser_ori		openrisow.ctr_ident_cnv115.ctr_ser_ori%TYPE,
            ctr_apur_dtini	openrisow.ctr_ident_cnv115.ctr_apur_dtini%TYPE, 
			ctr_apur_dtfin	openrisow.ctr_ident_cnv115.ctr_apur_dtfin%TYPE,
            ctr_num_nfini	openrisow.ctr_ident_cnv115.ctr_num_nfini%TYPE, 
			ctr_num_nffin	openrisow.ctr_ident_cnv115.ctr_num_nffin%TYPE
	);
	v_ds_sql_main VARCHAR2(32767) := q'[ SELECT /*+ CURSOR_SHARING_FORCE parallel(15) */
			nf.emps_cod, 
			nf.fili_cod, 
			nf.ctr_modelo,
			nf.mdoc_cod,
            nf.ctr_volume, 
			nf.ctr_serie, 
			nf.mnfst_serie,
			nf.ctr_ser_ori,
            nf.ctr_apur_dtini, 
			nf.ctr_apur_dtfin,
            nf.ctr_num_nfini, 
			nf.ctr_num_nffin
		FROM
		(SELECT /*+ index(nf CTR_IDENT_CNV115P1) */
				nf.emps_cod, 
			    nf.fili_cod, 
			    nf.ctr_modelo,
			    nf.ctr_modelo  as MDOC_COD,
                nf.ctr_volume, 
			    nf.ctr_serie, 
				nf.ctr_ser_ori as MNFST_SERIE,
			    nf.ctr_ser_ori,
                nf.ctr_apur_dtini, 
			    nf.ctr_apur_dtfin,
                nf.ctr_num_nfini, 
			    nf.ctr_num_nffin
         FROM openrisow.ctr_ident_cnv115 nf
         WHERE {REGRAS_SCRIPT_AUX} 
		 AND   nf.ctr_ind_retif = 'N'
		 AND   nf.ctr_apur_dtini >= TRUNC(TO_DATE('{DATA_INICIO}','DD/MM/YYYY'),'MM')
		 AND   nf.ctr_apur_dtini <= TRUNC(TO_DATE('{DATA_FIM}','DD/MM/YYYY'),'MM')
		 ) nf
		 WHERE {REGRAS_SCRIPT} 
         ORDER BY nf.emps_cod, 
		          nf.fili_cod, 
		          nf.ctr_apur_dtini,
		          nf.ctr_modelo,
				  nf.ctr_ser_ori,
		          nf.ctr_volume]';

		TYPE t_cursor_dinamico IS REF CURSOR;
		c_main t_cursor_dinamico;
		
		TYPE t_main IS TABLE OF r_main INDEX BY PLS_INTEGER;
		v_bk_main t_main;
		v_main    r_main;
   
   CURSOR c_inf(p_emps_cod           IN openrisow.ctr_ident_cnv115.emps_cod%type,
				p_fili_cod           IN openrisow.ctr_ident_cnv115.fili_cod%type,	
				p_ctr_modelo         IN openrisow.ctr_ident_cnv115.ctr_modelo%type,
				p_ctr_ser_ori        IN openrisow.ctr_ident_cnv115.ctr_ser_ori%type,
				p_ctr_volume         IN openrisow.ctr_ident_cnv115.ctr_volume%type,
				p_ctr_apur_dtini     IN openrisow.ctr_ident_cnv115.ctr_apur_dtini%type,
				p_ctr_apur_dtfin     IN openrisow.ctr_ident_cnv115.ctr_apur_dtfin%type,
				p_ctr_num_nfini      IN openrisow.ctr_ident_cnv115.ctr_num_nfini%type,
				p_ctr_num_nffin      IN openrisow.ctr_ident_cnv115.ctr_num_nffin%type
				)
	IS					
	WITH tmp_nf AS (SELECT 
		/*+  CURSOR_SHARING_FORCE index(inf ITEM_NFTL_SERVP1)  */
		p_ctr_volume as volume,
		nf.rowid  AS rowid_inf,  
		nf.emps_cod || '|' || nf.fili_cod || '|' || nf.infst_serie || '|' || to_char(nf.infst_dtemiss,'YYYY-MM-DD') || '|' || nf.infst_num as chave_nf,
		nf.emps_cod,
		nf.fili_cod,
		nf.mdoc_cod,
		nf.infst_serie,
		nf.infst_num,
		nf.infst_num_seq,
		nf.infst_dtemiss,
		nf.cadg_cod,
		nf.catg_cod,
		nf.infst_tribicms,
		nf.infst_aliq_icms,
		CASE WHEN nf.infst_tribicms = 'S' THEN nf.infst_aliq_icms ELSE 0 END fl_aliq_icms,
		nf.estb_cod, 
		nf.cfop,
		nf.infst_ind_canc,	
		TO_CHAR(nf.infst_dtemiss, 'MM/YYYY') as mes,
		NVL(nf.infst_val_cont,0)    AS infst_val_cont ,
		NVL(nf.infst_val_serv,0)    AS infst_val_serv ,
		NVL(nf.infst_val_desc,0)    AS infst_val_desc ,
		NVL(nf.infst_base_icms,0)   AS infst_base_icms ,
		NVL(nf.infst_val_icms,0)    AS infst_val_icms ,
		NVL(nf.infst_isenta_icms,0) AS infst_isenta_icms ,
		NVL(nf.infst_outras_icms,0) AS infst_outras_icms ,   
		ROW_NUMBER () OVER (PARTITION BY nf.emps_cod, nf.fili_cod, nf.infst_serie, nf.infst_dtemiss, nf.infst_num ORDER BY  'X' ) rnk_inf
	FROM openrisow.item_nftl_serv  nf
	WHERE   nf.emps_cod       = p_emps_cod
		AND nf.fili_cod       = p_fili_cod
		AND nf.mdoc_cod       = p_ctr_modelo 
		AND nf.infst_serie    = p_ctr_ser_ori
		AND TO_NUMBER(nf.infst_num)      BETWEEN TO_NUMBER(p_ctr_num_nfini) AND TO_NUMBER(p_ctr_num_nffin)
		AND nf.infst_dtemiss  BETWEEN p_ctr_apur_dtini AND p_ctr_apur_dtfin 
		AND nf.CFOP           <> '0000' 
        AND nf.infst_ind_canc =  'N'	
		)					
	, tmp_cli AS (SELECT 
					inf.chave_nf as chave_nf_cli,
					cli.rowid AS rowid_cli,
					cli.unfe_sig
				  FROM  tmp_nf inf, 
						openrisow.cli_fornec_transp cli
				  WHERE  inf.rnk_inf = 1
				    AND  cli.cadg_cod       = inf.cadg_cod
					AND  cli.catg_cod       = inf.catg_cod
					AND  cli.cadg_dat_atua  = (SELECT MAX(cli1.cadg_dat_atua)	FROM openrisow.cli_fornec_transp cli1 WHERE cli1.cadg_cod = inf.cadg_cod AND   cli1.catg_cod       = inf.catg_cod AND   cli1.cadg_dat_atua <= inf.infst_dtemiss)
				)
	, tmp_cfop AS (SELECT 
			 inf.volume,
			 inf.mes,
			 inf.emps_cod,
			 inf.fili_cod,
			 inf.mdoc_cod,		
			 inf.infst_serie,
			 inf.estb_cod,
			 inf.fl_aliq_icms,
			 inf.cfop,
			 cli.unfe_sig,
			 inf.infst_ind_canc,
			 MAX(inf.infst_num)         AS num_max,
			 MIN(inf.infst_num)         AS num_min,
			 MAX(inf.infst_dtemiss)     AS dtemiss_max,
			 MIN(inf.infst_dtemiss)     AS dtemiss_min,
			 SUM(inf.infst_val_cont)    AS val_cont,
			 SUM(inf.infst_val_serv)    AS val_serv,
			 SUM(inf.infst_val_desc)    AS val_desc,
			 SUM(inf.infst_base_icms)   AS base_icms,
			 SUM(inf.infst_val_icms)    AS val_icms,
			 SUM(inf.infst_isenta_icms) AS isenta_icms,
			 SUM(inf.infst_outras_icms) AS outras_icms,
			 COUNT(1)               	AS qtd_itens
		FROM  tmp_nf inf, tmp_cli cli
        WHERE cli.chave_nf_cli = inf.chave_nf 	
		GROUP BY inf.volume,
			     inf.mes,
				 inf.emps_cod,
				 inf.fili_cod,
				 inf.mdoc_cod,		
				 inf.infst_serie,
                 inf.fl_aliq_icms,
                 inf.estb_cod ,
                 inf.cfop,
                 inf.infst_ind_canc,
                 cli.unfe_sig
        HAVING    SUM(inf.infst_outras_icms)     < 0
               OR SUM(inf.infst_isenta_icms)     < 0 
               OR SUM(inf.infst_val_cont)        < 0 
               OR SUM(inf.infst_val_desc)        < 0 
               OR SUM(inf.infst_val_icms)        < 0 
               OR SUM(inf.infst_base_icms)       < 0 
               OR SUM(inf.infst_val_serv)        < 0)
	SELECT /*+ parallel(15) */ 
		   c.*,
		   inf.rowid_inf,
		   inf.infst_num,
		   inf.infst_dtemiss,
		   inf.infst_num_seq,
		   inf.infst_val_cont ,
		   inf.infst_val_serv ,
		   inf.infst_val_desc ,
		   inf.infst_base_icms ,
		   inf.infst_val_icms ,
		   inf.infst_isenta_icms ,
		   inf.infst_outras_icms
	FROM    tmp_cfop c, 
		    tmp_nf inf,
			tmp_cli cli
        
	WHERE   inf.volume       			= c.volume
		AND inf.emps_cod     			= c.emps_cod   
		AND inf.fili_cod     			= c.fili_cod   
		AND inf.mdoc_cod     			= c.mdoc_cod   
		AND inf.infst_serie  			= c.infst_serie
		AND inf.cfop         			= c.cfop
		AND inf.fl_aliq_icms 			= c.fl_aliq_icms
		AND inf.mes          			= c.mes
		AND inf.infst_serie  			= c.infst_serie
		AND inf.estb_cod     			= c.estb_cod
		AND inf.cfop         			= c.cfop
		AND inf.infst_ind_canc          = c.infst_ind_canc
		AND inf.infst_num               BETWEEN c.num_min AND c.num_max
		AND inf.infst_dtemiss           BETWEEN c.dtemiss_min AND c.dtemiss_max
		AND cli.chave_nf_cli            = inf.chave_nf
		AND cli.unfe_sig                = c.unfe_sig
	;

   TYPE t_inf IS TABLE OF c_inf%ROWTYPE INDEX BY PLS_INTEGER;
   v_bk_inf t_inf;
   v_inf    c_inf%ROWTYPE;
	/*
   TYPE r_rel_cfop_negativo_inf IS RECORD (nm_processo	       VARCHAR2(4000)
  , dt_processso       DATE
  , volume             VARCHAR2(10)
  , mes                VARCHAR2(10)
  , emps_cod           VARCHAR2(9)
  , fili_cod           VARCHAR2(9)
  , mdoc_cod           VARCHAR2(3)
  , infst_serie        VARCHAR2(5)
  , estb_cod           VARCHAR2(2)
  , fl_aliq_icms       NUMBER
  , cfop               VARCHAR2(6)
  , unfe_sig           VARCHAR2(2)
  , infst_ind_canc     VARCHAR2(1)
  , num_max            VARCHAR2(15)
  , num_min            VARCHAR2(15)
  , dtemiss_max        DATE
  , dtemiss_min        DATE 
  , val_cont           NUMBER 
  , val_serv           NUMBER
  , val_desc           NUMBER
  , base_icms          NUMBER 
  , val_icms           NUMBER
  , isenta_icms        NUMBER
  , outras_icms        NUMBER
  , qtd_itens          NUMBER
  , rowid_inf          ROWID
  , infst_num          VARCHAR2(15)
  , infst_dtemiss      DATE
  , infst_num_seq      NUMBER
  , infst_val_cont     NUMBER
  , infst_val_serv     NUMBER
  , infst_val_desc     NUMBER
  , infst_base_icms    NUMBER
  , infst_val_icms     NUMBER
  , infst_isenta_icms  NUMBER
  , infst_outras_icms  NUMBER);
   TYPE t_rel_cfop_negativo_inf IS TABLE OF r_rel_cfop_negativo_inf INDEX BY PLS_INTEGER;
   v_b_rel_cfop_negativo_inf t_rel_cfop_negativo_inf;
   v_rel_cfop_negativo_inf   r_rel_cfop_negativo_inf;   
   */ 
   -- v_rel                 GFCADASTRO.TB_REL_CFOP_NEGATIVO_AGRUPADO%ROWTYPE;
   TYPE t_rel_cfop_negativo_inf IS TABLE OF GFCADASTRO.TB_REL_CFOP_NEGATIVO_AGRUPADO%ROWTYPE INDEX BY PLS_INTEGER;
   v_b_rel_cfop_negativo_inf t_rel_cfop_negativo_inf;
   v_rel_cfop_negativo_inf   GFCADASTRO.TB_REL_CFOP_NEGATIVO_AGRUPADO%ROWTYPE;   
 	
   v_ds_etapa            VARCHAR2(4000);
   PROCEDURE prc_tempo(p_ds_ddo IN VARCHAR2) AS 
   BEGIN
     v_ds_etapa := substr(p_ds_ddo || ' >> ' || v_ds_etapa,1,4000); 
	 BEGIN
		DBMS_OUTPUT.PUT_LINE(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' ||  p_ds_ddo ,1,2000));
	 EXCEPTION
		WHEN OTHERS THEN
			NULL;
	 END;
	 BEGIN
	 	DBMS_APPLICATION_INFO.set_client_info(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS'),1,30) || ' - '|| substr(v_ds_etapa ,1,30));				 	
	 EXCEPTION
	 WHEN OTHERS THEN
	    NULL;
	 END;	 
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
  
    -- CP
    prc_tempo('main');
    
	    v_rel_cfop_negativo_inf.nm_processo   := v_PROCESSO;
	    v_rel_cfop_negativo_inf.dt_processso  := SYSDATE;
	    DELETE FROM GFCADASTRO.TB_REL_CFOP_NEGATIVO_AGRUPADO WHERE nm_processo = v_rel_cfop_negativo_inf.nm_processo;
	    v_ds_sql_main := REPLACE(REPLACE(REPLACE(REPLACE(v_ds_sql_main,
													  '{REGRAS_SCRIPT_AUX}',v_REGRAS_SCRIPT_AUX),
													  '{REGRAS_SCRIPT}',v_REGRAS_SCRIPT),
													  '{DATA_INICIO}',TO_CHAR(v_data_inicio,'DD/MM/YYYY')),
													  '{DATA_FIM}',TO_CHAR(v_data_fim,'DD/MM/YYYY'));
		OPEN c_main FOR v_ds_sql_main;
	    LOOP
			FETCH c_main BULK COLLECT INTO v_bk_main LIMIT CONSTANTE_LIMIT;   
			IF v_bk_main.COUNT > 0 THEN
		  
			FOR i IN v_bk_main.FIRST .. v_bk_main.LAST
			LOOP
				v_main := v_bk_main(i);
				
				OPEN c_inf(p_emps_cod =>  v_main.emps_cod ,
					  p_fili_cod           =>  v_main.fili_cod ,
					  p_ctr_modelo         =>  v_main.ctr_modelo ,
					  p_ctr_ser_ori        =>  v_main.ctr_ser_ori ,
					  p_ctr_volume         =>  v_main.ctr_volume ,
					  p_ctr_apur_dtini     =>  v_main.ctr_apur_dtini ,
					  p_ctr_apur_dtfin     =>  v_main.ctr_apur_dtfin ,
					  p_ctr_num_nfini      =>  v_main.ctr_num_nfini ,
					  p_ctr_num_nffin      =>  v_main.ctr_num_nffin 
					 );
				LOOP
					FETCH c_inf BULK COLLECT INTO v_bk_inf LIMIT CONSTANTE_LIMIT;   
					IF v_bk_inf.COUNT > 0 THEN
						v_qtd_processados       := v_qtd_processados + v_bk_inf.COUNT;
						prc_tempo(v_inf.mes || ' >> ' || v_inf.volume || ' >> ' || v_qtd_processados);
						FOR i IN v_bk_inf.FIRST .. v_bk_inf.LAST
						LOOP
							v_inf 				                           						:= v_bk_inf(i);
						    v_rel_cfop_negativo_inf.volume                 						:= v_inf.volume;
							v_rel_cfop_negativo_inf.mes                    						:= v_inf.mes;							
							v_rel_cfop_negativo_inf.emps_cod               						:= v_inf.emps_cod   ;
							v_rel_cfop_negativo_inf.fili_cod               						:= v_inf.fili_cod   ;
							v_rel_cfop_negativo_inf.mdoc_cod               						:= v_inf.mdoc_cod   ;			       
							v_rel_cfop_negativo_inf.infst_serie            						:= v_inf.infst_serie;
							v_rel_cfop_negativo_inf.estb_cod               						:= v_inf.estb_cod;
							v_rel_cfop_negativo_inf.fl_aliq_icms        						:= v_inf.fl_aliq_icms;
							v_rel_cfop_negativo_inf.cfop                   						:= v_inf.cfop;
							v_rel_cfop_negativo_inf.infst_ind_canc         						:= v_inf.infst_ind_canc;
							v_rel_cfop_negativo_inf.unfe_sig               						:= v_inf.unfe_sig;
							v_rel_cfop_negativo_inf.num_min                						:= v_inf.num_min;
							v_rel_cfop_negativo_inf.num_max                						:= v_inf.num_max;
							v_rel_cfop_negativo_inf.dtemiss_min            						:= v_inf.dtemiss_min;
							v_rel_cfop_negativo_inf.dtemiss_max            						:= v_inf.dtemiss_max;
							v_rel_cfop_negativo_inf.val_cont               						:= v_inf.val_cont;
							v_rel_cfop_negativo_inf.val_serv               						:= v_inf.val_serv;
							v_rel_cfop_negativo_inf.val_desc               						:= v_inf.val_serv;
							v_rel_cfop_negativo_inf.base_icms              						:= v_inf.base_icms;
							v_rel_cfop_negativo_inf.val_icms               						:= v_inf.val_icms;
							v_rel_cfop_negativo_inf.isenta_icms            						:= v_inf.isenta_icms;
							v_rel_cfop_negativo_inf.outras_icms            						:= v_inf.outras_icms;
							v_rel_cfop_negativo_inf.qtd_itens              						:= v_inf.qtd_itens;
							v_rel_cfop_negativo_inf.rowid_inf         	   						:= v_inf.rowid_inf;
							v_rel_cfop_negativo_inf.infst_num              						:= v_inf.infst_num;
							v_rel_cfop_negativo_inf.infst_dtemiss          						:= v_inf.infst_dtemiss;
							v_rel_cfop_negativo_inf.infst_num_seq          						:= v_inf.infst_num_seq;
							v_rel_cfop_negativo_inf.infst_val_cont         						:= v_inf.infst_val_cont; 
							v_rel_cfop_negativo_inf.infst_val_serv         						:= v_inf.infst_val_serv;
							v_rel_cfop_negativo_inf.infst_val_desc         						:= v_inf.infst_val_desc; 
							v_rel_cfop_negativo_inf.infst_base_icms        						:= v_inf.infst_base_icms;
							v_rel_cfop_negativo_inf.infst_val_icms         						:= v_inf.infst_val_icms;
							v_rel_cfop_negativo_inf.infst_isenta_icms      						:= v_inf.infst_isenta_icms;
							v_rel_cfop_negativo_inf.infst_outras_icms      						:= v_inf.infst_outras_icms;
							v_rel_cfop_negativo_inf.infst_outras_icms      						:= v_inf.infst_outras_icms;
							-- INSERT INTO GFCADASTRO.TB_REL_CFOP_NEGATIVO_AGRUPADO VALUES v_rel_cfop_negativo_inf; 
							-- IF v_COMMIT = 'COMMIT' THEN
							--	COMMIT;
							-- ELSE
							--	ROLLBACK;
							-- END IF; 							
							v_b_rel_cfop_negativo_inf(nvl(v_b_rel_cfop_negativo_inf.COUNT,0)+1) := v_rel_cfop_negativo_inf;
						END LOOP;
						BEGIN
							v_error_bk  := NULL;
							v_idx_error := 0; 
							IF v_b_rel_cfop_negativo_inf.COUNT > 0 THEN
								FORALL i IN v_b_rel_cfop_negativo_inf.FIRST .. v_b_rel_cfop_negativo_inf.LAST  SAVE EXCEPTIONS
									INSERT INTO GFCADASTRO.TB_REL_CFOP_NEGATIVO_AGRUPADO VALUES v_b_rel_cfop_negativo_inf(i); 
					
								v_b_rel_cfop_negativo_inf.delete;
								IF v_COMMIT = 'COMMIT' THEN
									COMMIT;
								ELSE
									ROLLBACK;
								END IF; 	              
							END IF;
						EXCEPTION
						WHEN ex_dml_errors THEN
						  	BEGIN 
								IF v_b_rel_cfop_negativo_inf.COUNT > 0 THEN
									l_error_count := SQL%BULK_EXCEPTIONS.count;
									FOR i IN 1 .. l_error_count LOOP			
											IF -SQL%BULK_EXCEPTIONS(i).ERROR_CODE != -1 THEN
												v_error_bk    := SUBSTR('Error: ' || i ||  
																		' Array Index: ' || SQL%BULK_EXCEPTIONS(i).error_index ||  
																		' Message: ' || SQLERRM(-SQL%BULK_EXCEPTIONS(i).ERROR_CODE),1,500)
																|| ' | ' ||
																SUBSTR(v_error_bk,1,3490);		
											ELSE
												v_idx_error    := SQL%BULK_EXCEPTIONS(i).error_index;		
												BEGIN
													INSERT INTO GFCADASTRO.TB_REL_CFOP_NEGATIVO_AGRUPADO VALUES v_b_rel_cfop_negativo_inf(v_idx_error); 
												EXCEPTION
												WHEN OTHERS THEN
													v_error_bk    := SUBSTR('Error: ' || i ||  
																		' Array Index: ' || SQL%BULK_EXCEPTIONS(i).error_index ||  
																		' Message: ' || SQLERRM(-SQL%BULK_EXCEPTIONS(i).ERROR_CODE),1,500)
																|| ' | ' ||
																SUBSTR(v_error_bk,1,3490);														
												END;	
											END IF;				 
									END LOOP;
								END IF;
							EXCEPTION
									WHEN OTHERS THEN
										v_error_bk    := NULL;
							END;
							v_b_rel_cfop_negativo_inf.delete;
							IF v_COMMIT = 'COMMIT' THEN
								COMMIT;
							ELSE
								ROLLBACK;
							END IF; 							
							IF NVL(LENGTH(TRIM(v_error_bk)),0) > 0 THEN
								v_error_bk    := SUBSTR('Number of failures: ' || l_error_count,1,500)
													|| ' | ' ||
													SUBSTR(v_error_bk,1,3490);
								RAISE_APPLICATION_ERROR (-20343, 'STOP! ' || SUBSTR(v_error_bk,1,1000));
							END IF;
						END;									
					END IF;
					EXIT WHEN c_inf%NOTFOUND;	
				END LOOP;        
				CLOSE c_inf;
			END LOOP;
		  END IF;       
		  EXIT WHEN c_main%NOTFOUND;	  
	    END LOOP;        
	    CLOSE c_main;
 
   prc_tempo('Fim - Processados ' ||  v_COMMIT ||  ' :      ' || v_qtd_processados);
   v_msg_erro :=   substr(substr(nvl(v_ds_etapa,' '),1,3000) || ' <||> ' || substr(v_msg_erro,1,990) ,1,4000);
   -----------------------------------------------------------------------------
   --> Eliminando a nomeação
   -----------------------------------------------------------------------------
   DBMS_APPLICATION_INFO.set_module(null,null);
   DBMS_APPLICATION_INFO.set_client_info (null);   
EXCEPTION           
   WHEN OTHERS THEN 
      ROLLBACK;     
      prc_tempo('ERRO : ' || SUBSTR(SQLERRM,1,500));
      v_msg_erro := SUBSTR(v_ds_etapa || ' >> ' || v_msg_erro,1,4000);
      v_st_processamento := 'Erro';
      exit_code := 1;
	  -----------------------------------------------------------------------------
	  --> Eliminando a nomeação
	  -----------------------------------------------------------------------------
	  DBMS_APPLICATION_INFO.set_module(null,null);
	  DBMS_APPLICATION_INFO.set_client_info (null);	 	  
END;
   :v_st_processamento := v_st_processamento;    
   :v_msg_erro         := v_msg_erro;            
   :exit_code          := exit_code;             
   :v_qtd_processados  := v_qtd_processados;     
END;             
/                   

PROMPT Processado   
ROLLBACK;          
BEGIN 
 UPDATE gfcadastro.CONTROLE_PROCESSAMENTO cp
   SET cp.dt_fim_proc            = SYSDATE,
       cp.st_processamento       = :v_st_processamento,
       cp.ds_msg_erro            = substr(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(cp.ds_msg_erro,1,990) ,1,4000),
       cp.qt_cad_nao_encontrado  = NVL(cp.qt_cad_nao_encontrado,0) + :v_qtd_processados
 WHERE cp.rowid = '${ROWID_CP}';
 COMMIT;
EXCEPTION
WHEN OTHERS THEN
	BEGIN
		UPDATE gfcadastro.CONTROLE_PROCESSAMENTO  
		SET ds_msg_erro              = substr(substr(TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' || nvl(:v_msg_erro,' '),1,3000) || ' | ' || substr(ds_msg_erro,1,990) ,1,4000),
			qt_cad_nao_encontrado    = NVL(qt_cad_nao_encontrado,0) + :v_qtd_processados
		WHERE dt_limite_inf_nf       = TO_DATE('${DATA_INICIO}','DD/MM/YYYY') 
		AND UPPER(TRIM(NM_PROCESSO)) = UPPER(TRIM('${PROCESSO}'))
        AND qt_registros_inf > 0
        AND qt_registros_nf  > 0		
		AND ROWNUM < 2;
		COMMIT;
	EXCEPTION
		WHEN OTHERS THEN
			ROLLBACK;          
	END;
END;
/
PROMPT Processado   
                    
exit :exit_code;    
                    
@EOF                

RETORNO=$?

${WAIT}

exit ${RETORNO}

