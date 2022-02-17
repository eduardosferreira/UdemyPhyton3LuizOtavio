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
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_REGRA_CFOP_0000'
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
PROMPT MAP_2_REGRA_CFOP_0000
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT

DECLARE

   -- Constantes da procedure 
   CURSOR c_servico (
             p_emps_cod   openrisow.servico_telcom.emps_cod%TYPE,
             p_fili_cod   openrisow.servico_telcom.fili_cod%TYPE,
             p_servtl_cod openrisow.servico_telcom.servtl_cod%TYPE,
             p_dtref      date
          )
       IS
    WITH s AS (
        SELECT  /*+ MATERIALIZE */
          st.rowid              rowid_st,
          st.clasfi_cod         ,
          st.servtl_tip_utiliz  ,
          st.emps_cod           ,
          st.fili_cod           ,
          st.servtl_dat_atua    ,
          st.servtl_cod         ,
          st.servtl_desc        ,
          st.servtl_compl       ,
          st.servtl_ind_tprec   ,
          st.servtl_ind_tpserv  ,
          st.servtl_cod_nat     ,
          st.var01              ,
          st.var02              ,
          st.var03              ,
          st.var04              ,
          st.var05              ,
          st.num01              ,
          st.num02              ,
          st.num03              ,
          st.servtl_ind_rec     ,
          ROW_NUMBER() OVER(PARTITION BY st.emps_cod,st.fili_cod,st.servtl_cod ORDER BY CASE WHEN st.SERVTL_DAT_ATUA-p_dtref
          <= 0 THEN 0 ELSE 1 END, ABS(st.SERVTL_DAT_ATUA-p_dtref)) nu
     FROM openrisow.SERVICO_TELCOM st
    WHERE st.emps_cod =  p_emps_cod
      AND st.fili_cod   = p_fili_cod
      AND st.servtl_cod = p_servtl_cod)
        SELECT   /*+ parallel(15) */
          s.rowid_st,
          s.clasfi_cod         ,
          s.servtl_tip_utiliz  ,
          s.emps_cod           ,
          s.fili_cod           ,
          (case when s.servtl_dat_atua > p_dtref then p_dtref else s.servtl_dat_atua end) servtl_dat_atua,
          s.servtl_cod         ,
          s.servtl_desc        ,
          s.servtl_compl       ,
          s.servtl_ind_tprec   ,
          s.servtl_ind_tpserv  ,
          s.servtl_cod_nat     ,
          s.var01              ,
          s.var02              ,
          s.var03              ,
          s.var04              ,
          s.var05              ,
          s.num01              ,
          s.num02              ,
          s.num03              ,
          s.servtl_ind_rec     
        FROM s          
    WHERE s.nu = 1;          

 
   CURSOR c_sanea
       IS
        SELECT  /*+ parallel(15) */
          CASE lead(nf.rowid, 1) over (order by nf.rowid)
                  WHEN nf.rowid THEN 'N'
                  ELSE 'S'
          END ultimo_item_nf,        
          nf.rowid               rowid_nf,
          nf.mnfst_serie,
          inf.rowid             rowid_inf,   -- usado para a atualizacao do iten de desconto
          inf.infst_val_icms    , 
          inf.infst_isenta_icms ,
          inf.infst_outras_icms ,
          inf.infst_tribicms    ,
          inf.infst_tip_isencao ,
          inf.estb_cod          ,
          inf.cfop              ,
          inf.infst_base_icms   ,
          inf.infst_val_desc    ,
          inf.infst_val_cont    ,
          inf.emps_cod          ,
          inf.fili_cod          ,
          inf.serv_cod          ,
          inf.infst_dtemiss     ,
      inf.infst_serie       ,
          inf.infst_num         ,
      inf.mdoc_cod          ,
      inf.infst_num_seq          
    FROM openrisow.item_nftl_serv   PARTITION (${PARTICAO_INF}) inf, -- item de desconto          
         openrisow.mestre_nftl_serv PARTITION (${PARTICAO_NF})  nf
    WHERE ${FILTRO}
          AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ( 'AS1', 'AS2', 'AS3', 'T1') 
	AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))
          -- AND nf.mnfst_dtemiss >= TO_DATE('01/01/2015','DD/MM/YYYY') AND nf.mnfst_dtemiss <= TO_DATE('31/12/2019','DD/MM/YYYY')        
           AND inf.emps_cod = nf.emps_cod
      AND inf.fili_cod = nf.fili_cod
      AND inf.infst_serie = nf.mnfst_serie
      AND inf.infst_num = nf.mnfst_num
      AND inf.infst_dtemiss = nf.mnfst_dtemiss
      AND inf.mdoc_cod = nf.mdoc_cod
      -- AND NVL(inf.infst_base_icms, 0)         = 0 
          -- AND NVL(inf.infst_val_icms, 0)         = 0 
          -- AND NVL(inf.infst_isenta_icms , 0) = 0  
          -- AND NVL(inf.infst_outras_icms, 0)  <> 0 
          AND inf.cfop = '0000'  --  
    ORDER BY rowid_nf, ultimo_item_nf;
 
   TYPE r_mestre_nftl_serv IS RECORD (
     rowid_nf                 rowid,
         mnfst_val_tot            openrisow.mestre_nftl_serv.mnfst_val_tot%TYPE,
     mnfst_val_isentas        openrisow.mestre_nftl_serv.mnfst_val_isentas%TYPE,
     mnfst_val_outras         openrisow.mestre_nftl_serv.mnfst_val_outras%TYPE,
         fili_cod_cgc             openrisow.filial.fili_cod_cgc%TYPE
   );
   v_mestre_nftl_serv         r_mestre_nftl_serv;
    
   TYPE r_item_nftl_serv IS RECORD (
     rowid_inf           rowid,
     infst_val_icms      openrisow.item_nftl_serv.infst_val_icms%TYPE,
     infst_isenta_icms   openrisow.item_nftl_serv.infst_isenta_icms%TYPE,
     infst_outras_icms   openrisow.item_nftl_serv.infst_outras_icms%TYPE,
     infst_tribicms      openrisow.item_nftl_serv.infst_tribicms%TYPE,
     infst_tip_isencao   openrisow.item_nftl_serv.infst_tip_isencao%TYPE,
     estb_cod            openrisow.item_nftl_serv.estb_cod%TYPE,
     cfop                openrisow.item_nftl_serv.cfop%TYPE,
     infst_base_icms     openrisow.item_nftl_serv.infst_base_icms%TYPE,
     infst_val_desc      openrisow.item_nftl_serv.infst_val_desc%TYPE,
     infst_val_cont      openrisow.item_nftl_serv.infst_val_cont%TYPE,
         emps_cod            openrisow.item_nftl_serv.emps_cod%TYPE,
         fili_cod            openrisow.item_nftl_serv.fili_cod%TYPE,
         serv_cod            openrisow.item_nftl_serv.serv_cod%TYPE,
     infst_dtemiss       openrisow.item_nftl_serv.infst_dtemiss%TYPE,
     infst_serie         openrisow.item_nftl_serv.infst_serie%TYPE,
         infst_num           openrisow.item_nftl_serv.infst_num%TYPE, 
     mdoc_cod            openrisow.item_nftl_serv.mdoc_cod%TYPE,
     infst_num_seq                   openrisow.item_nftl_serv.infst_num_seq%TYPE
   );
   v_item                r_item_nftl_serv;
 
   TYPE r_servico_telcom IS RECORD (
     rowid_st            rowid,
     clasfi_cod          openrisow.servico_telcom.clasfi_cod%TYPE,
     servtl_tip_utiliz   openrisow.servico_telcom.servtl_tip_utiliz%TYPE,
     emps_cod            openrisow.servico_telcom.emps_cod%TYPE,
     fili_cod            openrisow.servico_telcom.fili_cod%TYPE,
     servtl_dat_atua     openrisow.servico_telcom.servtl_dat_atua%TYPE,
     servtl_cod          openrisow.servico_telcom.servtl_cod%TYPE,
     servtl_desc         openrisow.servico_telcom.servtl_desc%TYPE,
     servtl_compl        openrisow.servico_telcom.servtl_compl%TYPE,
     servtl_ind_tprec    openrisow.servico_telcom.servtl_ind_tprec%TYPE,
     servtl_ind_tpserv   openrisow.servico_telcom.servtl_ind_tpserv%TYPE,
     servtl_cod_nat      openrisow.servico_telcom.servtl_cod_nat%TYPE,
     var01               openrisow.servico_telcom.var01%TYPE,
     var02               openrisow.servico_telcom.var02%TYPE,
     var03               openrisow.servico_telcom.var03%TYPE,
     var04               openrisow.servico_telcom.var04%TYPE,
     var05               openrisow.servico_telcom.var05%TYPE,
     num01               openrisow.servico_telcom.num01%TYPE,
     num02               openrisow.servico_telcom.num02%TYPE,
     num03               openrisow.servico_telcom.num03%TYPE,
     servtl_ind_rec      openrisow.servico_telcom.servtl_ind_rec%TYPE
   );
   v_servico             r_servico_telcom;
 
   v_serie               openrisow.item_nftl_serv.infst_serie%TYPE;

   v_reg                 c_sanea%ROWTYPE;
   v_alterou_item             BOOLEAN := false;
   v_inseriu_servico     BOOLEAN := false;
 
   v_pref_servfz         VARCHAR2(10);
   v_pref_servfzp        VARCHAR2(10);
   v_pref_servfzn        VARCHAR2(10);
   v_pref_var05          VARCHAR2(30);

   v_cnt      NUMBER     := 0;
   v_etapa    VARCHAR2(4000);
   PROCEDURE prc_tempo(pDDO IN VARCHAR2) AS 
   BEGIN
      v_etapa := substr(pDDO || ' >> ' || v_etapa,1,4000); 
      IF v_cnt <= 50 THEN
             v_cnt := v_cnt + 1;
         DBMS_OUTPUT.PUT_LINE(substr(TO_CHAR(v_cnt) || ' >> ' || TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') || ' ) ' ||  pDDO ,1,2000));
          END IF;
   EXCEPTION
     WHEN OTHERS THEN
           v_cnt := 51;
   END;
   
BEGIN

   prc_tempo('begin');
  
   OPEN c_sanea;
   LOOP
      FETCH c_sanea INTO v_reg;
      EXIT WHEN c_sanea%NOTFOUND;


      :v_qtd_processados := :v_qtd_processados+1;

          v_alterou_item    := FALSE;
          v_inseriu_servico := FALSE;
          
      -- Inicializacoes de INF
      v_serie                           := v_reg.mnfst_serie;
          v_pref_servfz                     := TRANSLATE(v_serie,'x ','x') || 'Z';
          v_pref_servfzp                    := TRANSLATE(v_serie,'x ','x') || 'ZP';
          v_pref_servfzn                    := TRANSLATE(v_serie,'x ','x') || 'ZN';
      v_pref_var05 := 'k' || lower(TRANSLATE(v_serie,'x ','x')) || 'rCFOP0';          
 
      v_mestre_nftl_serv.rowid_nf       := v_reg.rowid_nf;
      v_item.rowid_inf         := v_reg.rowid_inf;
      v_item.infst_val_icms    := v_reg.infst_val_icms;
      v_item.infst_isenta_icms := v_reg.infst_isenta_icms;
      v_item.infst_outras_icms := v_reg.infst_outras_icms;
      v_item.infst_tribicms    := v_reg.infst_tribicms;
      v_item.infst_tip_isencao := v_reg.infst_tip_isencao;
      v_item.estb_cod          := v_reg.estb_cod;
      v_item.cfop              := v_reg.cfop;
      v_item.infst_base_icms   := v_reg.infst_base_icms;
          v_item.infst_val_desc    := v_reg.infst_val_desc;
      v_item.infst_val_cont    := v_reg.infst_val_cont;
          v_item.emps_cod          := v_reg.emps_cod;
          v_item.fili_cod          := v_reg.fili_cod;
          v_item.serv_cod          := v_reg.serv_cod;
      v_item.infst_dtemiss     := v_reg.infst_dtemiss;          
      v_item.infst_serie       := v_reg.infst_serie;
      v_item.infst_num         := v_reg.infst_num;
      v_item.mdoc_cod          := v_reg.mdoc_cod;
          v_item.infst_num_seq     := v_reg.infst_num_seq;

          OPEN c_servico (
                 p_emps_cod   => v_item.emps_cod,
                 p_fili_cod   => v_item.fili_cod,
                 p_servtl_cod => v_item.serv_cod,
                 p_dtref      => v_item.infst_dtemiss
          );
          FETCH c_servico
          INTO v_servico.rowid_st          ,
                   v_servico.CLASFI_COD        ,
                   v_servico.SERVTL_TIP_UTILIZ ,
                   v_servico.emps_cod          ,
                   v_servico.fili_cod          ,
                   v_servico.servtl_dat_atua   ,
                   v_servico.servtl_cod        ,
                   v_servico.servtl_desc       ,
                   v_servico.servtl_compl      ,
                   v_servico.servtl_ind_tprec  ,
                   v_servico.servtl_ind_tpserv ,
                   v_servico.servtl_cod_nat    ,
                   v_servico.var01             ,
                   v_servico.var02             ,
                   v_servico.var03             ,
                   v_servico.var04             ,
                   v_servico.var05             ,
                   v_servico.num01             ,
                   v_servico.num02             ,
                   v_servico.num03             ,
                   v_servico.servtl_ind_rec    ;
      IF c_servico%NOTFOUND THEN
             CLOSE c_servico;
             raise_application_error (-20343, 'Servico nao encontrado! ' || ' >> emps_cod: '      || v_item.emps_cod 
                                                                                                                                 || ' >> fili_cod: '      || v_item.fili_cod 
                                                                                                                                         || ' >> serv_cod: '      || v_item.serv_cod 
                                                                                                                                         || ' >> infst_dtemiss: ' || v_item.infst_dtemiss                                                                                                                                         
                                                                                                                                         || ' >> rowid_inf: '     || v_item.rowid_inf);  
          END IF;                                   
          CLOSE c_servico;     
          
 
          
          IF NVL(v_item.infst_val_cont,0) >= 0 AND v_servico.clasfi_cod != '0899' and v_servico.servtl_cod is not null and v_servico.servtl_cod NOT LIKE  v_pref_servfzp || '%' THEN 
                v_servico.var05 := substr(v_pref_var05 || '[0899] ' || v_servico.servtl_cod || '|' || v_servico.clasfi_cod || '|' || v_servico.SERVTL_TIP_UTILIZ || ' >> ' || v_servico.var05,1,150);
                v_servico.servtl_cod := trim(substr(replace(replace(replace(v_servico.servtl_cod,v_pref_servfzp,''),v_pref_servfzn,''),v_pref_servfz,''),1,60));  
				v_servico.servtl_cod := trim(substr(v_pref_servfzp || v_servico.servtl_cod,1,60));
                v_servico.clasfi_cod := '0899';
				v_servico.SERVTL_TIP_UTILIZ := '6';
                v_item.serv_cod := v_servico.servtl_cod;
                v_inseriu_servico  := TRUE;
				v_alterou_item := TRUE;
          ELSIF NVL(v_item.infst_val_cont,0) < 0 and v_servico.clasfi_cod != '0999' and v_servico.servtl_cod is not null and v_servico.servtl_cod NOT LIKE  v_pref_servfzn || '%' THEN
                v_servico.var05 := substr(v_pref_var05 || '[0999] ' || v_servico.servtl_cod || '|' || v_servico.clasfi_cod || '|' || v_servico.SERVTL_TIP_UTILIZ || ' >> ' || v_servico.var05,1,150);
                v_servico.servtl_cod := trim(substr(replace(replace(replace(v_servico.servtl_cod,v_pref_servfzp,''),v_pref_servfzn,''),v_pref_servfz,''),1,60)); 
                v_servico.servtl_cod := trim(substr(v_pref_servfzn || v_servico.servtl_cod,1,60));                
                v_servico.clasfi_cod := '0999';
				v_servico.SERVTL_TIP_UTILIZ := '6';
                v_item.serv_cod := v_servico.servtl_cod;
                v_inseriu_servico  := TRUE;
				v_alterou_item := TRUE;          
          END IF;
          
          

          
          IF v_inseriu_servico THEN
          
                 :v_msg_erro := SUBSTR('Cria servico desconto' || ' >> ' || :v_msg_erro,1,4000);
                 INSERT INTO gfcadastro.tmp_servico_telcom (
                        emps_cod, fili_cod, servtl_dat_atua, servtl_cod, clasfi_cod, servtl_desc, 
                        servtl_compl, servtl_ind_tprec, servtl_ind_tpserv, servtl_cod_nat, var01, var02,
                        var03, var04, var05, num01, num02, num03,
                        servtl_ind_rec, servtl_tip_utiliz
                 )
                 VALUES(
                        v_servico.emps_cod, v_servico.fili_cod, v_servico.servtl_dat_atua,
                        v_servico.servtl_cod, v_servico.clasfi_cod, v_servico.servtl_desc, 
                        v_servico.servtl_compl, v_servico.servtl_ind_tprec, v_servico.servtl_ind_tpserv,
                        v_servico.servtl_cod_nat, v_servico.var01, v_servico.var02,
                        v_servico.var03, v_servico.var04, v_servico.var05,
                        v_servico.num01, v_servico.num02, v_servico.num03,
                        v_servico.servtl_ind_rec, v_servico.servtl_tip_utiliz
                 );

                 v_inseriu_servico := FALSE;
                 
          END IF;
          


          IF v_alterou_item THEN
          
              UPDATE openrisow.item_nftl_serv inf
                        SET var05             = SUBSTR(v_pref_var05 ||':' || inf.serv_cod || '>>'|| inf.VAR05,1,150)
                          , serv_cod          = v_item.serv_cod
                  WHERE rowid = v_item.rowid_inf;
          :v_qtd_atu_inf := :v_qtd_atu_inf + 1;
                  v_alterou_item := false;

                 :v_qtd_atu_inf := :v_qtd_atu_inf + 1;
                 
          END IF;


      
          IF v_reg.ultimo_item_nf  = 'S' THEN                
            

            MERGE INTO openrisow.Servico_Telcom e
                 USING (SELECT DISTINCT * FROM gfcadastro.tmp_Servico_Telcom) h
                   ON (e.EMPS_COD = h.EMPS_COD AND
                           e.FILI_COD = h.FILI_COD AND
                           e.SERVTL_COD = h.SERVTL_COD AND
                           e.SERVTL_DAT_ATUA = h.SERVTL_DAT_ATUA)
                 WHEN NOT MATCHED THEN
                   INSERT (emps_cod, fili_cod, servtl_dat_atua, servtl_cod, clasfi_cod, servtl_desc,                                                                                    servtl_compl, servtl_ind_tprec, servtl_ind_tpserv, servtl_cod_nat, var01, var02,
                                   var03, var04, var05, num01, num02, num03, servtl_ind_rec, servtl_tip_utiliz)
                   VALUES (h.emps_cod, h.fili_cod, h.servtl_dat_atua, h.servtl_cod, h.clasfi_cod, h.servtl_desc,
                                   h.servtl_compl, h.servtl_ind_tprec, h.servtl_ind_tpserv, h.servtl_cod_nat, h.var01, h.var02,
                                   h.var03, h.var04, h.var05, h.num01, h.num02, h.num03, h.servtl_ind_rec, h.servtl_tip_utiliz);


                ${COMMIT};
                
          END IF;           
                        
   END LOOP;

   CLOSE c_sanea;
   
   ${COMMIT};
   prc_tempo('end');
   prc_tempo('Processados ${COMMIT} : ' || :v_qtd_processados || ' >> INF : ' || :v_qtd_atu_inf);
EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK;
      prc_tempo('Erro : ' || SUBSTR(SQLERRM,1,500) || ' - rowid_inf >> ' || v_item.rowid_inf);
      :v_msg_erro := SUBSTR(v_etapa || ' >> ' || :v_msg_erro,1,4000);
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
       cp.qt_atualizados_comp = NVL(cp.qt_atualizados_comp,0) + :v_qtd_atu_comp--,
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
