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
var v_st_processamento    VARCHAR2(50)   = 'Em Processamento'
var v_msg_erro            VARCHAR2(4000) = 'MAP_2_24X7'
var exit_code             NUMBER         = 0
var v_qtd_processados     NUMBER         = 0
var v_qtd_atu_nf          NUMBER         = 0
var v_qtd_atu_inf         NUMBER         = 0
var v_qtd_ins_inf         NUMBER         = 0
var v_qtd_atu_cli         NUMBER         = 0
var v_qtd_atu_comp        NUMBER         = 0
var v_qtd_reg_paralizacao NUMBER         = 0
var v_qtd_nao_encontrados NUMBER         = 0
WHENEVER OSERROR EXIT 1;
WHENEVER SQLERROR EXIT 2;
PROMPT
PROMPT MAP_2_24X7
PROMPT ### Inicio do processo ${0} - ${SERIE}  ###
PROMPT


DECLARE

   CURSOR c_sanea_nf
       IS
     WITH
     tab1 AS (
        SELECT /*+ MATERIALIZE */ nf.rowid rowid_nf, inf.rowid rowid_inf,
               nf.cnpj_cpf, nf.cadg_cod, nf.catg_cod, nf.mnfst_dtemiss, inf.cgc_cpf, inf.ie, inf.infst_tel, inf.cfop, inf.INFST_BASE_ICMS
          FROM openrisow.item_nftl_serv PARTITION (${PARTICAO_INF}) inf,
               openrisow.mestre_nftl_serv PARTITION (${PARTICAO_NF}) nf
         WHERE ${FILTRO}
		   AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ( 'AS1', 'AS2', 'AS3', 'T1') 
		AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))
		   AND INF.EMPS_COD = NF.EMPS_COD
           AND INF.FILI_COD = NF.FILI_COD
           AND INF.INFST_SERIE = NF.MNFST_SERIE
           AND INF.INFST_NUM = NF.MNFST_NUM
           AND INF.INFST_DTEMISS = NF.mnfst_dtemiss
           AND INF.FILI_COD NOT IN ('3506','3501')
     ),
     tab2 AS (
       SELECT /*+ PARALLEL(8,8) */ tmp.rowid_nf, tmp.rowid_cli, comp.rowid rowid_comp,
              tmp.CADG_COD_CGCCPF, tmp.CADG_COD_INSEST, comp.CADG_NUM_CONTA, comp.CADG_TIP_ASSIN, comp.CADG_TIP_CLI, tmp.cadg_dat_atua
         FROM openrisow.COMPLVU_CLIFORNEC comp,
--         FROM gfcadastro.COMPLVU_CLIFORNEC_bkp comp,
              (
               SELECT nf.rowid rowid_nf, cli.rowid rowid_cli,
                      cli.cadg_cod, cli.catg_cod, cli.cadg_dat_atua, cli.CADG_COD_CGCCPF, cli.CADG_COD_INSEST, 
                      ROW_NUMBER() OVER (PARTITION BY nf.rowid ORDER BY cli.CADG_DAT_ATUA DESC) nu
                 FROM openrisow.CLI_FORNEC_TRANSP cli,
                      openrisow.mestre_nftl_serv PARTITION (${PARTICAO_NF}) nf
                WHERE ${FILTRO}
				  AND UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('AS1', 'AS2', 'AS3', 'T1') 
					AND (UPPER(TRANSLATE(nf.mnfst_serie,'x ','x')) NOT IN ('ASS') OR nf.mnfst_dtemiss >= TO_DATE('01/04/2017','DD/MM/YYYY'))
				  AND cli.CADG_COD = nf.cadg_cod
                  AND cli.CATG_COD = nf.catg_cod
                  AND cli.CADG_DAT_ATUA <= nf.mnfst_dtemiss
                  
              ) tmp
        WHERE nu = 1
          AND comp.cadg_cod = tmp.cadg_cod
          AND comp.catg_cod = tmp.catg_cod
          AND comp.cadg_dat_atua = tmp.cadg_dat_atua
     )
     SELECT t1.rowid_nf, t1.rowid_inf, t2.rowid_cli, t2.rowid_comp,
            t1.cnpj_cpf, t1.cgc_cpf, t1.ie, t1.infst_tel, t1.cfop, t1.INFST_BASE_ICMS, t1.cadg_cod, t1.catg_cod, t1.mnfst_dtemiss, 
			t2.CADG_COD_CGCCPF, t2.CADG_COD_INSEST, t2.CADG_NUM_CONTA, t2.CADG_TIP_ASSIN, 
			t2.CADG_TIP_CLI
			, t2.cadg_dat_atua
       FROM tab2 t2,
            tab1 t1
      WHERE t2.rowid_nf = t1.rowid_nf
      ORDER BY t1.rowid_nf, t2.rowid_cli, CASE WHEN NVL(t1.cfop,'0000') = '0000' THEN 1 ELSE 0 END;

   TYPE t_sanea_nf IS TABLE OF c_sanea_nf%ROWTYPE INDEX BY BINARY_INTEGER;

   v_bk_sanea_nf  t_sanea_nf;

   v_rowid_nf_ant  ROWID := NULL;
   v_rowid_cli_ant ROWID := NULL;

   v_cpf_cnpj       openrisow.mestre_nftl_serv.CNPJ_CPF%TYPE := NULL;
   v_CADG_TIP_CLI   openrisow.COMPLVU_CLIFORNEC.CADG_TIP_CLI%TYPE := NULL;
   V_CADG_TIP_ASSIN openrisow.COMPLVU_CLIFORNEC.CADG_TIP_ASSIN%TYPE := NULL;
   v_ie             openrisow.item_nftl_serv.IE%TYPE := NULL;
   v_num_terminal   openrisow.item_nftl_serv.INFST_TEL%TYPE := NULL;

  
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

   FOR reg IN c_sanea_nf
   LOOP
      :v_qtd_processados := :v_qtd_processados+1;
	  
      IF reg.rowid_cli IS NULL THEN
         IF v_rowid_nf_ant IS NULL OR v_rowid_nf_ant != reg.rowid_nf THEN
            v_rowid_nf_ant := reg.rowid_nf;

            --INSERT INTO gfcadastro.gftb_nf_sem_cadastro (rowid_nf, cadg_cod, catg_cod, mnfst_dtemiss)
            --VALUES(reg.rowid_nf, reg.cadg_cod, reg.catg_cod, reg.mnfst_dtemiss);

            ${COMMIT};

            :v_qtd_nao_encontrados := :v_qtd_nao_encontrados + 1;

            CONTINUE;
         END IF;
      ELSE
         IF (NVL(reg.cadg_num_conta,'1135497777') = '1135497777' OR reg.cadg_num_conta LIKE '%00000') AND TRIM(reg.infst_tel) IS NOT NULL AND reg.infst_tel NOT LIKE '%00000' THEN
            IF LENGTH(reg.infst_tel) NOT IN (10,11) OR
                  SUBSTR(reg.infst_tel,1,2) NOT IN (
                        '11', '12', '13', '14', '15', '16', '17', '18', '19',
                        '21', '22', '24', '27', '28',
                        '31', '32', '33', '34', '35', '37', '38',
                        '41', '42', '43', '44', '45', '46', '47', '48', '49',
                        '51', '53', '54', '55',
                        '61', '62', '63', '64', '65', '66', '67', '68', '69',
                        '71', '73', '74', '75', '77', '79',
                        '81', '82', '83', '84', '85', '86', '87', '88', '89',
                        '91', '92', '93', '94', '95', '96', '97', '98', '99') THEN
               v_num_terminal := '1135497777';
            ELSIF SUBSTR(reg.infst_tel,3,1) = '0' THEN
               WITH num AS (
                  -- SELECT '320988060535' CADG_NUM_CONTA FROM dual
                  SELECT reg.infst_tel CADG_NUM_CONTA FROM dual
               ), terminal AS (
                  SELECT SUBSTR(CADG_NUM_CONTA,1,2) AS DDD,
                         SUBSTR(CADG_NUM_CONTA,4) AS TELEFONE_APOS_0,
                         LENGTH(SUBSTR(CADG_NUM_CONTA,4)) AS TAM_TELEFONE_APOS_0,
                         SUBSTR(CADG_NUM_CONTA,4,1) AS PRIMEIRO_DIG_APOS_0
                    FROM num
                   WHERE SUBSTR(CADG_NUM_CONTA,3,1) = '0'
               )
               SELECT CASE 
                      WHEN PRIMEIRO_DIG_APOS_0 in ('1', '2', '3','4','5','6','7','8','9') AND TAM_TELEFONE_APOS_0 in (8,9) THEN
                         DDD || TELEFONE_APOS_0
                      ELSE
                         '1135497777'
                      END AS NOVO_CADG_NUM_CONTA
                 INTO v_num_terminal
                 FROM TERMINAL;
            ELSE
               v_num_terminal := reg.infst_tel;
            END IF;
         ELSE
            v_num_terminal := CASE WHEN reg.cadg_num_conta LIKE '%00000' OR reg.cadg_num_conta IS NULL THEN '1135497777' ELSE reg.cadg_num_conta END;
         END IF;

         IF v_rowid_nf_ant IS NULL OR v_rowid_nf_ant != reg.rowid_nf THEN
            v_rowid_nf_ant := reg.rowid_nf;
			
			IF TRIM(reg.cadg_cod_cgccpf) IS NOT NULL AND REGEXP_LIKE(TRIM(reg.cadg_cod_cgccpf),'[0-9]') THEN
            -- IF SANEAMENTO_GF_FLA.VALIDA_CPF_CNPJ(reg.cadg_cod_cgccpf) THEN
               v_cpf_cnpj := reg.cadg_cod_cgccpf;
            -- ELSIF SANEAMENTO_GF_FLA.VALIDA_CPF_CNPJ(TRIM(reg.CNPJ_CPF)) THEN
            --   v_cpf_cnpj := TRANSLATE(reg.CNPJ_CPF,'0./- ','0');
            ELSE
               v_cpf_cnpj := '11111111111';
            END IF;

            IF v_cpf_cnpj != reg.CNPJ_CPF THEN
               UPDATE openrisow.mestre_nftl_serv PARTITION (${PARTICAO_NF}) nf
                  SET nf.CNPJ_CPF = v_cpf_cnpj,
                      nf.var05 = SUBSTR('ANT_CPF_CNPJ='||nf.cnpj_cpf|| '>>'|| nf.VAR05,1,150)
                WHERE nf.rowid = reg.rowid_nf;

               :v_qtd_atu_nf := :v_qtd_atu_nf + 1;
            END IF;

            IF v_rowid_cli_ant IS NULL OR v_rowid_cli_ant != reg.rowid_cli THEN
               v_rowid_cli_ant := reg.rowid_cli;

               IF reg.rowid_comp IS NOT NULL THEN -- Verificar tambem historico ordenar por este tambem 
                  
				  V_CADG_TIP_ASSIN := reg.CADG_TIP_ASSIN;
				  
				  IF reg.INFST_BASE_ICMS > 0 AND reg.cfop != '0000' THEN
                     v_CADG_TIP_CLI := CASE LENGTH(v_cpf_cnpj)
                                           WHEN 11 THEN '03'
                                           WHEN 14 THEN
                                              CASE WHEN reg.cfop LIKE '_303' THEN '01' WHEN reg.cfop LIKE '_302' THEN '02' WHEN reg.cfop LIKE '_306' THEN '04' ELSE '99' END
                                           ELSE '99'
                                       END;
   
                     V_CADG_TIP_ASSIN := CASE LENGTH(v_cpf_cnpj)
                                            WHEN 11 THEN '3'
                                            WHEN 14 THEN
                                               CASE WHEN reg.cfop LIKE '_302' THEN '1' WHEN reg.cfop LIKE '_303' THEN '1' WHEN reg.cfop = '5306' THEN '4' ELSE '6' END
                                            ELSE '6'
                                         END;

                  END IF;

				  IF reg.CADG_DAT_ATUA >= TO_DATE('01/01/2017','DD/MM/YYYY')  THEN
					  V_CADG_TIP_ASSIN      := '0';	
				  END IF;

                  IF v_cadg_tip_cli != NVL(reg.CADG_TIP_CLI,'X') 
					OR v_cadg_tip_assin != NVL(reg.CADG_TIP_ASSIN,'X') 
					OR v_num_terminal != reg.cadg_num_conta 
				  THEN
                     UPDATE openrisow.COMPLVU_CLIFORNEC comp
                        SET comp.CADG_TIP_ASSIN = v_cadg_tip_assin,
                            comp.CADG_TIP_CLI = v_cadg_tip_cli
                      WHERE comp.rowid = reg.rowid_comp;
/*
                     UPDATE gfcadastro.COMPLVU_CLIFORNEC_bkp comp
                        SET comp.CADG_TIP_ASSIN = v_cadg_tip_assin,
                            comp.CADG_TIP_CLI = v_cadg_tip_cli,
                            comp.cadg_num_conta = v_num_terminal,
                            comp.cadg_tel_contato = v_num_terminal,
                            fl_alterou = 'X'
                      WHERE comp.rowid = reg.rowid_comp;
*/

                      :v_qtd_atu_comp := :v_qtd_atu_comp + 1;
                  END IF;
				  
				  
               END IF;

               IF v_cpf_cnpj != reg.CADG_COD_CGCCPF THEN
                  UPDATE openrisow.CLI_FORNEC_TRANSP cli
                     SET cli.CADG_COD_CGCCPF = v_cpf_cnpj,
                         cli.CADG_TIP = DECODE(LENGTH(v_cpf_cnpj), 11, 'F', 14, 'J', cli.cadg_tip)
                   WHERE cli.rowid = reg.rowid_cli;

                  :v_qtd_atu_cli := :v_qtd_atu_cli + 1;
               END IF;
            END IF;
         END IF;

         v_ie := reg.cadg_cod_insest;

         IF v_cpf_cnpj != reg.cgc_cpf OR v_ie != reg.ie OR v_num_terminal != reg.infst_tel THEN
            UPDATE openrisow.item_nftl_serv PARTITION (${PARTICAO_INF}) inf
               SET inf.CGC_CPF = v_cpf_cnpj,
                   inf.IE = v_ie,
                   inf.INFST_TEL = v_num_terminal,
                   inf.var05 = SUBSTR('ANT_CGC_CPF='||inf.CGC_CPF||'_IE='||inf.IE||'_INFST_TEL='||inf.INFST_TEL|| '>>'|| inf.VAR05,1,150)
             WHERE inf.rowid = reg.rowid_inf;

            :v_qtd_atu_inf := :v_qtd_atu_inf  + 1;
         END IF;
      END IF;

      ${COMMIT};
   END LOOP;

   ${COMMIT};
   prc_tempo('FIM');
   prc_tempo('Processados ${COMMIT} : ' || :v_qtd_processados || ' >> NF : ' || :v_qtd_atu_nf || ' >> INF : ' || :v_qtd_atu_inf || ' >> CLI : ' || :v_qtd_atu_cli);

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
       cp.ds_msg_erro          = substr(substr(nvl(:v_msg_erro,' '),1,900) || ' >> ' || :v_qtd_nao_encontrados || ' >> ' || cp.ds_msg_erro ,1,4000),
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


