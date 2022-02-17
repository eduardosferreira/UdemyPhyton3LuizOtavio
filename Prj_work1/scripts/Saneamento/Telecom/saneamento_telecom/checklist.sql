set serverout on;                                                
set feed on;
DECLARE
/*
[15:02] Hudson De Campos Cruz (Convidado)
    
SELECT max(id_execucao) FROM gfcadastro.tb_log_valida_checklist ;

SELECT * FROM gfcadastro.tb_log_valida_checklist where id_execucao = 134;
SELECT * FROM gfcadastro.tb_valida_checklist where id_execucao = 134 ;

*/
   v_dtini      VARCHAR2(15);
   v_dtfim      VARCHAR2(15);
   v_serie      VARCHAR2(50);
   v_filial     VARCHAR2(50);
   v_estado     VARCHAR2(2);
   v_nivelexec  VARCHAR2(1);     -- C -> Completa   |  P -> Parcial  
   v_retorno    VARCHAR2(20000);
   
BEGIN
   v_dtini      := '01/04/2020';
   v_dtfim      := TO_CHAR(LAST_DAY(TO_DATE(v_dtini,'DD/MM/YYYY')),'DD/MM/YYYY');
   v_serie      := NULL;
   v_filial     := '0001,9144,9201';
   v_estado     := 'SP';
   v_nivelexec  := 'C';
  
   gfcadastro.pkg_valida_checklist.sp_valida_checklist (p_dtinip   => v_dtini,
                                                        p_dtfimp   => v_dtfim,
                                                        p_seriep   => v_serie,
                                                        p_filialp  => v_filial,
                                                        p_estadop  => v_estado,
                                                        p_nivelex  => v_nivelexec,
                                                        p_returnp  => v_retorno);


EXCEPTION
  WHEN OTHERS THEN
    dbms_output.put_line('Falha: '||SQLERRM);
END;
/
