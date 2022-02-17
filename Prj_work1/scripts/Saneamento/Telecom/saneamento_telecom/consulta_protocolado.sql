        WITH pt as (
                     SELECT /*+ PARALLEL(mes,8) */
                            to_number(mes.numero_nf)      as numero_nf_protocolado,
                            mes.serie,
                            SUM(nvl(MES.BASE_ICMS, 0))    as base_icms_prot,
                            SUM(nvl(MES.valor_icms, 0))   as valor_icms_prot,
                            SUM(nvl(mes.isentas_icms, 0)) as isentas_icms_prot,
                            SUM(nvl(mes.desconto,0))      as desconto_prot,
                            SUM(nvl(mes.VALOR_TOTAL, 0) - nvl(mes.desconto, 0)) as valor_total_protocolado                            
                       FROM gfcarga.tsh_item_conv_115 mes
                      WHERE  1=1
                       AND mes.sit_doc   = 'N'
                       AND replace(mes.serie,' ','')     = 'UT'
                       AND mes.id_arq_conv115 in ( select /*+ first_rows(1)*/ 
                                                          c.id_arq_conv115 
                                                    from gfcarga.tsh_controle_arq_conv_115 c
                                                   where c.area        IN ('PROTOCOLADO')
                                                     and c.emps_cod    = 'TBRA'
                                                     and replace(c.serie,' ','')       = 'UT'
                                                     and c.mes_ano     >= TO_DATE('01/12/2017', 'DD/MM/YYYY')
                                                     and c.mes_ano     <  LAST_DAY(to_date('01/12/2017','dd/mm/yyyy')) + 1
                                                  )                        
                      GROUP BY mes.numero_nf, mes.serie
                   )
        select /*+ PARALLEL(nf,8) */ 
              nf.rowid, 
              nf.emps_cod, 
              nf.fili_cod, 
              nf.mnfst_serie,
              nf.mnfst_num,
              nf.mnfst_dtemiss,
              nf.mdoc_cod,
              nf.mnfst_val_tot,
              pt.valor_total_protocolado,
              nf.mnfst_val_basicms, 
              pt.base_icms_prot,
              nf.mnfst_val_icms,
              pt.valor_icms_prot, 
              nf.mnfst_val_isentas, 
              pt.isentas_icms_prot,
              nf.mnfst_val_tot,
              pt.desconto_prot,
              mnfst_val_tot - valor_total_protocolado   as diff_total,
              mnfst_val_basicms - base_icms_prot        as diff_base,
              mnfst_val_icms - valor_icms_prot          as diff_icms,
              mnfst_val_isentas - isentas_icms_prot     as diff_isentas              
         from pt, 
              openrisow.mestre_nftl_serv nf
        where nf.emps_cod              = 'TBRA' 
          and nf.fili_cod              IN ('0001')
          and nf.mnfst_serie           = 'U  T'
          and translate(nf.mnfst_serie, 'x ', 'x') = translate(pt.serie, 'x ', 'x')
          and nf.mnfst_dtemiss         >= to_date('01/12/2017','dd/mm/yyyy')
          and nf.mnfst_dtemiss         <  LAST_DAY(to_date('01/12/2017','dd/mm/yyyy')) + 1
          and nf.mdoc_cod              IN (22,21) 
          and to_number(nf.mnfst_num)  = pt.numero_nf_protocolado
          and (nf.mnfst_val_basicms    != pt.base_icms_prot
               or nf.mnfst_val_tot     != pt.valor_total_protocolado
               or nf.mnfst_val_icms    != pt.valor_icms_prot
               --or nf.mnfst_val_isentas != pt.isentas_icms_prot
              );