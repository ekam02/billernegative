from sqlmodel import text


BILLER_QUERY_MEMOS_BY_NUMBERS = text(
    """WITH equivalent_store AS (
    SELECT 'CABECERA (322)' store_desc, 2 biller_line, 322 biller_store_id, 1 jano_line, 322 jano_store_id UNION ALL
    SELECT 'E_COMMERCE (204)' store_desc, 3 biller_line, 204 biller_store_id, 1 jano_line, 204 jano_store_id UNION ALL
    SELECT 'EASY 4SUR MEDELLIN (3814)' store_desc, 4 biller_line, 3814 biller_store_id, 2 jano_line, 814 jano_store_id UNION ALL
    SELECT 'EASY ALTOS DEL PRADO (3043)' store_desc, 4 biller_line, 3043 biller_store_id, 2 jano_line, 43 jano_store_id UNION ALL
    SELECT 'EASY AMERICAS (5)' store_desc, 4 biller_line, 5 biller_store_id, 2 jano_line, 802 jano_store_id UNION ALL
    SELECT 'EASY AUTOSUR (1006)' store_desc, 4 biller_line, 1006 biller_store_id, 2 jano_line, 18 jano_store_id UNION ALL
    SELECT 'EASY BOSA (1002)' store_desc, 4 biller_line, 1002 biller_store_id, 2 jano_line, 10 jano_store_id UNION ALL
    SELECT 'EASY CALLE 13 (3811)' store_desc, 4 biller_line, 3811 biller_store_id, 2 jano_line, 811 jano_store_id UNION ALL
    SELECT 'EASY CALLE 170 (3014)' store_desc, 4 biller_line, 3014 biller_store_id, 2 jano_line, 14 jano_store_id UNION ALL
    SELECT 'EASY CALLE 80 (1004)' store_desc, 4 biller_line, 1004 biller_store_id, 2 jano_line, 11 jano_store_id UNION ALL
    SELECT 'EASY CENTRO MAYOR (3805)' store_desc, 4 biller_line, 3805 biller_store_id, 2 jano_line, 805 jano_store_id UNION ALL
    SELECT 'EASY GAITAN CORTES (3813)' store_desc, 4 biller_line, 3813 biller_store_id, 2 jano_line, 813 jano_store_id UNION ALL
    SELECT 'EASY HAYUELOS (12)' store_desc, 4 biller_line, 12 biller_store_id, 2 jano_line, 15 jano_store_id UNION ALL
    SELECT 'EASY MAYALES VALLEDUPAR (3809)' store_desc, 4 biller_line, 3809 biller_store_id, 2 jano_line, 809 jano_store_id UNION ALL
    SELECT 'EASY NORTE (3801)' store_desc, 4 biller_line, 3801 biller_store_id, 2 jano_line, 801 jano_store_id UNION ALL
    SELECT 'EASY OCCIDENTE (3800)' store_desc, 4 biller_line, 3800 biller_store_id, 2 jano_line, 800 jano_store_id UNION ALL
    SELECT 'EASY PRADO MEDELLIN (3806)' store_desc, 4 biller_line, 3806 biller_store_id, 2 jano_line, 806 jano_store_id UNION ALL
    SELECT 'EASY SOACHA (3812)' store_desc, 4 biller_line, 3812 biller_store_id, 2 jano_line, 812 jano_store_id UNION ALL
    SELECT 'EDS 1 MAYO (8)' store_desc, 3 biller_line, 8 biller_store_id, 1 jano_line, 8 jano_store_id UNION ALL
    SELECT 'EDS ALAMOS (139)' store_desc, 3 biller_line, 139 biller_store_id, 1 jano_line, 139 jano_store_id UNION ALL
    SELECT 'EDS ALQUERIA (149)' store_desc, 3 biller_line, 149 biller_store_id, 1 jano_line, 149 jano_store_id UNION ALL
    SELECT 'EDS APOLO AUTOSUR (21)' store_desc, 3 biller_line, 21 biller_store_id, 1 jano_line, 21 jano_store_id UNION ALL
    SELECT 'EDS AV DEL RIO (98)' store_desc, 3 biller_line, 98 biller_store_id, 1 jano_line, 98 jano_store_id UNION ALL
    SELECT 'EDS AVD CIUDAD CALI (145)' store_desc, 3 biller_line, 145 biller_store_id, 1 jano_line, 145 jano_store_id UNION ALL
    SELECT 'EDS BAHIA CL 46 (136)' store_desc, 3 biller_line, 136 biller_store_id, 1 jano_line, 136 jano_store_id UNION ALL
    SELECT 'EDS CALLE 190 (161)' store_desc, 3 biller_line, 161 biller_store_id, 1 jano_line, 161 jano_store_id UNION ALL
    SELECT 'EDS COLOMBIA CALLE 50 (7)' store_desc, 3 biller_line, 7 biller_store_id, 1 jano_line, 7 jano_store_id UNION ALL
    SELECT 'EDS IVESUR CIRCUN (123)' store_desc, 3 biller_line, 123 biller_store_id, 1 jano_line, 123 jano_store_id UNION ALL
    SELECT 'EDS LA 17 CRA 17 (159)' store_desc, 3 biller_line, 159 biller_store_id, 1 jano_line, 159 jano_store_id UNION ALL
    SELECT 'EDS LA 24 CL 5B (132)' store_desc, 3 biller_line, 132 biller_store_id, 1 jano_line, 132 jano_store_id UNION ALL
    SELECT 'EDS LA 42 CL 34B (158)' store_desc, 3 biller_line, 158 biller_store_id, 1 jano_line, 158 jano_store_id UNION ALL
    SELECT 'EDS MARRUECOS (2)' store_desc, 3 biller_line, 2 biller_store_id, 1 jano_line, 2 jano_store_id UNION ALL
    SELECT 'EDS SAN GABRIEL AERO (133)' store_desc, 3 biller_line, 133 biller_store_id, 1 jano_line, 133 jano_store_id UNION ALL
    SELECT 'EDS SEVILLANA (146)' store_desc, 3 biller_line, 146 biller_store_id, 1 jano_line, 146 jano_store_id UNION ALL
    SELECT 'EDS SIMON BOLIVAR CL 25 (134)' store_desc, 3 biller_line, 134 biller_store_id, 1 jano_line, 134 jano_store_id UNION ALL
    SELECT 'EDS SOACHA TERRENOS (160)' store_desc, 3 biller_line, 160 biller_store_id, 1 jano_line, 160 jano_store_id UNION ALL
    SELECT 'EDS TERCERA CL 34 (125)' store_desc, 3 biller_line, 125 biller_store_id, 1 jano_line, 125 jano_store_id UNION ALL
    SELECT 'EDS TINTALITO (101)' store_desc, 3 biller_line, 101 biller_store_id, 1 jano_line, 101 jano_store_id UNION ALL
    SELECT 'EDS TREBOL CL 76 (121)' store_desc, 3 biller_line, 121 biller_store_id, 1 jano_line, 121 jano_store_id UNION ALL
    SELECT 'EDS YUMBO CRA 33 (102)' store_desc, 3 biller_line, 102 biller_store_id, 1 jano_line, 102 jano_store_id UNION ALL
    SELECT 'JUMBO 20 DE JULIO (17)' store_desc, 2 biller_line, 17 biller_store_id, 1 jano_line, 17 jano_store_id UNION ALL
    SELECT 'JUMBO ALTOS DEL COUNTRY (249)' store_desc, 1 biller_line, 249 biller_store_id, 1 jano_line, 249 jano_store_id UNION ALL
    SELECT 'JUMBO ALTOS DEL PRADO (43)' store_desc, 1 biller_line, 43 biller_store_id, 1 jano_line, 43 jano_store_id UNION ALL
    SELECT 'JUMBO AMERICANO (86)' store_desc, 1 biller_line, 86 biller_store_id, 1 jano_line, 86 jano_store_id UNION ALL
    SELECT 'JUMBO ATLANTIS (194)' store_desc, 1 biller_line, 194 biller_store_id, 1 jano_line, 194 jano_store_id UNION ALL
    SELECT 'JUMBO ATRIO (253)' store_desc, 1 biller_line, 253 biller_store_id, 1 jano_line, 253 jano_store_id UNION ALL
    SELECT 'JUMBO AUTOPISTA SUR (18)' store_desc, 1 biller_line, 18 biller_store_id, 1 jano_line, 18 jano_store_id UNION ALL
    SELECT 'JUMBO BUENAVISTA (88)' store_desc, 1 biller_line, 88 biller_store_id, 1 jano_line, 88 jano_store_id UNION ALL
    SELECT 'JUMBO BULEVAR NIZA (3)' store_desc, 1 biller_line, 3 biller_store_id, 1 jano_line, 3 jano_store_id UNION ALL
    SELECT 'JUMBO CABECERA (103)' store_desc, 1 biller_line, 103 biller_store_id, 1 jano_line, 103 jano_store_id UNION ALL
    SELECT 'JUMBO CALLE 170 (14)' store_desc, 1 biller_line, 14 biller_store_id, 1 jano_line, 14 jano_store_id UNION ALL
    SELECT 'JUMBO CALLE 80 (11)' store_desc, 1 biller_line, 11 biller_store_id, 1 jano_line, 11 jano_store_id UNION ALL
    SELECT 'JUMBO CANAVERALL (108)' store_desc, 1 biller_line, 108 biller_store_id, 1 jano_line, 108 jano_store_id UNION ALL
    SELECT 'JUMBO CARIBE PLAZA (83)' store_desc, 1 biller_line, 83 biller_store_id, 1 jano_line, 83 jano_store_id UNION ALL
    SELECT 'JUMBO CARRERA 30 (16)' store_desc, 1 biller_line, 16 biller_store_id, 1 jano_line, 16 jano_store_id UNION ALL
    SELECT 'JUMBO CHIA (44)' store_desc, 1 biller_line, 44 biller_store_id, 1 jano_line, 44 jano_store_id UNION ALL
    SELECT 'JUMBO CHIA BAZAR (191)' store_desc, 1 biller_line, 191 biller_store_id, 1 jano_line, 191 jano_store_id UNION ALL
    SELECT 'JUMBO CHIPICHAPE (31)' store_desc, 1 biller_line, 31 biller_store_id, 1 jano_line, 31 jano_store_id UNION ALL
    SELECT 'JUMBO DE LA 65 (23)' store_desc, 1 biller_line, 23 biller_store_id, 1 jano_line, 23 jano_store_id UNION ALL
    SELECT 'JUMBO EL CASTILLO (174)' store_desc, 1 biller_line, 174 biller_store_id, 1 jano_line, 174 jano_store_id UNION ALL
    SELECT 'JUMBO GIRARDOT (47)' store_desc, 1 biller_line, 47 biller_store_id, 1 jano_line, 47 jano_store_id UNION ALL
    SELECT 'JUMBO GUATAPURI (87)' store_desc, 1 biller_line, 87 biller_store_id, 1 jano_line, 87 jano_store_id UNION ALL
    SELECT 'JUMBO HAYUELOS (15)' store_desc, 1 biller_line, 15 biller_store_id, 1 jano_line, 15 jano_store_id UNION ALL
    SELECT 'JUMBO LAS VEGAS (22)' store_desc, 1 biller_line, 22 biller_store_id, 1 jano_line, 22 jano_store_id UNION ALL
    SELECT 'JUMBO LIMONAR PREMIER (30)' store_desc, 1 biller_line, 30 biller_store_id, 1 jano_line, 30 jano_store_id UNION ALL
    SELECT 'JUMBO MEGA MALL (84)' store_desc, 1 biller_line, 84 biller_store_id, 1 jano_line, 84 jano_store_id UNION ALL
    SELECT 'JUMBO PASTO (128)' store_desc, 2 biller_line, 128 biller_store_id, 1 jano_line, 128 jano_store_id UNION ALL
    SELECT 'JUMBO PLAZA CENTRAL (193)' store_desc, 1 biller_line, 193 biller_store_id, 1 jano_line, 193 jano_store_id UNION ALL
    SELECT 'JUMBO POPAYAN (36)' store_desc, 1 biller_line, 36 biller_store_id, 1 jano_line, 36 jano_store_id UNION ALL
    SELECT 'JUMBO PREMIUM PLAZA (24)' store_desc, 1 biller_line, 24 biller_store_id, 1 jano_line, 24 jano_store_id UNION ALL
    SELECT 'JUMBO RIONEGRO (27)' store_desc, 1 biller_line, 27 biller_store_id, 1 jano_line, 27 jano_store_id UNION ALL
    SELECT 'JUMBO SAN SILVESTRE (126)' store_desc, 1 biller_line, 126 biller_store_id, 1 jano_line, 126 jano_store_id UNION ALL
    SELECT 'JUMBO SANTA ANA (19)' store_desc, 1 biller_line, 19 biller_store_id, 1 jano_line, 19 jano_store_id UNION ALL
    SELECT 'JUMBO SANTA FE (72)' store_desc, 1 biller_line, 72 biller_store_id, 1 jano_line, 72 jano_store_id UNION ALL
    SELECT 'JUMBO SANTA MARTA (45)' store_desc, 1 biller_line, 45 biller_store_id, 1 jano_line, 45 jano_store_id UNION ALL
    SELECT 'JUMBO SANTAFE (148)' store_desc, 1 biller_line, 148 biller_store_id, 1 jano_line, 148 jano_store_id UNION ALL
    SELECT 'JUMBO SOACHA (48)' store_desc, 2 biller_line, 48 biller_store_id, 1 jano_line, 48 jano_store_id UNION ALL
    SELECT 'JUMBO SUBA (70)' store_desc, 1 biller_line, 70 biller_store_id, 1 jano_line, 70 jano_store_id UNION ALL
    SELECT 'JUMBO TITAN (167)' store_desc, 1 biller_line, 167 biller_store_id, 1 jano_line, 167 jano_store_id UNION ALL
    SELECT 'JUMBO TUNJA (76)' store_desc, 1 biller_line, 76 biller_store_id, 1 jano_line, 76 jano_store_id UNION ALL
    SELECT 'JUMBO UNICENTRO (9)' store_desc, 1 biller_line, 9 biller_store_id, 1 jano_line, 9 jano_store_id UNION ALL
    SELECT 'JUMBO VALLE DE LILI (13)' store_desc, 1 biller_line, 13 biller_store_id, 1 jano_line, 13 jano_store_id UNION ALL
    SELECT 'JUMBO YOPAL (185)' store_desc, 1 biller_line, 185 biller_store_id, 1 jano_line, 185 jano_store_id UNION ALL
    SELECT 'METRO  FATELARES (806)' store_desc, 2 biller_line, 806 biller_store_id, 1 jano_line, 806 jano_store_id UNION ALL
    SELECT 'METRO  FONTIBON (78)' store_desc, 2 biller_line, 78 biller_store_id, 1 jano_line, 78 jano_store_id UNION ALL
    SELECT 'METRO  MAYALES (184)' store_desc, 2 biller_line, 184 biller_store_id, 1 jano_line, 184 jano_store_id UNION ALL
    SELECT 'METRO ALQUERIA (124)' store_desc, 2 biller_line, 124 biller_store_id, 1 jano_line, 124 jano_store_id UNION ALL
    SELECT 'METRO ATALAYAS (82)' store_desc, 2 biller_line, 82 biller_store_id, 1 jano_line, 82 jano_store_id UNION ALL
    SELECT 'METRO BANDERAS (26)' store_desc, 2 biller_line, 26 biller_store_id, 1 jano_line, 26 jano_store_id UNION ALL
    SELECT 'METRO BELLO ANTIOQUIA (25)' store_desc, 2 biller_line, 25 biller_store_id, 1 jano_line, 25 jano_store_id UNION ALL
    SELECT 'METRO BOSA (10)' store_desc, 2 biller_line, 10 biller_store_id, 1 jano_line, 10 jano_store_id UNION ALL
    SELECT 'METRO BUGA (35)' store_desc, 2 biller_line, 35 biller_store_id, 1 jano_line, 35 jano_store_id UNION ALL
    SELECT 'METRO CARTAGO (33)' store_desc, 2 biller_line, 33 biller_store_id, 1 jano_line, 33 jano_store_id UNION ALL
    SELECT 'METRO DOSQUEBRADAS (166)' store_desc, 2 biller_line, 166 biller_store_id, 1 jano_line, 166 jano_store_id UNION ALL
    SELECT 'METRO DUITAMA (28)' store_desc, 2 biller_line, 28 biller_store_id, 1 jano_line, 28 jano_store_id UNION ALL
    SELECT 'METRO E_COMMERCE (203)' store_desc, 3 biller_line, 203 biller_store_id, 1 jano_line, 203 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS ALHAMBRA (334)' store_desc, 2 biller_line, 334 biller_store_id, 1 jano_line, 334 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS ANT COUNTRY (305)' store_desc, 2 biller_line, 305 biller_store_id, 1 jano_line, 305 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CALLE 125 (340)' store_desc, 2 biller_line, 340 biller_store_id, 1 jano_line, 340 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CALLE 148 (329)' store_desc, 2 biller_line, 329 biller_store_id, 1 jano_line, 329 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CALLE 45 (309)' store_desc, 2 biller_line, 309 biller_store_id, 1 jano_line, 309 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CALLE 93 (324)' store_desc, 2 biller_line, 324 biller_store_id, 1 jano_line, 324 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CALLE 95 (303)' store_desc, 2 biller_line, 303 biller_store_id, 1 jano_line, 303 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CARACAS CON 49 (304)' store_desc, 2 biller_line, 304 biller_store_id, 1 jano_line, 304 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CARRERA 3 (336)' store_desc, 2 biller_line, 336 biller_store_id, 1 jano_line, 336 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CEDRITOS  CALL (301)' store_desc, 2 biller_line, 301 biller_store_id, 1 jano_line, 301 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CIUDAD SALITRE (337)' store_desc, 2 biller_line, 337 biller_store_id, 1 jano_line, 337 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CORFERIAS (333)' store_desc, 2 biller_line, 333 biller_store_id, 1 jano_line, 333 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS JAVERIANA 45 (335)' store_desc, 2 biller_line, 335 biller_store_id, 1 jano_line, 335 jano_store_id UNION ALL
    SELECT 'METRO FACATATIVA (74)' store_desc, 2 biller_line, 74 biller_store_id, 1 jano_line, 74 jano_store_id UNION ALL
    SELECT 'METRO FLORIDABLANCA (34)' store_desc, 2 biller_line, 34 biller_store_id, 1 jano_line, 34 jano_store_id UNION ALL
    SELECT 'METRO GIRON (39)' store_desc, 2 biller_line, 39 biller_store_id, 1 jano_line, 39 jano_store_id UNION ALL
    SELECT 'METRO IBAGUE (42)' store_desc, 2 biller_line, 42 biller_store_id, 1 jano_line, 42 jano_store_id UNION ALL
    SELECT 'METRO ITAGUI (46)' store_desc, 2 biller_line, 46 biller_store_id, 1 jano_line, 46 jano_store_id UNION ALL
    SELECT 'METRO LA 70 (32)' store_desc, 2 biller_line, 32 biller_store_id, 1 jano_line, 32 jano_store_id UNION ALL
    SELECT 'METRO LIBERTADORES (80)' store_desc, 2 biller_line, 80 biller_store_id, 1 jano_line, 80 jano_store_id UNION ALL
    SELECT 'METRO MONTERIA (85)' store_desc, 2 biller_line, 85 biller_store_id, 1 jano_line, 85 jano_store_id UNION ALL
    SELECT 'METRO MOSQUERA (73)' store_desc, 2 biller_line, 73 biller_store_id, 1 jano_line, 73 jano_store_id UNION ALL
    SELECT 'METRO NEIVA (38)' store_desc, 2 biller_line, 38 biller_store_id, 1 jano_line, 38 jano_store_id UNION ALL
    SELECT 'METRO PALMIRA (6)' store_desc, 2 biller_line, 6 biller_store_id, 1 jano_line, 6 jano_store_id UNION ALL
    SELECT 'METRO PASTO UNICO (171)' store_desc, 2 biller_line, 171 biller_store_id, 1 jano_line, 171 jano_store_id UNION ALL
    SELECT 'METRO PITALITO (179)' store_desc, 2 biller_line, 179 biller_store_id, 1 jano_line, 179 jano_store_id UNION ALL
    SELECT 'METRO RIOHACHA (40)' store_desc, 2 biller_line, 40 biller_store_id, 1 jano_line, 40 jano_store_id UNION ALL
    SELECT 'METRO SAN CAYETANO (79)' store_desc, 2 biller_line, 79 biller_store_id, 1 jano_line, 79 jano_store_id UNION ALL
    SELECT 'METRO SAN GIL (183)' store_desc, 2 biller_line, 183 biller_store_id, 1 jano_line, 183 jano_store_id UNION ALL
    SELECT 'METRO SANTA LUCIA (195)' store_desc, 2 biller_line, 195 biller_store_id, 1 jano_line, 195 jano_store_id UNION ALL
    SELECT 'METRO SIMON BOLIVAR (75)' store_desc, 2 biller_line, 75 biller_store_id, 1 jano_line, 75 jano_store_id UNION ALL
    SELECT 'METRO SOGAMOSO (97)' store_desc, 2 biller_line, 97 biller_store_id, 1 jano_line, 97 jano_store_id UNION ALL
    SELECT 'METRO TINTALITO (71)' store_desc, 2 biller_line, 71 biller_store_id, 1 jano_line, 71 jano_store_id UNION ALL
    SELECT 'METRO VENTURA (81)' store_desc, 2 biller_line, 81 biller_store_id, 1 jano_line, 81 jano_store_id UNION ALL
    SELECT 'METRO ZIPAQUIRA (77)' store_desc, 2 biller_line, 77 biller_store_id, 1 jano_line, 77 jano_store_id
)
SELECT
    es.store_desc AS store_name,
    ef.num_doc_fact_elec AS doc_num,
    ef.c_origen AS doc_type,
    es.jano_line AS line,
    es.biller_store_id AS store,
    ef.nro_terminal AS pos,
    ef.cod_prov AS trx,
    ef.fec_fact::DATE AS billed_at,
    efe.f_envio_dian::DATE AS sent_at,
    CASE
        WHEN ef.c_origen IN (4, 9) THEN ef.vt_factura::INT8 * -1
        ELSE ef.vt_factura::INT8
    END AS amount,
    ef.nit_prov AS customer,
    ef.factura_duplicada AS duplicated,
    efe.estado_dian AS status,
    efe.log_errores_dian AS log_dian,
    efe.cufe AS uuid
FROM
    factura.estado_fact_electronica AS efe
        INNER JOIN factura.encabezado_factura AS ef ON ef.encfactura_col_id = efe.encfactura_id
        INNER JOIN equivalent_store es ON ef.codigo_tienda = es.biller_store_id
WHERE
    ef.num_doc_fact_elec IN :numbers
    AND ef.c_origen IN (4, 9, 15)
    AND efe.estado_dian = 'OK'
    AND ef.factura_duplicada IS NULL"""
)


BILLER_QUERY_NEGATIVE_INVOICES = text(
    """WITH equivalent_store AS (
    SELECT 'CABECERA (322)' store_desc, 2 biller_line, 322 biller_store_id, 1 jano_line, 322 jano_store_id UNION ALL
    SELECT 'E_COMMERCE (204)' store_desc, 3 biller_line, 204 biller_store_id, 1 jano_line, 204 jano_store_id UNION ALL
    SELECT 'EASY 4SUR MEDELLIN (3814)' store_desc, 4 biller_line, 3814 biller_store_id, 2 jano_line, 814 jano_store_id UNION ALL
    SELECT 'EASY ALTOS DEL PRADO (3043)' store_desc, 4 biller_line, 3043 biller_store_id, 2 jano_line, 43 jano_store_id UNION ALL
    SELECT 'EASY AMERICAS (5)' store_desc, 4 biller_line, 5 biller_store_id, 2 jano_line, 802 jano_store_id UNION ALL
    SELECT 'EASY AUTOSUR (1006)' store_desc, 4 biller_line, 1006 biller_store_id, 2 jano_line, 18 jano_store_id UNION ALL
    SELECT 'EASY BOSA (1002)' store_desc, 4 biller_line, 1002 biller_store_id, 2 jano_line, 10 jano_store_id UNION ALL
    SELECT 'EASY CALLE 13 (3811)' store_desc, 4 biller_line, 3811 biller_store_id, 2 jano_line, 811 jano_store_id UNION ALL
    SELECT 'EASY CALLE 170 (3014)' store_desc, 4 biller_line, 3014 biller_store_id, 2 jano_line, 14 jano_store_id UNION ALL
    SELECT 'EASY CALLE 80 (1004)' store_desc, 4 biller_line, 1004 biller_store_id, 2 jano_line, 11 jano_store_id UNION ALL
    SELECT 'EASY CENTRO MAYOR (3805)' store_desc, 4 biller_line, 3805 biller_store_id, 2 jano_line, 805 jano_store_id UNION ALL
    SELECT 'EASY GAITAN CORTES (3813)' store_desc, 4 biller_line, 3813 biller_store_id, 2 jano_line, 813 jano_store_id UNION ALL
    SELECT 'EASY HAYUELOS (12)' store_desc, 4 biller_line, 12 biller_store_id, 2 jano_line, 15 jano_store_id UNION ALL
    SELECT 'EASY MAYALES VALLEDUPAR (3809)' store_desc, 4 biller_line, 3809 biller_store_id, 2 jano_line, 809 jano_store_id UNION ALL
    SELECT 'EASY NORTE (3801)' store_desc, 4 biller_line, 3801 biller_store_id, 2 jano_line, 801 jano_store_id UNION ALL
    SELECT 'EASY OCCIDENTE (3800)' store_desc, 4 biller_line, 3800 biller_store_id, 2 jano_line, 800 jano_store_id UNION ALL
    SELECT 'EASY PRADO MEDELLIN (3806)' store_desc, 4 biller_line, 3806 biller_store_id, 2 jano_line, 806 jano_store_id UNION ALL
    SELECT 'EASY SOACHA (3812)' store_desc, 4 biller_line, 3812 biller_store_id, 2 jano_line, 812 jano_store_id UNION ALL
    SELECT 'EDS 1 MAYO (8)' store_desc, 3 biller_line, 8 biller_store_id, 1 jano_line, 8 jano_store_id UNION ALL
    SELECT 'EDS ALAMOS (139)' store_desc, 3 biller_line, 139 biller_store_id, 1 jano_line, 139 jano_store_id UNION ALL
    SELECT 'EDS ALQUERIA (149)' store_desc, 3 biller_line, 149 biller_store_id, 1 jano_line, 149 jano_store_id UNION ALL
    SELECT 'EDS APOLO AUTOSUR (21)' store_desc, 3 biller_line, 21 biller_store_id, 1 jano_line, 21 jano_store_id UNION ALL
    SELECT 'EDS AV DEL RIO (98)' store_desc, 3 biller_line, 98 biller_store_id, 1 jano_line, 98 jano_store_id UNION ALL
    SELECT 'EDS AVD CIUDAD CALI (145)' store_desc, 3 biller_line, 145 biller_store_id, 1 jano_line, 145 jano_store_id UNION ALL
    SELECT 'EDS BAHIA CL 46 (136)' store_desc, 3 biller_line, 136 biller_store_id, 1 jano_line, 136 jano_store_id UNION ALL
    SELECT 'EDS CALLE 190 (161)' store_desc, 3 biller_line, 161 biller_store_id, 1 jano_line, 161 jano_store_id UNION ALL
    SELECT 'EDS COLOMBIA CALLE 50 (7)' store_desc, 3 biller_line, 7 biller_store_id, 1 jano_line, 7 jano_store_id UNION ALL
    SELECT 'EDS IVESUR CIRCUN (123)' store_desc, 3 biller_line, 123 biller_store_id, 1 jano_line, 123 jano_store_id UNION ALL
    SELECT 'EDS LA 17 CRA 17 (159)' store_desc, 3 biller_line, 159 biller_store_id, 1 jano_line, 159 jano_store_id UNION ALL
    SELECT 'EDS LA 24 CL 5B (132)' store_desc, 3 biller_line, 132 biller_store_id, 1 jano_line, 132 jano_store_id UNION ALL
    SELECT 'EDS LA 42 CL 34B (158)' store_desc, 3 biller_line, 158 biller_store_id, 1 jano_line, 158 jano_store_id UNION ALL
    SELECT 'EDS MARRUECOS (2)' store_desc, 3 biller_line, 2 biller_store_id, 1 jano_line, 2 jano_store_id UNION ALL
    SELECT 'EDS SAN GABRIEL AERO (133)' store_desc, 3 biller_line, 133 biller_store_id, 1 jano_line, 133 jano_store_id UNION ALL
    SELECT 'EDS SEVILLANA (146)' store_desc, 3 biller_line, 146 biller_store_id, 1 jano_line, 146 jano_store_id UNION ALL
    SELECT 'EDS SIMON BOLIVAR CL 25 (134)' store_desc, 3 biller_line, 134 biller_store_id, 1 jano_line, 134 jano_store_id UNION ALL
    SELECT 'EDS SOACHA TERRENOS (160)' store_desc, 3 biller_line, 160 biller_store_id, 1 jano_line, 160 jano_store_id UNION ALL
    SELECT 'EDS TERCERA CL 34 (125)' store_desc, 3 biller_line, 125 biller_store_id, 1 jano_line, 125 jano_store_id UNION ALL
    SELECT 'EDS TINTALITO (101)' store_desc, 3 biller_line, 101 biller_store_id, 1 jano_line, 101 jano_store_id UNION ALL
    SELECT 'EDS TREBOL CL 76 (121)' store_desc, 3 biller_line, 121 biller_store_id, 1 jano_line, 121 jano_store_id UNION ALL
    SELECT 'EDS YUMBO CRA 33 (102)' store_desc, 3 biller_line, 102 biller_store_id, 1 jano_line, 102 jano_store_id UNION ALL
    SELECT 'JUMBO 20 DE JULIO (17)' store_desc, 2 biller_line, 17 biller_store_id, 1 jano_line, 17 jano_store_id UNION ALL
    SELECT 'JUMBO ALTOS DEL COUNTRY (249)' store_desc, 1 biller_line, 249 biller_store_id, 1 jano_line, 249 jano_store_id UNION ALL
    SELECT 'JUMBO ALTOS DEL PRADO (43)' store_desc, 1 biller_line, 43 biller_store_id, 1 jano_line, 43 jano_store_id UNION ALL
    SELECT 'JUMBO AMERICANO (86)' store_desc, 1 biller_line, 86 biller_store_id, 1 jano_line, 86 jano_store_id UNION ALL
    SELECT 'JUMBO ATLANTIS (194)' store_desc, 1 biller_line, 194 biller_store_id, 1 jano_line, 194 jano_store_id UNION ALL
    SELECT 'JUMBO ATRIO (253)' store_desc, 1 biller_line, 253 biller_store_id, 1 jano_line, 253 jano_store_id UNION ALL
    SELECT 'JUMBO AUTOPISTA SUR (18)' store_desc, 1 biller_line, 18 biller_store_id, 1 jano_line, 18 jano_store_id UNION ALL
    SELECT 'JUMBO BUENAVISTA (88)' store_desc, 1 biller_line, 88 biller_store_id, 1 jano_line, 88 jano_store_id UNION ALL
    SELECT 'JUMBO BULEVAR NIZA (3)' store_desc, 1 biller_line, 3 biller_store_id, 1 jano_line, 3 jano_store_id UNION ALL
    SELECT 'JUMBO CABECERA (103)' store_desc, 1 biller_line, 103 biller_store_id, 1 jano_line, 103 jano_store_id UNION ALL
    SELECT 'JUMBO CALLE 170 (14)' store_desc, 1 biller_line, 14 biller_store_id, 1 jano_line, 14 jano_store_id UNION ALL
    SELECT 'JUMBO CALLE 80 (11)' store_desc, 1 biller_line, 11 biller_store_id, 1 jano_line, 11 jano_store_id UNION ALL
    SELECT 'JUMBO CANAVERALL (108)' store_desc, 1 biller_line, 108 biller_store_id, 1 jano_line, 108 jano_store_id UNION ALL
    SELECT 'JUMBO CARIBE PLAZA (83)' store_desc, 1 biller_line, 83 biller_store_id, 1 jano_line, 83 jano_store_id UNION ALL
    SELECT 'JUMBO CARRERA 30 (16)' store_desc, 1 biller_line, 16 biller_store_id, 1 jano_line, 16 jano_store_id UNION ALL
    SELECT 'JUMBO CHIA (44)' store_desc, 1 biller_line, 44 biller_store_id, 1 jano_line, 44 jano_store_id UNION ALL
    SELECT 'JUMBO CHIA BAZAR (191)' store_desc, 1 biller_line, 191 biller_store_id, 1 jano_line, 191 jano_store_id UNION ALL
    SELECT 'JUMBO CHIPICHAPE (31)' store_desc, 1 biller_line, 31 biller_store_id, 1 jano_line, 31 jano_store_id UNION ALL
    SELECT 'JUMBO DE LA 65 (23)' store_desc, 1 biller_line, 23 biller_store_id, 1 jano_line, 23 jano_store_id UNION ALL
    SELECT 'JUMBO EL CASTILLO (174)' store_desc, 1 biller_line, 174 biller_store_id, 1 jano_line, 174 jano_store_id UNION ALL
    SELECT 'JUMBO GIRARDOT (47)' store_desc, 1 biller_line, 47 biller_store_id, 1 jano_line, 47 jano_store_id UNION ALL
    SELECT 'JUMBO GUATAPURI (87)' store_desc, 1 biller_line, 87 biller_store_id, 1 jano_line, 87 jano_store_id UNION ALL
    SELECT 'JUMBO HAYUELOS (15)' store_desc, 1 biller_line, 15 biller_store_id, 1 jano_line, 15 jano_store_id UNION ALL
    SELECT 'JUMBO LAS VEGAS (22)' store_desc, 1 biller_line, 22 biller_store_id, 1 jano_line, 22 jano_store_id UNION ALL
    SELECT 'JUMBO LIMONAR PREMIER (30)' store_desc, 1 biller_line, 30 biller_store_id, 1 jano_line, 30 jano_store_id UNION ALL
    SELECT 'JUMBO MEGA MALL (84)' store_desc, 1 biller_line, 84 biller_store_id, 1 jano_line, 84 jano_store_id UNION ALL
    SELECT 'JUMBO PASTO (128)' store_desc, 2 biller_line, 128 biller_store_id, 1 jano_line, 128 jano_store_id UNION ALL
    SELECT 'JUMBO PLAZA CENTRAL (193)' store_desc, 1 biller_line, 193 biller_store_id, 1 jano_line, 193 jano_store_id UNION ALL
    SELECT 'JUMBO POPAYAN (36)' store_desc, 1 biller_line, 36 biller_store_id, 1 jano_line, 36 jano_store_id UNION ALL
    SELECT 'JUMBO PREMIUM PLAZA (24)' store_desc, 1 biller_line, 24 biller_store_id, 1 jano_line, 24 jano_store_id UNION ALL
    SELECT 'JUMBO RIONEGRO (27)' store_desc, 1 biller_line, 27 biller_store_id, 1 jano_line, 27 jano_store_id UNION ALL
    SELECT 'JUMBO SAN SILVESTRE (126)' store_desc, 1 biller_line, 126 biller_store_id, 1 jano_line, 126 jano_store_id UNION ALL
    SELECT 'JUMBO SANTA ANA (19)' store_desc, 1 biller_line, 19 biller_store_id, 1 jano_line, 19 jano_store_id UNION ALL
    SELECT 'JUMBO SANTA FE (72)' store_desc, 1 biller_line, 72 biller_store_id, 1 jano_line, 72 jano_store_id UNION ALL
    SELECT 'JUMBO SANTA MARTA (45)' store_desc, 1 biller_line, 45 biller_store_id, 1 jano_line, 45 jano_store_id UNION ALL
    SELECT 'JUMBO SANTAFE (148)' store_desc, 1 biller_line, 148 biller_store_id, 1 jano_line, 148 jano_store_id UNION ALL
    SELECT 'JUMBO SOACHA (48)' store_desc, 2 biller_line, 48 biller_store_id, 1 jano_line, 48 jano_store_id UNION ALL
    SELECT 'JUMBO SUBA (70)' store_desc, 1 biller_line, 70 biller_store_id, 1 jano_line, 70 jano_store_id UNION ALL
    SELECT 'JUMBO TITAN (167)' store_desc, 1 biller_line, 167 biller_store_id, 1 jano_line, 167 jano_store_id UNION ALL
    SELECT 'JUMBO TUNJA (76)' store_desc, 1 biller_line, 76 biller_store_id, 1 jano_line, 76 jano_store_id UNION ALL
    SELECT 'JUMBO UNICENTRO (9)' store_desc, 1 biller_line, 9 biller_store_id, 1 jano_line, 9 jano_store_id UNION ALL
    SELECT 'JUMBO VALLE DE LILI (13)' store_desc, 1 biller_line, 13 biller_store_id, 1 jano_line, 13 jano_store_id UNION ALL
    SELECT 'JUMBO YOPAL (185)' store_desc, 1 biller_line, 185 biller_store_id, 1 jano_line, 185 jano_store_id UNION ALL
    SELECT 'METRO  FATELARES (806)' store_desc, 2 biller_line, 806 biller_store_id, 1 jano_line, 806 jano_store_id UNION ALL
    SELECT 'METRO  FONTIBON (78)' store_desc, 2 biller_line, 78 biller_store_id, 1 jano_line, 78 jano_store_id UNION ALL
    SELECT 'METRO  MAYALES (184)' store_desc, 2 biller_line, 184 biller_store_id, 1 jano_line, 184 jano_store_id UNION ALL
    SELECT 'METRO ALQUERIA (124)' store_desc, 2 biller_line, 124 biller_store_id, 1 jano_line, 124 jano_store_id UNION ALL
    SELECT 'METRO ATALAYAS (82)' store_desc, 2 biller_line, 82 biller_store_id, 1 jano_line, 82 jano_store_id UNION ALL
    SELECT 'METRO BANDERAS (26)' store_desc, 2 biller_line, 26 biller_store_id, 1 jano_line, 26 jano_store_id UNION ALL
    SELECT 'METRO BELLO ANTIOQUIA (25)' store_desc, 2 biller_line, 25 biller_store_id, 1 jano_line, 25 jano_store_id UNION ALL
    SELECT 'METRO BOSA (10)' store_desc, 2 biller_line, 10 biller_store_id, 1 jano_line, 10 jano_store_id UNION ALL
    SELECT 'METRO BUGA (35)' store_desc, 2 biller_line, 35 biller_store_id, 1 jano_line, 35 jano_store_id UNION ALL
    SELECT 'METRO CARTAGO (33)' store_desc, 2 biller_line, 33 biller_store_id, 1 jano_line, 33 jano_store_id UNION ALL
    SELECT 'METRO DOSQUEBRADAS (166)' store_desc, 2 biller_line, 166 biller_store_id, 1 jano_line, 166 jano_store_id UNION ALL
    SELECT 'METRO DUITAMA (28)' store_desc, 2 biller_line, 28 biller_store_id, 1 jano_line, 28 jano_store_id UNION ALL
    SELECT 'METRO E_COMMERCE (203)' store_desc, 3 biller_line, 203 biller_store_id, 1 jano_line, 203 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS ALHAMBRA (334)' store_desc, 2 biller_line, 334 biller_store_id, 1 jano_line, 334 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS ANT COUNTRY (305)' store_desc, 2 biller_line, 305 biller_store_id, 1 jano_line, 305 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CALLE 125 (340)' store_desc, 2 biller_line, 340 biller_store_id, 1 jano_line, 340 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CALLE 148 (329)' store_desc, 2 biller_line, 329 biller_store_id, 1 jano_line, 329 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CALLE 45 (309)' store_desc, 2 biller_line, 309 biller_store_id, 1 jano_line, 309 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CALLE 93 (324)' store_desc, 2 biller_line, 324 biller_store_id, 1 jano_line, 324 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CALLE 95 (303)' store_desc, 2 biller_line, 303 biller_store_id, 1 jano_line, 303 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CARACAS CON 49 (304)' store_desc, 2 biller_line, 304 biller_store_id, 1 jano_line, 304 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CARRERA 3 (336)' store_desc, 2 biller_line, 336 biller_store_id, 1 jano_line, 336 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CEDRITOS  CALL (301)' store_desc, 2 biller_line, 301 biller_store_id, 1 jano_line, 301 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CIUDAD SALITRE (337)' store_desc, 2 biller_line, 337 biller_store_id, 1 jano_line, 337 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS CORFERIAS (333)' store_desc, 2 biller_line, 333 biller_store_id, 1 jano_line, 333 jano_store_id UNION ALL
    SELECT 'METRO EXPRESS JAVERIANA 45 (335)' store_desc, 2 biller_line, 335 biller_store_id, 1 jano_line, 335 jano_store_id UNION ALL
    SELECT 'METRO FACATATIVA (74)' store_desc, 2 biller_line, 74 biller_store_id, 1 jano_line, 74 jano_store_id UNION ALL
    SELECT 'METRO FLORIDABLANCA (34)' store_desc, 2 biller_line, 34 biller_store_id, 1 jano_line, 34 jano_store_id UNION ALL
    SELECT 'METRO GIRON (39)' store_desc, 2 biller_line, 39 biller_store_id, 1 jano_line, 39 jano_store_id UNION ALL
    SELECT 'METRO IBAGUE (42)' store_desc, 2 biller_line, 42 biller_store_id, 1 jano_line, 42 jano_store_id UNION ALL
    SELECT 'METRO ITAGUI (46)' store_desc, 2 biller_line, 46 biller_store_id, 1 jano_line, 46 jano_store_id UNION ALL
    SELECT 'METRO LA 70 (32)' store_desc, 2 biller_line, 32 biller_store_id, 1 jano_line, 32 jano_store_id UNION ALL
    SELECT 'METRO LIBERTADORES (80)' store_desc, 2 biller_line, 80 biller_store_id, 1 jano_line, 80 jano_store_id UNION ALL
    SELECT 'METRO MONTERIA (85)' store_desc, 2 biller_line, 85 biller_store_id, 1 jano_line, 85 jano_store_id UNION ALL
    SELECT 'METRO MOSQUERA (73)' store_desc, 2 biller_line, 73 biller_store_id, 1 jano_line, 73 jano_store_id UNION ALL
    SELECT 'METRO NEIVA (38)' store_desc, 2 biller_line, 38 biller_store_id, 1 jano_line, 38 jano_store_id UNION ALL
    SELECT 'METRO PALMIRA (6)' store_desc, 2 biller_line, 6 biller_store_id, 1 jano_line, 6 jano_store_id UNION ALL
    SELECT 'METRO PASTO UNICO (171)' store_desc, 2 biller_line, 171 biller_store_id, 1 jano_line, 171 jano_store_id UNION ALL
    SELECT 'METRO PITALITO (179)' store_desc, 2 biller_line, 179 biller_store_id, 1 jano_line, 179 jano_store_id UNION ALL
    SELECT 'METRO RIOHACHA (40)' store_desc, 2 biller_line, 40 biller_store_id, 1 jano_line, 40 jano_store_id UNION ALL
    SELECT 'METRO SAN CAYETANO (79)' store_desc, 2 biller_line, 79 biller_store_id, 1 jano_line, 79 jano_store_id UNION ALL
    SELECT 'METRO SAN GIL (183)' store_desc, 2 biller_line, 183 biller_store_id, 1 jano_line, 183 jano_store_id UNION ALL
    SELECT 'METRO SANTA LUCIA (195)' store_desc, 2 biller_line, 195 biller_store_id, 1 jano_line, 195 jano_store_id UNION ALL
    SELECT 'METRO SIMON BOLIVAR (75)' store_desc, 2 biller_line, 75 biller_store_id, 1 jano_line, 75 jano_store_id UNION ALL
    SELECT 'METRO SOGAMOSO (97)' store_desc, 2 biller_line, 97 biller_store_id, 1 jano_line, 97 jano_store_id UNION ALL
    SELECT 'METRO TINTALITO (71)' store_desc, 2 biller_line, 71 biller_store_id, 1 jano_line, 71 jano_store_id UNION ALL
    SELECT 'METRO VENTURA (81)' store_desc, 2 biller_line, 81 biller_store_id, 1 jano_line, 81 jano_store_id UNION ALL
    SELECT 'METRO ZIPAQUIRA (77)' store_desc, 2 biller_line, 77 biller_store_id, 1 jano_line, 77 jano_store_id
)
SELECT
    es.store_desc AS store_name,
    invoice.num_doc_fact_elec AS doc_num,
    invoice.c_origen AS doc_type,
    es.jano_line AS line,
    es.biller_store_id AS store,
    invoice.nro_terminal AS pos,
    invoice.cod_prov AS trx,
    invoice.fec_fact::DATE AS billed_at,
    invoicee.f_envio_dian::DATE AS sent_at,
    STRING_AGG(memo.num_doc_fact_elec, '|') AS memo_lts,
    CASE
        WHEN invoice.c_origen IN (4, 9) THEN invoice.vt_factura::INT8 *-1
        ELSE invoice.vt_factura::INT8
    END AS amount,
    SUM(
        CASE
            WHEN memo.c_origen IN (4, 9) THEN memo.vt_factura::INT8 * -1
            ELSE memo.vt_factura::INT8
        END
    ) AS memo_amouts,
    (invoice.vt_factura::INT8 + SUM(
        CASE
            WHEN memo.c_origen IN (4, 9) THEN memo.vt_factura::INT8 * -1
            ELSE memo.vt_factura::INT8
        END
    )) AS sub_total,
    invoice.nit_prov AS customer,
    invoice.factura_duplicada AS duplicated,
    invoicee.estado_dian AS status,
    invoicee.log_errores_dian AS log_dian,
    invoicee.cufe AS uuid
FROM
    equivalent_store AS es
        INNER JOIN factura.encabezado_factura AS invoice ON invoice.codigo_tienda = es.biller_store_id
        INNER JOIN factura.estado_fact_electronica AS invoicee ON invoice.encfactura_col_id = invoicee.encfactura_id
        INNER JOIN factura.encabezado_factura AS memo ON invoice.num_doc_fact_elec = memo.nro_factura_afec
        INNER JOIN factura.estado_fact_electronica AS memoe ON memoe.encfactura_id = memo.encfactura_col_id
WHERE
    invoice.fec_fact::DATE BETWEEN :start_date AND :end_date
  AND invoicee.estado_dian = 'OK'
  AND memoe.estado_dian = 'OK'
  AND invoice.c_origen IN (5, 8, 13)
  AND memo.c_origen IN (4, 9, 15)
  AND invoice.factura_duplicada IS NULL
  AND memo.factura_duplicada IS NULL
GROUP BY
    es.store_desc,
    invoice.num_doc_fact_elec,
    invoice.c_origen,
    es.jano_line,
    es.biller_store_id,
    invoice.nro_terminal,
    invoice.cod_prov,
    invoice.fec_fact::DATE,
    invoicee.f_envio_dian::DATE,
    invoice.vt_factura,
    invoice.nit_prov,
    invoice.factura_duplicada,
    invoicee.estado_dian,
    invoicee.log_errores_dian,
    invoicee.cufe
HAVING
    (invoice.vt_factura::INT8 + SUM(
        CASE
            WHEN memo.c_origen IN (4, 9) THEN memo.vt_factura::INT8 * -1
            ELSE memo.vt_factura::INT8
        END
    )) < -4"""
)


JANO_QUERY_PARTNER_BY_ATTRIBUTES = text(
    """WITH equivalent_store AS (
    SELECT 'CABECERA (322)' store_desc, 2 biller_line, 322 biller_store_id, 1 jano_line, 322 jano_store_id FROM DUAL UNION ALL
    SELECT 'E_COMMERCE (204)' store_desc, 3 biller_line, 204 biller_store_id, 1 jano_line, 204 jano_store_id FROM DUAL UNION ALL
    SELECT 'EASY 4SUR MEDELLIN (3814)' store_desc, 4 biller_line, 3814 biller_store_id, 2 jano_line, 814 jano_store_id FROM DUAL UNION ALL
    SELECT 'EASY ALTOS DEL PRADO (3043)' store_desc, 4 biller_line, 3043 biller_store_id, 2 jano_line, 43 jano_store_id FROM DUAL UNION ALL
    SELECT 'EASY AMERICAS (5)' store_desc, 4 biller_line, 5 biller_store_id, 2 jano_line, 802 jano_store_id FROM DUAL UNION ALL
    SELECT 'EASY AUTOSUR (1006)' store_desc, 4 biller_line, 1006 biller_store_id, 2 jano_line, 18 jano_store_id FROM DUAL UNION ALL
    SELECT 'EASY BOSA (1002)' store_desc, 4 biller_line, 1002 biller_store_id, 2 jano_line, 10 jano_store_id FROM DUAL UNION ALL
    SELECT 'EASY CALLE 13 (3811)' store_desc, 4 biller_line, 3811 biller_store_id, 2 jano_line, 811 jano_store_id FROM DUAL UNION ALL
    SELECT 'EASY CALLE 170 (3014)' store_desc, 4 biller_line, 3014 biller_store_id, 2 jano_line, 14 jano_store_id FROM DUAL UNION ALL
    SELECT 'EASY CALLE 80 (1004)' store_desc, 4 biller_line, 1004 biller_store_id, 2 jano_line, 11 jano_store_id FROM DUAL UNION ALL
    SELECT 'EASY CENTRO MAYOR (3805)' store_desc, 4 biller_line, 3805 biller_store_id, 2 jano_line, 805 jano_store_id FROM DUAL UNION ALL
    SELECT 'EASY GAITAN CORTES (3813)' store_desc, 4 biller_line, 3813 biller_store_id, 2 jano_line, 813 jano_store_id FROM DUAL UNION ALL
    SELECT 'EASY HAYUELOS (12)' store_desc, 4 biller_line, 12 biller_store_id, 2 jano_line, 15 jano_store_id FROM DUAL UNION ALL
    SELECT 'EASY MAYALES VALLEDUPAR (3809)' store_desc, 4 biller_line, 3809 biller_store_id, 2 jano_line, 809 jano_store_id FROM DUAL UNION ALL
    SELECT 'EASY NORTE (3801)' store_desc, 4 biller_line, 3801 biller_store_id, 2 jano_line, 801 jano_store_id FROM DUAL UNION ALL
    SELECT 'EASY OCCIDENTE (3800)' store_desc, 4 biller_line, 3800 biller_store_id, 2 jano_line, 800 jano_store_id FROM DUAL UNION ALL
    SELECT 'EASY PRADO MEDELLIN (3806)' store_desc, 4 biller_line, 3806 biller_store_id, 2 jano_line, 806 jano_store_id FROM DUAL UNION ALL
    SELECT 'EASY SOACHA (3812)' store_desc, 4 biller_line, 3812 biller_store_id, 2 jano_line, 812 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS 1 MAYO (8)' store_desc, 3 biller_line, 8 biller_store_id, 1 jano_line, 8 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS ALAMOS (139)' store_desc, 3 biller_line, 139 biller_store_id, 1 jano_line, 139 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS ALQUERIA (149)' store_desc, 3 biller_line, 149 biller_store_id, 1 jano_line, 149 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS APOLO AUTOSUR (21)' store_desc, 3 biller_line, 21 biller_store_id, 1 jano_line, 21 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS AV DEL RIO (98)' store_desc, 3 biller_line, 98 biller_store_id, 1 jano_line, 98 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS AVD CIUDAD CALI (145)' store_desc, 3 biller_line, 145 biller_store_id, 1 jano_line, 145 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS BAHIA CL 46 (136)' store_desc, 3 biller_line, 136 biller_store_id, 1 jano_line, 136 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS CALLE 190 (161)' store_desc, 3 biller_line, 161 biller_store_id, 1 jano_line, 161 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS COLOMBIA CALLE 50 (7)' store_desc, 3 biller_line, 7 biller_store_id, 1 jano_line, 7 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS IVESUR CIRCUN (123)' store_desc, 3 biller_line, 123 biller_store_id, 1 jano_line, 123 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS LA 17 CRA 17 (159)' store_desc, 3 biller_line, 159 biller_store_id, 1 jano_line, 159 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS LA 24 CL 5B (132)' store_desc, 3 biller_line, 132 biller_store_id, 1 jano_line, 132 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS LA 42 CL 34B (158)' store_desc, 3 biller_line, 158 biller_store_id, 1 jano_line, 158 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS MARRUECOS (2)' store_desc, 3 biller_line, 2 biller_store_id, 1 jano_line, 2 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS SAN GABRIEL AERO (133)' store_desc, 3 biller_line, 133 biller_store_id, 1 jano_line, 133 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS SEVILLANA (146)' store_desc, 3 biller_line, 146 biller_store_id, 1 jano_line, 146 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS SIMON BOLIVAR CL 25 (134)' store_desc, 3 biller_line, 134 biller_store_id, 1 jano_line, 134 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS SOACHA TERRENOS (160)' store_desc, 3 biller_line, 160 biller_store_id, 1 jano_line, 160 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS TERCERA CL 34 (125)' store_desc, 3 biller_line, 125 biller_store_id, 1 jano_line, 125 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS TINTALITO (101)' store_desc, 3 biller_line, 101 biller_store_id, 1 jano_line, 101 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS TREBOL CL 76 (121)' store_desc, 3 biller_line, 121 biller_store_id, 1 jano_line, 121 jano_store_id FROM DUAL UNION ALL
    SELECT 'EDS YUMBO CRA 33 (102)' store_desc, 3 biller_line, 102 biller_store_id, 1 jano_line, 102 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO 20 DE JULIO (17)' store_desc, 2 biller_line, 17 biller_store_id, 1 jano_line, 17 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO ALTOS DEL COUNTRY (249)' store_desc, 1 biller_line, 249 biller_store_id, 1 jano_line, 249 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO ALTOS DEL PRADO (43)' store_desc, 1 biller_line, 43 biller_store_id, 1 jano_line, 43 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO AMERICANO (86)' store_desc, 1 biller_line, 86 biller_store_id, 1 jano_line, 86 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO ATLANTIS (194)' store_desc, 1 biller_line, 194 biller_store_id, 1 jano_line, 194 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO ATRIO (253)' store_desc, 1 biller_line, 253 biller_store_id, 1 jano_line, 253 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO AUTOPISTA SUR (18)' store_desc, 1 biller_line, 18 biller_store_id, 1 jano_line, 18 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO BUENAVISTA (88)' store_desc, 1 biller_line, 88 biller_store_id, 1 jano_line, 88 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO BULEVAR NIZA (3)' store_desc, 1 biller_line, 3 biller_store_id, 1 jano_line, 3 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO CABECERA (103)' store_desc, 1 biller_line, 103 biller_store_id, 1 jano_line, 103 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO CALLE 170 (14)' store_desc, 1 biller_line, 14 biller_store_id, 1 jano_line, 14 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO CALLE 80 (11)' store_desc, 1 biller_line, 11 biller_store_id, 1 jano_line, 11 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO CANAVERALL (108)' store_desc, 1 biller_line, 108 biller_store_id, 1 jano_line, 108 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO CARIBE PLAZA (83)' store_desc, 1 biller_line, 83 biller_store_id, 1 jano_line, 83 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO CARRERA 30 (16)' store_desc, 1 biller_line, 16 biller_store_id, 1 jano_line, 16 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO CHIA (44)' store_desc, 1 biller_line, 44 biller_store_id, 1 jano_line, 44 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO CHIA BAZAR (191)' store_desc, 1 biller_line, 191 biller_store_id, 1 jano_line, 191 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO CHIPICHAPE (31)' store_desc, 1 biller_line, 31 biller_store_id, 1 jano_line, 31 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO DE LA 65 (23)' store_desc, 1 biller_line, 23 biller_store_id, 1 jano_line, 23 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO EL CASTILLO (174)' store_desc, 1 biller_line, 174 biller_store_id, 1 jano_line, 174 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO GIRARDOT (47)' store_desc, 1 biller_line, 47 biller_store_id, 1 jano_line, 47 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO GUATAPURI (87)' store_desc, 1 biller_line, 87 biller_store_id, 1 jano_line, 87 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO HAYUELOS (15)' store_desc, 1 biller_line, 15 biller_store_id, 1 jano_line, 15 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO LAS VEGAS (22)' store_desc, 1 biller_line, 22 biller_store_id, 1 jano_line, 22 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO LIMONAR PREMIER (30)' store_desc, 1 biller_line, 30 biller_store_id, 1 jano_line, 30 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO MEGA MALL (84)' store_desc, 1 biller_line, 84 biller_store_id, 1 jano_line, 84 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO PASTO (128)' store_desc, 2 biller_line, 128 biller_store_id, 1 jano_line, 128 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO PLAZA CENTRAL (193)' store_desc, 1 biller_line, 193 biller_store_id, 1 jano_line, 193 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO POPAYAN (36)' store_desc, 1 biller_line, 36 biller_store_id, 1 jano_line, 36 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO PREMIUM PLAZA (24)' store_desc, 1 biller_line, 24 biller_store_id, 1 jano_line, 24 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO RIONEGRO (27)' store_desc, 1 biller_line, 27 biller_store_id, 1 jano_line, 27 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO SAN SILVESTRE (126)' store_desc, 1 biller_line, 126 biller_store_id, 1 jano_line, 126 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO SANTA ANA (19)' store_desc, 1 biller_line, 19 biller_store_id, 1 jano_line, 19 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO SANTA FE (72)' store_desc, 1 biller_line, 72 biller_store_id, 1 jano_line, 72 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO SANTA MARTA (45)' store_desc, 1 biller_line, 45 biller_store_id, 1 jano_line, 45 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO SANTAFE (148)' store_desc, 1 biller_line, 148 biller_store_id, 1 jano_line, 148 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO SOACHA (48)' store_desc, 2 biller_line, 48 biller_store_id, 1 jano_line, 48 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO SUBA (70)' store_desc, 1 biller_line, 70 biller_store_id, 1 jano_line, 70 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO TITAN (167)' store_desc, 1 biller_line, 167 biller_store_id, 1 jano_line, 167 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO TUNJA (76)' store_desc, 1 biller_line, 76 biller_store_id, 1 jano_line, 76 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO UNICENTRO (9)' store_desc, 1 biller_line, 9 biller_store_id, 1 jano_line, 9 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO VALLE DE LILI (13)' store_desc, 1 biller_line, 13 biller_store_id, 1 jano_line, 13 jano_store_id FROM DUAL UNION ALL
    SELECT 'JUMBO YOPAL (185)' store_desc, 1 biller_line, 185 biller_store_id, 1 jano_line, 185 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO  FATELARES (806)' store_desc, 2 biller_line, 806 biller_store_id, 1 jano_line, 806 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO  FONTIBON (78)' store_desc, 2 biller_line, 78 biller_store_id, 1 jano_line, 78 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO  MAYALES (184)' store_desc, 2 biller_line, 184 biller_store_id, 1 jano_line, 184 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO ALQUERIA (124)' store_desc, 2 biller_line, 124 biller_store_id, 1 jano_line, 124 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO ATALAYAS (82)' store_desc, 2 biller_line, 82 biller_store_id, 1 jano_line, 82 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO BANDERAS (26)' store_desc, 2 biller_line, 26 biller_store_id, 1 jano_line, 26 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO BELLO ANTIOQUIA (25)' store_desc, 2 biller_line, 25 biller_store_id, 1 jano_line, 25 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO BOSA (10)' store_desc, 2 biller_line, 10 biller_store_id, 1 jano_line, 10 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO BUGA (35)' store_desc, 2 biller_line, 35 biller_store_id, 1 jano_line, 35 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO CARTAGO (33)' store_desc, 2 biller_line, 33 biller_store_id, 1 jano_line, 33 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO DOSQUEBRADAS (166)' store_desc, 2 biller_line, 166 biller_store_id, 1 jano_line, 166 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO DUITAMA (28)' store_desc, 2 biller_line, 28 biller_store_id, 1 jano_line, 28 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO E_COMMERCE (203)' store_desc, 3 biller_line, 203 biller_store_id, 1 jano_line, 203 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO EXPRESS ALHAMBRA (334)' store_desc, 2 biller_line, 334 biller_store_id, 1 jano_line, 334 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO EXPRESS ANT COUNTRY (305)' store_desc, 2 biller_line, 305 biller_store_id, 1 jano_line, 305 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO EXPRESS CALLE 125 (340)' store_desc, 2 biller_line, 340 biller_store_id, 1 jano_line, 340 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO EXPRESS CALLE 148 (329)' store_desc, 2 biller_line, 329 biller_store_id, 1 jano_line, 329 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO EXPRESS CALLE 45 (309)' store_desc, 2 biller_line, 309 biller_store_id, 1 jano_line, 309 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO EXPRESS CALLE 93 (324)' store_desc, 2 biller_line, 324 biller_store_id, 1 jano_line, 324 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO EXPRESS CALLE 95 (303)' store_desc, 2 biller_line, 303 biller_store_id, 1 jano_line, 303 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO EXPRESS CARACAS CON 49 (304)' store_desc, 2 biller_line, 304 biller_store_id, 1 jano_line, 304 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO EXPRESS CARRERA 3 (336)' store_desc, 2 biller_line, 336 biller_store_id, 1 jano_line, 336 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO EXPRESS CEDRITOS  CALL (301)' store_desc, 2 biller_line, 301 biller_store_id, 1 jano_line, 301 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO EXPRESS CIUDAD SALITRE (337)' store_desc, 2 biller_line, 337 biller_store_id, 1 jano_line, 337 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO EXPRESS CORFERIAS (333)' store_desc, 2 biller_line, 333 biller_store_id, 1 jano_line, 333 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO EXPRESS JAVERIANA 45 (335)' store_desc, 2 biller_line, 335 biller_store_id, 1 jano_line, 335 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO FACATATIVA (74)' store_desc, 2 biller_line, 74 biller_store_id, 1 jano_line, 74 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO FLORIDABLANCA (34)' store_desc, 2 biller_line, 34 biller_store_id, 1 jano_line, 34 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO GIRON (39)' store_desc, 2 biller_line, 39 biller_store_id, 1 jano_line, 39 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO IBAGUE (42)' store_desc, 2 biller_line, 42 biller_store_id, 1 jano_line, 42 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO ITAGUI (46)' store_desc, 2 biller_line, 46 biller_store_id, 1 jano_line, 46 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO LA 70 (32)' store_desc, 2 biller_line, 32 biller_store_id, 1 jano_line, 32 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO LIBERTADORES (80)' store_desc, 2 biller_line, 80 biller_store_id, 1 jano_line, 80 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO MONTERIA (85)' store_desc, 2 biller_line, 85 biller_store_id, 1 jano_line, 85 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO MOSQUERA (73)' store_desc, 2 biller_line, 73 biller_store_id, 1 jano_line, 73 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO NEIVA (38)' store_desc, 2 biller_line, 38 biller_store_id, 1 jano_line, 38 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO PALMIRA (6)' store_desc, 2 biller_line, 6 biller_store_id, 1 jano_line, 6 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO PASTO UNICO (171)' store_desc, 2 biller_line, 171 biller_store_id, 1 jano_line, 171 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO PITALITO (179)' store_desc, 2 biller_line, 179 biller_store_id, 1 jano_line, 179 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO RIOHACHA (40)' store_desc, 2 biller_line, 40 biller_store_id, 1 jano_line, 40 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO SAN CAYETANO (79)' store_desc, 2 biller_line, 79 biller_store_id, 1 jano_line, 79 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO SAN GIL (183)' store_desc, 2 biller_line, 183 biller_store_id, 1 jano_line, 183 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO SANTA LUCIA (195)' store_desc, 2 biller_line, 195 biller_store_id, 1 jano_line, 195 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO SIMON BOLIVAR (75)' store_desc, 2 biller_line, 75 biller_store_id, 1 jano_line, 75 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO SOGAMOSO (97)' store_desc, 2 biller_line, 97 biller_store_id, 1 jano_line, 97 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO TINTALITO (71)' store_desc, 2 biller_line, 71 biller_store_id, 1 jano_line, 71 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO VENTURA (81)' store_desc, 2 biller_line, 81 biller_store_id, 1 jano_line, 81 jano_store_id FROM DUAL UNION ALL
    SELECT 'METRO ZIPAQUIRA (77)' store_desc, 2 biller_line, 77 biller_store_id, 1 jano_line, 77 jano_store_id FROM DUAL
)
SELECT
    es.store_desc store_name,
    TRIM(vp.prfjo_fctra_vep) || TRIM(vp.fctra_vep) doc_num,
    es.jano_line line,
    es.biller_store_id store,
    tr.pos_tra pos,
    tr.transccion_tra trx,
    TO_CHAR(tr.fcha_crre_fdc, 'YYYY-MM-DD') billed_at,
    vp.vlor_pgdo_vep amount,
    vp.id_clie customer
FROM
    equivalent_store es
        INNER JOIN jano.trnsccion tr ON (es.jano_line = tr.id_neg AND es.jano_store_id = tr.id_tie)
        INNER JOIN jano.vntas_pos vp ON (tr.id_tra = vp.id_tra AND tr.id_neg = vp.id_neg)
WHERE
    tr.fcha_crre_fdc BETWEEN :start_date AND :end_date
  AND tr.estdo_tra IN ('A', 'E')
  AND es.jano_line = :line
  AND es.biller_store_id = :store
  AND tr.pos_tra = :pos
  AND tr.transccion_tra = :trx"""
)
