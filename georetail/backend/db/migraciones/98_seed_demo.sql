-- ============================================================
-- Archivo  : 98_seed_demo.sql
-- Proyecto : GeoRetail
-- Propósito: Seeds demo: 10 distritos, 28 barrios, ~100 zonas BCN
--            + 1 local por zona + scores_zona (zonas × sectores).
-- Orden    : Ejecutar después de 01..10_*.sql.
--            Depende de: distritos, barrios, zonas, locales, sectores,
--            scores_zona (tablas de 02_geografia.sql y 05_sectores_scoring.sql).
-- Cambios vs legacy seed_demo.sql:
--   · INSERT INTO locales: columna "disponible" → "esta_disponible"
--     (renombrada en 03_inmuebles.sql).
-- ============================================================

-- ── Distritos ──────────────────────────────────────────────────────────────────
INSERT INTO distritos (codigo, nombre) VALUES
  ('01','Ciutat Vella'),('02','Eixample'),('03','Sants-Montjuïc'),
  ('04','Les Corts'),('05','Sarrià-Sant Gervasi'),('06','Gràcia'),
  ('07','Horta-Guinardó'),('08','Nou Barris'),('09','Sant Andreu'),('10','Sant Martí')
ON CONFLICT (codigo) DO NOTHING;

-- ── Barrios ────────────────────────────────────────────────────────────────────
INSERT INTO barrios (codigo, nombre, distrito_id) VALUES
  ('01001','El Raval',          (SELECT id FROM distritos WHERE codigo='01')),
  ('01002','El Gòtic',          (SELECT id FROM distritos WHERE codigo='01')),
  ('01003','La Barceloneta',    (SELECT id FROM distritos WHERE codigo='01')),
  ('01004','El Born',           (SELECT id FROM distritos WHERE codigo='01')),
  ('02001','Esquerra Eixample', (SELECT id FROM distritos WHERE codigo='02')),
  ('02002','Dreta Eixample',    (SELECT id FROM distritos WHERE codigo='02')),
  ('02003','Sagrada Família',   (SELECT id FROM distritos WHERE codigo='02')),
  ('02004','Fort Pienc',        (SELECT id FROM distritos WHERE codigo='02')),
  ('03001','Sants',             (SELECT id FROM distritos WHERE codigo='03')),
  ('03002','Hostafrancs',       (SELECT id FROM distritos WHERE codigo='03')),
  ('03003','La Bordeta',        (SELECT id FROM distritos WHERE codigo='03')),
  ('04001','Les Corts',         (SELECT id FROM distritos WHERE codigo='04')),
  ('04002','La Maternitat',     (SELECT id FROM distritos WHERE codigo='04')),
  ('05001','Sant Gervasi',      (SELECT id FROM distritos WHERE codigo='05')),
  ('05002','Sarrià',            (SELECT id FROM distritos WHERE codigo='05')),
  ('06001','Vila de Gràcia',    (SELECT id FROM distritos WHERE codigo='06')),
  ('06002','El Camp de Gràcia', (SELECT id FROM distritos WHERE codigo='06')),
  ('06003','La Salut',          (SELECT id FROM distritos WHERE codigo='06')),
  ('07001','El Guinardó',       (SELECT id FROM distritos WHERE codigo='07')),
  ('07002','La Vall d''Hebron', (SELECT id FROM distritos WHERE codigo='07')),
  ('08001','Nou Barris Nord',   (SELECT id FROM distritos WHERE codigo='08')),
  ('08002','Prosperitat',       (SELECT id FROM distritos WHERE codigo='08')),
  ('09001','Sant Andreu',       (SELECT id FROM distritos WHERE codigo='09')),
  ('09002','La Sagrera',        (SELECT id FROM distritos WHERE codigo='09')),
  ('10001','El Poblenou',       (SELECT id FROM distritos WHERE codigo='10')),
  ('10002','El Clot',           (SELECT id FROM distritos WHERE codigo='10')),
  ('10003','El Parc',           (SELECT id FROM distritos WHERE codigo='10')),
  ('10004','La Vila Olímpica',  (SELECT id FROM distritos WHERE codigo='10'))
ON CONFLICT (codigo) DO NOTHING;

-- ── Zonas (~100) ───────────────────────────────────────────────────────────────
-- Cada zona es un polígono de ~180m × 180m (aprox 0.0015° lat/lng)
-- Generamos una cuadrícula de zonas por barrio

INSERT INTO zonas (id, nombre, barrio_id, geometria) VALUES
-- EL RAVAL
('bcn_raval_01','Rambla del Raval',(SELECT id FROM barrios WHERE codigo='01001'),ST_GeomFromText('POLYGON((2.1680 41.3790,2.1695 41.3790,2.1695 41.3805,2.1680 41.3805,2.1680 41.3790))',4326)),
('bcn_raval_02','C/ Hospital',(SELECT id FROM barrios WHERE codigo='01001'),ST_GeomFromText('POLYGON((2.1700 41.3790,2.1715 41.3790,2.1715 41.3805,2.1700 41.3805,2.1700 41.3790))',4326)),
('bcn_raval_03','C/ Robadors',(SELECT id FROM barrios WHERE codigo='01001'),ST_GeomFromText('POLYGON((2.1660 41.3805,2.1675 41.3805,2.1675 41.3820,2.1660 41.3820,2.1660 41.3805))',4326)),
('bcn_raval_04','Mercat Boqueria',(SELECT id FROM barrios WHERE codigo='01001'),ST_GeomFromText('POLYGON((2.1725 41.3805,2.1740 41.3805,2.1740 41.3820,2.1725 41.3820,2.1725 41.3805))',4326)),
('bcn_raval_05','C/ Tallers',(SELECT id FROM barrios WHERE codigo='01001'),ST_GeomFromText('POLYGON((2.1680 41.3820,2.1695 41.3820,2.1695 41.3835,2.1680 41.3835,2.1680 41.3820))',4326)),
('bcn_raval_06','Pl. Castella',(SELECT id FROM barrios WHERE codigo='01001'),ST_GeomFromText('POLYGON((2.1640 41.3790,2.1655 41.3790,2.1655 41.3805,2.1640 41.3805,2.1640 41.3790))',4326)),
-- EL GÒTIC
('bcn_gotic_01','Portal de l''Àngel',(SELECT id FROM barrios WHERE codigo='01002'),ST_GeomFromText('POLYGON((2.1740 41.3820,2.1755 41.3820,2.1755 41.3835,2.1740 41.3835,2.1740 41.3820))',4326)),
('bcn_gotic_02','C/ Ferran',(SELECT id FROM barrios WHERE codigo='01002'),ST_GeomFromText('POLYGON((2.1760 41.3810,2.1775 41.3810,2.1775 41.3825,2.1760 41.3825,2.1760 41.3810))',4326)),
('bcn_gotic_03','Pl. Reial',(SELECT id FROM barrios WHERE codigo='01002'),ST_GeomFromText('POLYGON((2.1760 41.3790,2.1775 41.3790,2.1775 41.3805,2.1760 41.3805,2.1760 41.3790))',4326)),
('bcn_gotic_04','C/ Jaume I',(SELECT id FROM barrios WHERE codigo='01002'),ST_GeomFromText('POLYGON((2.1775 41.3820,2.1790 41.3820,2.1790 41.3835,2.1775 41.3835,2.1775 41.3820))',4326)),
('bcn_gotic_05','Pl. Sant Jaume',(SELECT id FROM barrios WHERE codigo='01002'),ST_GeomFromText('POLYGON((2.1765 41.3830,2.1780 41.3830,2.1780 41.3845,2.1765 41.3845,2.1765 41.3830))',4326)),
-- BARCELONETA
('bcn_barceloneta_01','Pg. Joan de Borbó',(SELECT id FROM barrios WHERE codigo='01003'),ST_GeomFromText('POLYGON((2.1880 41.3770,2.1895 41.3770,2.1895 41.3785,2.1880 41.3785,2.1880 41.3770))',4326)),
('bcn_barceloneta_02','Platja Barceloneta',(SELECT id FROM barrios WHERE codigo='01003'),ST_GeomFromText('POLYGON((2.1910 41.3760,2.1925 41.3760,2.1925 41.3775,2.1910 41.3775,2.1910 41.3760))',4326)),
('bcn_barceloneta_03','C/ Marina',(SELECT id FROM barrios WHERE codigo='01003'),ST_GeomFromText('POLYGON((2.1860 41.3785,2.1875 41.3785,2.1875 41.3800,2.1860 41.3800,2.1860 41.3785))',4326)),
-- EL BORN
('bcn_born_01','Pg. del Born',(SELECT id FROM barrios WHERE codigo='01004'),ST_GeomFromText('POLYGON((2.1810 41.3835,2.1825 41.3835,2.1825 41.3850,2.1810 41.3850,2.1810 41.3835))',4326)),
('bcn_born_02','C/ Montcada',(SELECT id FROM barrios WHERE codigo='01004'),ST_GeomFromText('POLYGON((2.1790 41.3840,2.1805 41.3840,2.1805 41.3855,2.1790 41.3855,2.1790 41.3840))',4326)),
('bcn_born_03','C/ Rec',(SELECT id FROM barrios WHERE codigo='01004'),ST_GeomFromText('POLYGON((2.1825 41.3845,2.1840 41.3845,2.1840 41.3860,2.1825 41.3860,2.1825 41.3845))',4326)),
('bcn_born_04','Pl. Comercial',(SELECT id FROM barrios WHERE codigo='01004'),ST_GeomFromText('POLYGON((2.1840 41.3835,2.1855 41.3835,2.1855 41.3850,2.1840 41.3850,2.1840 41.3835))',4326)),
('bcn_born_05','Arc de Triomf',(SELECT id FROM barrios WHERE codigo='01004'),ST_GeomFromText('POLYGON((2.1800 41.3870,2.1815 41.3870,2.1815 41.3885,2.1800 41.3885,2.1800 41.3870))',4326)),
-- EIXAMPLE ESQUERRA
('bcn_esqeix_01','C/ Muntaner',(SELECT id FROM barrios WHERE codigo='02001'),ST_GeomFromText('POLYGON((2.1520 41.3880,2.1535 41.3880,2.1535 41.3895,2.1520 41.3895,2.1520 41.3880))',4326)),
('bcn_esqeix_02','C/ Consell de Cent',(SELECT id FROM barrios WHERE codigo='02001'),ST_GeomFromText('POLYGON((2.1540 41.3890,2.1555 41.3890,2.1555 41.3905,2.1540 41.3905,2.1540 41.3890))',4326)),
('bcn_esqeix_03','C/ Enrique Granados',(SELECT id FROM barrios WHERE codigo='02001'),ST_GeomFromText('POLYGON((2.1560 41.3900,2.1575 41.3900,2.1575 41.3915,2.1560 41.3915,2.1560 41.3900))',4326)),
('bcn_esqeix_04','Gran Via Esquerra',(SELECT id FROM barrios WHERE codigo='02001'),ST_GeomFromText('POLYGON((2.1520 41.3860,2.1535 41.3860,2.1535 41.3875,2.1520 41.3875,2.1520 41.3860))',4326)),
('bcn_esqeix_05','C/ Urgell',(SELECT id FROM barrios WHERE codigo='02001'),ST_GeomFromText('POLYGON((2.1500 41.3870,2.1515 41.3870,2.1515 41.3885,2.1500 41.3885,2.1500 41.3870))',4326)),
('bcn_esqeix_06','C/ Viladomat',(SELECT id FROM barrios WHERE codigo='02001'),ST_GeomFromText('POLYGON((2.1490 41.3890,2.1505 41.3890,2.1505 41.3905,2.1490 41.3905,2.1490 41.3890))',4326)),
('bcn_esqeix_07','C/ Comte d''Urgell',(SELECT id FROM barrios WHERE codigo='02001'),ST_GeomFromText('POLYGON((2.1510 41.3910,2.1525 41.3910,2.1525 41.3925,2.1510 41.3925,2.1510 41.3910))',4326)),
('bcn_esqeix_08','Ronda Sant Antoni',(SELECT id FROM barrios WHERE codigo='02001'),ST_GeomFromText('POLYGON((2.1560 41.3855,2.1575 41.3855,2.1575 41.3870,2.1560 41.3870,2.1560 41.3855))',4326)),
-- EIXAMPLE DRETA
('bcn_dreix_01','Pg. de Gràcia',(SELECT id FROM barrios WHERE codigo='02002'),ST_GeomFromText('POLYGON((2.1635 41.3920,2.1650 41.3920,2.1650 41.3935,2.1635 41.3935,2.1635 41.3920))',4326)),
('bcn_dreix_02','C/ Provença',(SELECT id FROM barrios WHERE codigo='02002'),ST_GeomFromText('POLYGON((2.1620 41.3940,2.1635 41.3940,2.1635 41.3955,2.1620 41.3955,2.1620 41.3940))',4326)),
('bcn_dreix_03','C/ Diagonal Dreta',(SELECT id FROM barrios WHERE codigo='02002'),ST_GeomFromText('POLYGON((2.1650 41.3960,2.1665 41.3960,2.1665 41.3975,2.1650 41.3975,2.1650 41.3960))',4326)),
('bcn_dreix_04','C/ Mallorca',(SELECT id FROM barrios WHERE codigo='02002'),ST_GeomFromText('POLYGON((2.1600 41.3910,2.1615 41.3910,2.1615 41.3925,2.1600 41.3925,2.1600 41.3910))',4326)),
('bcn_dreix_05','Rambla Catalunya',(SELECT id FROM barrios WHERE codigo='02002'),ST_GeomFromText('POLYGON((2.1630 41.3900,2.1645 41.3900,2.1645 41.3915,2.1630 41.3915,2.1630 41.3900))',4326)),
('bcn_dreix_06','C/ Pau Claris',(SELECT id FROM barrios WHERE codigo='02002'),ST_GeomFromText('POLYGON((2.1660 41.3905,2.1675 41.3905,2.1675 41.3920,2.1660 41.3920,2.1660 41.3905))',4326)),
('bcn_dreix_07','C/ Girona',(SELECT id FROM barrios WHERE codigo='02002'),ST_GeomFromText('POLYGON((2.1680 41.3930,2.1695 41.3930,2.1695 41.3945,2.1680 41.3945,2.1680 41.3930))',4326)),
('bcn_dreix_08','C/ Bruc',(SELECT id FROM barrios WHERE codigo='02002'),ST_GeomFromText('POLYGON((2.1695 41.3910,2.1710 41.3910,2.1710 41.3925,2.1695 41.3925,2.1695 41.3910))',4326)),
-- SAGRADA FAMÍLIA
('bcn_sagfam_01','Pl. Sagrada Família',(SELECT id FROM barrios WHERE codigo='02003'),ST_GeomFromText('POLYGON((2.1740 41.4030,2.1755 41.4030,2.1755 41.4045,2.1740 41.4045,2.1740 41.4030))',4326)),
('bcn_sagfam_02','C/ Provença SF',(SELECT id FROM barrios WHERE codigo='02003'),ST_GeomFromText('POLYGON((2.1720 41.4010,2.1735 41.4010,2.1735 41.4025,2.1720 41.4025,2.1720 41.4010))',4326)),
('bcn_sagfam_03','C/ Mallorca SF',(SELECT id FROM barrios WHERE codigo='02003'),ST_GeomFromText('POLYGON((2.1760 41.4010,2.1775 41.4010,2.1775 41.4025,2.1760 41.4025,2.1760 41.4010))',4326)),
('bcn_sagfam_04','C/ Valencia SF',(SELECT id FROM barrios WHERE codigo='02003'),ST_GeomFromText('POLYGON((2.1700 41.3990,2.1715 41.3990,2.1715 41.4005,2.1700 41.4005,2.1700 41.3990))',4326)),
-- FORT PIENC
('bcn_fortpienc_01','C/ Castillejos',(SELECT id FROM barrios WHERE codigo='02004'),ST_GeomFromText('POLYGON((2.1800 41.3970,2.1815 41.3970,2.1815 41.3985,2.1800 41.3985,2.1800 41.3970))',4326)),
('bcn_fortpienc_02','C/ Ribes',(SELECT id FROM barrios WHERE codigo='02004'),ST_GeomFromText('POLYGON((2.1820 41.3955,2.1835 41.3955,2.1835 41.3970,2.1820 41.3970,2.1820 41.3955))',4326)),
-- SANTS
('bcn_sants_01','C/ Sants',(SELECT id FROM barrios WHERE codigo='03001'),ST_GeomFromText('POLYGON((2.1370 41.3760,2.1385 41.3760,2.1385 41.3775,2.1370 41.3775,2.1370 41.3760))',4326)),
('bcn_sants_02','Mercat Sants',(SELECT id FROM barrios WHERE codigo='03001'),ST_GeomFromText('POLYGON((2.1350 41.3770,2.1365 41.3770,2.1365 41.3785,2.1350 41.3785,2.1350 41.3770))',4326)),
('bcn_sants_03','C/ Creu Coberta',(SELECT id FROM barrios WHERE codigo='03001'),ST_GeomFromText('POLYGON((2.1330 41.3750,2.1345 41.3750,2.1345 41.3765,2.1330 41.3765,2.1330 41.3750))',4326)),
('bcn_sants_04','Pl. Espanya',(SELECT id FROM barrios WHERE codigo='03001'),ST_GeomFromText('POLYGON((2.1490 41.3740,2.1505 41.3740,2.1505 41.3755,2.1490 41.3755,2.1490 41.3740))',4326)),
-- HOSTAFRANCS
('bcn_hostafrancs_01','C/ Tarragona',(SELECT id FROM barrios WHERE codigo='03002'),ST_GeomFromText('POLYGON((2.1500 41.3760,2.1515 41.3760,2.1515 41.3775,2.1500 41.3775,2.1500 41.3760))',4326)),
('bcn_hostafrancs_02','C/ Parlàment',(SELECT id FROM barrios WHERE codigo='03002'),ST_GeomFromText('POLYGON((2.1520 41.3775,2.1535 41.3775,2.1535 41.3790,2.1520 41.3790,2.1520 41.3775))',4326)),
-- LES CORTS
('bcn_corts_01','C/ Entença',(SELECT id FROM barrios WHERE codigo='04001'),ST_GeomFromText('POLYGON((2.1420 41.3870,2.1435 41.3870,2.1435 41.3885,2.1420 41.3885,2.1420 41.3870))',4326)),
('bcn_corts_02','C/ Diagonal Corts',(SELECT id FROM barrios WHERE codigo='04001'),ST_GeomFromText('POLYGON((2.1400 41.3890,2.1415 41.3890,2.1415 41.3905,2.1400 41.3905,2.1400 41.3890))',4326)),
('bcn_corts_03','C/ Joan Güell',(SELECT id FROM barrios WHERE codigo='04001'),ST_GeomFromText('POLYGON((2.1380 41.3850,2.1395 41.3850,2.1395 41.3865,2.1380 41.3865,2.1380 41.3850))',4326)),
-- SANT GERVASI
('bcn_sgervasi_01','C/ Muntaner SG',(SELECT id FROM barrios WHERE codigo='05001'),ST_GeomFromText('POLYGON((2.1360 41.4050,2.1375 41.4050,2.1375 41.4065,2.1360 41.4065,2.1360 41.4050))',4326)),
('bcn_sgervasi_02','Pl. Bonanova',(SELECT id FROM barrios WHERE codigo='05001'),ST_GeomFromText('POLYGON((2.1320 41.4080,2.1335 41.4080,2.1335 41.4095,2.1320 41.4095,2.1320 41.4080))',4326)),
('bcn_sgervasi_03','C/ Balmes SG',(SELECT id FROM barrios WHERE codigo='05001'),ST_GeomFromText('POLYGON((2.1380 41.4030,2.1395 41.4030,2.1395 41.4045,2.1380 41.4045,2.1380 41.4030))',4326)),
-- SARRIÀ
('bcn_sarria_01','C/ Major de Sarrià',(SELECT id FROM barrios WHERE codigo='05002'),ST_GeomFromText('POLYGON((2.1210 41.4050,2.1225 41.4050,2.1225 41.4065,2.1210 41.4065,2.1210 41.4050))',4326)),
('bcn_sarria_02','Pg. Reina Elisenda',(SELECT id FROM barrios WHERE codigo='05002'),ST_GeomFromText('POLYGON((2.1190 41.4060,2.1205 41.4060,2.1205 41.4075,2.1190 41.4075,2.1190 41.4060))',4326)),
-- VILA DE GRÀCIA
('bcn_gracia_01','Pl. del Sol',(SELECT id FROM barrios WHERE codigo='06001'),ST_GeomFromText('POLYGON((2.1555 41.4025,2.1570 41.4025,2.1570 41.4040,2.1555 41.4040,2.1555 41.4025))',4326)),
('bcn_gracia_02','C/ Verdi',(SELECT id FROM barrios WHERE codigo='06001'),ST_GeomFromText('POLYGON((2.1570 41.4040,2.1585 41.4040,2.1585 41.4055,2.1570 41.4055,2.1570 41.4040))',4326)),
('bcn_gracia_03','Pl. de la Vila',(SELECT id FROM barrios WHERE codigo='06001'),ST_GeomFromText('POLYGON((2.1540 41.4010,2.1555 41.4010,2.1555 41.4025,2.1540 41.4025,2.1540 41.4010))',4326)),
('bcn_gracia_04','C/ Gran de Gràcia',(SELECT id FROM barrios WHERE codigo='06001'),ST_GeomFromText('POLYGON((2.1600 41.4020,2.1615 41.4020,2.1615 41.4035,2.1600 41.4035,2.1600 41.4020))',4326)),
('bcn_gracia_05','Pg. de Sant Joan',(SELECT id FROM barrios WHERE codigo='06001'),ST_GeomFromText('POLYGON((2.1660 41.4000,2.1675 41.4000,2.1675 41.4015,2.1660 41.4015,2.1660 41.4000))',4326)),
-- CAMP DE GRÀCIA
('bcn_campgracia_01','C/ Còrsega',(SELECT id FROM barrios WHERE codigo='06002'),ST_GeomFromText('POLYGON((2.1620 41.3970,2.1635 41.3970,2.1635 41.3985,2.1620 41.3985,2.1620 41.3970))',4326)),
('bcn_campgracia_02','C/ Travessera Gràcia',(SELECT id FROM barrios WHERE codigo='06002'),ST_GeomFromText('POLYGON((2.1640 41.3980,2.1655 41.3980,2.1655 41.3995,2.1640 41.3995,2.1640 41.3980))',4326)),
('bcn_campgracia_03','C/ Diagonal Gràcia',(SELECT id FROM barrios WHERE codigo='06002'),ST_GeomFromText('POLYGON((2.1580 41.3970,2.1595 41.3970,2.1595 41.3985,2.1580 41.3985,2.1580 41.3970))',4326)),
-- LA SALUT
('bcn_salut_01','C/ Salut',(SELECT id FROM barrios WHERE codigo='06003'),ST_GeomFromText('POLYGON((2.1530 41.4060,2.1545 41.4060,2.1545 41.4075,2.1530 41.4075,2.1530 41.4060))',4326)),
('bcn_salut_02','Mercat de l''Abaceria',(SELECT id FROM barrios WHERE codigo='06003'),ST_GeomFromText('POLYGON((2.1510 41.4045,2.1525 41.4045,2.1525 41.4060,2.1510 41.4060,2.1510 41.4045))',4326)),
-- EL GUINARDÓ
('bcn_guinardo_01','C/ Cartagena',(SELECT id FROM barrios WHERE codigo='07001'),ST_GeomFromText('POLYGON((2.1760 41.4120,2.1775 41.4120,2.1775 41.4135,2.1760 41.4135,2.1760 41.4120))',4326)),
('bcn_guinardo_02','Pl. Maragall',(SELECT id FROM barrios WHERE codigo='07001'),ST_GeomFromText('POLYGON((2.1800 41.4140,2.1815 41.4140,2.1815 41.4155,2.1800 41.4155,2.1800 41.4140))',4326)),
-- NOU BARRIS
('bcn_noubarris_01','C/ Fabra i Puig',(SELECT id FROM barrios WHERE codigo='08001'),ST_GeomFromText('POLYGON((2.1780 41.4350,2.1795 41.4350,2.1795 41.4365,2.1780 41.4365,2.1780 41.4350))',4326)),
('bcn_noubarris_02','Via Júlia',(SELECT id FROM barrios WHERE codigo='08001'),ST_GeomFromText('POLYGON((2.1750 41.4380,2.1765 41.4380,2.1765 41.4395,2.1750 41.4395,2.1750 41.4380))',4326)),
('bcn_prosperitat_01','C/ Prosperitat',(SELECT id FROM barrios WHERE codigo='08002'),ST_GeomFromText('POLYGON((2.1760 41.4300,2.1775 41.4300,2.1775 41.4315,2.1760 41.4315,2.1760 41.4300))',4326)),
-- SANT ANDREU
('bcn_standreu_01','C/ Gran de Sant Andreu',(SELECT id FROM barrios WHERE codigo='09001'),ST_GeomFromText('POLYGON((2.1900 41.4300,2.1915 41.4300,2.1915 41.4315,2.1900 41.4315,2.1900 41.4300))',4326)),
('bcn_standreu_02','Mercat Felip II',(SELECT id FROM barrios WHERE codigo='09001'),ST_GeomFromText('POLYGON((2.1880 41.4280,2.1895 41.4280,2.1895 41.4295,2.1880 41.4295,2.1880 41.4280))',4326)),
('bcn_standreu_03','C/ Navas',(SELECT id FROM barrios WHERE codigo='09001'),ST_GeomFromText('POLYGON((2.1920 41.4260,2.1935 41.4260,2.1935 41.4275,2.1920 41.4275,2.1920 41.4260))',4326)),
('bcn_sagrera_01','C/ de la Sagrera',(SELECT id FROM barrios WHERE codigo='09002'),ST_GeomFromText('POLYGON((2.2010 41.4200,2.2025 41.4200,2.2025 41.4215,2.2010 41.4215,2.2010 41.4200))',4326)),
('bcn_sagrera_02','Pl. Comercial SA',(SELECT id FROM barrios WHERE codigo='09002'),ST_GeomFromText('POLYGON((2.2030 41.4190,2.2045 41.4190,2.2045 41.4205,2.2030 41.4205,2.2030 41.4190))',4326)),
-- EL POBLENOU
('bcn_poblenou_01','Rambla del Poblenou',(SELECT id FROM barrios WHERE codigo='10001'),ST_GeomFromText('POLYGON((2.1950 41.4010,2.1965 41.4010,2.1965 41.4025,2.1950 41.4025,2.1950 41.4010))',4326)),
('bcn_poblenou_02','Poblenou 22@',(SELECT id FROM barrios WHERE codigo='10001'),ST_GeomFromText('POLYGON((2.1880 41.3990,2.1895 41.3990,2.1895 41.4005,2.1880 41.4005,2.1880 41.3990))',4326)),
('bcn_poblenou_03','C/ Pallars',(SELECT id FROM barrios WHERE codigo='10001'),ST_GeomFromText('POLYGON((2.1920 41.4030,2.1935 41.4030,2.1935 41.4045,2.1920 41.4045,2.1920 41.4030))',4326)),
('bcn_poblenou_04','C/ Bac de Roda',(SELECT id FROM barrios WHERE codigo='10001'),ST_GeomFromText('POLYGON((2.1970 41.4060,2.1985 41.4060,2.1985 41.4075,2.1970 41.4075,2.1970 41.4060))',4326)),
('bcn_poblenou_05','C/ Taulat',(SELECT id FROM barrios WHERE codigo='10001'),ST_GeomFromText('POLYGON((2.2000 41.4030,2.2015 41.4030,2.2015 41.4045,2.2000 41.4045,2.2000 41.4030))',4326)),
-- EL CLOT
('bcn_clot_01','Mercat del Clot',(SELECT id FROM barrios WHERE codigo='10002'),ST_GeomFromText('POLYGON((2.1870 41.4070,2.1885 41.4070,2.1885 41.4085,2.1870 41.4085,2.1870 41.4070))',4326)),
('bcn_clot_02','C/ Aragó Clot',(SELECT id FROM barrios WHERE codigo='10002'),ST_GeomFromText('POLYGON((2.1850 41.4050,2.1865 41.4050,2.1865 41.4065,2.1850 41.4065,2.1850 41.4050))',4326)),
('bcn_clot_03','C/ Clot',(SELECT id FROM barrios WHERE codigo='10002'),ST_GeomFromText('POLYGON((2.1890 41.4090,2.1905 41.4090,2.1905 41.4105,2.1890 41.4105,2.1890 41.4090))',4326)),
-- VILA OLÍMPICA
('bcn_vilaolim_01','Pg. Marítim',(SELECT id FROM barrios WHERE codigo='10004'),ST_GeomFromText('POLYGON((2.1960 41.3890,2.1975 41.3890,2.1975 41.3905,2.1960 41.3905,2.1960 41.3890))',4326)),
('bcn_vilaolim_02','C/ Marina VO',(SELECT id FROM barrios WHERE codigo='10004'),ST_GeomFromText('POLYGON((2.1940 41.3910,2.1955 41.3910,2.1955 41.3925,2.1940 41.3925,2.1940 41.3910))',4326)),
('bcn_vilaolim_03','Port Olímpic',(SELECT id FROM barrios WHERE codigo='10004'),ST_GeomFromText('POLYGON((2.2000 41.3870,2.2015 41.3870,2.2015 41.3885,2.2000 41.3885,2.2000 41.3870))',4326))
ON CONFLICT (id) DO NOTHING;

-- ── Locales (uno por zona, lat/lng = centroide del polígono) ──────────────────
-- CAMBIO respecto al legacy: columna "disponible" → "esta_disponible" (03_inmuebles.sql).
INSERT INTO locales (id, zona_id, lat, lng, m2, planta, alquiler_mensual, esta_disponible, escaparate_ml)
SELECT
    'loc_' || z.id,
    z.id,
    ST_Y(ST_Centroid(z.geometria)),
    ST_X(ST_Centroid(z.geometria)),
    CASE
        WHEN z.id LIKE '%poblenou_02%' THEN 200
        WHEN z.id LIKE '%dreix%'       THEN 115
        WHEN z.id LIKE '%born%'        THEN 95
        WHEN z.id LIKE '%gracia%'      THEN 70
        WHEN z.id LIKE '%gotic%'       THEN 65
        WHEN z.id LIKE '%esqeix%'      THEN 125
        WHEN z.id LIKE '%sagfam%'      THEN 80
        WHEN z.id LIKE '%sants%'       THEN 90
        WHEN z.id LIKE '%vilaolim%'    THEN 110
        ELSE 75
    END,
    'PB',
    CASE
        WHEN z.id LIKE '%poblenou_02%' THEN 3500
        WHEN z.id LIKE '%dreix_01%'    THEN 3200
        WHEN z.id LIKE '%dreix%'       THEN 2800
        WHEN z.id LIKE '%born_01%'     THEN 2800
        WHEN z.id LIKE '%born%'        THEN 2400
        WHEN z.id LIKE '%sgervasi%'    THEN 2600
        WHEN z.id LIKE '%sarria%'      THEN 2400
        WHEN z.id LIKE '%gotic%'       THEN 2200
        WHEN z.id LIKE '%barceloneta%' THEN 2000
        WHEN z.id LIKE '%esqeix%'      THEN 2200
        WHEN z.id LIKE '%campgracia%'  THEN 1900
        WHEN z.id LIKE '%gracia%'      THEN 1800
        WHEN z.id LIKE '%vilaolim%'    THEN 2200
        WHEN z.id LIKE '%poblenou%'    THEN 1700
        WHEN z.id LIKE '%corts%'       THEN 1900
        WHEN z.id LIKE '%sants%'       THEN 1400
        WHEN z.id LIKE '%clot%'        THEN 1300
        WHEN z.id LIKE '%standreu%'    THEN 1200
        WHEN z.id LIKE '%sagrera%'     THEN 1100
        WHEN z.id LIKE '%guinardo%'    THEN 1100
        WHEN z.id LIKE '%salut%'       THEN 1400
        WHEN z.id LIKE '%sagfam%'      THEN 1600
        WHEN z.id LIKE '%raval%'       THEN 1600
        WHEN z.id LIKE '%noubarris%'   THEN 900
        WHEN z.id LIKE '%prosperitat%' THEN 850
        WHEN z.id LIKE '%fortpienc%'   THEN 1500
        WHEN z.id LIKE '%hostafrancs%' THEN 1200
        ELSE 1500
    END,
    TRUE,
    8.0
FROM zonas z
WHERE NOT EXISTS (SELECT 1 FROM locales l WHERE l.id = 'loc_' || z.id)
ON CONFLICT (id) DO NOTHING;

-- ── Scores para todos los sectores ────────────────────────────────────────────
INSERT INTO scores_zona
    (zona_id, sector_id, modelo_version, score_global,
     score_flujo_peatonal, score_demografia, score_competencia,
     score_precio_alquiler, score_transporte, score_turismo,
     score_entorno_comercial, probabilidad_supervivencia)
SELECT
    z.id,
    s.id,
    'seed_v1',
    GREATEST(20, LEAST(98,
        CASE
            -- Top tier (85-98)
            WHEN z.id IN ('bcn_born_01','bcn_dreix_01','bcn_dreix_05')                     THEN 88 + (s.id % 5)
            WHEN z.id IN ('bcn_gotic_01','bcn_born_02','bcn_dreix_02')                     THEN 85 + (s.id % 4)
            WHEN z.id IN ('bcn_gracia_01','bcn_esqeix_03','bcn_dreix_03')                  THEN 83 + (s.id % 4)
            -- High tier (70-84)
            WHEN z.id IN ('bcn_born_03','bcn_born_04','bcn_esqeix_01','bcn_gracia_02')     THEN 80 + (s.id % 5)
            WHEN z.id IN ('bcn_dreix_04','bcn_dreix_06','bcn_campgracia_02')               THEN 77 + (s.id % 5)
            WHEN z.id IN ('bcn_gotic_02','bcn_gotic_04','bcn_esqeix_02','bcn_poblenou_01') THEN 75 + (s.id % 4)
            WHEN z.id IN ('bcn_gracia_04','bcn_gracia_03','bcn_campgracia_01')             THEN 73 + (s.id % 4)
            WHEN z.id IN ('bcn_vilaolim_01','bcn_born_05','bcn_barceloneta_01')            THEN 71 + (s.id % 4)
            -- Mid tier (50-69)
            WHEN z.id IN ('bcn_esqeix_04','bcn_esqeix_05','bcn_dreix_07','bcn_dreix_08')   THEN 68 + (s.id % 4)
            WHEN z.id IN ('bcn_sagfam_01','bcn_sgervasi_01','bcn_gracia_05')               THEN 66 + (s.id % 4)
            WHEN z.id IN ('bcn_poblenou_02','bcn_poblenou_03','bcn_salut_01')              THEN 64 + (s.id % 4)
            WHEN z.id IN ('bcn_campgracia_03','bcn_sgervasi_02','bcn_corts_01')            THEN 62 + (s.id % 3)
            WHEN z.id IN ('bcn_gotic_03','bcn_gotic_05','bcn_barceloneta_02')              THEN 60 + (s.id % 3)
            WHEN z.id IN ('bcn_sagfam_02','bcn_sagfam_03','bcn_esqeix_06')                 THEN 58 + (s.id % 3)
            WHEN z.id IN ('bcn_vilaolim_02','bcn_vilaolim_03','bcn_fortpienc_01')          THEN 56 + (s.id % 3)
            WHEN z.id IN ('bcn_raval_04','bcn_sants_04','bcn_hostafrancs_02')              THEN 55 + (s.id % 3)
            WHEN z.id IN ('bcn_clot_01','bcn_clot_02','bcn_poblenou_04')                   THEN 53 + (s.id % 3)
            -- Lower tier (30-49)
            WHEN z.id IN ('bcn_raval_01','bcn_raval_02','bcn_sants_01')                    THEN 48 + (s.id % 3)
            WHEN z.id IN ('bcn_sgervasi_03','bcn_sarria_01','bcn_corts_02')                THEN 45 + (s.id % 3)
            WHEN z.id IN ('bcn_esqeix_07','bcn_esqeix_08','bcn_sagfam_04')                 THEN 43 + (s.id % 3)
            WHEN z.id IN ('bcn_raval_03','bcn_raval_05','bcn_sants_02')                    THEN 40 + (s.id % 3)
            WHEN z.id IN ('bcn_guinardo_01','bcn_guinardo_02','bcn_standreu_01')            THEN 38 + (s.id % 3)
            WHEN z.id IN ('bcn_clot_03','bcn_poblenou_05','bcn_fortpienc_02')              THEN 36 + (s.id % 2)
            WHEN z.id IN ('bcn_salut_02','bcn_sarria_02','bcn_corts_03')                   THEN 35 + (s.id % 2)
            WHEN z.id IN ('bcn_standreu_02','bcn_standreu_03','bcn_sagrera_01')            THEN 33 + (s.id % 2)
            WHEN z.id IN ('bcn_barceloneta_03','bcn_raval_06','bcn_sants_03')              THEN 31 + (s.id % 2)
            WHEN z.id IN ('bcn_sagrera_02','bcn_noubarris_01','bcn_prosperitat_01')        THEN 28 + (s.id % 2)
            WHEN z.id IN ('bcn_noubarris_02','bcn_hostafrancs_01')                         THEN 25 + (s.id % 2)
            ELSE 40 + (s.id % 10)
        END
    )),
    -- scores por dimensión (variados pero coherentes con el global)
    GREATEST(10, LEAST(100, 60 + (hashtext(z.id || 'flujo')  % 40))),
    GREATEST(10, LEAST(100, 55 + (hashtext(z.id || 'demo')   % 45))),
    GREATEST(10, LEAST(100, 50 + (hashtext(z.id || 'comp')   % 50))),
    GREATEST(10, LEAST(100, 45 + (hashtext(z.id || 'precio') % 55))),
    GREATEST(10, LEAST(100, 60 + (hashtext(z.id || 'trans')  % 40))),
    GREATEST(10, LEAST(100, 40 + (hashtext(z.id || 'tur')    % 60))),
    GREATEST(10, LEAST(100, 50 + (hashtext(z.id || 'entorn') % 50))),
    -- probabilidad de supervivencia (correlacionada con score)
    ROUND(CAST(GREATEST(0.15, LEAST(0.95,
        CASE
            WHEN z.id IN ('bcn_born_01','bcn_dreix_01','bcn_dreix_05') THEN 0.83
            WHEN z.id IN ('bcn_gotic_01','bcn_born_02','bcn_dreix_02') THEN 0.79
            WHEN z.id IN ('bcn_gracia_01','bcn_esqeix_03')              THEN 0.76
            WHEN z.id LIKE '%born%'                                      THEN 0.74
            WHEN z.id LIKE '%dreix%'                                     THEN 0.72
            WHEN z.id LIKE '%gracia%'                                    THEN 0.69
            WHEN z.id LIKE '%esqeix%'                                    THEN 0.67
            WHEN z.id LIKE '%gotic%'                                     THEN 0.65
            WHEN z.id LIKE '%poblenou%'                                  THEN 0.62
            WHEN z.id LIKE '%sagfam%'                                    THEN 0.60
            WHEN z.id LIKE '%sants%'                                     THEN 0.55
            WHEN z.id LIKE '%clot%'                                      THEN 0.52
            WHEN z.id LIKE '%raval%'                                     THEN 0.50
            WHEN z.id LIKE '%standreu%'                                  THEN 0.45
            WHEN z.id LIKE '%noubarris%'                                 THEN 0.35
            ELSE 0.55
        END
    )) AS NUMERIC), 2)
FROM zonas z, sectores s
ON CONFLICT (zona_id, sector_id, modelo_version) DO NOTHING;
