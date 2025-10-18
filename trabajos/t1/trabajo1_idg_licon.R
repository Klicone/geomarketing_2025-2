################################################################################
# 1. LIBRERIAS                               
################################################################################

library(DBI)
library(RPostgres)
library(sf)
library(ggplot2)
library(RColorBrewer)
library(cowplot)
library(biscale)

################################################################################
# 2. CONFIGURACIÓN DB
################################################################################


db_host = 'localhost'
db_port = 5432
db_name = 'censo_v_2017'
db_user = 'postgres'
db_password = 'postgres'

################################################################################
# 3. CONEXIÓN
################################################################################

con = dbConnect(
  Postgres(),
  dbname = db_name,
  host = db_host,
  port = db_port,
  user = db_user,
  password = db_password
)

################################################################################
# 4. QUERY: % MOVILIDAD INTERCOMUNAL VS % EDUCACIÓN SUPERIOR
################################################################################

sql_indicadores = "
WITH agg AS (
  SELECT 
	z.geocodigo::double precision AS geocodigo,
	c.nom_comuna,

	ROUND(
	COUNT(*) FILTER (
		WHERE p.p10 = 1 	-- Comuna de residencia habitual en la actual
		AND p.p11 = 3 		-- Comuna de residencia anterior en otra
	) * 100.0 / COUNT(*), 2)
	AS ptje_mov_intercomunal,

	ROUND(
	COUNT(*) FILTER (
		WHERE p.p09 >= 18
		AND p.escolaridad >= 13
	) * 100.0 / COUNT(*) FILTER (
		WHERE p.p09 >= 18
	), 2)
	AS ptje_ed_superior

  FROM public.personas AS p
  
  JOIN public.hogares AS h ON p.hogar_ref_id = h.hogar_ref_id
  JOIN public.viviendas AS v ON h.vivienda_ref_id = v.vivienda_ref_id
  JOIN public.zonas AS z ON v.zonaloc_ref_id = z.zonaloc_ref_id
  JOIN public.comunas AS c ON z.codigo_comuna = c.codigo_comuna
  JOIN public.provincias AS pr ON c.provincia_ref_id = pr.provincia_ref_id
  
  WHERE pr.nom_provincia = 'VALPARAÍSO'
    AND c.nom_comuna <> 'JUAN FERNÁNDEZ'
  GROUP BY z.geocodigo, c.nom_comuna
),
-- Partes continentales (mayor área) por comuna
comunas_mainland AS (
  WITH partes AS (
    SELECT nom_comuna, (ST_Dump(geom)).geom AS geom
    FROM dpa.comunas_v
    WHERE nom_provin = 'VALPARAÍSO'
      AND nom_comuna <> 'JUAN FERNÁNDEZ'
  ),
  ranked AS (
    SELECT nom_comuna, geom,
           ROW_NUMBER() OVER (
             PARTITION BY nom_comuna
             ORDER BY ST_Area(ST_Transform(geom, 32719)) DESC
           ) AS rn
    FROM partes
  )
  SELECT nom_comuna, geom
  FROM ranked
  WHERE rn = 1
),
-- Máscara continental (una sola geometría)
mainland AS (
  SELECT ST_Union(geom) AS geom
  FROM comunas_mainland
)
SELECT
  a.geocodigo,
  shp.geom,
  a.nom_comuna,
  a.ptje_mov_intercomunal,
  a.ptje_ed_superior
FROM agg a
JOIN dpa.zonas_censales_v AS shp
  ON a.geocodigo = shp.geocodigo
JOIN mainland m
  ON ST_Intersects(shp.geom, m.geom);  -- deja fuera zonas insulares

;

"
# ALMACENAR DATAFRAME
df_indicadores = st_read(con, query = sql_indicadores)

################################################################################
# 5. CARGAR GEOMETRÍAS
################################################################################

sql_comunas = "

WITH partes AS (
  SELECT
    nom_comuna,
    (ST_Dump(geom)).geom AS geom
  FROM dpa.comunas_v
  WHERE nom_provin = 'VALPARAÍSO'
    AND nom_comuna <> 'JUAN FERNÁNDEZ'
),
ranked AS (
  SELECT
    nom_comuna,
    geom,
    ROW_NUMBER() OVER (
      PARTITION BY nom_comuna
      ORDER BY ST_Area(ST_Transform(geom, 32719)) DESC
    ) AS rn
  FROM partes
)
SELECT nom_comuna, geom
FROM ranked
WHERE rn = 1;   -- solo la parte continental (mayor área) por comuna
;

"

# ALMACENAR DATAFRAME
sf_comunas = st_read(con, query = sql_comunas)

# CENTROIDES COMUNALES
sf_centroides = st_centroid(sf_comunas)

################################################################################
# 6. EDA
################################################################################

# HISTOGRAMA DISTRIBUCIÓN DEL % MIR
ggplot(df_indicadores, aes(x = ptje_mov_intercomunal)) +
  geom_histogram(bins = 30, fill = '#226e6e', color = 'white', alpha = 0.9) +
  geom_vline(aes(xintercept = mean(ptje_mov_intercomunal, na.rm = TRUE)),
             color = "red", linetype = "dashed", linewidth = 1) +
  annotate("text", x = mean(df_indicadores$ptje_mov_intercomunal, na.rm = TRUE),
           y = Inf, vjust = 2, label = "Media", color = "red",size = 3)+
  labs(title = 'Distribución del % MIR', subtitle = 'Provincia de Valparaíso',
       x = '% MIR',
       y = 'Frecuencia') +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold", hjust = 0.5),
        plot.subtitle = element_text(hjust = 0.5),
        panel.grid.minor = element_blank())

# HISTOGRAMA DISTRIBUCIÓN DEL % PES
ggplot(df_indicadores, aes(x = ptje_ed_superior)) +
  geom_histogram(bins = 30, fill = '#81762f', color = 'white', alpha = 0.9) +
  geom_vline(aes(xintercept = mean(ptje_ed_superior, na.rm = TRUE)),
             color = "red", linetype = "dashed", linewidth = 1) +
  annotate("text", x = mean(df_indicadores$ptje_ed_superior, na.rm = TRUE),
           y = Inf, vjust = 2, label = "Media", color = "red",size = 3)+
  labs(title = 'Distribución del % PES', subtitle = 'Provincia de Valparaíso',
       x = '% PES',
       y = 'Frecuencia') +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold", hjust = 0.5),
        plot.subtitle = element_text(hjust = 0.5),
        panel.grid.minor = element_blank())


# DIAGRAMA DE DISPERSIÓN
ggplot(df_indicadores, aes(x = ptje_mov_intercomunal, y = ptje_ed_superior)) +
  geom_point(color = "steelblue") +
  labs(title = 'Relación entre % MIR vs. % PES',
       x = '% MIR',
       y = '% PES') +
theme_minimal()


################################################################################
# 7. MAPAS UNIVARIADOS
################################################################################

# MAPA % DE MOVILIDAD INTERCOMUNAL POR ZONA CENSAL
ggplot() +
  geom_sf(data = df_indicadores, aes(fill = ptje_mov_intercomunal), color = NA) +
  geom_sf(data = sf_comunas, fill = NA, color = "black", size = 1) +
  geom_sf_text(data = sf_centroides, aes(label = nom_comuna),
               size = 3, fontface = "bold") +
  scale_fill_distiller(palette = "BuPu", name = "% de Movilidad Intercomunal", direction = 1) +
  theme_void() +
  theme(plot.title = element_text(hjust = 0.5, face = "bold"),
        plot.subtitle = element_text(hjust = 0.5)) +
  labs(title = "% de Movilidad Intercomunal por zona censal",
       subtitle = "Provincia de VALPARAÍSO")

# MAPA % DE EDUCACIÓN SUPERIOR POR ZONA CENSAL
ggplot() +
  geom_sf(data = df_indicadores, aes(fill = ptje_ed_superior), color = NA) +
  geom_sf(data = sf_comunas, fill = NA, color = "black", size = 1) +
  geom_sf_text(data = sf_centroides, aes(label = nom_comuna),
               size = 3, fontface = "bold") +
  scale_fill_distiller(palette = "BuPu", name = "% de Educación Superior", direction = 1) +
  theme_void() +
  theme(plot.title = element_text(hjust = 0.5, face = "bold"),
        plot.subtitle = element_text(hjust = 0.5)) +
  labs(title = "% de Educación Superior por zona censal",
       subtitle = "Provincia de VALPARAÍSO")

################################################################################
# 8. MAPAS BIVARIADOS
################################################################################

sf_mapa_bi = bi_class(
  df_indicadores,
  x = ptje_mov_intercomunal,
  y = ptje_ed_superior,
  dim = 3,
  style = "jenks"
)


mapa_bivariado = ggplot() +
  geom_sf(data = sf_mapa_bi, aes(fill = bi_class), color = NA, show.legend = FALSE) +
  geom_sf(data = sf_comunas, fill = NA, color = "black", size = 1) +
  geom_sf_text(data = sf_centroides, aes(label = nom_comuna),
               size = 3, fontface = "bold") +
  bi_scale_fill(pal = "DkBlue", dim = 3) +
  coord_sf(xlim = c(caja['xmin'], caja['xmax']), ylim = c(caja['ymin'], caja['ymax']), expand = FALSE) +
  theme_void() +
  theme(plot.title = element_text(hjust = 0.5, face = "bold"),
        plot.subtitle = element_text(hjust = 0.5)) +
  labs(title = "Mapa Bivariado: % de Movilidad Intercomunal vs % de Educación Superior",
       subtitle = "Provincia de VALPARAÍSO")


leyenda_bivariada = bi_legend(
  pal = "DkBlue", 
  dim = 3,
  xlab = "% de Movilidad Intercomunal (bajo → alto)",
  ylab = "% de Educación Superior (bajo → alto)",
  size = 5
)

mapa_final = ggdraw() +
  draw_plot(mapa_bivariado, x = 0,    y = 0,    width = 1,    height = 1) +
  draw_plot(leyenda_bivariada,        x = 0.72, y = 0.05, width = 0.26, height = 0.26)
print(mapa_final)
