## Librerías

library(factoextra)
library(ggfortify)
library(plotly)
library(DBI)
library(RPostgres)
library(sf)
library(ggplot2)
library(cowplot)
library(GGally)

## Entradas 
zonas_gs_ingreso = st_read("output/zonas_gs_ingreso.geojson")
zonas_gs_tenencia = st_read("output/zonas_gs_tenencia.geojson")

## Conexión a BD

# Definir parámetros de conexión
db_host     = "localhost"       # servidor de BD
db_port     = 5432                # puerto de escucha
db_name     = "censo_rm_clases"   # nombre de la base
db_user     = "postgres"        # usuario de conexión
db_password = "postgres"        # clave de usuario

# Establecer conexión usando RPostgres
con = dbConnect(
  Postgres(),
  dbname   = db_name,
  host     = db_host,
  port     = db_port,
  user     = db_user,
  password = db_password
)


sql_indicadores = "
SELECT
  z.geocodigo AS geocodigo,
  c.nom_comuna,
  
  -- Años promedio de escolaridad para personas de 18+ años
  ROUND(
    AVG(p.escolaridad) FILTER (WHERE p.p09 >= 18 AND p.escolaridad IS NOT NULL),
    2) AS promedio_escolaridad_18mas,
  
  -- Promedio de personas por hogar en la zona
  ROUND(
    COUNT(*)::numeric
    / NULLIF(COUNT(DISTINCT h.hogar_ref_id), 0),
    2) AS promedio_personas_por_hogar

FROM public.personas   AS p
JOIN public.hogares    AS h ON p.hogar_ref_id    = h.hogar_ref_id
JOIN public.viviendas  AS v ON h.vivienda_ref_id = v.vivienda_ref_id
JOIN public.zonas      AS z ON v.zonaloc_ref_id  = z.zonaloc_ref_id
JOIN public.comunas    AS c ON z.codigo_comuna   = c.codigo_comuna

GROUP BY z.geocodigo, c.nom_comuna
ORDER BY promedio_escolaridad_18mas DESC;
"
# Ejecutar consulta y importar resultados a data.frame en R
df_indicadores = dbGetQuery(con, sql_indicadores)

zonas_gs_tenencia <- zonas_gs_tenencia %>% 
  mutate(diferencia = arrendada - propia)


# 1. Unimos ingresos + indicadores (df_indicadores NO es sf, así que no hay problema)
zonas_gs_indicadores <- merge(
  zonas_gs_ingreso,
  df_indicadores[, c("geocodigo",
                     "promedio_escolaridad_18mas",
                     "promedio_personas_por_hogar")],
  by = "geocodigo",
  all.x = TRUE,
  all.y = FALSE
)

# 2. Unimos la columna 'diferencia' PERO sin geometría para evitar conflicto
zonas_gs_indicadores <- merge(
  zonas_gs_indicadores,
  st_drop_geometry(zonas_gs_tenencia)[, c("geocodigo", "diferencia")],
  by = "geocodigo",
  all.x = TRUE,
  all.y = FALSE
)

#zonas_gs_indicadores = na.omit(zonas_gs_indicadores)

zonas_gs_indicadores <- zonas_gs_indicadores %>%
  group_by(nom_zona) %>%
  mutate(
    mediana_ingreso = ifelse(
      is.na(mediana_ingreso),
      mean(mediana_ingreso, na.rm = TRUE),
      mediana_ingreso
    ),
    promedio_escolaridad_18mas = ifelse(
      is.na(promedio_escolaridad_18mas),
      mean(promedio_escolaridad_18mas, na.rm = TRUE),
      promedio_escolaridad_18mas
    ),
    promedio_personas_por_hogar = ifelse(
      is.na(promedio_personas_por_hogar),
      mean(promedio_personas_por_hogar, na.rm = TRUE),
      promedio_personas_por_hogar
    )
  ) %>%
  ungroup()


## Seleccionar variables y escalarlas

vars_clusters = zonas_gs_indicadores[, c('diferencia',
                                         'mediana_ingreso',
                                         'promedio_escolaridad_18mas', 
                                         'promedio_personas_por_hogar')]

# Se elimina la geometría
vars_clusters$geometry = NULL

vars_clusters = na.omit(vars_clusters)

# Se escalan las variables
vars_scaled = scale(vars_clusters)

# Método del codo para elegir K 
fviz_nbclust(vars_scaled, kmeans, method = "wss") +
  labs(title = "Método del codo", x = "Número de clusters (k)", y = "WSS")

## Ejecutar kmeans
set.seed(123)
km = kmeans(vars_scaled, centers = 4, nstart = 25)

zonas_gs_indicadores$cluster = as.factor(km$cluster)

# Escolaridad 18+ v/s Ingreso
ggplot(zonas_gs_indicadores,
       aes(x = promedio_escolaridad_18mas,
           y = mediana_ingreso,
           color = cluster)) +
  geom_point(size = 2) +
  labs(
    title = "Escolaridad (18+) v/s Ingreso",
    x = "Promedio años de escolaridad (>= 18 años)",
    y = "Mediana de ingreso",
    color = "Cluster"
  ) +
  theme_minimal()

# Tamaño de hogar v/s Ingreso
ggplot(zonas_gs_indicadores,
       aes(x = promedio_personas_por_hogar,
           y = mediana_ingreso,
           color = cluster)) +
  geom_point(size = 2) +
  labs(
    title = "Tamaño del hogar v/s Ingreso",
    x = "Promedio personas por hogar",
    y = "Mediana de ingreso",
    color = "Cluster"
  ) +
  theme_minimal()

# Tenencia (diferencia) v/s Ingreso
ggplot(zonas_gs_indicadores,
       aes(x = diferencia,
           y = mediana_ingreso,
           color = cluster)) +
  geom_point(size = 2) +
  labs(
    title = "Tenencia v/s Ingreso",
    x = "Diferencia % arrendada - % propia",
    y = "Mediana de ingreso",
    color = "Cluster"
  ) +
  theme_minimal()

# Tenencia v/s Escolaridad
ggplot(zonas_gs_indicadores,
       aes(x = diferencia,
           y = promedio_escolaridad_18mas,
           color = cluster)) +
  geom_point(size = 2) +
  labs(
    title = "Tenencia v/s Escolaridad (18+)",
    x = "Diferencia % arrendada - % propia",
    y = "Promedio años de escolaridad (>= 18 años)",
    color = "Cluster"
  ) +
  theme_minimal()

##GG

datos_pairs <- zonas_gs_indicadores %>%
  st_drop_geometry() %>%
  select(
    diferencia,
    mediana_ingreso,
    promedio_escolaridad_18mas,
    promedio_personas_por_hogar,
    cluster
  )

ggpairs(
  datos_pairs,
  columns = 1:4,  # solo las numéricas
  mapping = aes(color = cluster, alpha = 0.6)
)


# Se obtiene geometría comunal para Santiago
sql_comunas = "
SELECT cut, nom_comuna, geom
FROM dpa.comunas_rm_shp
WHERE nom_provin = 'SANTIAGO';
"


sf_comunas_santiago = st_read(con, query = sql_comunas)


# bbox del área urbana / zona de interés
bbox <- st_bbox(zonas_gs_indicadores)


mapa_clusters <- ggplot() +
  geom_sf(
    data  = zonas_gs_indicadores,
    aes(fill = cluster),
    color = "white",
    linewidth = 0.05
  ) +
  geom_sf(
    data  = sf_comunas_santiago,
    fill  = NA,
    color = "grey20",
    linewidth = 0.3
  ) +
  scale_fill_brewer(
    palette = "Set2",
    name    = "Cluster k-means"
  ) +
  coord_sf(
    xlim   = c(bbox["xmin"], bbox["xmax"]),
    ylim   = c(bbox["ymin"], bbox["ymax"]),
    expand = FALSE
  ) +
  theme_minimal(base_size = 10) +
  theme(
    plot.background  = element_rect(fill = "white", color = NA),
    
    # Grilla REAL para mapas sf
    panel.grid.major = element_line(color = "grey85", linewidth = 0.3),
    panel.grid.minor = element_line(color = "grey93", linewidth = 0.2),
    
    legend.position  = "right",
    legend.title     = element_text(face = "bold"),
    legend.text      = element_text(size = 8),
    
    plot.title       = element_text(face = "bold", hjust = 0.5, size = 12),
    plot.subtitle    = element_text(hjust = 0.5, colour = "grey30", size = 9),
    plot.caption     = element_text(colour = "grey50", size = 7, hjust = 1)
  ) +
  labs(
    title    = "Clusters de zonas censales del Gran Santiago",
    subtitle = "k-means basado en ingreso, escolaridad, tamaño del hogar y tenencia",
    caption  = "Fuente: Censo 2017 (microdatos), elaboración propia"
  )

print(mapa_clusters)
