## 1. Librerias
library(rakeR)
library(RPostgres)
library(DBI)
library(sf)
library(dplyr)
library(tidyr)
library(ggplot2)
library(patchwork)

## 2. Entradas

# Rutas de las entradas
ruta_casen = 'data/casen_rm.rds'
ruta_censo = 'data/cons_censo_df.rds'

# DF de CASEN Y CENSO
casen_raw = readRDS(ruta_casen)
cons_censo_df = readRDS(ruta_censo)

## 3. Preprocesado

# 3.1 CENSO
col_cons = sort(setdiff(names(cons_censo_df),
                        c('GEOCODIGO','COMUNA')))

# niveles para prefijos
age_levels = grep('^edad', col_cons, value = TRUE)
esc_levels = grep('^esco', col_cons, value = TRUE)
sex_levels = grep('^sexo', col_cons, value = TRUE)

# 3.2 CASEN

# Variables de interes de CASEN
vars_base = c(
  'estrato', #extraer comuna
  'esc', #años de escolaridad
  'edad',
  'sexo',
  'e6a', #para imputar esc
  'v13' #situación de ocupacion de la vivienda
)

# filtrar casen con las variables de interés
casen = casen_raw[, vars_base, drop = FALSE]

# limpiar memoria
rm(casen_raw)

# Extraer la comuna desde estrato (primero 5 digitos)
casen$Comuna = substr(as.character(casen$estrato),1,5)

#eliminar la columna estrato
casen$estrato = NULL

#Quitar etiquetas y pasar a tipo base
casen$esc = as.integer(unclass(casen$esc))
casen$edad = as.integer(unclass(casen$edad))
casen$e6a = as.numeric(unclass(casen$e6a))
casen$sexo = as.integer(unclass(casen$sexo))
casen$v13 = as.integer(unclass(casen$v13))

#imputar datos de esc en base a e6a
idx_na = which(is.na(casen$esc))

#Ajustar modelo con casos donde no hay NA's
fit = lm(esc ~ e6a,data = casen[-idx_na,])

#Predicción para los casos con NA
pred = predict(fit, newdata = casen[idx_na, ,drop = FALSE])

#Imputar acotada
casen$esc[idx_na] = as.integer(round(pmax(0, pmin(29, pred))))

#añadimos ID unico por registro
casen$ ID = as.character(seq_len(nrow(casen)))

# 3.3 recodificación

#categorizar edad usando nombres del censo
casen$edad_cat = cut(
  casen$edad,
  breaks = c(0, 30, 40, 50, 60, 70, 80, Inf),
  labels = age_levels,
  right = FALSE,
  include.lowest = TRUE
)

#categorizar escolaridad (4 tramos) usando nombres del censo
casen$esc_cat = factor(
  with(casen,
       ifelse(esc == 0, esc_levels[1],
              ifelse(esc <= 8, esc_levels[2],
                     ifelse(esc <= 12, esc_levels[3],
                            esc_levels[4])))),
  levels = esc_levels
)

#recodificación de sexo usando nombres del censo
casen$sexo_cat = factor(
  ifelse(casen$sexo == 2, sex_levels[1],
         ifelse(casen$sexo == 1, sex_levels[2], NA)),
  levels = sex_levels
)

## 4. Microsimulación

#Crear lista de constrains por comuna (CENSO)
cons_censo_comunas = split(cons_censo_df, cons_censo_df$COMUNA)

#crear lista de individuos por comuna (CASEN)
inds_list = split(casen, casen$Comuna)

# Microsimulación por comuna
sim_list <- lapply(names(cons_censo_comunas), function(zona) {
  
  # --- Censo: constraints de la comuna ---
  cons_i <- cons_censo_comunas[[zona]]
  col_order <- sort(setdiff(names(cons_i), c("COMUNA","GEOCODIGO")))
  cons_i <- cons_i[, c("GEOCODIGO", col_order), drop = FALSE]
  
  # --- CASEN: individuos de la comuna ---
  tmp <- inds_list[[zona]]
  inds_i <- tmp[, c("ID","edad_cat","esc_cat","sexo_cat"), drop = FALSE]
  names(inds_i) <- c("ID","Edad","Escolaridad","Sexo")
  
  # --- Ponderación (raking) ---
  w_frac <- weight(
    cons = cons_i,
    inds = inds_i,
    vars = c("Edad","Escolaridad","Sexo")
  )
  
  # --- Integerización (crear población sintética) ---
  sim_i <- integerise(weights = w_frac, inds = inds_i, seed = 123)
  
  # --- Merge con v13 (la variable a microsimular) ---
  merge(sim_i,
        tmp[, c("ID","v13")],
        by = "ID",
        all.x = TRUE)
})

#Unir odas las comunas en un solo dataframe
sim_df = data.table::rbindlist(sim_list, idcol = 'COMUNA')


#Cálculo de proporciones de cada categoría de v13 por zona censal
zonas_v13 = sim_df %>%
  #Renombrar identificador de zona
  rename(geocodigo = zone) %>%
  
  #Conteo de individuos por zona y categoría de v13
  group_by(geocodigo, v13) %>%
  summarise(n = n(), .groups = "drop") %>%
  
  #Cálculo de proporciones dentro de cada zona
  group_by(geocodigo) %>%
  mutate(prop = n / sum(n)) %>%
  ungroup() %>%
  
  #Mantener solo las columnas necesarias
  select(geocodigo, v13, prop) %>%
  distinct(geocodigo, v13, .keep_all = TRUE) %>%
  
  #Transformación de formato largo a ancho (una fila por zona)
  pivot_wider(
    names_from  = v13,
    values_from = prop,
    names_prefix = "prop_v13_",
    values_fill = 0
  ) %>%
  
  #Ordenar por código de zona
  arrange(geocodigo)

#renombrar encabezados
names(zonas_v13) = c(
  "geocodigo",
  "propia",
  "arrendada",
  "cedida",
  "usufructo",
  "ocup_irregular",
  "poseedor_irregular"
)

# 5. Conexión DB

#Parámetros de conexión
db_host = "localhost"
db_port = 5432
db_name = "censo_rm_clases"
db_user = "postgres"
db_password = "postgres"

#Conexión
con = dbConnect(
  Postgres(),
  dbname   = db_name,
  host     = db_host,
  port     = db_port,
  user     = db_user,
  password = db_password
)

#Escribir tabla en la DB
dbWriteTable(
  con,
  name = DBI::SQL("output.zonas_v13"),
  value = zonas_v13,
  row.names = FALSE,
  overwrite = TRUE
)

#Leer zonas censales desde PostGIS
query_zonas = "
SELECT *
FROM dpa.zonas_censales_rm
WHERE urbano = 1
  AND (
    nom_provin = 'SANTIAGO'
    OR nom_comuna IN ('PUENTE ALTO', 'SAN BERNARDO')
  );
"
zonas_gs = st_read(con, query = query_zonas)

#Asegurar mismo tipo que en zonas_v13
zonas_gs$geocodigo = as.character(zonas_gs$geocodigo)

#Unir la geometría con la v13
zonas_gs_v13 = zonas_gs %>%
  left_join(zonas_v13, by = 'geocodigo')

# imputar valores NA usando la mediana de la misma comuna
zonas_gs_v13 <- zonas_gs_v13 %>%
  group_by(nom_comuna) %>%
  mutate(
    propia             = ifelse(is.na(propia),             median(propia,             na.rm = TRUE), propia),
    arrendada          = ifelse(is.na(arrendada),          median(arrendada,          na.rm = TRUE), arrendada),
    cedida             = ifelse(is.na(cedida),             median(cedida,             na.rm = TRUE), cedida),
    usufructo          = ifelse(is.na(usufructo),          median(usufructo,          na.rm = TRUE), usufructo),
    ocup_irregular     = ifelse(is.na(ocup_irregular),     median(ocup_irregular,     na.rm = TRUE), ocup_irregular),
    poseedor_irregular = ifelse(is.na(poseedor_irregular), median(poseedor_irregular, na.rm = TRUE), poseedor_irregular)
  ) %>%
  ungroup()


#Escribir tabla espacial
st_write(
  zonas_gs_v13,
  dsn = con,
  layer = DBI::SQL('output.zc_tenencia_microsim'),
  delete_layer = TRUE
)

# 6. Gráficos

## 6.1 Distribución por Comuna

# resumir proporciones por comuna
comunas_v13 <- zonas_gs_v13 %>%
  st_drop_geometry() %>%
  group_by(nom_comuna) %>%
  summarise(
    propia             = mean(propia, na.rm = TRUE),
    arrendada          = mean(arrendada, na.rm = TRUE),
    cedida             = mean(cedida, na.rm = TRUE),
    usufructo          = mean(usufructo, na.rm = TRUE),
    ocup_irregular     = mean(ocup_irregular, na.rm = TRUE),
    poseedor_irregular = mean(poseedor_irregular, na.rm = TRUE)
  ) %>%
  pivot_longer(-nom_comuna, names_to = "categoria", values_to = "proporcion")

ggplot(comunas_v13, aes(x = reorder(nom_comuna, -proporcion), y = proporcion, fill = categoria)) +
  geom_bar(stat = "identity", position = "fill") +
  scale_fill_brewer(palette = "Set2", name = "Categoría") +
  coord_flip() +
  labs(
    title = "Estructura de tenencia de la vivienda por comuna (microsimulación)",
    y = "Proporción dentro de la comuna",
    x = NULL
  ) +
  theme_minimal(base_size = 10)

# 6.2 Distribución índice
ggplot(zonas_gs_v13, aes(x = arr_vs_prop)) +
  geom_density(fill = "#3182bd", alpha = 0.4) +
  geom_vline(xintercept = 0, linetype = "dashed", color = "grey30") +
  labs(
    title = "Distribución del índice Arriendo – Propiedad",
    x = "Arriendo – Propia (puntos proporcionales)",
    y = "Densidad de zonas censales"
  ) +
  theme_minimal(base_size = 10)

# 7. Visualización

## 7.1 Observación de variable

# 1) comunas desde zonas
comunas_rm <- zonas_gs_v13 %>%
  group_by(nom_comuna) %>%
  summarise() %>%
  ungroup()

# 2) crear la diferencia (si no existe)
#    arriendo - propia
zonas_gs_v13 <- zonas_gs_v13 %>%
  mutate(
    arr_vs_prop = arrendada - propia   # aquí sí usamos las columnas que dijiste que existen
  )

# 3) truncar para visualización
zonas_gs_v13 <- zonas_gs_v13 %>%
  mutate(
    arr_vs_prop_plot = pmax(pmin(arr_vs_prop, 0.5), -0.5)
  )

# 4) mapa
ggplot() +
  geom_sf(
    data = zonas_gs_v13,
    aes(fill = arr_vs_prop_plot),
    color = "white",
    size = 0.05
  ) +
  geom_sf(
    data = comunas_rm,
    fill = NA,
    color = "grey20",
    size = 0.3
  ) +
  scale_fill_gradient2(
    low = "#2b8cbe",      # más propiedad
    mid = "#f7f7f7",      # equilibrio
    high = "#e34a33",     # más arriendo
    midpoint = 0,
    limits = c(-0.5, 0.5),
    breaks = c(-0.5, -0.25, 0, 0.25, 0.5),
    labels = c("-50%", "-25%", "0", "25%", "50%"),
    name = "Arriendo – Propia"
  ) +
  coord_sf(datum = NA) +
  theme_void(base_size = 10) +
  theme(
    plot.background = element_rect(fill = "white", color = NA),
    legend.position = "right",
    legend.title = element_text(face = "bold"),
    plot.title = element_text(face = "bold", hjust = 0.5, size = 12),
    plot.subtitle = element_text(hjust = 0.5, colour = "grey30", size = 9),
    plot.caption = element_text(colour = "grey50", size = 7, hjust = 1)
  ) +
  labs(
    title = "Arriendo vs vivienda propia",
    subtitle = "Rojo: mayor presencia de arriendo · Azul: mayor presencia de propiedad\nValores truncados en ±50% para mejorar lectura",
    caption = "Fuente: microsimulación CASEN–Censo"
  )

## 7.2 PROPORCIONES DE TENENCIA

# función base SIN normalizar
mapa_tenencia_abs <- function(data, var, titulo) {
  ggplot() +
    geom_sf(data = data, aes(fill = .data[[var]]), color = NA) +
    geom_sf(data = comunas_rm, fill = NA, color = "white", size = 0.25) +
    # paleta secuencial verde
    scale_fill_gradient(
      low = "#e5f5e0",
      high = "#006d2c",
      limits = c(0, 1),
      name = "Proporción"
    ) +
    coord_sf(datum = NA) +
    theme_void(base_size = 9) +
    theme(
      plot.title = element_text(face = "bold", hjust = 0.5)
    ) +
    labs(title = titulo)
}

m1_abs <- mapa_tenencia_abs(zonas_gs_v13, "propia", "Propia")
m2_abs <- mapa_tenencia_abs(zonas_gs_v13, "arrendada", "Arrendada")
m3_abs <- mapa_tenencia_abs(zonas_gs_v13, "cedida", "Cedida")
m4_abs <- mapa_tenencia_abs(zonas_gs_v13, "usufructo", "Usufructo")
m5_abs <- mapa_tenencia_abs(zonas_gs_v13, "ocup_irregular", "Ocup. irregular")
m6_abs <- mapa_tenencia_abs(zonas_gs_v13, "poseedor_irregular", "Poseedor irregular")

# panel con UNA sola leyenda
panel_abs <- (m1_abs + m2_abs + m3_abs + m4_abs + m5_abs + m6_abs) +
  plot_layout(ncol = 3, guides = "collect") &
  theme(legend.position = "right")

panel_abs + plot_annotation(
  title = "Tenencia de la vivienda (microsimulación) · proporciones reales",
  caption = "Fuente: microsimulación CASEN–Censo"
)

## 7.3 TENENCIA NORMALIZADA

# función que normaliza SOLO la variable de ese mapa
mapa_tenencia_norm <- function(data, var, titulo) {
  v <- data[[var]]
  vmin <- min(v, na.rm = TRUE)
  vmax <- max(v, na.rm = TRUE)
  
  if (isTRUE(all.equal(vmin, vmax))) {
    vnorm <- rep(0, length(v))
  } else {
    vnorm <- (v - vmin) / (vmax - vmin)
  }
  
  data2 <- data
  data2[[paste0(var, "_norm")]] <- vnorm
  
  ggplot() +
    geom_sf(data = data2,
            aes(fill = .data[[paste0(var, "_norm")]]),
            color = NA) +
    geom_sf(data = comunas_rm,
            fill = NA,
            color = "white",
            size = 0.25) +
    # paleta azul más clara
    scale_fill_gradient(
      low = "#deebf7",
      high = "#08519c",
      limits = c(0, 1),
      name = "Intensidad\n(normalizada)"
    ) +
    coord_sf(datum = NA) +
    theme_void(base_size = 9) +
    theme(
      plot.title = element_text(face = "bold", hjust = 0.5)
    ) +
    labs(title = titulo)
}

m1_norm <- mapa_tenencia_norm(zonas_gs_v13, "propia", "Propia")
m2_norm <- mapa_tenencia_norm(zonas_gs_v13, "arrendada", "Arrendada")
m3_norm <- mapa_tenencia_norm(zonas_gs_v13, "cedida", "Cedida")
m4_norm <- mapa_tenencia_norm(zonas_gs_v13, "usufructo", "Usufructo")
m5_norm <- mapa_tenencia_norm(zonas_gs_v13, "ocup_irregular", "Ocup. irregular")
m6_norm <- mapa_tenencia_norm(zonas_gs_v13, "poseedor_irregular", "Poseedor irregular")

panel_norm <- (m1_norm + m2_norm + m3_norm + m4_norm + m5_norm + m6_norm) +
  plot_layout(ncol = 3, guides = "collect") &
  theme(legend.position = "right")

panel_norm + plot_annotation(
  title = "Tenencia de la vivienda (microsimulación) · cada categoría normalizada",
  caption = "Fuente: microsimulación CASEN–Censo"
)

