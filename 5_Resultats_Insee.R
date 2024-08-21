# Manipulation, chargement
library(dplyr)
library(tidyr)
library(purrr)
library(stringr)
library(arrow)
options(arrow.use_threads = TRUE)
arrow::set_cpu_count(parallel::detectCores() %/% 4)

# Cartographie
library(sf)
library(ggplot2)
library(viridis)
library(mapsf)
library(cartography) #outdated, use only for waffleLayer
library(gganimate)

# Chemins
path_data <- "X:/HAB-immoclimat/Data/rga_et_swi/"
path_outputs_coffre <- "X:/HAB-immoclimat/Outputs/RGA/"
path_base_intermediaire <- "Z:/Memoire/dv3f_memoire/Bases_intermediaires/"
path_outputs <- "Z:/Memoire/dv3f_memoire/Outputs/"

################################
#### PRESENTATION DU SCRIPT ####
################################

# Ce script adapte les travaux réalisés dans le cadre du stage à l'Insee afin de caractériser les ménages exposés
# au risque RGA, maintenant and dans le futur. 

#########################################
### 1. Gif de projections climatiques ###
#########################################

## 1.1 Traitement de la base
secheresses_projections_long <- arrow::open_dataset(paste0(path_data, "secheresses_projections_long"))

secheresses_projections_long |> 
  summarize(
    n_annees = n_distinct(annee), 
    n_mailles = n_distinct(maille_drias),
    n_scenario = n_distinct(scenario)
  )|> 
  collect()
# 94 * 2 * 8981 = 1,6M

secheresse_annuelle <- secheresses_projections_long |>
  to_duckdb()|>
  group_by(maille_drias, annee, scenario) |>
  mutate(secheresse_annuelle_p25 = max(secheresse_p25)) |> #Seule la sécheresse au sens de 2019 nous intéresse
  to_arrow() |>
  ungroup()|>
  select(-c('horizon', 'saison', 'SSWI_trimestre', 'secheresse_p25')) |>
  compute()

complete <- read.csv("X:/HAB-immoclimat/Data/feu_meteo/grilleSafran_complete_drias2021.csv", sep=";")%>% 
  filter(!is.na(latitude))%>%  
  rename("maille_drias"= "X..idPointDrias",
         "x" = "E.lambert93.m.",
         "y" = "N.lambert93.m.",
         "altitude" ="altitude.non.ponderee.m.")%>% 
  sf::st_as_sf(coords=c("x","y"),crs=2154) %>% 
  btb::btb_ptsToGrid(
    iCellSize=8000,
    sEPSG = 2154)%>% 
  mutate(maille_drias = as.numeric(maille_drias)) %>% 
  select(maille_drias)

secheresse_annuelle_sf <- secheresse_annuelle|>
  collect() |>
  distinct() |>
  left_join(complete, by = 'maille_drias') |>
  sf::st_as_sf(crs=2154) 
st_crs(secheresse_annuelle_sf) 

secheresse_annuelle_sf <- secheresse_annuelle_sf[order(secheresse_annuelle_sf$annee), ]
secheresse_annuelle_sf <- secheresse_annuelle_sf %>%
  mutate(frame_number = match(annee, unique(annee)))

## 1.2 Gif par grille Safran

france_metro <- sf::st_read('X:/HAB-immoclimat/Data/frontieres_admin/francemetro_2023.gpkg')

plot_meteo_p25 <- ggplot(data = secheresse_annuelle_sf) +
  geom_sf(color="white", size=.1, aes(fill=as.factor(secheresse_annuelle_p25))) +
  theme_void() +
  scale_fill_manual(values=c("0" = "lightpink", "1" = "darkred"), 
                    name= NULL, 
                    labels = c("1" = 'Meteorological criterion valid',
                               '0' = 'Meteorological criterion invalid')) +
  theme(legend.position="bottom",
        plot.title=element_text(hjust=0),
        plot.subtitle=element_text(hjust=0),
        plot.caption=element_text(hjust=0)) +
  labs(title="Comparison of drought projections: Soil Wetness Index with return periods of at least 25 years", 
       caption="Source : Drias climate models, Météo France 2020; Insee 2024.") +
  geom_sf(data = france_metro, col = 'black', fill = NA) + 
  facet_wrap(~ scenario)

anim_p25 <- plot_meteo_p25 + 
  transition_manual(frame_number) +
  enter_fade() +
  exit_fade() + 
  labs(subtitle = paste("Validation of the meteorological criterion by Safran grid (8km x 8km) at least once during the year",
                        '{secheresse_annuelle_sf$annee[secheresse_annuelle_sf$frame_number == frame]}'))

gganimate::animate(anim_p25, renderer=gifski_renderer(),width = 1000, height = 400,
                   nframes=100,fps=4)

gganimate::anim_save(paste0(path_outputs, "comparaison_modeles_p25.gif"), animation = last_animation())

# Ce serait plus joli avec les communes ngl. Une autre fois peut être. 

############################################
### 2. Graphe de projections climatiques ###
############################################

# 2.1 Chargement des bases
intersection_sol_meteo <-sf::st_read("X:/HAB-immoclimat/Data/rga_et_swi/intersection_sol_meteo.gpkg") %>% 
  sf::st_transform(crs=2154) %>% 
  select(c('maille_drias', starts_with('nbr_secheresses_p25'), 'NIVEAU', 'ALEA'))
communes <- sf::st_read('X:/HAB-immoclimat/Data/frontieres_admin/commune_francemetro_2023.gpkg')%>% 
  sf::st_transform(crs=2154) %>% 
  select(c('code'))

# 2.2 Calculons les sécheresses pour les communes avec au moins 3 % du territoire communal
communes$area_commune <- sf::st_area(communes)
communes_sol_meteo <- sf::st_intersection(communes,intersection_sol_meteo)

# On calcule les superficies de chaque élément, afin de retirer les communes sans argiles
communes_sol_meteo$area_intersection <- sf::st_area(communes_sol_meteo)

communes_NA <- communes_sol_meteo |>
  sf::st_drop_geometry()|>
  arrow::as_arrow_table()|>
  select(c('code', 'area_commune', 'ALEA', 'area_intersection'))|>
  filter(!is.na(ALEA)) |>
  compute()|>
  to_duckdb()|>
  group_by(code, area_commune)|>
  summarize(share_non_NA = sum(as.numeric(area_intersection), na.rm=TRUE) / as.numeric(area_commune)) |> 
  to_arrow()|>
  collect() |>
  filter(share_non_NA < 0.03) |>
  pull(code)

# Création d'une base avec les sécheresses annuelles par commune
mailles_par_communes <- communes_sol_meteo |> 
  sf::st_drop_geometry()|>
  as_arrow_table()|>
  select(code, maille_drias)|>
  distinct() |>
  compute()

secheresse_annuelle <- arrow::open_dataset(paste0(path_data, "secheresses_projections_long")) |>
  to_duckdb()|>
  group_by(maille_drias, annee, scenario) |>
  mutate(secheresse_annuelle_p25 = max(secheresse_p25, na.rm = TRUE)) |> #Seule la sécheresse au sens de 2019 nous intéresse
  to_arrow() |>
  ungroup()|>
  select(c('maille_drias', 'annee', 'scenario', 'secheresse_annuelle_p25')) |>
  distinct()|>
  compute()

communes_avec_secheresses <- mailles_par_communes |> 
  filter(!(code %in% communes_NA)) |> 
  left_join(secheresse_annuelle, by = 'maille_drias') |>
  # Ici, on a une observation par code x maille x annee x scenario
  compute()|>
  to_duckdb()|>
  group_by(code, scenario, annee)|>
  summarize(secheresse_annuelle_p25 = max(secheresse_annuelle_p25, na.rm = TRUE))|>
  to_arrow()|>
  compute()

# On doit avoir 2 scenarios x 34K communes x 94 ans = environ 6.4M

## 2.3. Préparons le graphe : part de la population exposée par année
base_communale <- readRDS('P:/PSAR_SL/SL45/Lot 3/202401/bases/communes/base_communale.rds') %>% 
  select(c('code' = 'DEPCOM', 'Population_totale')) %>% 
  arrow::as_arrow_table()
population_nationale= base_communale |> summarize(sum(Population_totale)) |> collect()|> pull()

graph_df <- communes_avec_secheresses |>
  left_join(base_communale, by = 'code')|>
  compute()|>
  to_duckdb()|>
  group_by(annee, scenario) |>
  summarize(share_struck = sum(as.numeric(ifelse(secheresse_annuelle_p25, Population_totale, 0)))/population_nationale)|>
  to_arrow()|>
  collect()|>
  mutate(year_group = case_when(
    annee >= 2010 & annee <= 2019 ~ "2010-2019",
    annee >= 2020 & annee <= 2029 ~ "2020-2029",
    annee >= 2030 & annee <= 2039 ~ "2030-2039",
    annee >= 2040 & annee <= 2049 ~ "2040-2049",
    annee >= 2050 & annee <= 2059 ~ "2050-2059",
    annee >= 2060 & annee <= 2069 ~ "2060-2069",
    annee >= 2070 & annee <= 2079 ~ "2070-2079",
    annee >= 2080 & annee <= 2089 ~ "2080-2089",
    annee >= 2090 & annee <= 2099 ~ "2090-2099"
  )) |>
  #On perd 2006-2009
  filter(!is.na(scenario) & !is.na(year_group))|>
  group_by(year_group, scenario)|>
  mutate(average = mean(share_struck)) 

plot_communes <- ggplot(graph_df, aes(x = annee, y = average, color = scenario)) +
  geom_line() +
  scale_color_manual(
    name = 'IPCC Emissions Scenario',
    values = c("RCP8.5" = "#191970","RCP4.5" = "#D68A59"),
    labels = c("RCP4.5" = "RCP 4.5", "RCP8.5" = "RCP 8.5"), 
    breaks = c("RCP8.5", "RCP4.5")
  ) +
  scale_x_continuous(limits = c(2010, 2100), n.breaks = 10) +
  scale_y_continuous(labels = scales::percent_format(scale = 100)) +
  theme_minimal() +
  theme(
    legend.position = "right",
    plot.caption = element_text(hjust = 0)
  ) +
  labs(
    title = "With current rules, share eligible for natural disaster recognition is set to soar",
    subtitle = "Share of the current French population living in municipalities with at least 3% clay soils and experiencing at least one abnormal\ndrought per year (10-year average)",
    caption = paste0(str_wrap("Note: A drought is considered abnormal when its return period exceeds 25 years based on historical data. The actual criterion for disaster relief relies on observed data to establish a reference period, so the share of the population affected will likely be lower, even under current rules. Averages over 10 years are used to smooth model volatility.", width = 160),
                     "\nSource: Drought projections, Drias 2020, Météo France; 2021 Census, Insee; Insee 2024"),
    x = "Decade",
    y = "Share living in exposed municipality"
  )
print(plot_communes)

ggsave(paste0(path_outputs, "communes_projections.pdf"),
       plot = plot_communes, width = 10.7, height = 5.5, dpi = 300)

plot_communes_no_title <- ggplot(graph_df, aes(x = annee, y = average, color = scenario)) +
  geom_line() +
  scale_color_manual(
    name = 'IPCC Emissions Scenario',
    values = c("RCP8.5" = "#191970","RCP4.5" = "#D68A59"),
    labels = c("RCP4.5" = "RCP 4.5", "RCP8.5" = "RCP 8.5"), 
    breaks = c("RCP8.5", "RCP4.5")
  ) +
  scale_x_continuous(limits = c(2010, 2100), n.breaks = 10) +
  scale_y_continuous(labels = scales::percent_format(scale = 100)) +
  theme_minimal() +
  theme(
    legend.position = "right",
    plot.caption = element_text(hjust = 0)
  ) +
  labs(
    title = NULL,
    subtitle = NULL,
    caption = paste0(str_wrap("Note: A drought is considered abnormal when its return period exceeds 25 years based on historical data. The actual criterion for disaster relief relies on observed data to establish a reference period, so the share of the population affected will likely be lower, even under current rules. Averages over 10 years are used to smooth model volatility.", width = 160),
                     "\nSource: Drought projections, Drias 2020, Météo France; 2021 Census, Insee; Insee 2024"),
    x = "Decade",
    y = "Share living in exposed municipality"
  )
print(plot_communes_no_title)

ggsave(paste0(path_outputs, "communes_projections_sans_titre.pdf"),
       plot = plot_communes_no_title, width = 10.7, height = 5.5, dpi = 300)

# Ici, sera utile pour commenter la salience ! 

######################################
### 3. Graphe des maisons exposées ###
######################################

## 3.1 Calcul des parts départementales en RP 25
table_locaux_enrichie_utile <- arrow::open_dataset(
  "X:/HAB-immoclimat/Data/rga_et_swi/table_locaux_enrichie_utile",
) # lorsqu'on ne regarde que les maisons (avec un filtre hexagonal)

compte_par_dept <- table_locaux_enrichie_utile |> #i.e. s'applique aux maisons individuelles hors outre mer
  to_duckdb() |>
  group_by(csdep) |>
  summarize(
    # Total départemental
    total_logements = n(),
    
    # RCP4.5 no risk (i.e. pas d'argile, ou pas de sécheresse)
    logement_sauf_p25_ref_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP4_5 == 0 | is.na(NIVEAU)), na.rm = TRUE),
    logement_sauf_p25_H1_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP4_5 == 0 | is.na(NIVEAU)), na.rm = TRUE),
    logement_sauf_p25_H2_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP4_5 == 0 | is.na(NIVEAU)), na.rm = TRUE),
    logement_sauf_p25_H3_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP4_5 == 0 | is.na(NIVEAU)), na.rm = TRUE),
    
    # RCP8.5 no risk (i.e. pas d'argile, ou pas de sécheresse)
    logement_sauf_p25_ref_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP8_5 == 0 | is.na(NIVEAU)), na.rm = TRUE),
    logement_sauf_p25_H1_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP8_5 == 0 | is.na(NIVEAU)), na.rm = TRUE),
    logement_sauf_p25_H2_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP8_5 == 0 | is.na(NIVEAU)), na.rm = TRUE),
    logement_sauf_p25_H3_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP8_5 == 0 | is.na(NIVEAU)), na.rm = TRUE),
    
    # RCP4.5 exactly 1 drought
    exactly_1_p25_ref_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP4_5 == 1 & NIVEAU >= 1), na.rm = TRUE),
    exactly_1_p25_H1_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP4_5 == 1 & NIVEAU >= 1), na.rm = TRUE),
    exactly_1_p25_H2_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP4_5 == 1 & NIVEAU >= 1), na.rm = TRUE),
    exactly_1_p25_H3_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP4_5 == 1 & NIVEAU >= 1), na.rm = TRUE),
    
    # RCP8.5 exactly 1 drought
    exactly_1_p25_ref_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP8_5 == 1 & NIVEAU >= 1), na.rm = TRUE),
    exactly_1_p25_H1_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP8_5 == 1 & NIVEAU >= 1), na.rm = TRUE),
    exactly_1_p25_H2_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP8_5 == 1 & NIVEAU >= 1), na.rm = TRUE),
    exactly_1_p25_H3_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP8_5 == 1 & NIVEAU >= 1), na.rm = TRUE),
    
    # RCP4.5 between 2 and 7 droughts
    between_2_and_7_p25_ref_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP4_5 >= 2 & nbr_secheresses_p25_ref_RCP4_5 <= 7 & NIVEAU >= 1), na.rm = TRUE),
    between_2_and_7_p25_H1_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP4_5 >= 2 & nbr_secheresses_p25_H1_RCP4_5 <= 7 & NIVEAU >= 1), na.rm = TRUE),
    between_2_and_7_p25_H2_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP4_5 >= 2 & nbr_secheresses_p25_H2_RCP4_5 <= 7 & NIVEAU >= 1), na.rm = TRUE),
    between_2_and_7_p25_H3_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP4_5 >= 2 & nbr_secheresses_p25_H3_RCP4_5 <= 7 & NIVEAU >= 1), na.rm = TRUE),
    
    # RCP8.5 between 2 and 7 droughts
    between_2_and_7_p25_ref_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP8_5 >= 2 & nbr_secheresses_p25_ref_RCP8_5 <= 7 & NIVEAU >= 1), na.rm = TRUE),
    between_2_and_7_p25_H1_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP8_5 >= 2 & nbr_secheresses_p25_H1_RCP8_5 <= 7 & NIVEAU >= 1), na.rm = TRUE),
    between_2_and_7_p25_H2_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP8_5 >= 2 & nbr_secheresses_p25_H2_RCP8_5 <= 7 & NIVEAU >= 1), na.rm = TRUE),
    between_2_and_7_p25_H3_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP8_5 >= 2 & nbr_secheresses_p25_H3_RCP8_5 <= 7 & NIVEAU >= 1), na.rm = TRUE),
    
    # RCP4.5 between 8 and 15 droughts
    between_8_and_15_p25_ref_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP4_5 >= 8 & nbr_secheresses_p25_ref_RCP4_5 <= 15 & NIVEAU >= 1), na.rm = TRUE),
    between_8_and_15_p25_H1_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP4_5 >= 8 & nbr_secheresses_p25_H1_RCP4_5 <= 15 & NIVEAU >= 1), na.rm = TRUE),
    between_8_and_15_p25_H2_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP4_5 >= 8 & nbr_secheresses_p25_H2_RCP4_5 <= 15 & NIVEAU >= 1), na.rm = TRUE),
    between_8_and_15_p25_H3_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP4_5 >= 8 & nbr_secheresses_p25_H3_RCP4_5 <= 15 & NIVEAU >= 1), na.rm = TRUE),
    
    # RCP8.5 between 8 and 15 droughts
    between_8_and_15_p25_ref_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP8_5 >= 8 & nbr_secheresses_p25_ref_RCP8_5 <= 15 & NIVEAU >= 1), na.rm = TRUE),
    between_8_and_15_p25_H1_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP8_5 >= 8 & nbr_secheresses_p25_H1_RCP8_5 <= 15 & NIVEAU >= 1), na.rm = TRUE),
    between_8_and_15_p25_H2_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP8_5 >= 8 & nbr_secheresses_p25_H2_RCP8_5 <= 15 & NIVEAU >= 1), na.rm = TRUE),
    between_8_and_15_p25_H3_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP8_5 >= 8 & nbr_secheresses_p25_H3_RCP8_5 <= 15 & NIVEAU >= 1), na.rm = TRUE),
    
    # RCP4.5 above 16 droughts
    above_16_p25_ref_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP4_5 >= 16 & NIVEAU >= 1), na.rm = TRUE),
    above_16_p25_H1_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP4_5 >= 16 & NIVEAU >= 1), na.rm = TRUE),
    above_16_p25_H2_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP4_5 >= 16 & NIVEAU >= 1), na.rm = TRUE),
    above_16_p25_H3_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP4_5 >= 16 & NIVEAU >= 1), na.rm = TRUE),
    
    # RCP8.5 above 16 droughts
    above_16_p25_ref_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP8_5 >= 16 & NIVEAU >= 1), na.rm = TRUE),
    above_16_p25_H1_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP8_5 >= 16 & NIVEAU >= 1), na.rm = TRUE),
    above_16_p25_H2_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP8_5 >= 16 & NIVEAU >= 1), na.rm = TRUE),
    above_16_p25_H3_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP8_5 >= 16 & NIVEAU >= 1), na.rm = TRUE)

  ) |>
  ungroup() |>
  to_arrow() |>
  collect()

# Par rapport à celui de l'Insee : on a retiré la catégorie 30+, car très rare en RP 25

departements <- sf::st_read("X:/HAB-immoclimat/Data/frontieres_admin/dep_francemetro_2023.gpkg")

# On déplace Paris et la petite couronne : 
departements_hors_petite_couronne <- sf::st_read("X:/HAB-immoclimat/Data/frontieres_admin/dep_francemetro_2023.gpkg") %>% 
  filter(!(code %in% c('75', '92', '93', '94')))

zoom_petite_couronne <- sf::st_read("X:/HAB-immoclimat/Data/frontieres_admin/dep_francemetro_2023.gpkg") %>% 
  filter(code %in% c('75', '92', '93', '94')) %>% 
  mutate(the_geom = the_geom * 3.5 + c(-1104506,-17000235)) %>% 
  st_set_crs(2154)

departements_avec_zoom <- rbind(departements_hors_petite_couronne,zoom_petite_couronne)

compte_par_dept_filled <-compte_par_dept %>% 
  left_join(departements_avec_zoom, by = c("csdep"="code")) %>% 
  sf::st_as_sf()

# On prépare le cadre autour du zoom
bbox <- st_bbox(zoom_petite_couronne)
padding <- 12000
coords <- c(bbox[1] - padding, bbox[2] - padding, bbox[3] + padding, bbox[4] + padding)

plot(departements_avec_zoom)

saveRDS(compte_par_dept_filled, paste0(path_base_intermediaire, '3_compte_par_dept_filled.RDS'))
saveRDS(coords, paste0(path_base_intermediaire, '3_coords.RDS'))

## 3.2 Création des cartes

compte_par_dept_filled <- readRDS(paste0(path_base_intermediaire, '3_compte_par_dept_filled.RDS'))
coords <- readRDS(paste0(path_base_intermediaire, '3_coords.RDS'))

# Fonction pour créer les cartes départementales
create_map <- function(var1, var2, var3, var4, var5, filename, period, scenario) {
  title_period <- switch(period,
                         "ref" = "between 2005 and 2034",
                         "H1" = "between 2021 and 2050",
                         "H2" = "between 2041 and 2070",
                         "H3" = "between 2071 and 2100"
  )
  title_scenario <- switch(scenario, 
                           "4_5" = "according to the IPCC's 4.5 Representative Concentration Pathway",
                           "8_5" = "according to the IPCC's 8.5 Representative Concentration Pathway"
  )

  png(filename = paste0(path_outputs, filename),  width = 1450, height = 1550, res = 150)
  plot(st_geometry(compte_par_dept_filled),
       col = "white",
       border = "black",
       lwd = 0.5)
  # Carré du zoom sur l'IDF
  graphics::rect(coords[1], coords[2], coords[3],coords[4], border = "black", lwd = 2, lty = 3)
  waffleLayer(
    x = compte_par_dept_filled,
    var = c(var1, var2, var3, var4, var5),
    cellvalue = 20000,
    cellsize = 10000,
    cellrnd = "ceiling",
    celltxt = "\nReading note: 1 square for 20000 houses",
    labels = c("No abnormal drought or house outside clay patches", "1 abnormal drought in 30 years", "Between 2 and 7 abnormal droughts (every 4 years or less)",
               "Between 8 and 15 abnormal droughts (every 2 to 4 years)", "16+ abnormal droughts (every other year or more)"),
    ncols = 6,
    col = c("#FDF0C9", "#F4A582", "#EF5A33", "#A50026", "black"),
    border = "black",
    legend.pos = "topleft",
    legend.title.cex = 0.8,
    legend.title.txt = paste("House count per number of abnormal droughts", title_period, title_scenario),
    legend.values.cex = 0.6,
    add = TRUE
  )
  
  layoutLayer(
    title = "Houses exposed to geotechnical droughts likely to trigger shrink-swell by departement",
    col = "black",
    tabtitle = TRUE,
    scale = FALSE,
    sources = "Sources: Drought projections, Météo France, 2020; RGA Exposure Map, BRGM, 2019; Fidéli, 2017, Insee; Insee 2024.",
    author = "Definition: Droughts are defined by quarters, and are abnormal when the soil wetness indicator has a return period of at least 25 years. The return period is defined over past data, and not model\noutputs. Only houses built over clay patches are considered exposed to geotechnical droughts likely to trigger shrink-swell."
  )
  
  dev.off()
}

## Cartes générées : 4 horizons * 2 scénarios
# RCP 4.5
create_map("logement_sauf_p25_ref_RCP4_5", "exactly_1_p25_ref_RCP4_5", "between_2_and_7_p25_ref_RCP4_5", "between_8_and_15_p25_ref_RCP4_5", "above_16_p25_ref_RCP4_5", "Carte_dept_p25_4_5_ref.png", "ref", "4_5")
create_map("logement_sauf_p25_H1_RCP4_5", "exactly_1_p25_H1_RCP4_5", "between_2_and_7_p25_H1_RCP4_5", "between_8_and_15_p25_H1_RCP4_5", "above_16_p25_H1_RCP4_5","Carte_dept_p25_4_5_H1.png", "H1", "4_5")
create_map("logement_sauf_p25_H2_RCP4_5", "exactly_1_p25_H2_RCP4_5", "between_2_and_7_p25_H2_RCP4_5", "between_8_and_15_p25_H2_RCP4_5", "above_16_p25_H2_RCP4_5",  "Carte_dept_p25_4_5_H2.png", "H2", "4_5")
create_map("logement_sauf_p25_H3_RCP4_5", "exactly_1_p25_H3_RCP4_5", "between_2_and_7_p25_H3_RCP4_5", "between_8_and_15_p25_H3_RCP4_5", "above_16_p25_H3_RCP4_5", "Carte_dept_p25_4_5_H3.png", "H3", "4_5")

# RCP 8.5
create_map("logement_sauf_p25_ref_RCP8_5", "exactly_1_p25_ref_RCP8_5", "between_2_and_7_p25_ref_RCP8_5", "between_8_and_15_p25_ref_RCP8_5", "above_16_p25_ref_RCP8_5","Carte_dept_p25_8_5_ref.png", "ref", "8_5")
create_map("logement_sauf_p25_H1_RCP8_5", "exactly_1_p25_H1_RCP8_5", "between_2_and_7_p25_H1_RCP8_5", "between_8_and_15_p25_H1_RCP8_5", "above_16_p25_H1_RCP8_5", "Carte_dept_p25_8_5_H1.png", "H1", "8_5")
create_map("logement_sauf_p25_H2_RCP8_5", "exactly_1_p25_H2_RCP8_5", "between_2_and_7_p25_H2_RCP8_5", "between_8_and_15_p25_H2_RCP8_5", "above_16_p25_H2_RCP8_5", "Carte_dept_p25_8_5_H2.png", "H2", "8_5")
create_map("logement_sauf_p25_H3_RCP8_5", "exactly_1_p25_H3_RCP8_5", "between_2_and_7_p25_H3_RCP8_5", "between_8_and_15_p25_H3_RCP8_5", "above_16_p25_H3_RCP8_5", "Carte_dept_p25_8_5_H3.png", "H3", "8_5")

## Pour les commentaires:on veut comparer le nombre médian à chaque horizon, et la part de maisons avec au moins 1 et avec au moins 16

medians <- table_locaux_enrichie_utile |> #i.e. s'applique aux maisons individuelles hors outre mer
  summarize(
    median_ref_8_5 = median(as.numeric(nbr_secheresses_p25_ref_RCP8_5), na.rm = TRUE),
    median_H1_8_5 = median(as.numeric(nbr_secheresses_p25_H1_RCP8_5), na.rm = TRUE),
    median_H2_8_5 = median(as.numeric(nbr_secheresses_p25_H2_RCP8_5), na.rm = TRUE),
    median_H3_8_5 = median(as.numeric(nbr_secheresses_p25_H3_RCP8_5), na.rm = TRUE),
    at_least_1_ref_8_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP8_5>=1 & NIVEAU>=1), na.rm = TRUE),
    at_least_1_H1_8_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP8_5>=1 & NIVEAU>=1), na.rm = TRUE),
    at_least_1_H2_8_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP8_5>=1 & NIVEAU>=1), na.rm = TRUE),
    at_least_1_H3_8_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP8_5>=1 & NIVEAU>=1), na.rm = TRUE),
    at_least_16_ref_8_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP8_5>=16 & NIVEAU>=1), na.rm = TRUE),
    at_least_16_H1_8_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP8_5>=16 & NIVEAU>=1), na.rm = TRUE),
    at_least_16_H2_8_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP8_5>=16 & NIVEAU>=1), na.rm = TRUE),
    at_least_16_H3_8_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP8_5>=16 & NIVEAU>=1), na.rm = TRUE)
    ) |>
  collect()

saveRDS(medians, paste0(path_outputs, 'compte_maisons_pour_commentaire.RDS'))

# Points intéressants : médiane à 0 pour l'ensemble des horizons, sauf 2070-2100 (>4), puis commenter le % de multiplication des >1 (généralisation)
# et des >16 (intensifications)

medians <- table_locaux_enrichie_utile |> #i.e. s'applique aux maisons individuelles hors outre mer
  summarize(
    median_ref_8_5 = median(as.numeric(nbr_secheresses_p25_ref_RCP8_5), na.rm = TRUE),
    median_H1_8_5 = median(as.numeric(nbr_secheresses_p25_H1_RCP8_5), na.rm = TRUE),
    median_H2_8_5 = median(as.numeric(nbr_secheresses_p25_H2_RCP8_5), na.rm = TRUE),
    median_H3_8_5 = median(as.numeric(nbr_secheresses_p25_H3_RCP8_5), na.rm = TRUE),
    at_least_1_ref_8_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP8_5>=1 & NIVEAU>=1), na.rm = TRUE),
    at_least_1_H1_8_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP8_5>=1 & NIVEAU>=1), na.rm = TRUE),
    at_least_1_H2_8_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP8_5>=1 & NIVEAU>=1), na.rm = TRUE),
    at_least_1_H3_8_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP8_5>=1 & NIVEAU>=1), na.rm = TRUE),
    at_least_16_ref_8_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP8_5>=16 & NIVEAU>=1), na.rm = TRUE),
    at_least_16_H1_8_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP8_5>=16 & NIVEAU>=1), na.rm = TRUE),
    at_least_16_H2_8_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP8_5>=16 & NIVEAU>=1), na.rm = TRUE),
    at_least_16_H3_8_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP8_5>=16 & NIVEAU>=1), na.rm = TRUE)
  ) |>
  collect()

######################################
### 4. Niveau de vie et exposition ###
######################################

# 4.1 Création des variables en RP 25 et appariemment

#### 1.1 Chargement des bases #### 
table_locaux_enrichie_utile <- arrow::open_dataset(
  "X:/HAB-immoclimat/Data/rga_et_swi/table_locaux_enrichie_utile",
) # lorsqu'on ne regarde que les maisons (avec un filtre hexagonal)

locaux_exposes <- table_locaux_enrichie_utile |>
  select(id_log, NIVEAU,
         nbr_secheresses_p25_ref_RCP4_5, nbr_secheresses_p25_H1_RCP4_5,
         nbr_secheresses_p25_H2_RCP4_5, nbr_secheresses_p25_H3_RCP4_5,
         nbr_secheresses_p25_ref_RCP8_5, nbr_secheresses_p25_H1_RCP8_5, 
         nbr_secheresses_p25_H2_RCP8_5, nbr_secheresses_p25_H3_RCP8_5)|>
  compute() #200K duplications d'id_log, qu'il faudrait traiter..

table_menages <- arrow::open_dataset(
  "X:/HAB-immoclimat/Data/fideli/table_menages",
) 

#### 1.2 Left join ménage ####
menages_enrichis_RGA_RP <- table_menages |>
  left_join(
    y=locaux_exposes, 
    by = "id_log"
  ) |>
  compute()

menages_enrichis_RGA_RP <- menages_enrichis_RGA_RP |>
  mutate(nbr_secheresses_p25_ref_RCP4_5_RP = nbr_secheresses_p25_ref_RCP4_5,
         nbr_secheresses_p25_H1_RCP4_5_RP = nbr_secheresses_p25_H1_RCP4_5,
         nbr_secheresses_p25_H2_RCP4_5_RP = nbr_secheresses_p25_H2_RCP4_5,
         nbr_secheresses_p25_H3_RCP4_5_RP = nbr_secheresses_p25_H3_RCP4_5,
         nbr_secheresses_p25_ref_RCP8_5_RP = nbr_secheresses_p25_ref_RCP8_5,
         nbr_secheresses_p25_H1_RCP8_5_RP = nbr_secheresses_p25_H1_RCP8_5,
         nbr_secheresses_p25_H2_RCP8_5_RP = nbr_secheresses_p25_H2_RCP8_5,
         nbr_secheresses_p25_H3_RCP8_5_RP = nbr_secheresses_p25_H3_RCP8_5,
         NIVEAU_RP = NIVEAU) |>
  select(-c(nbr_secheresses_p25_ref_RCP4_5, nbr_secheresses_p25_H1_RCP4_5,
            nbr_secheresses_p25_H2_RCP4_5, nbr_secheresses_p25_H3_RCP4_5, 
            nbr_secheresses_p25_ref_RCP8_5, nbr_secheresses_p25_H1_RCP8_5, 
            nbr_secheresses_p25_H2_RCP8_5, nbr_secheresses_p25_H3_RCP8_5,
            NIVEAU)) |>
  compute()
gc()

# Puis RS1 :  (468 323) #NOTE : les résidences secondaires n'ont pas été utilisées ici, mais ça s'ajoute à moindre frais
menages_RGA_RS1 <- table_menages |>
  inner_join(
    y=locaux_exposes, 
    by = join_by("id_log_sec1"=="id_log")
  ) |>
  compute()

menages_RGA_RS1 <- menages_RGA_RS1|>
  mutate(nbr_secheresses_p25_ref_RCP4_5_RS1 = nbr_secheresses_p25_ref_RCP4_5,
         nbr_secheresses_p25_H1_RCP4_5_RS1 = nbr_secheresses_p25_H1_RCP4_5,
         nbr_secheresses_p25_H2_RCP4_5_RS1 = nbr_secheresses_p25_H2_RCP4_5,
         nbr_secheresses_p25_H3_RCP4_5_RS1 = nbr_secheresses_p25_H3_RCP4_5,
         nbr_secheresses_p25_ref_RCP8_5_RS1 = nbr_secheresses_p25_ref_RCP8_5,
         nbr_secheresses_p25_H1_RCP8_5_RS1 = nbr_secheresses_p25_H1_RCP8_5,
         nbr_secheresses_p25_H2_RCP8_5_RS1 = nbr_secheresses_p25_H2_RCP8_5,
         nbr_secheresses_p25_H3_RCP8_5_RS1 = nbr_secheresses_p25_H3_RCP8_5,
         #depcom_RS1 = depcom_geo, (je l'ai retiré, mais éventuellement considérer)
         NIVEAU_RS1 = NIVEAU) |>
  select(-c(nbr_secheresses_p25_ref_RCP4_5, nbr_secheresses_p25_H1_RCP4_5,
            nbr_secheresses_p25_H2_RCP4_5, nbr_secheresses_p25_H3_RCP4_5, 
            nbr_secheresses_p25_ref_RCP8_5, nbr_secheresses_p25_H1_RCP8_5, 
            nbr_secheresses_p25_H2_RCP8_5, nbr_secheresses_p25_H3_RCP8_5, 
            NIVEAU)) |>
  compute()
gc()

# Puis RS2 : (41 723)
menages_RGA_RS2 <- table_menages |>
  inner_join(
    y=locaux_exposes, 
    by = join_by("id_log_sec2"=="id_log")
  ) |>
  compute()

menages_RGA_RS2 <- menages_RGA_RS2|>
  mutate(nbr_secheresses_p25_ref_RCP4_5_RS2 = nbr_secheresses_p25_ref_RCP4_5,
         nbr_secheresses_p25_H1_RCP4_5_RS2 = nbr_secheresses_p25_H1_RCP4_5,
         nbr_secheresses_p25_H2_RCP4_5_RS2 = nbr_secheresses_p25_H2_RCP4_5,
         nbr_secheresses_p25_H3_RCP4_5_RS2 = nbr_secheresses_p25_H3_RCP4_5,
         nbr_secheresses_p25_ref_RCP8_5_RS2 = nbr_secheresses_p25_ref_RCP8_5,
         nbr_secheresses_p25_H1_RCP8_5_RS2 = nbr_secheresses_p25_H1_RCP8_5,
         nbr_secheresses_p25_H2_RCP8_5_RS2 = nbr_secheresses_p25_H2_RCP8_5,
         nbr_secheresses_p25_H3_RCP8_5_RS2 = nbr_secheresses_p25_H3_RCP8_5,
         NIVEAU_RS2 = NIVEAU) |>
  select(-c(nbr_secheresses_p25_ref_RCP4_5, nbr_secheresses_p25_H1_RCP4_5,
            nbr_secheresses_p25_H2_RCP4_5, nbr_secheresses_p25_H3_RCP4_5, 
            nbr_secheresses_p25_ref_RCP8_5, nbr_secheresses_p25_H1_RCP8_5, 
            nbr_secheresses_p25_H2_RCP8_5, nbr_secheresses_p25_H3_RCP8_5,
            NIVEAU)) |>
  compute()
gc()

## On forme une table unique
menages_enrichis_RGA <- menages_enrichis_RGA_RP |>
  left_join(menages_RGA_RS1 %>% select(contains("RS1"), id_log, id_log2016),
            by = c("id_log", "id_log2016")) |>
  left_join(menages_RGA_RS2 %>% select(contains("RS2"), id_log, id_log2016),
            by = c("id_log", "id_log2016")) |>
  mutate(
    RS1 = ifelse(!is.na(id_log_sec1), 1, 0),
    RS2 = ifelse(!is.na(id_log_sec2), 1, 0),
    exposition_RGA_RS = case_when(
      (NIVEAU_RS1 ==1 & NIVEAU_RS2 == 1) ~ "2",
      (NIVEAU_RS1 ==1 | NIVEAU_RS2 == 1) ~ "1",
      (RS1 == 1 | RS2 == 1) ~ "0",
      (RS1 == 0 | RS2 == 0) ~ "NA")
  ) |> 
  compute()
gc()

# 4.2 Calcul des parts par centiles de niveau de vie
df_nv_vie_exposition_ref <- menages_enrichis_RGA |>
  to_duckdb()|>
  group_by(centieme)|>
  summarise(faible = sum(as.numeric(NIVEAU_RP==1))/n(), 
            moyenne = sum(as.numeric(NIVEAU_RP==2))/n(),
            forte = sum(as.numeric(NIVEAU_RP==3))/n(),
            non_expose = sum(as.numeric(is.na(NIVEAU_RP)))/n())|>
  collect()|>
  arrange(centieme)
saveRDS(df_nv_vie_exposition_ref, paste0(path_base_intermediaire, 'df_nv_vie_exposition_ref.RDS'))

df_nv_vie_menaces <- menages_enrichis_RGA |>
  to_duckdb()|>
  group_by(centieme)|>
  summarise(
    menage_sauf_p25_ref_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP4_5_RP == 0 | is.na(NIVEAU_RP)), na.rm = TRUE)/n(),
    menage_sauf_p25_H1_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP4_5_RP == 0 | is.na(NIVEAU_RP)), na.rm = TRUE)/n(),
    menage_sauf_p25_H2_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP4_5_RP == 0 | is.na(NIVEAU_RP)), na.rm = TRUE)/n(),
    menage_sauf_p25_H3_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP4_5_RP == 0 | is.na(NIVEAU_RP)), na.rm = TRUE)/n(),
    
    # RCP8.5 no risk (i.e. pas d'argile, ou pas de sécheresse)
    menage_sauf_p25_ref_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP8_5_RP == 0 | is.na(NIVEAU_RP)), na.rm = TRUE)/n(),
    menage_sauf_p25_H1_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP8_5_RP == 0 | is.na(NIVEAU_RP)), na.rm = TRUE)/n(),
    menage_sauf_p25_H2_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP8_5_RP == 0 | is.na(NIVEAU_RP)), na.rm = TRUE)/n(),
    menage_sauf_p25_H3_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP8_5_RP == 0 | is.na(NIVEAU_RP)), na.rm = TRUE)/n(),
    
    # RCP4.5 exactly 1 drought
    exactly_1_p25_ref_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP4_5_RP == 1 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    exactly_1_p25_H1_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP4_5_RP == 1 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    exactly_1_p25_H2_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP4_5_RP == 1 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    exactly_1_p25_H3_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP4_5_RP == 1 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    
    # RCP8.5 exactly 1 drought
    exactly_1_p25_ref_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP8_5_RP == 1 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    exactly_1_p25_H1_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP8_5_RP == 1 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    exactly_1_p25_H2_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP8_5_RP == 1 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    exactly_1_p25_H3_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP8_5_RP == 1 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    
    # RCP4.5 between 2 and 7 droughts
    between_2_and_7_p25_ref_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP4_5_RP >= 2 & nbr_secheresses_p25_ref_RCP4_5_RP <= 7 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    between_2_and_7_p25_H1_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP4_5_RP >= 2 & nbr_secheresses_p25_H1_RCP4_5_RP <= 7 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    between_2_and_7_p25_H2_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP4_5_RP >= 2 & nbr_secheresses_p25_H2_RCP4_5_RP <= 7 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    between_2_and_7_p25_H3_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP4_5_RP >= 2 & nbr_secheresses_p25_H3_RCP4_5_RP <= 7 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    
    # RCP8.5 between 2 and 7 droughts
    between_2_and_7_p25_ref_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP8_5_RP >= 2 & nbr_secheresses_p25_ref_RCP8_5_RP <= 7 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    between_2_and_7_p25_H1_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP8_5_RP >= 2 & nbr_secheresses_p25_H1_RCP8_5_RP <= 7 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    between_2_and_7_p25_H2_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP8_5_RP >= 2 & nbr_secheresses_p25_H2_RCP8_5_RP <= 7 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    between_2_and_7_p25_H3_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP8_5_RP >= 2 & nbr_secheresses_p25_H3_RCP8_5_RP <= 7 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    
    # RCP4.5 between 8 and 15 droughts
    between_8_and_15_p25_ref_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP4_5_RP >= 8 & nbr_secheresses_p25_ref_RCP4_5_RP <= 15 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    between_8_and_15_p25_H1_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP4_5_RP >= 8 & nbr_secheresses_p25_H1_RCP4_5_RP <= 15 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    between_8_and_15_p25_H2_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP4_5_RP >= 8 & nbr_secheresses_p25_H2_RCP4_5_RP <= 15 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    between_8_and_15_p25_H3_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP4_5_RP >= 8 & nbr_secheresses_p25_H3_RCP4_5_RP <= 15 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    
    # RCP8.5 between 8 and 15 droughts
    between_8_and_15_p25_ref_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP8_5_RP >= 8 & nbr_secheresses_p25_ref_RCP8_5_RP <= 15 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    between_8_and_15_p25_H1_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP8_5_RP >= 8 & nbr_secheresses_p25_H1_RCP8_5_RP <= 15 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    between_8_and_15_p25_H2_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP8_5_RP >= 8 & nbr_secheresses_p25_H2_RCP8_5_RP <= 15 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    between_8_and_15_p25_H3_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP8_5_RP >= 8 & nbr_secheresses_p25_H3_RCP8_5_RP <= 15 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    
    # RCP4.5 above 16
    above_16_p25_ref_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP4_5_RP >= 16 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    above_16_p25_H1_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP4_5_RP >= 16 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    above_16_p25_H2_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP4_5_RP >= 16 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    above_16_p25_H3_RCP4_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP4_5_RP >= 16 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    
    # RCP8.5 above 16
    above_16_p25_ref_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_ref_RCP8_5_RP >= 16 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    above_16_p25_H1_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H1_RCP8_5_RP >= 16 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    above_16_p25_H2_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H2_RCP8_5_RP >= 16 & NIVEAU_RP >= 1), na.rm = TRUE)/n(),
    above_16_p25_H3_RCP8_5 = sum(as.numeric(nbr_secheresses_p25_H3_RCP8_5_RP >= 16 & NIVEAU_RP >= 1), na.rm = TRUE)/n()
  )|>
  collect()|>
  arrange(centieme)

saveRDS(df_nv_vie_menaces, paste0(path_base_intermediaire, 'df_nv_vie_menaces.RDS'))

# 4.3 Graphes
bases_quarto <- "X:/HAB-immoclimat/Outputs/RGA/bases_quarto/"
df_nv_vie_menaces <- readRDS(paste0(path_base_intermediaire, 'df_nv_vie_menaces.RDS'))
df_nv_vie_exposition_ref <- readRDS(paste0(bases_quarto, '6_df_nv_vie_exposition_ref.RDS'))

## En statique : 
graphe_nv_vie_ref <- ggplot(df_nv_vie_exposition_ref, aes(x = centieme)) +
  geom_area(aes(y = faible + moyenne + forte + non_expose, fill = "Not Exposed")) +
  geom_area(aes(y = forte + moyenne + faible, fill = "Low")) +
  geom_area(aes(y = forte + moyenne, fill = "Medium")) +
  geom_area(aes(y = forte, fill = "High")) +
  scale_fill_manual(values = c("Low" = "#8DB0E1", "Medium" = "#286AC7", "High" = "#0F417A", "Not Exposed" = "#FFE58E"),
                    labels = c("Not Exposed" = "Dwelling not exposed", "Low" = "House with low exposure", "Medium" = "House with medium exposure", "High" = "House with high exposure"),
                    breaks = c("Not Exposed", "Low", "Medium", "High"),
                    name = stringr::str_wrap("Exposure Level", width = 25)) +
  theme_minimal() +
  theme(legend.position = "right",
        plot.caption = element_text(hjust = 0), 
        plot.title.position = "plot",
        plot.caption.position = "plot",
        panel.grid = element_blank(),
        axis.text.x = element_text(margin = margin(-10, 0, 0, 15)),
        axis.text.y = element_text(margin = margin(0, -20, 0, 0))) + 
  scale_y_continuous(breaks = seq(0.2, 1, by = 0.2), labels = scales::percent_format()) +
  scale_x_continuous(breaks = seq(0, 100, by = 10)) +
  labs(x = "Standard of Living Percentile", y = NULL, 
       title = "Current Exposure of Households' Main Residence to Shrink-Swell in Metropolitan France",
       subtitle = stringr::str_wrap("Distribution of households by standard of living and exposure to shrink-swell based on the clay shrinkage and swelling exposure map (BRGM 2019)", width = 150),
       caption = paste("All flats and houses located outside of clay patches are considered not exposed to shrink-swell.\nSources: Shrink-Swell Exposure Mapping, BRGM 2019; Fidéli 2017, Insee; Insee 2024"))

# Save the plot
ggsave(paste0(path_outputs, "graphe_nv_vie_ref.pdf"), plot = graphe_nv_vie_ref, width = 10.7, height = 4.8, dpi = 400)

# Display the plot
graphe_nv_vie_ref

## En dynamique :

### 8.5
graphe_nv_vie_8_5_ref <- ggplot(df_nv_vie_menaces, aes(x = centieme)) +
  geom_area(aes(y = menage_sauf_p25_ref_RCP8_5 + exactly_1_p25_ref_RCP8_5 + between_2_and_7_p25_ref_RCP8_5 +
                  between_8_and_15_p25_ref_RCP8_5 + above_16_p25_ref_RCP8_5, fill = "Safe Housing")) +
  geom_area(aes(y = exactly_1_p25_ref_RCP8_5 + between_2_and_7_p25_ref_RCP8_5 +
                  between_8_and_15_p25_ref_RCP8_5 + above_16_p25_ref_RCP8_5, fill = "1 drought")) +
  geom_area(aes(y = between_2_and_7_p25_ref_RCP8_5 +
                  between_8_and_15_p25_ref_RCP8_5 + above_16_p25_ref_RCP8_5, fill = "2 to 7 droughts")) +
  geom_area(aes(y = between_8_and_15_p25_ref_RCP8_5 + above_16_p25_ref_RCP8_5, fill = "8 to 15 droughts")) +
  geom_area(aes(y = above_16_p25_ref_RCP8_5, fill = "At least 16 droughts")) +
  scale_fill_manual(values = c("1 drought" = "#8DB0E1", "2 to 7 droughts" = "#5F8DC9",
                               "8 to 15 droughts" = "#0F417A","At least 16 droughts" = "black", 
                               "Safe Housing" = "#FFE58E"),
                    labels = c("Safe Housing" = "Dwelling not threatened", 
                               "1 drought" = "1 abnormal drought\nin 30 years",
                               "2 to 7 droughts" = "Between 2 and 7 abnormal droughts\n(every 4 years or less)",
                               "8 to 15 droughts" = "Between 8 and 15 abnormal droughts\n(every 2 to 4 years)",
                               "At least 16 droughts" = "More than 16 abnormal droughts\n(at least every other year)"),
                    breaks = c("Safe Housing", "1 drought","2 to 7 droughts", "8 to 15 droughts", "At least 16 droughts"),
                    name = NULL) +
  theme_minimal() +
  theme(legend.position = "top",
        plot.caption = element_text(hjust = 0), 
        plot.title.position = "plot",
        plot.caption.position =  "plot",
        panel.grid = element_blank(),
        axis.text.x = element_text(margin = margin(-10, 0, 0, 15)),
        axis.text.y = element_text(margin = margin(0, -20, 0, 0)))+ 
  scale_y_continuous(breaks = seq(0.2, 1, by = 0.2), labels = scales::percent_format()) +
  scale_x_continuous(breaks = seq(0, 100, by = 10)) +
  labs(x = "Standard of Living Percentile", y = NULL, 
       title = "Households' Main Residence Threatened by Shrink-Swell in Metropolitan France between 2005 and 2034",
       subtitle = stringr::str_wrap("Distribution of households by standard of living and frequency of geotechnical droughts likely to trigger shrink-swell and damage the main residence  according to the IPCC's 8.5 Representative Concentration Pathway", width = 160),
       caption = paste("Dwellings not threatened by clay shrink-swell are houses located outside of clay areas, houses in low to high exposure zones without any geotechnical droughts with a return period of at least\n25 years in 30 years, and all flats.\nSources: Clay Shrink-Swell Exposure Mapping, BRGM 2019; Soil Wetness Index projections, Météo France 2020; Fidéli 2017, Insee; Insee 2024"))
ggsave(paste0(path_outputs, "graphe_nv_vie_8_5_ref.pdf"), plot = graphe_nv_vie_8_5_ref, width = 11, height = 5.2, dpi = 400)

graphe_nv_vie_8_5_H1 <- ggplot(df_nv_vie_menaces, aes(x = centieme)) +
  geom_area(aes(y = menage_sauf_p25_H1_RCP8_5 + exactly_1_p25_H1_RCP8_5 + between_2_and_7_p25_H1_RCP8_5 +
                  between_8_and_15_p25_H1_RCP8_5 + above_16_p25_H1_RCP8_5, fill = "Safe Housing")) +
  geom_area(aes(y = exactly_1_p25_H1_RCP8_5 + between_2_and_7_p25_H1_RCP8_5 +
                  between_8_and_15_p25_H1_RCP8_5 + above_16_p25_H1_RCP8_5, fill = "1 drought")) +
  geom_area(aes(y = between_2_and_7_p25_H1_RCP8_5 +
                  between_8_and_15_p25_H1_RCP8_5 + above_16_p25_H1_RCP8_5, fill = "2 to 7 droughts")) +
  geom_area(aes(y = between_8_and_15_p25_H1_RCP8_5 + above_16_p25_H1_RCP8_5, fill = "8 to 15 droughts")) +
  geom_area(aes(y = above_16_p25_H1_RCP8_5, fill = "At least 16 droughts")) +
  scale_fill_manual(values = c("1 drought" = "#8DB0E1", "2 to 7 droughts" = "#5F8DC9",
                               "8 to 15 droughts" = "#0F417A","At least 16 droughts" = "black", 
                               "Safe Housing" = "#FFE58E"),
                    labels = c("Safe Housing" = "Dwelling not threatened", 
                               "1 drought" = "1 abnormal drought\nin 30 years",
                               "2 to 7 droughts" = "Between 2 and 7 abnormal droughts\n(every 4 years or less)",
                               "8 to 15 droughts" = "Between 8 and 15 abnormal droughts\n(every 2 to 4 years)",
                               "At least 16 droughts" = "More than 16 abnormal droughts\n(at least every other year)"),
                    breaks = c("Safe Housing", "1 drought","2 to 7 droughts", "8 to 15 droughts", "At least 16 droughts"),
                    name = NULL) +
  theme_minimal() +
  theme(legend.position = "top",
        plot.caption = element_text(hjust = 0), 
        plot.title.position = "plot",
        plot.caption.position =  "plot",
        panel.grid = element_blank(),
        axis.text.x = element_text(margin = margin(-10, 0, 0, 15)),
        axis.text.y = element_text(margin = margin(0, -20, 0, 0)))+ 
  scale_y_continuous(breaks = seq(0.2, 1, by = 0.2), labels = scales::percent_format()) +
  scale_x_continuous(breaks = seq(0, 100, by = 10)) +
  labs(x = "Standard of Living Percentile", y = NULL, 
       title = "Households' Main Residence Threatened by Shrink-Swell in Metropolitan France between 2021 and 2050",
       subtitle = stringr::str_wrap("Distribution of households by standard of living and frequency of geotechnical droughts likely to trigger shrink-swell and damage the main residence according to the IPCC's 8.5 Representative Concentration Pathway", width = 160),
       caption = paste("Dwellings not threatened by clay shrink-swell are houses located outside of clay areas, houses in low to high exposure zones without any geotechnical droughts with a return period of at least\n25 years in 30 years, and all flats.\nSources: Clay Shrink-Swell Exposure Mapping, BRGM 2019; Soil Wetness Index projections, Météo France 2020; Fidéli 2017, Insee; Insee 2024"))
ggsave(paste0(path_outputs, "graphe_nv_vie_8_5_H1.pdf"), plot = graphe_nv_vie_8_5_H1, width = 11, height = 5.2, dpi = 400)


graphe_nv_vie_8_5_H2 <- ggplot(df_nv_vie_menaces, aes(x = centieme)) +
  geom_area(aes(y = menage_sauf_p25_H2_RCP8_5 + exactly_1_p25_H2_RCP8_5 + between_2_and_7_p25_H2_RCP8_5 +
                  between_8_and_15_p25_H2_RCP8_5 + above_16_p25_H2_RCP8_5, fill = "Safe Housing")) +
  geom_area(aes(y = exactly_1_p25_H2_RCP8_5 + between_2_and_7_p25_H2_RCP8_5 +
                  between_8_and_15_p25_H2_RCP8_5 + above_16_p25_H2_RCP8_5, fill = "1 drought")) +
  geom_area(aes(y = between_2_and_7_p25_H2_RCP8_5 +
                  between_8_and_15_p25_H2_RCP8_5 + above_16_p25_H2_RCP8_5, fill = "2 to 7 droughts")) +
  geom_area(aes(y = between_8_and_15_p25_H2_RCP8_5 + above_16_p25_H2_RCP8_5, fill = "8 to 15 droughts")) +
  geom_area(aes(y = above_16_p25_H2_RCP8_5, fill = "At least 16 droughts")) +
  scale_fill_manual(values = c("1 drought" = "#8DB0E1", "2 to 7 droughts" = "#5F8DC9",
                               "8 to 15 droughts" = "#0F417A","At least 16 droughts" = "black", 
                               "Safe Housing" = "#FFE58E"),
                    labels = c("Safe Housing" = "Dwelling not threatened", 
                               "1 drought" = "1 abnormal drought\nin 30 years",
                               "2 to 7 droughts" = "Between 2 and 7 abnormal droughts\n(every 4 years or less)",
                               "8 to 15 droughts" = "Between 8 and 15 abnormal droughts\n(every 2 to 4 years)",
                               "At least 16 droughts" = "More than 16 abnormal droughts\n(at least every other year)"),
                    breaks = c("Safe Housing", "1 drought","2 to 7 droughts", "8 to 15 droughts", "At least 16 droughts"),
                    name = NULL) +
  theme_minimal() +
  theme(legend.position = "top",
        plot.caption = element_text(hjust = 0), 
        plot.title.position = "plot",
        plot.caption.position =  "plot",
        panel.grid = element_blank(),
        axis.text.x = element_text(margin = margin(-10, 0, 0, 15)),
        axis.text.y = element_text(margin = margin(0, -20, 0, 0)))+ 
  scale_y_continuous(breaks = seq(0.2, 1, by = 0.2), labels = scales::percent_format()) +
  scale_x_continuous(breaks = seq(0, 100, by = 10)) +
  labs(x = "Standard of Living Percentile", y = NULL, 
       title = "Households' Main Residence Threatened by Shrink-Swell in Metropolitan France between 2041 and 2070",
       subtitle = stringr::str_wrap("Distribution of households by standard of living and frequency of geotechnical droughts likely to trigger shrink-swell and damage the main residence according to the IPCC's 8.5 Representative Concentration Pathway", width = 160),
       caption = paste("Dwellings not threatened by clay shrink-swell are houses located outside of clay areas, houses in low to high exposure zones without any geotechnical droughts with a return period of at least\n25 years in 30 years, and all flats.\nSources: Clay Shrink-Swell Exposure Mapping, BRGM 2019; Soil Wetness Index projections, Météo France 2020; Fidéli 2017, Insee; Insee 2024"))
ggsave(paste0(path_outputs, "graphe_nv_vie_8_5_H2.pdf"), plot = graphe_nv_vie_8_5_H2, width = 11, height = 5.2, dpi = 400)


graphe_nv_vie_8_5_H3 <- ggplot(df_nv_vie_menaces, aes(x = centieme)) +
  geom_area(aes(y = menage_sauf_p25_H3_RCP8_5 + exactly_1_p25_H3_RCP8_5 + between_2_and_7_p25_H3_RCP8_5 +
                  between_8_and_15_p25_H3_RCP8_5 + above_16_p25_H3_RCP8_5, fill = "Safe Housing")) +
  geom_area(aes(y = exactly_1_p25_H3_RCP8_5 + between_2_and_7_p25_H3_RCP8_5 +
                  between_8_and_15_p25_H3_RCP8_5 + above_16_p25_H3_RCP8_5, fill = "1 drought")) +
  geom_area(aes(y = between_2_and_7_p25_H3_RCP8_5 +
                  between_8_and_15_p25_H3_RCP8_5 + above_16_p25_H3_RCP8_5, fill = "2 to 7 droughts")) +
  geom_area(aes(y = between_8_and_15_p25_H3_RCP8_5 + above_16_p25_H3_RCP8_5, fill = "8 to 15 droughts")) +
  geom_area(aes(y = above_16_p25_H3_RCP8_5, fill = "At least 16 droughts")) +
  scale_fill_manual(values = c("1 drought" = "#8DB0E1", "2 to 7 droughts" = "#5F8DC9",
                               "8 to 15 droughts" = "#0F417A","At least 16 droughts" = "black", 
                               "Safe Housing" = "#FFE58E"),
                    labels = c("Safe Housing" = "Dwelling not threatened", 
                               "1 drought" = "1 abnormal drought\nin 30 years",
                               "2 to 7 droughts" = "Between 2 and 7 abnormal droughts\n(every 4 years or less)",
                               "8 to 15 droughts" = "Between 8 and 15 abnormal droughts\n(every 2 to 4 years)",
                               "At least 16 droughts" = "More than 16 abnormal droughts\n(at least every other year)"),
                    breaks = c("Safe Housing", "1 drought","2 to 7 droughts", "8 to 15 droughts", "At least 16 droughts"),
                    name = NULL) +
  theme_minimal() +
  theme(legend.position = "top",
        plot.caption = element_text(hjust = 0), 
        plot.title.position = "plot",
        plot.caption.position =  "plot",
        panel.grid = element_blank(),
        axis.text.x = element_text(margin = margin(-10, 0, 0, 15)),
        axis.text.y = element_text(margin = margin(0, -20, 0, 0)))+ 
  scale_y_continuous(breaks = seq(0.2, 1, by = 0.2), labels = scales::percent_format()) +
  scale_x_continuous(breaks = seq(0, 100, by = 10)) +
  labs(x = "Standard of Living Percentile", y = NULL, 
       title = "Households' Main Residence Threatened by Shrink-Swell in Metropolitan France between 2071 and 2100",
       subtitle = stringr::str_wrap("Distribution of households by standard of living and frequency of geotechnical droughts likely to trigger shrink-swell and damage the main residence according to the IPCC's 8.5 Representative Concentration Pathway", width = 160),
       caption = paste("Dwellings not threatened by clay shrink-swell are houses located outside of clay areas, houses in low to high exposure zones without any geotechnical droughts with a return period of at least\n25 years in 30 years, and all flats.\nSources: Clay Shrink-Swell Exposure Mapping, BRGM 2019; Soil Wetness Index projections, Météo France 2020; Fidéli 2017, Insee; Insee 2024"))
ggsave(paste0(path_outputs, "graphe_nv_vie_8_5_H3.pdf"), plot = graphe_nv_vie_8_5_H3, width = 11, height = 5.2, dpi = 400)

### 4.5
graphe_nv_vie_4_5_ref <- ggplot(df_nv_vie_menaces, aes(x = centieme)) +
  geom_area(aes(y = menage_sauf_p25_ref_RCP4_5 + exactly_1_p25_ref_RCP4_5 + between_2_and_7_p25_ref_RCP4_5 +
                  between_8_and_15_p25_ref_RCP4_5 + above_16_p25_ref_RCP4_5, fill = "Safe Housing")) +
  geom_area(aes(y = exactly_1_p25_ref_RCP4_5 + between_2_and_7_p25_ref_RCP4_5 +
                  between_8_and_15_p25_ref_RCP4_5 + above_16_p25_ref_RCP4_5, fill = "1 drought")) +
  geom_area(aes(y = between_2_and_7_p25_ref_RCP4_5 +
                  between_8_and_15_p25_ref_RCP4_5 + above_16_p25_ref_RCP4_5, fill = "2 to 7 droughts")) +
  geom_area(aes(y = between_8_and_15_p25_ref_RCP4_5 + above_16_p25_ref_RCP4_5, fill = "8 to 15 droughts")) +
  geom_area(aes(y = above_16_p25_ref_RCP4_5, fill = "At least 16 droughts")) +
  scale_fill_manual(values = c("1 drought" = "#8DB0E1", "2 to 7 droughts" = "#5F8DC9",
                               "8 to 15 droughts" = "#0F417A","At least 16 droughts" = "black", 
                               "Safe Housing" = "#FFE58E"),
                    labels = c("Safe Housing" = "Dwelling not threatened", 
                               "1 drought" = "1 abnormal drought\nin 30 years",
                               "2 to 7 droughts" = "Between 2 and 7 abnormal droughts\n(every 4 years or less)",
                               "8 to 15 droughts" = "Between 8 and 15 abnormal droughts\n(every 2 to 4 years)",
                               "At least 16 droughts" = "More than 16 abnormal droughts\n(at least every other year)"),
                    breaks = c("Safe Housing", "1 drought","2 to 7 droughts", "8 to 15 droughts", "At least 16 droughts"),
                    name = NULL) +
  theme_minimal() +
  theme(legend.position = "top",
        plot.caption = element_text(hjust = 0), 
        plot.title.position = "plot",
        plot.caption.position =  "plot",
        panel.grid = element_blank(),
        axis.text.x = element_text(margin = margin(-10, 0, 0, 15)),
        axis.text.y = element_text(margin = margin(0, -20, 0, 0)))+ 
  scale_y_continuous(breaks = seq(0.2, 1, by = 0.2), labels = scales::percent_format()) +
  scale_x_continuous(breaks = seq(0, 100, by = 10)) +
  labs(x = "Standard of Living Percentile", y = NULL, 
       title = "Households' Main Residence Threatened by Shrink-Swell in Metropolitan France between 2005 and 2034",
       subtitle = stringr::str_wrap("Distribution of households by standard of living and frequency of geotechnical droughts likely to trigger shrink-swell and damage the main residence according to the IPCC's 4.5 Representative Concentration Pathway", width = 160),
       caption = paste("Dwellings not threatened by clay shrink-swell are houses located outside of clay areas, houses in low to high exposure zones without any geotechnical droughts with a return period of at least\n25 years in 30 years, and all flats.\nSources: Clay Shrink-Swell Exposure Mapping, BRGM 2019; Soil Wetness Index projections, Météo France 2020; Fidéli 2017, Insee; Insee 2024"))
ggsave(paste0(path_outputs, "graphe_nv_vie_4_5_ref.pdf"), plot = graphe_nv_vie_4_5_ref, width = 11, height = 5.2, dpi = 400)

graphe_nv_vie_4_5_H1 <- ggplot(df_nv_vie_menaces, aes(x = centieme)) +
  geom_area(aes(y = menage_sauf_p25_H1_RCP4_5 + exactly_1_p25_H1_RCP4_5 + between_2_and_7_p25_H1_RCP4_5 +
                  between_8_and_15_p25_H1_RCP4_5 + above_16_p25_H1_RCP4_5, fill = "Safe Housing")) +
  geom_area(aes(y = exactly_1_p25_H1_RCP4_5 + between_2_and_7_p25_H1_RCP4_5 +
                  between_8_and_15_p25_H1_RCP4_5 + above_16_p25_H1_RCP4_5, fill = "1 drought")) +
  geom_area(aes(y = between_2_and_7_p25_H1_RCP4_5 +
                  between_8_and_15_p25_H1_RCP4_5 + above_16_p25_H1_RCP4_5, fill = "2 to 7 droughts")) +
  geom_area(aes(y = between_8_and_15_p25_H1_RCP4_5 + above_16_p25_H1_RCP4_5, fill = "8 to 15 droughts")) +
  geom_area(aes(y = above_16_p25_H1_RCP4_5, fill = "At least 16 droughts")) +
  scale_fill_manual(values = c("1 drought" = "#8DB0E1", "2 to 7 droughts" = "#5F8DC9",
                               "8 to 15 droughts" = "#0F417A","At least 16 droughts" = "black", 
                               "Safe Housing" = "#FFE58E"),
                    labels = c("Safe Housing" = "Dwelling not threatened", 
                               "1 drought" = "1 abnormal drought\nin 30 years",
                               "2 to 7 droughts" = "Between 2 and 7 abnormal droughts\n(every 4 years or less)",
                               "8 to 15 droughts" = "Between 8 and 15 abnormal droughts\n(every 2 to 4 years)",
                               "At least 16 droughts" = "More than 16 abnormal droughts\n(at least every other year)"),
                    breaks = c("Safe Housing", "1 drought","2 to 7 droughts", "8 to 15 droughts", "At least 16 droughts"),
                    name = NULL) +
  theme_minimal() +
  theme(legend.position = "top",
        plot.caption = element_text(hjust = 0), 
        plot.title.position = "plot",
        plot.caption.position =  "plot",
        panel.grid = element_blank(),
        axis.text.x = element_text(margin = margin(-10, 0, 0, 15)),
        axis.text.y = element_text(margin = margin(0, -20, 0, 0)))+ 
  scale_y_continuous(breaks = seq(0.2, 1, by = 0.2), labels = scales::percent_format()) +
  scale_x_continuous(breaks = seq(0, 100, by = 10)) +
  labs(x = "Standard of Living Percentile", y = NULL, 
       title = "Households' Main Residence Threatened by Shrink-Swell in Metropolitan France between 2021 and 2050",
       subtitle = stringr::str_wrap("Distribution of households by standard of living and frequency of geotechnical droughts likely to trigger shrink-swell and damage the main residence according to the IPCC's 4.5 Representative Concentration Pathway", width = 160),
       caption = paste("Dwellings not threatened by clay shrink-swell are houses located outside of clay areas, houses in low to high exposure zones without any geotechnical droughts with a return period of at least\n25 years in 30 years, and all flats.\nSources: Clay Shrink-Swell Exposure Mapping, BRGM 2019; Soil Wetness Index projections, Météo France 2020; Fidéli 2017, Insee; Insee 2024"))
ggsave(paste0(path_outputs, "graphe_nv_vie_4_5_H1.pdf"), plot = graphe_nv_vie_4_5_H1, width = 11, height = 5.2, dpi = 400)


graphe_nv_vie_4_5_H2 <- ggplot(df_nv_vie_menaces, aes(x = centieme)) +
  geom_area(aes(y = menage_sauf_p25_H2_RCP4_5 + exactly_1_p25_H2_RCP4_5 + between_2_and_7_p25_H2_RCP4_5 +
                  between_8_and_15_p25_H2_RCP4_5 + above_16_p25_H2_RCP4_5, fill = "Safe Housing")) +
  geom_area(aes(y = exactly_1_p25_H2_RCP4_5 + between_2_and_7_p25_H2_RCP4_5 +
                  between_8_and_15_p25_H2_RCP4_5 + above_16_p25_H2_RCP4_5, fill = "1 drought")) +
  geom_area(aes(y = between_2_and_7_p25_H2_RCP4_5 +
                  between_8_and_15_p25_H2_RCP4_5 + above_16_p25_H2_RCP4_5, fill = "2 to 7 droughts")) +
  geom_area(aes(y = between_8_and_15_p25_H2_RCP4_5 + above_16_p25_H2_RCP4_5, fill = "8 to 15 droughts")) +
  geom_area(aes(y = above_16_p25_H2_RCP4_5, fill = "At least 16 droughts")) +
  scale_fill_manual(values = c("1 drought" = "#8DB0E1", "2 to 7 droughts" = "#5F8DC9",
                               "8 to 15 droughts" = "#0F417A","At least 16 droughts" = "black", 
                               "Safe Housing" = "#FFE58E"),
                    labels = c("Safe Housing" = "Dwelling not threatened", 
                               "1 drought" = "1 abnormal drought\nin 30 years",
                               "2 to 7 droughts" = "Between 2 and 7 abnormal droughts\n(every 4 years or less)",
                               "8 to 15 droughts" = "Between 8 and 15 abnormal droughts\n(every 2 to 4 years)",
                               "At least 16 droughts" = "More than 16 abnormal droughts\n(at least every other year)"),
                    breaks = c("Safe Housing", "1 drought","2 to 7 droughts", "8 to 15 droughts", "At least 16 droughts"),
                    name = NULL) +
  theme_minimal() +
  theme(legend.position = "top",
        plot.caption = element_text(hjust = 0), 
        plot.title.position = "plot",
        plot.caption.position =  "plot",
        panel.grid = element_blank(),
        axis.text.x = element_text(margin = margin(-10, 0, 0, 15)),
        axis.text.y = element_text(margin = margin(0, -20, 0, 0)))+ 
  scale_y_continuous(breaks = seq(0.2, 1, by = 0.2), labels = scales::percent_format()) +
  scale_x_continuous(breaks = seq(0, 100, by = 10)) +
  labs(x = "Standard of Living Percentile", y = NULL, 
       title = "Households' Main Residence Threatened by Shrink-Swell in Metropolitan France between 2041 and 2070",
       subtitle = stringr::str_wrap("Distribution of households by standard of living and frequency of geotechnical droughts likely to trigger shrink-swell and damage the main residence according to the IPCC's 4.5 Representative Concentration Pathway", width = 160),
       caption = paste("Dwellings not threatened by clay shrink-swell are houses located outside of clay areas, houses in low to high exposure zones without any geotechnical droughts with a return period of at least\n25 years in 30 years, and all flats.\nSources: Clay Shrink-Swell Exposure Mapping, BRGM 2019; Soil Wetness Index projections, Météo France 2020; Fidéli 2017, Insee; Insee 2024"))
ggsave(paste0(path_outputs, "graphe_nv_vie_4_5_H2.pdf"), plot = graphe_nv_vie_4_5_H2, width = 11, height = 5.2, dpi = 400)


graphe_nv_vie_4_5_H3 <- ggplot(df_nv_vie_menaces, aes(x = centieme)) +
  geom_area(aes(y = menage_sauf_p25_H3_RCP4_5 + exactly_1_p25_H3_RCP4_5 + between_2_and_7_p25_H3_RCP4_5 +
                  between_8_and_15_p25_H3_RCP4_5 + above_16_p25_H3_RCP4_5, fill = "Safe Housing")) +
  geom_area(aes(y = exactly_1_p25_H3_RCP4_5 + between_2_and_7_p25_H3_RCP4_5 +
                  between_8_and_15_p25_H3_RCP4_5 + above_16_p25_H3_RCP4_5, fill = "1 drought")) +
  geom_area(aes(y = between_2_and_7_p25_H3_RCP4_5 +
                  between_8_and_15_p25_H3_RCP4_5 + above_16_p25_H3_RCP4_5, fill = "2 to 7 droughts")) +
  geom_area(aes(y = between_8_and_15_p25_H3_RCP4_5 + above_16_p25_H3_RCP4_5, fill = "8 to 15 droughts")) +
  geom_area(aes(y = above_16_p25_H3_RCP4_5, fill = "At least 16 droughts")) +
  scale_fill_manual(values = c("1 drought" = "#8DB0E1", "2 to 7 droughts" = "#5F8DC9",
                               "8 to 15 droughts" = "#0F417A","At least 16 droughts" = "black", 
                               "Safe Housing" = "#FFE58E"),
                    labels = c("Safe Housing" = "Dwelling not threatened", 
                               "1 drought" = "1 abnormal drought\nin 30 years",
                               "2 to 7 droughts" = "Between 2 and 7 abnormal droughts\n(every 4 years or less)",
                               "8 to 15 droughts" = "Between 8 and 15 abnormal droughts\n(every 2 to 4 years)",
                               "At least 16 droughts" = "More than 16 abnormal droughts\n(at least every other year)"),
                    breaks = c("Safe Housing", "1 drought","2 to 7 droughts", "8 to 15 droughts", "At least 16 droughts"),
                    name = NULL) +
  theme_minimal() +
  theme(legend.position = "top",
        plot.caption = element_text(hjust = 0), 
        plot.title.position = "plot",
        plot.caption.position =  "plot",
        panel.grid = element_blank(),
        axis.text.x = element_text(margin = margin(-10, 0, 0, 15)),
        axis.text.y = element_text(margin = margin(0, -20, 0, 0)))+ 
  scale_y_continuous(breaks = seq(0.2, 1, by = 0.2), labels = scales::percent_format()) +
  scale_x_continuous(breaks = seq(0, 100, by = 10)) +
  labs(x = "Standard of Living Percentile", y = NULL, 
       title = "Households' Main Residence Threatened by Shrink-Swell in Metropolitan France between 2071 and 2100",
       subtitle = stringr::str_wrap("Distribution of households by standard of living and frequency of geotechnical droughts likely to trigger shrink-swell and damage the main residence according to the IPCC's 4.5 Representative Concentration Pathway", width = 160),
       caption = paste("Dwellings not threatened by clay shrink-swell are houses located outside of clay areas, houses in low to high exposure zones without any geotechnical droughts with a return period of at least\n25 years in 30 years, and all flats.\nSources: Clay Shrink-Swell Exposure Mapping, BRGM 2019; Soil Wetness Index projections, Météo France 2020; Fidéli 2017, Insee; Insee 2024"))
ggsave(paste0(path_outputs, "graphe_nv_vie_4_5_H3.pdf"), plot = graphe_nv_vie_4_5_H3, width = 11, height = 5.2, dpi = 400)

## On utilisera surtout H1 à H3, RCP 8.5
