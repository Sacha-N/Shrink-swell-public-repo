# Manipulation de données
library(dplyr)
library(tidyr)
library(stringr)
library(arrow)
library(hutils)
options(arrow.use_threads = TRUE)
arrow::set_cpu_count(parallel::detectCores() %/% 4)

# Cartographie
library(sf)
library(ggplot2)

# Chemin
path_input <- "Z:/Memoire/dv3f_memoire/Inputs_climat/"
path_base_intermediaire <- "Z:/Memoire/dv3f_memoire/Bases_intermediaires/"
path_outputs <- "Z:/Memoire/dv3f_memoire/Outputs/"

rep_donnees <-  "P:/PSAR_SL/SL45/Lot 3/202401/bases/comptages"

################################
#### PRESENTATION DU SCRIPT ####
################################

# L'objectif de ce script est de réaliser un compte des mutations communales par saison, entre 2017 et fin 2022.
# Pour ce faire, nous adaptons le script réalisé par le PSAR-SL, que nous remercions. 

years <- 2015:2022 # CHANGER POUR AVOIR LES ANNEES SOUHAITEES

#####################################
##### 1. Chargement des bases #######
#####################################

base_maisons <- arrow::open_dataset(paste0(path_base_intermediaire, 'bases_maisons_enrichie'))

df_climat <- readRDS(paste0(path_input, 'df_climat.RDS'))|> arrow::as_arrow_table()
# On réemploie la base filtrée par le PSAR : il ne nous reste que les mutations d'appartements
# et des maisons non exceptionnelles, entre 2010 et 2022, sur le champ de l'ancien. Voir 
# liste complète des filtres sur le github. 

#############################################
##### 2. Ajout des données climatiques ######
#############################################

table_transactions_climat <- base_maisons |>
  mutate(saison =  as.Date(case_when(
    lubridate::month(datemut) %in% c(1, 2, 3) ~ paste0(anneemut, "-01-01"),
    lubridate::month(datemut) %in% c(4, 5, 6) ~ paste0(anneemut, "-04-01"),
    lubridate::month(datemut) %in% c(7, 8, 9) ~ paste0(anneemut, "-07-01"),
    lubridate::month(datemut) %in% c(10, 11, 12) ~ paste0(anneemut, "-10-01")
  )))|>
  select(-c('dep', 'cateaav20', 'codtypbien', 'moismut', 'filtre', 'ffcommune', 'ffnovoie', 
            'reg', 'surf', 'geometry', 'srcgeom','ffcodinsee')) |>
  left_join(df_climat, by = c('saison' = 'saison','codgeo'='code')) |>
  select(-libelle) |>
  compute()

# Quelques vérifications
base_maisons |> summarize(length = n()) |> collect() #5 252 112
table_transactions_climat |> summarize(length = n()) |> collect() #5 252 112
table_transactions_climat |>
  to_duckdb()|>
  group_by(saison)|>
  summarize(length = n()) |>
  to_arrow()|>
  arrange(saison)|>
  collect() #Saisonalité observable

table_transactions_climat |> filter(is.na(rang_circ_2019)) |> summarize(length = n()) |> collect() #2,8M of missing climate info?
table_transactions_climat |> filter(is.na(rang_circ_2019) & anneemut >=2018) |> summarize(length = n()) |> collect()  
# of which, 0 problematic ! 
 
arrow::write_parquet(table_transactions_climat, paste0(path_base_intermediaire, 'table_transactions_climat'))

########################################################################
##### 3. Travail sur les volumes : création d'une base par saison ######
########################################################################

# Cette section n'a finalement pas été utilisée, car travailler sur les volumes rend difficile l'ajout de contrôles, et nos
# groupes sont trop dissemblabes.

## 3.1 Chargement des bases
base_totale <- arrow::open_dataset(paste0(path_base_intermediaire, 'mutations_RGA'))

## 3.2 Nombre d'observation par saison
compte_total_par_saison <- base_totale |>
  to_duckdb() |>
  group_by(saison) |>
  summarize(nbr_mutations = n())|>
  to_arrow()|>
  collect() |>
  filter(lubridate::year(saison) %in% 2018:2022)

compte_maisons_par_saison <- base_totale |>
  filter(codtypbien == '1112' | codtypbien == '1113')|>
  to_duckdb() |>
  group_by(saison) |>
  summarize(nbr_mutations = n())|>
  to_arrow()|>
  collect() |>
  filter(lubridate::year(saison) %in% 2018:2022)

# 3.3 On crée la table des communes x saison
communes_2023 <- sf::st_read('X:/HAB-immoclimat/Data/frontieres_admin/commune_francemetro_2023.gpkg') %>% 
  select(code, libelle) %>% 
  sf::st_drop_geometry()

years <- years #définition en haut
seasons <- c("01-01", "04-01", "07-01", "10-01")
saisons <- expand.grid(year = years, season = seasons) %>%
  mutate(saison = as.Date(paste(year, season, sep = "-"), format = "%Y-%m-%d")) %>% 
  select(saison)

combinaisons <- communes_2023 %>%
  crossing(saisons)

# 3.4 On y ajoute les mutations
compte_mutations <- base_totale |>
  filter(codtypbien == '1112' | codtypbien == '1113') |>
  to_duckdb() |>
  group_by(codgeo, saison) |>
  summarize(nbr_mutations = n()) |>
  to_arrow() |>
  collect() |>
  mutate(saison = as.Date(saison))

table_mutations_commune_saison <- combinaisons %>% 
  left_join(compte_mutations, by = c('saison', 'code' = 'codgeo'))

saveRDS(table_mutations_commune_saison,
        paste0(path_base_intermediaire, 'table_mutations_commune_saison.RDS'))

# 3.5 Finalisation : une table commune x volume x saison

df_climat <- readRDS(paste0(path_input, 'df_climat.RDS'))
table_mutations_commune_saison <- readRDS(paste0(path_base_intermediaire, 'table_mutations_commune_saison.RDS'))

## On associe à chaque ligne l'information climatique (clefs : saison, commune)
table_volumes_climat <- table_mutations_commune_saison %>% 
  filter(!(code %in% c(29083, 29155))) %>% #On retire l'Ile de Sein et Ouessant, non localisés dans nos mailles
  select(-libelle) %>% 
  left_join(df_climat, by = c('code', 'saison'))

## On enregistre la base obtenue pour la RDD
saveRDS(table_volumes_climat, paste0(path_base_intermediaire, 'table_volumes_climat.RDS'))