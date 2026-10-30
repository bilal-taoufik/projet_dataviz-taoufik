# Projet Data Viz - Analyse des décès en France

## Sommaire

1. [Présentation du projet](#présentation-du-projet)
2. [Source](#source)
3. [Volume des données](#volume-des-données)
4. [Structure des données](#structure-des-données)
5. [Persona](#persona)
6. [Problématique métier](#problématique-métier)

## Présentation du projet

Ce projet a pour objectif de concevoir un dashboard interactif permettant d'explorer et d'analyser les décès enregistrés en France à partir des données disponibles sur data.gouv.fr.

L'analyse portera principalement sur l'évolution du nombre de décès dans le temps, leur répartition géographique et les caractéristiques démographiques disponibles.

Le projet s'appuie sur une architecture de traitement de données utilisant :

- Docker
- Spark
- Delta Lake
- PostgreSQL
- Streamlit

## Source

**Nom :** Agrégation des fichiers des personnes décédées

**Période :** Dejanvier 1970 jusqu'en octobre 2026 (mis à jour tous les mois)

**Source :** [data.gouv.fr](https://www.data.gouv.fr/datasets/agregation-des-fichiers-des-personnes-decedees)

**Origine :** Fichiers des personnes décédées produits par l'INSEE, agrégés sur data.gouv.fr

**Format utilisé :** Parquet

**Licence :** Licence Ouverte

**Dernière mise à jour indiquée :** 16 octobre 2026

## Volume des données

Une première exploration du fichier Parquet avec PySpark a permis d'identifier :

**29 411 388 lignes**

Le volume des données dépasse donc le minimum de 2 millions de lignes demandé dans le cadre du projet.

## Structure des données

Le jeu de données contient les informations suivantes :

| Colonne | Description |
| :--- | :--- |
| `nom` | Nom de famille |
| `prenoms` | Prénoms, séparés par des virgules |
| `sexe` | Sexe (M pour masculin, F pour féminin) |
| `date_naissance` | Date de naissance au format AAAAMMJJ (des 0 indiquent une partie inconnue) |
| `code_insee_naissance` | Code INSEE du lieu de naissance |
| `commune_naissance` | Nom de la commune de naissance |
| `pays_naissance` | Pays de naissance |
| `date_deces` | Date de décès au format AAAAMMJJ (des 0 indiquent une partie inconnue) |
| `code_insee_deces` | Code INSEE du lieu de décès |
| `numero_acte_deces` | Numéro d'acte de décès |
| `fichier_origine` | Fichier source dont provient l'enregistrement |
| `opposition` | Booléen indiquant si une opposition a été enregistrée (True ou False) |

Ces informations permettront notamment d'analyser l'évolution des décès selon les années, le sexe, l'âge au décès et les territoires.

## Persona

### Profil

**Fonction :** Journaliste spécialisé dans l'analyse de données.

**Mission :** Comprendre et expliquer l'évolution des décès en France à travers des chiffres et des graphiques accessibles au grand public.

### Besoins

Le journaliste souhaite :

- Suivre l'évolution du nombre de décès en France au fil des années.
- Identifier les années où il y a eu le plus de décès.
- Comparer les décès selon les régions et les départements.
- Analyser l'âge des personnes décédées.
- Comparer le nombre de décès entre les hommes et les femmes.

### Fréquence d'utilisation

Le tableau de bord sera utilisé lors de la préparation d'articles ou de reportages, notamment pour analyser les tendances annuelles et comparer différentes périodes.

## Problématique

**Comment le nombre de décès en France a-t-il évolué depuis 1970, et quelles différences peut-on observer selon les périodes, les territoires, l'âge et le sexe des personnes décédées ?**

L'objectif du dashboard est de permettre au journaliste d'identifier rapidement les tendances importantes et de les présenter au grand public à travers des visualisations simples et compréhensibles.