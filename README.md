# Projet Data Engineering - Analyse du Marché Immobilier Français (DVF)

[![dbt](https://img.shields.io/badge/dbt-1.11-orange)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Cloud-blue)](https://www.snowflake.com/)
[![Python](https://img.shields.io/badge/Python-3.11-green)](https://www.python.org/)

## Vue d'ensemble

Pipeline data engineering end-to-end analysant **9+ millions de transactions immobilières françaises** (2020-2023) issues de la base DVF (Demandes de Valeurs Foncières).

**Stack technique** : Snowflake • dbt Core • Python • Streamlit

---

## Objectifs du Projet

- Construire une architecture data moderne (ELT)
- Implémenter des transformations SQL avec dbt
- Créer des KPIs business actionnables
- Assurer la qualité des données via des tests automatisés

---

## Architecture

```
┌─────────────┐
│  Données    │
│  DVF (txt)  │  2.4 Go
└──────┬──────┘
       │ Python Script
       ▼
┌─────────────┐
│  SNOWFLAKE  │
│     RAW     │  9M+ lignes
└──────┬──────┘
       │ dbt transformations
       ▼
┌─────────────┐
│   STAGING   │  Nettoyage + Typage
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    MARTS    │  Tables analytiques
│             │  • fct_transactions
│             │  • agg_communes
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  STREAMLIT  │  Dashboard interactif
└─────────────┘
```

---

## Résultats Clés

### Métriques Business

- **9 078 000+** transactions analysées
- **36 000+** communes couvertes
- **4 années** de données (2020-2023)
- **Prix médian national** : ~2 800€/m² (2023)

### Insights

- 🏆 Commune la plus chère : Vélizy-Villacoublay (8 316€/m²)
- 📊 Volume stable post-COVID (~2M transactions/an)
- 🌍 Forte activité outre-mer (Guadeloupe)

---

## Installation & Usage

### Prérequis

```bash
Python 3.11+
Compte Snowflake (trial gratuit)
dbt Core 1.11+
```

### Setup

1. **Cloner le repo**

```bash
git clone https://github.com/Sidi4PF/projet-dvf.git
cd projet_dvf
```

2. **Environnement Python**

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. **Configuration Snowflake**

```sql
CREATE DATABASE DVF_DB;
CREATE WAREHOUSE DVF_WH WITH WAREHOUSE_SIZE = 'XSMALL';
CREATE SCHEMA DVF_DB.RAW;
CREATE SCHEMA DVF_DB.STAGING;
CREATE SCHEMA DVF_DB.MARTS;
```

4. **Configuration dbt**

Voir la [documentation dbt](https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup) pour configurer `~/.dbt/profiles.yml` avec vos credentials Snowflake.

```bash
cd dvf_project
dbt debug  # Vérifier la connexion
dbt deps   # Installer les dépendances
dbt run    # Exécuter les transformations
dbt test   # Lancer les tests qualité
```

5. **Données DVF**

Télécharger les fichiers depuis [data.gouv.fr](https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/) et placer dans `data/raw/`.

6. **Dashboard Streamlit**

```bash
streamlit run app.py
```

---

## Structure du Projet

```
projet_dvf/
├── data/
│   └── raw/              # Données sources DVF (non versionnées)
├── scripts/
│   └── load_raw_data.py  # Ingestion Python → Snowflake
├── dvf_project/          # Projet dbt
│   ├── models/
│   │   ├── staging/      # Modèles de nettoyage
│   │   │   ├── stg_dvf__mutations.sql
│   │   │   ├── sources.yml
│   │   │   └── schema.yml
│   │   └── marts/        # Tables analytiques finales
│   │       ├── fct_transactions.sql
│   │       ├── agg_communes.sql
│   │       └── schema.yml
│   ├── dbt_project.yml
│   └── packages.yml
├── app.py                # Dashboard Streamlit
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Tests de Qualité

- ✅ 9 tests dbt automatisés
- ✅ Validation des nulls, unicité, valeurs acceptées
- ✅ Détection d'anomalies (prix aberrants, données manquantes)

```bash
dbt test
# Done. PASS=9 WARN=0 ERROR=0 ✅
```

**Anomalies détectées et traitées :**

- 15M lignes sans identifiant → ID synthétique généré
- 143k transactions sans prix → Filtrées
- 147k prix/m² aberrants → Exclus des analyses

---

## Stack Technique

| Technologie   | Usage                   |
| ------------- | ----------------------- |
| **Snowflake** | Data warehouse cloud    |
| **dbt Core**  | Transformations ELT     |
| **Python**    | Ingestion de données    |
| **Streamlit** | Dashboard interactif    |
| **pandas**    | Manipulation de données |

---

## Modèles dbt Créés

### Staging Layer

- **`stg_dvf__mutations`** : Union et nettoyage des 4 années de données
  - Conversion des types (dates, nombres)
  - Gestion des formats français (virgules → points)
  - Génération d'IDs uniques

### Marts Layer

- **`fct_transactions`** : Table de faits avec enrichissements
  - Calcul automatique du prix au m²
  - Extraction année/mois/trimestre
  - Flags de qualité (has_prix, has_surface)
  - Filtres métier (ventes uniquement, prix cohérents)

- **`agg_communes`** : KPIs agrégés par commune et année
  - Prix médian/moyen et prix/m²
  - Volume de transactions
  - Surfaces médianes
  - Répartition maisons/appartements

---

## Analyses Disponibles

Le dashboard Streamlit permet d'explorer :

- 📈 **Évolution temporelle** : Tendances de prix 2020-2023
- 🗺️ **Analyse géographique** : Comparaison départements/communes
- 🏘️ **Segmentation** : Maisons vs Appartements
- 💰 **Prix au m²** : Ranking des communes les plus chères
- 📊 **Volumes** : Activité du marché par zone

---

## Sources de Données

**DVF (Demandes de Valeurs Foncières)**

- Source : [data.gouv.fr](https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/)
- Organisme : Direction Générale des Finances Publiques (DGFiP)
- Fréquence : Mise à jour semestrielle
- Format : Fichiers .txt pipe-delimited (|)
- Licence : Licence Ouverte / Open License (Etalab)
- Période couverte : 2020-2023

**Colonnes principales utilisées :**

- `date_mutation`, `nature_mutation`, `valeur_fonciere`
- `code_postal`, `commune`, `code_departement`
- `type_local`, `surface_reelle_bati`, `nombre_pieces_principales`

---

## Compétences Démontrées

### Techniques

- ✅ Architecture data moderne en couches (RAW/STAGING/MARTS)
- ✅ SQL avancé (CTEs, Window Functions, agrégations complexes)
- ✅ Data Quality Engineering (tests automatisés, détection d'anomalies)
- ✅ Cloud Data Warehousing (Snowflake)
- ✅ ELT avec dbt (transformations, documentation, lineage)
- ✅ Python pour l'ingestion de données
- ✅ Data Visualization (Streamlit, Plotly)

### Méthodologiques

- ✅ Debugging et résolution de problèmes techniques
- ✅ Documentation technique complète
- ✅ Versioning et bonnes pratiques Git
- ✅ Pensée analytique et business intelligence

---

## Améliorations Futures

- [ ] Incremental loading pour nouvelles données 2024/2025
- [ ] Enrichissement avec données INSEE (revenus, démographie)
- [ ] Modèle ML de prédiction de prix
- [ ] API REST pour exposer les données
- [ ] CI/CD avec GitHub Actions
- [ ] Documentation auto-générée (dbt docs)
- [ ] Analyse géospatiale avancée

---

## Contact

**[TON NOM]**

- 🌐 Portfolio : [lien-vers-ton-portfolio]
- 💼 LinkedIn : https://www.linkedin.com/in/sidi-amadou-bocoum-046b691b6/
- 📧 Email : sidi.bocoum02@gmail.com
- 💻 GitHub : https://github.com/Sidi4PF

---

## Licence

Ce projet est sous licence MIT.

Les données DVF sont sous [Licence Ouverte / Open License Etalab](https://www.etalab.gouv.fr/licence-ouverte-open-licence).

---
