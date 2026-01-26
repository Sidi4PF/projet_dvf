{{ config(materialized='table') }}

WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY identifiant_de_document 
            ORDER BY surface_reelle_bati DESC NULLS LAST
        ) AS rn
    FROM {{ ref('stg_dvf__mutations') }}
    WHERE nature_mutation = 'Vente'
      AND valeur_fonciere > 1000
      AND valeur_fonciere < 50000000
      AND type_local IN ('Maison', 'Appartement')
)

SELECT
    identifiant_de_document,
    date_mutation,
    EXTRACT(
        YEAR
        FROM date_mutation
    ) AS annee,
    EXTRACT(
        MONTH
        FROM date_mutation
    ) AS mois,
    EXTRACT(
        QUARTER
        FROM date_mutation
    ) AS trimestre,
    nature_mutation,
    valeur_fonciere,
    code_postal,
    commune,
    code_departement,
    code_commune,
    type_local,
    surface_reelle_bati,
    nombre_pieces_principales,
    surface_terrain,
    nombre_de_lots,
    CASE
        WHEN surface_reelle_bati > 0 THEN valeur_fonciere / surface_reelle_bati
        ELSE NULL
    END AS prix_m2,
    CASE
        WHEN valeur_fonciere > 0 THEN 1
        ELSE 0
    END AS has_prix,
    CASE
        WHEN surface_reelle_bati > 0 THEN 1
        ELSE 0
    END AS has_surface,
    source_file,
    loaded_at
FROM deduped
WHERE
    rn = 1
    AND (
        surface_reelle_bati IS NULL
        OR surface_reelle_bati = 0
        OR (
            valeur_fonciere / surface_reelle_bati BETWEEN 500 AND 30000
        )
    )