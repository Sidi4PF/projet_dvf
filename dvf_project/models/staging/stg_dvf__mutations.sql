{{ config(materialized='view') }}


WITH unioned AS (
    SELECT *, '2020_S2' AS source_file FROM {{ source('raw', 'DVF_2020_S2') }}
    UNION ALL
    SELECT *, '2021'    AS source_file FROM {{ source('raw', 'DVF_2021') }}
    UNION ALL
    SELECT *, '2022'    AS source_file FROM {{ source('raw', 'DVF_2022') }}
    UNION ALL
    SELECT *, '2023'    AS source_file FROM {{ source('raw', 'DVF_2023') }}
),

typed AS (
    SELECT
        -- Créer un ID unique si manquant
        COALESCE("identifiant_de_document", ROW_NUMBER() OVER (ORDER BY "date_mutation")) AS identifiant_de_document,
        "reference_document"             AS reference_document,
        "no_disposition"                 AS no_disposition,

        TRY_TO_DATE("date_mutation", 'DD/MM/YYYY') AS date_mutation,
        "nature_mutation"                AS nature_mutation,

        TRY_TO_NUMBER(REPLACE("valeur_fonciere", ',', '.')) AS valeur_fonciere,

        "no_voie"                        AS no_voie,
        "b/t/q"                          AS btq,
        "type_de_voie"                   AS type_de_voie,
        "code_voie"                      AS code_voie,
        "voie"                           AS voie,
        "code_postal"                    AS code_postal,
        "commune"                        AS commune,
        "code_departement"               AS code_departement,
        "code_commune"                   AS code_commune,

        "prefixe_de_section"             AS prefixe_de_section,
        "section"                        AS section,
        "no_plan"                        AS no_plan,

        "code_type_local"                AS code_type_local,
        "type_local"                     AS type_local,
        "identifiant_local"              AS identifiant_local,

        TRY_TO_NUMBER(REPLACE("surface_reelle_bati", ',', '.')) AS surface_reelle_bati,
        TRY_TO_NUMBER("nombre_pieces_principales")              AS nombre_pieces_principales,
        TRY_TO_NUMBER(REPLACE("surface_terrain", ',', '.'))     AS surface_terrain,

        "nature_culture"                 AS nature_culture,
        "nature_culture_speciale"        AS nature_culture_speciale,

        TRY_TO_NUMBER("nombre_de_lots")  AS nombre_de_lots,

        source_file,
        CURRENT_TIMESTAMP()              AS loaded_at

    FROM unioned
)

SELECT * FROM typed WHERE date_mutation IS NOT NULL