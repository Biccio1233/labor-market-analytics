CREATE OR REPLACE VIEW istat.kpi_lavoro AS
WITH base AS (
    SELECT
        t.time_period,
        t.obs_value::numeric AS valore,
        r.name_it AS regione,
        s.name_it AS sesso,
        e.name_it AS classe_eta,
        c.name_it AS condizione_lavoro,
        df.nome_it AS indicatore
    FROM istat.dataflow df
    JOIN istat.dataflow_categories dc 
        ON df.id = dc.dataflow_id
    JOIN istat.categories cat
        ON dc.category_id = cat.category_id
    JOIN istat.tabella_lav t
        ON t.dataflow_id = df.id -- ipotesi: corrispondenza diretta
    LEFT JOIN istat.cl_itter107_import r 
        ON t.territorio = r.code_id
    LEFT JOIN istat.cl_sesso_import s 
        ON t.sesso = s.code_id
    LEFT JOIN istat.cl_eta1_import e 
        ON t.eta = e.code_id
    LEFT JOIN istat.cl_condizione_prof_import c
        ON t.condizione_prof = c.code_id
    WHERE cat.category_id LIKE 'LAV%'
),
 aggregati AS (
    SELECT
        time_period,
        regione,
        SUM(CASE WHEN condizione_lavoro = 'Occupati'
                 THEN valore ELSE 0 END) AS tot_occupati,
        SUM(CASE WHEN condizione_lavoro = 'In cerca di occupazione'
                 THEN valore ELSE 0 END) AS tot_disoccupati,
        SUM(valore) AS tot_persone
    FROM base
    GROUP BY time_period, regione
)
SELECT
    time_period,
    regione,
    tot_occupati,
    tot_disoccupati,
    ROUND(
        CASE WHEN tot_persone = 0 THEN 0
             ELSE (tot_occupati / tot_persone * 100)
        END, 2
    ) AS tasso_occupazione,
    ROUND(
        CASE WHEN tot_persone = 0 THEN 0
             ELSE (tot_disoccupati / tot_persone * 100)
        END, 2
    ) AS tasso_disoccupazione
FROM aggregati
;
