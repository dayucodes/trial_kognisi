SELECT *
FROM (
    /* Part 1: course packages */
    SELECT 
        u.email, 
        u.name,
        u.nik,
        cp.title AS title,
        MAX(cmu.updated_at) AS last_updated,
        SUM(cmu.progress_duration) AS duration,
        ROUND(AVG(cmu.progress), 0) AS progress, 
        'Course' AS type, 
        'Kognisi MyKG' AS platform
    FROM course_material_users cmu
        JOIN users u ON u.id = cmu.user_id
        JOIN course_materials cm ON cmu.course_material_id = cm.id
        JOIN course_packages cp ON cmu.course_package_id = cp.id
        /* removed course_sections cs (unused) */
        /* removed cpue join (unused in SELECT/WHERE) */
    WHERE 
        u.name NOT IN ('SISDMv2','SEMOGA LAST TEST','Testing aja','TEST')
        AND cm.deleted_at IS NULL
        AND cp.title NOT IN ('Kursus Contoh')
    GROUP BY u.email, u.name, u.nik, cp.title

    UNION ALL
    
    /* Part 2: single courses */
    SELECT 
        u.email, 
        u.name, 
        u.nik,
        c.title AS title,
        MAX(cu.updated_at) AS last_updated,
        SUM(
            CASE 
                WHEN c.type = 1 THEN cu.progress_duration
                WHEN c.type = 2 THEN TIME_TO_SEC(TIMEDIFF(cs2.end_time, cs2.start_time)) 
                ELSE 0
            END
        ) AS duration,
        ROUND(MAX(cu.progress), 0) AS progress,
        CASE 
            WHEN c.type = 1 THEN 'Video'
            WHEN c.type = 2 THEN 'Inclass'
            ELSE 'Other'
        END AS type,
        'Kognisi MyKG' AS platform
    FROM course_users cu
        JOIN users u ON cu.user_id = u.id 
        JOIN courses c ON cu.course_id = c.id
        LEFT JOIN course_schedules cs ON cs.course_id = c.id 
        LEFT JOIN course_sessions cs2 ON cs2.course_schedule_id = cs.id 
    WHERE 
        u.name NOT IN ('SISDMv2','SEMOGA LAST TEST','Testing aja','TEST')
        AND c.deleted_at IS NULL
    GROUP BY u.email, u.name, u.nik, c.title, c.type
) AS combined_results
ORDER BY last_updated DESC;
