SELECT 
    u.email, 
    u.full_name AS 'name', 
    '' AS 'nik',
    c.title AS 'title',  -- Use only c.title
    MAX(cup.updated_at) AS 'last_updated',  -- Get the latest update date
    SUM(ROUND(cup.progress_duration, 0)) AS 'duration',  -- Sum up the duration
    AVG(cup.progress_percentage) as 'progress',
    'Course' AS 'type', 
    'Kognisi.id' AS 'platform'
FROM 
    course_user_progress cup 
LEFT JOIN
    course_users cu ON cup.course_serial = cu.course_serial 
    AND cup.user_serial = cu.user_serial 
LEFT JOIN 
    course_contents cc ON cup.course_content_serial = cc.serial
LEFT JOIN 
    course_sections cs ON cup.course_section_serial = cs.serial
LEFT JOIN 
    courses c ON cup.course_serial = c.serial
LEFT JOIN 
    categories cat ON c.category_serial = cat.serial  
LEFT JOIN 
    users u ON cup.user_serial = u.serial
WHERE 
    cc.type = 'VIDEO'
    AND u.id IS NOT NULL
    AND u.email NOT LIKE '%mailinator%'
    AND u.full_name NOT IN ('intan tesst test course', 'test alfi')
GROUP BY 
    u.email, u.full_name, c.title  -- Group by necessary fields
ORDER BY 
    last_updated DESC
