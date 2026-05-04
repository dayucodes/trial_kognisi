SELECT
	u.email,
	u.name,
	u.nik,
	uabr.name AS 'title',
	uabr.end_date AS 'last_updated',
	uabr.duration_in_seconds AS 'duration',
	100 AS 'progress',
	'Book' AS 'type',
	'Self-Input'AS 'platform'
FROM
	user_activity_book_reads uabr
LEFT JOIN
	users u ON uabr.user_id =u.id
WHERE
    LOWER(uabr.name) NOT LIKE '%[test]%'
    AND LOWER(uabr.name) NOT IN ('aaa', 'bbb', 'cccc', 'dddd', 'eee', 'eeee', 'qqqqq', 'test', 'testing', 'wkwkwk', 'wwew', 'wwwe')
	
UNION ALL

SELECT
	u.email,
	u.name,
	u.nik,
	uamas.name AS 'title',
	uamas.end_date AS 'last_updated',
	uamas.duration_in_seconds AS 'duration',
	'100' AS 'progress',
	'Mentoring' AS 'type',
	'Self-Input' AS 'platform'
FROM
	user_activity_mentor_and_speakers uamas
LEFT JOIN
	users u ON uamas.user_id = u.id
WHERE
    LOWER(uamas.name) NOT LIKE '%[test]%'
    AND LOWER(uamas.name) NOT IN ('aaa', 'bbb', 'cccc', 'dddd', 'eee', 'eeee', 'qqqqq', 'test', 'testing', 'wkwkwk', 'wwew', 'wwwe')

UNION ALL

SELECT
	u.email,
	u.name,
	u.nik,
	uat.name AS 'title',
	uat.end_date AS 'last_updated',
	uat.duration_in_seconds AS 'duration',
	'100' AS 'progress',
	'Training' AS 'type',
	'Self-Input' AS 'platform'
FROM
	user_activity_trainings uat 
LEFT JOIN
	users u ON uat.user_id = u.id
WHERE
    LOWER(uat.name) NOT LIKE '%[test]%'
    AND LOWER(uat.name) NOT IN ('aaa', 'bbb', 'cccc', 'dddd', 'eee', 'eeee', 'qqqqq', 'test', 'testing', 'wkwkwk', 'wwew', 'wwwe')
	
UNION ALL

SELECT
	u.email,
	u.name,
	u.nik,
	uawc.name AS 'title',
	uawc.updated_at AS 'last_updated',
	uawc.duration_in_seconds AS 'duration',
	'100' AS 'progress',
	'Content' AS 'type',
	'Self-Input' AS 'platform'
FROM
	user_activity_watched_contents uawc 
LEFT JOIN
	users u ON uawc.user_id = u.id
WHERE
    LOWER(uawc.name) NOT LIKE '%[test]%'
    AND LOWER(uawc.name) NOT IN ('aaa', 'bbb', 'cccc', 'dddd', 'eee', 'eeee', 'qqqqq', 'test', 'testing', 'wkwkwk', 'wwew', 'wwwe')
	
UNION ALL

SELECT
	u.email,
	u.name,
	u.nik,
	uav.name AS 'title',
	uav.end_date AS 'last_updated',
	uav.duration_in_seconds AS 'duration',
	'100' AS 'progress',
	'Volunteer' AS 'type',
	'Self-Input' AS 'platform'
FROM
	user_activity_volunteers uav 
LEFT JOIN
	users u ON uav.user_id = u.id
WHERE
    LOWER(uav.name) NOT LIKE '%[test]%'
    AND LOWER(uav.name) NOT IN ('aaa', 'bbb', 'cccc', 'dddd', 'eee', 'eeee', 'qqqqq', 'test', 'testing', 'wkwkwk', 'wwew', 'wwwe')
