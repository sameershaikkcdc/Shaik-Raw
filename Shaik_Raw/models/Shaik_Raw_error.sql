select 
	Name,
	course,
	Roll_No,
	year
fron {{ ref('Shaik_Raw_temp') }} temp
where array_length(temp.failure_reason) > 0
