select 
	Name,
	course,
	Roll_No,
  Place,
	year
fron {{ ref('testing_raw_temp') }} temp
where temp.failure_reason is null
