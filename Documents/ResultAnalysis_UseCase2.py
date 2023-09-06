#2 - ●	Find those Subscribers having age less than 30 and they subscribe any subgroup

# a separate schema and table for query result
Create table project_output.UseCase2(first_name varchar(100), subgrp_name varchar(100));

# Use Insert into command to insert the result in the table
Insert Into project_output.UseCase2(first_name, subgrp_name) Select subscriber.first_name, subgrp.subgrp_name FROM project.subscriber
INNER JOIN project.subgrp ON subscriber.subgrp_id = subgrp.subgrp_id
Where DATEDIFF(YEAR, CAST(birth_date AS DATE), GETDATE()) < 30

#Select command to check the insert
Select * from project_output.UseCase2