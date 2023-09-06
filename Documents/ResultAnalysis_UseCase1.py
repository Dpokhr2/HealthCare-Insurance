# SQL Code
# USE CASE 1: Which disease has a maximum number of claims.

# a separate schema and table for query result
Create table project_output.UseCase1(disease_name varchar(100), Max_number_claim varchar(100));

# Use Insert into command to insert the result in the table
Insert INTO project_output.usecase1(disease_name, Max_number_claim) Select disease.disease_name, COUNT(claims.claim_id) AS claim_count from project.disease
INNER JOIN project.claims
ON disease.disease_name = claims.disease_name
Group By disease.disease_name
ORDER BY claim_count DESC
LIMIT 1;

# To check if data is successfully inserted into the table
Select * from project_output.usecase1