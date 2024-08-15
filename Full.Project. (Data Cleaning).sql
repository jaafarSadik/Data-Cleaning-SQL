-- Data Cleaning

-- 1. Remove Duplicates
-- 2. Standarize the Data
-- 3. Null Values or blank values
-- 4. Remove Any Columns

select *
from layoffs;
select *
from layoffs_staging;
select *
from layoffs_staging2;





#Create Copy of Origenal table

create table layoffs_staging
like layoffs;
insert layoffs_staging
select *
from layoffs;
select *
from layoffs_staging;





# 1. Remove Duplicates

select *,
row_number() over( partition by company,industry,
 location, total_laid_off, percentage_laid_off,
 `date` , stage, country, funds_raised_millions) as row_num
from layoffs_staging;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
select *,
row_number() over( partition by company,industry,
 location, total_laid_off, percentage_laid_off,
 `date` , stage, country, funds_raised_millions) as row_num
from layoffs_staging;

select *
from layoffs_staging2
where row_num > 1;
delete
from layoffs_staging2
where row_num > 1;





# 2. Standarize the Data

-- a. Standarize company
select company, trim(company)
from layoffs_staging2;
update layoffs_staging2
set company = trim(company);

-- b. Standarize industry
select distinct industry
from layoffs_staging2
order by 1;
update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

-- c. Standarize country
select distinct country
from layoffs_staging2
order by 1;
update layoffs_staging2
set country = 'United States'
where country like 'United States%';

-- d. Standarize date
select `date`
from layoffs_staging2;
update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;





# 3. Null Values or blank values

update layoffs_staging2
set industry = null
where industry = '';
select *
from layoffs_staging2
where industry is null
or industry = '';

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where t1.industry is null
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;





# 4. Remove Any Columns

select *
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;
delete
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

-- delete row_num 
alter table layoffs_staging2
drop column row_num;







