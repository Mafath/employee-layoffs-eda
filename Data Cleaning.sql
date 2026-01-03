select *
from layoffs;

-- Data Cleaning
-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null values or blank values
-- 4. Remove any columns or rows

# Since we are about to change the database alot, and if we made a mistake, we won't have the raw data available. So creating a copy is the best practise
# create a copy of the original table(an empty copy)
create table layoffs_staging
like layoffs;

# Insert data to the new table
insert layoffs_staging
select *
from layoffs;

select *
from layoffs_staging;
-- --------------------------------------
# STEP 01: REMOVE DUPLICATES
select *, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
from layoffs_staging;

# Find duplicate rows
with duplicate_cte as
(
select *, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;


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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;

insert into layoffs_staging2
select *, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
from layoffs_staging;

select *
from layoffs_staging2;

select *
from layoffs_staging2
where row_num > 1;

delete
from layoffs_staging2
where row_num > 1;


# STEP 02: STANDARDIZE THE DATA - finding issues in data and fixing

-- Lets look at company column
select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

-- --------------------------------
-- Lets look at industry column

select distinct industry
from layoffs_staging2
order by 1;
# seems like the crypto one is coming again and again

select *
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'crypto%';

-- --------------------------------
-- Lets look at location column

select distinct location
from layoffs_staging2
order by 1;

-- --------------------------------
-- Lets look at country column

select distinct country
from layoffs_staging2
order by 1;
# one Unite States has a period at the end

select *
from layoffs_staging2
where country like 'United States%';

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

-- --------------------------------
-- Lets look at date column
# rn date column is  a text column. can check it from the right panel. lets give a better style and change the data type

select `date`
from layoffs_staging2;

select `date`, str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set date = str_to_date(`date`, '%m/%d/%Y');

# Now change the data type

alter table layoffs_staging2
modify column `date` DATE;

# STEP 03: NULL VALUES OR BLANK VALUES
-- --------------------------------
-- Lets look at industry column

select *
from layoffs_staging2
where industry is null OR industry = '';

update layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2
where company = 'airbnb';

select t1.industry, t2.industry
from layoffs_staging2 t1 join layoffs_staging2 t2
on t1.company = t2.company
where (t1.industry is null or t1.industry = '') and t2.industry is not null;

update layoffs_staging2 t1 join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null and t2.industry is not null;


select *
from layoffs_staging2
where industry is null OR industry = '';
# Now only the Bally's are left


# so we populated some cells in industry column. Now there are few numerical columns left which we can not populate the values.
# total laid offs and perscentage laid offs could be populatable if we had the total no of employees
# for funds column, it might be able to scrape some data from the web and populate, but not in this project


# STEP 04: REMOVE ANY COLUMNS OR ROWS

select *
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;

select *
from layoffs_staging2;

# get rid of row_num column
alter table layoffs_staging2
drop column row_num;














