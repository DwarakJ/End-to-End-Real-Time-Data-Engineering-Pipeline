-- Create a schema named retail
CREATE SCHEMA IF NOT EXISTS retail;

-- Create the employees table
CREATE TABLE IF NOT EXISTS retail.customers
(
    name text COLLATE pg_catalog."default" NOT NULL,
    gender text COLLATE pg_catalog."default",
    address text COLLATE pg_catalog."default",
    city text COLLATE pg_catalog."default",
    nation text COLLATE pg_catalog."default",
    zip text COLLATE pg_catalog."default",
    latitude numeric,
    longitude numeric,
    email text COLLATE pg_catalog."default"
)