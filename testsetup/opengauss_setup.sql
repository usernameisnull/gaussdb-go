-- Create extensions and types.
create extension if not exists hstore;

create database pgx_test DBCOMPATIBILITY 'pg';

-- Create users for different types of connections and authentication.
create user pgx_ssl with SYSADMIN PASSWORD 'Gaussdb@123!';
create user pgx_sslcert with SYSADMIN PASSWORD 'Gaussdb@123!';
create user pgx_md5 with SYSADMIN PASSWORD 'Gaussdb@123!';
create user pgx_pw with SYSADMIN PASSWORD 'Gaussdb@123!';
create user pgx_scram with SYSADMIN PASSWORD 'Gaussdb@123!';

-- The tricky test user, below, has to actually exist so that it can be used in a test
-- of aclitem formatting. It turns out aclitems cannot contain non-existing users/roles.
-- todo ERROR:  Invalid name:  tricky, ' } " \\ test user .
--create user " tricky, ' } "" \\ test user " superuser password 'secret';
