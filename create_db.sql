/*
drop database if exists pre_geoip;

create database pre_geoip
    character set = 'utf8'
    collate = 'utf8_general_ci';

use pre_geoip;
*/

use geoip;

drop table if exists csv_country4;
drop table if exists csv_country6;
drop table if exists csv_city4;
drop table if exists ip_new;
drop table if exists ip_city_new;

create table csv_country4 (
    start_ip varCHAR(40) NOT NULL,
    end_ip varCHAR(40) NOT NULL,
    start decimal(40) unsigned NOT NULL,
    end decimal(40) unsigned NOT NULL,
    ccode CHAR(2) NOT NULL,
    cname VARCHAR(50) NOT NULL
    );

create table csv_country6 (
    start_ip varCHAR(40) NOT NULL,
    end_ip varCHAR(40) NOT NULL,
    start decimal(40) unsigned NOT NULL,
    end decimal(40) unsigned NOT NULL,
    ccode CHAR(2) NOT NULL,
    cname VARCHAR(50) NOT NULL
    );

create table csv_city4 (
    start decimal(40) unsigned NOT NULL,
    end decimal(40) unsigned NOT NULL,
    locid int not null
    );

create table ip_new (
    start decimal(40) unsigned not null,
    end decimal(40) unsigned not null,
    cid int unsigned not null,
    primary key(start, end)
    );

create table ip_city_new (
    start decimal(40) unsigned not null,
    end decimal(40) unsigned not null,
    cid int unsigned not null,
    primary key(start, end)
    );