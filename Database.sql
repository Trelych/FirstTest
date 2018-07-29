--creating database and tables

CREATE DATABASE weather;


CREATE TABLE sities
(
  id serial PRIMARY KEY NOT NULL,
  name varchar NOT NULL,
  country varchar NOT NULL,
  weather_api_id int NOT NULL
);
CREATE UNIQUE INDEX sities_id_uindex ON sities1 (id);
CREATE UNIQUE INDEX sities_weather_api_id_uindex ON sities1 (weather_api_id)



CREATE TABLE public.forecast
(
  id serial PRIMARY KEY NOT NULL,
  time int NOT NULL,
  temp decimal NOT NULL,
  humidity decimal NOT NULL,
  pressure decimal NOT NULL,
  city_id int,
  CONSTRAINT city_Id FOREIGN KEY (city_id) REFERENCES public.sities1 (id)
);
CREATE UNIQUE INDEX forecast1_id_uindex ON public.forecast1 (id);
