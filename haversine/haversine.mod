# Расчет расстояния доставки на основе координат

set STORES; #магазины
set DC; #распределительные центры

# Параметры магазинов
param store_lat{STORES}; #Координаты магазинов
param store_lon{STORES};

# Параметры РЦ
param dc_lat{DC}; #Координаты РЦ
param dc_lon{DC};


param rdf default 1.3; #коэффициент связности дорожной сети (1.3 в среднем по России)
param min_distance default 50; #минимальное расстояние от РЦ до магазина в км - для случая, когда склад и магазин в одном городе


/* 
 Старая формула - расстояния на основе расстояния в градусах
 min_distance - это минимальное расстояние от РЦ до магазина, 
 параметр необходим для корректного расчета затрат для тех маршрутов,
 где РЦ и магазин находятся в одном городе
 
 param distance {d in DC, s in STORES} := 
 			max(rdf * sqrt((71.5 * (store_lon[s] - dc_lon[d]))^2 
 							+ (111 * (store_lat[s] - dc_lat[d]))^2), min_distance);

Формула дает завышенное расстояние, в среднем на 10% */


/* 
 Расчет расстояний по формуле gis_lab
 http://gis-lab.info/qa/great-circles.html
 */

#Пересчет координат в радианы
param pi := 3.14159265358979;
param dc_lat_r {d in DC} := dc_lat[d] * pi / 180;
param dc_lon_r {d in DC} := dc_lon[d] * pi / 180;
param store_lat_r {s in STORES} := store_lat[s] * pi / 180;
param store_lon_r {s in STORES} := store_lon[s] * pi / 180;

#разница координат по долготе:
param delta_lon_r {d in DC, s in STORES} := store_lon_r[s] - dc_lon_r[d];

#угловая разница координтат (в радианах):
param delta {d in DC, s in STORES} :=
	atan( sqrt( (cos(store_lat_r[s])*sin(delta_lon_r[d,s]))^2 
					+ (cos(dc_lat_r[d])*sin(store_lat_r[s]) 
					- sin(dc_lat_r[d])*cos(store_lat_r[s])*cos(delta_lon_r[d,s]))^2 )
		/ ( sin(dc_lat_r[d])*sin(store_lat_r[s]) 
			+ cos(dc_lat_r[d])*cos(store_lat_r[s])*cos(delta_lon_r[d,s]) )
		);
				
param km_radian := 6372.795; #радиус Земли в км - для пересчета расстояния в радианах в км

param distance {d in DC, s in STORES} := 
		max(delta[d, s] * km_radian * rdf, min_distance); #расстояние в км