create database demostracion;
use demostracion;

DROP TABLE IF EXISTS `producto`;
CREATE TABLE IF NOT EXISTS `producto` (
	`id` 					VARCHAR(500),
    `marca` 				VARCHAR(500),
    `nombre` 				VARCHAR(600), 
    `presentacion` 			VARCHAR(600),
    `categoria1` 			VARCHAR(600),
    `categoria2` 			VARCHAR(600),
    `categoria3` 			VARCHAR(600)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_spanish_ci;
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 5.7\\\Uploads\\archivo_2_finish.csv'
INTO TABLE `producto`
FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY ''
LINES TERMINATED BY '\n' IGNORE 1 LINES;


select AVG(precio) from tabla_general tg
where sucursal_id='9-1-688';