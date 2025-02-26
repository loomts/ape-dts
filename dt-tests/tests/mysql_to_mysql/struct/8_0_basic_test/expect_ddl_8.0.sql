struct_it_mysql2mysql_1
CREATE DATABASE `struct_it_mysql2mysql_1` /*!40100 DEFAULT CHARACTER SET utf8mb3 */ /*!80016 DEFAULT ENCRYPTION='N' */

struct_it_mysql2mysql_1.expression_defaults
CREATE TABLE `expression_defaults` (
  `i` int DEFAULT '0',
  `c` varchar(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT '',
  `f` float DEFAULT ((rand() * rand())),
  `b` binary(16) DEFAULT (uuid_to_bin(uuid())),
  `d` date DEFAULT ((curdate() + interval 1 year)),
  `p` point DEFAULT (point(0,0)),
  `j` json DEFAULT (json_array())
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3