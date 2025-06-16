CREATE USER 'test_create'@'%' IDENTIFIED BY '123456_old';
ALTER USER 'test_create'@'%' IDENTIFIED BY '123456';

CREATE USER 'test_drop'@'%' IDENTIFIED BY '123456';
DROP USER 'test_drop'@'%';

CREATE USER 'test_grant'@'%' IDENTIFIED BY '123456';
GRANT SELECT ON dcl_test_1.tb1 TO 'test_grant'@'%';

CREATE USER 'test_revoke'@'%' IDENTIFIED BY '123456';
GRANT SELECT ON dcl_test_1.tb1 TO 'test_revoke'@'%';
GRANT SELECT ON dcl_test_1.tb2 TO 'test_revoke'@'%';
REVOKE SELECT ON dcl_test_1.tb2 FROM 'test_revoke'@'%';

CREATE USER 'test_role1'@'%' IDENTIFIED BY '123456';
CREATE ROLE 'role1';
GRANT SELECT ON dcl_test_1.tb1 to 'role1';
CREATE ROLE 'role2';
GRANT SELECT ON dcl_test_1.tb2 to 'role2';
GRANT 'role1','role2' TO 'test_role1'@'%';
SET DEFAULT ROLE 'role1' TO 'test_role1'@'%';

CREATE ROLE 'role3';
GRANT SELECT ON dcl_test_1.* to 'role3';
CREATE USER 'test_role2'@'%' IDENTIFIED BY '123456';
GRANT 'role3' TO 'test_role2'@'%';
DROP ROLE 'role3';