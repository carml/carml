USE test;
-- SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS test.Student_Sport;
DROP TABLE IF EXISTS test.Student;
-- SET FOREIGN_KEY_CHECKS = 1;

CREATE TABLE Student (
ID INTEGER,
Name CHAR(15)
);
INSERT INTO Student (ID,Name) VALUES (10,'Venus');
INSERT INTO Student (ID,Name) VALUES (20,'Fernando');
INSERT INTO Student (ID,Name) VALUES (30,'David');
