-- Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License"); you may not use
-- this file except in compliance with the License. You may obtain a copy of the
-- License at  http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software distributed
-- under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
-- CONDITIONS OF ANY KIND, either express or implied.

DELIMITER //
DROP PROCEDURE IF EXISTS create_index_if_not_exists //
CREATE PROCEDURE create_index_if_not_exists (
	 IN schemaName VARCHAR(128)
	,IN tableName VARCHAR(128)
	,IN indexName VARCHAR(128)
	,IN indexType ENUM('unique')
	,IN indexDefinition VARCHAR(1024))
BEGIN
	DECLARE sqlStatement VARCHAR(4095) DEFAULT '';
	SET schemaName = COALESCE(schemaName, SCHEMA());
	IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.STATISTICS
			WHERE table_schema = schemaName AND table_name = tableName
			AND index_name = indexName) THEN
		SET indexType = COALESCE(indexType, '');
		SET sqlStatement = CONCAT('ALTER TABLE `', schemaName, '`.`', tableName, '` ADD ', UPPER(indexType), ' INDEX `', indexName, '` ', indexDefinition);
		SET @sql = sqlStatement;
		PREPARE preparedSqlStatement FROM @sql;
		EXECUTE preparedSqlStatement;
		DEALLOCATE PREPARE preparedSqlStatement;
	END IF;
END //
DELIMITER ;

DELIMITER //
DROP PROCEDURE IF EXISTS drop_index_if_exists //
CREATE PROCEDURE drop_index_if_exists (
	 IN schemaName VARCHAR(128)
	,IN tableName VARCHAR(128)
	,IN indexName VARCHAR(128))
BEGIN
	DECLARE sqlStatement VARCHAR(4095) DEFAULT '';
	SET schemaName = COALESCE(schemaName, SCHEMA());
	IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.STATISTICS
			WHERE table_schema = schemaName AND table_name = tableName
			AND index_name = indexName) THEN
		SET sqlStatement = CONCAT('ALTER TABLE `', schemaName, '`.`', tableName, '` DROP INDEX `', indexName, '`');
		SET @sql = sqlStatement;
		PREPARE preparedSqlStatement FROM @sql;
		EXECUTE preparedSqlStatement;
		DEALLOCATE PREPARE preparedSqlStatement;
	END IF;
END //
DELIMITER ;