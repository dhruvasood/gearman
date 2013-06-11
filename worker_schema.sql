DROP TABLE IF EXISTS worker;
CREATE TABLE worker (
	id int(11) NOT NULL AUTO_INCREMENT,
	job_id varchar(48) NOT NULL,
	execution varchar(255) NOT NULL,
	cleanup varchar(255),
	num_retries int(11),
	status varchar(16),
	hostname varchar(255),
	pid int(11),
	created_at TIMESTAMP DEFAULT '000-00-00 00:00:00',
	updated_at TIMESTAMP DEFAULT '0000-00-00 00:00:00',
	PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TRIGGER recTime_insert_worker BEFORE INSERT ON worker
FOR EACH ROW SET NEW.created_at = NOW(), NEW.updated_at = NOW();

CREATE TRIGGER recTime_update_worker BEFORE UPDATE ON worker
FOR EACH ROW SET NEW.updated_at = NOW();