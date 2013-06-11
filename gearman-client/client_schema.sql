DROP TABLE IF EXISTS workflow;
CREATE TABLE workflow (
	id int(11) NOT NULL AUTO_INCREMENT,
	name varchar(255) NOT NULL,
	status varchar(16),
  hostname varchar(255),
	pid int(11),
	created_at TIMESTAMP DEFAULT '000-00-00 00:00:00',
	updated_at TIMESTAMP DEFAULT '000-00-00 00:00:00',
	PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TRIGGER recTime_insert_workflow BEFORE INSERT ON workflow
FOR EACH ROW SET NEW.created_at = NOW(), NEW.updated_at = NOW();

CREATE TRIGGER recTime_update_workflow BEFORE UPDATE ON workflow
FOR EACH ROW SET NEW.updated_at = NOW();

DROP TABLE IF EXISTS workflow_steps;
CREATE TABLE workflow_steps (
	id int(11) NOT NULL AUTO_INCREMENT,
	workflow_id int(11),
	step_number int(11),
	job_id varchar(48),
	created_at TIMESTAMP DEFAULT '000-00-00 00:00:00',
  updated_at TIMESTAMP DEFAULT '000-00-00 00:00:00',
	PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TRIGGER recTime_insert_workflow_steps BEFORE INSERT ON workflow_steps
FOR EACH ROW SET NEW.created_at = NOW(), NEW.updated_at = NOW();

CREATE TRIGGER recTime_update_workflow_steps BEFORE UPDATE ON workflow_steps
FOR EACH ROW SET NEW.updated_at = NOW();