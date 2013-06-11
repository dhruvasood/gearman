DELIMITER $$

DROP PROCEDURE IF EXISTS spInsertNewWorkflow;

CREATE PROCEDURE `spInsertNewWorkflow`(
	pName varchar(255),
	pHostname varchar(255),
	pPid int(11),
	pNumSteps int(11)
)

BEGIN

	DECLARE counter int unsigned default 0;
	START TRANSACTION;
	INSERT INTO workflow(name,status,hostname,pid) VALUES (pName,'STARTED',pHostname,pPid);
	SET @workflow_id = LAST_INSERT_ID();
	while counter < pNumSteps do
                INSERT INTO workflow_steps(workflow_id,step_number)  VALUES (@workflow_id, counter+1);
  end while;
	SELECT @workflow_id as 'workflow_id';
	COMMIT;

END $$

DROP PROCEDURE IF EXISTS spUpdateWorkflowFailed;

CREATE PROCEDURE `spUpdateWorkflowFailed`(
	pWorkflowId int(11)
)

BEGIN

	UPDATE spUpdateWorkflowFailed SET status = 'FAILED' WHERE workflow_id = pWorkflowId;

END $$

DROP PROCEDURE IF EXISTS spUpdateWorkflowCompleted;

CREATE PROCEDURE `spUpdateWorkflowCompleted`(
	pWorkflowId int(11)
)

BEGIN

	UPDATE spUpdateWorkflowCompleted SET status = 'COMPLETED' WHERE workflow_id = pWorkflowId;

END $$

DROP PROCEDURE IF EXISTS spUpdateWorkflowStep;

CREATE PROCEDURE `spUpdateWorkflowStep`(
	pWorkflowId int(11),
	pStepNum int(11),
	pJobId varchar(48)
)

BEGIN

	UPDATE workflow_steps SET job_id = pJobId WHERE workflow_id = pWorkflowId AND step_number = pStepNum;

END $$