DELIMITER $$

DROP PROCEDURE IF EXISTS spInsertNewJobRun;

CREATE PROCEDURE `spInsertNewJobRun`(
	pJobId varchar(48),
	pExecution varchar(255),
	pCleanup varchar(255),
	pRetries int(11),
	pHostname varchar(255),
	pPid int(11)
)

BEGIN

	INSERT INTO worker(job_id,execution,cleanup,num_retries,hostname,status,pid) VALUES(pJobId,pExecution,pCleanup,pRetries,pHostname,'STARTED',pPid);

END $$

DROP PROCEDURE IF EXISTS spUpdateLastJobRunFailed;

CREATE PROCEDURE `spUpdateLastJobRunFailed`(
	pJobId varchar(48)
)

BEGIN

	UPDATE worker SET status = 'FAILED' WHERE job_id = pJobId ORDER BY created_at DESC LIMIT 1;

END $$

DROP PROCEDURE IF EXISTS spUpdateLastJobRunCompleted;

CREATE PROCEDURE `spUpdateLastJobRunCompleted`(
	pJobId varchar(48)
)

BEGIN

	UPDATE worker SET status = 'COMPLETED' WHERE job_id = pJobId ORDER BY created_at DESC LIMIT 1;

END $$

DROP PROCEDURE IF EXISTS spGetJobRuns;

CREATE PROCEDURE `spGetJobRuns`(
	pJobId varchar(48)
)

BEGIN

	DECLARE pCount int;
	START TRANSACTION;
	SELECT count(*) FROM worker WHERE job_id = pJobId INTO pCount;
	SELECT pCount, status FROM worker WHERE job_id = pJobId AND created_at = (SELECT MAX(created_at) FROM worker WHERE job_id = pJobId);
	COMMIT;

END $$