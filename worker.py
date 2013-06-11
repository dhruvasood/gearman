import MySQLdb, gearman, json, argparse, sys, daemon, signal, socket, os, subprocess, time, signal
import logging, logging.config
import ConfigParser

def setup_sighandlers():
    for i in [x for x in dir(signal) if x.startswith("SIG")]:
        try:
            signum = getattr(signal, i)
            signal.signal(signum, signal.SIG_DFL)
        except RuntimeError, m:
            logging.error("Skipping signal: %s" % i)

def key_val_strict(dict1, key):
    if dict1.has_key(key):
        return dict1[key]
    else:
        raise Exception("Error: key %s not present in dict!" % key)

def key_val_relaxed(dict1, key):
    if dict1.has_key(key):
        return dict1[key]
    else:
        return False

def parse_section(config, section):
    map = {}
    options = config.options(section)
    for option in options:
        try:
            map[option] = config.get(section, option)
            if map[option] == -1:
                logging.debug("skipping option: %s in section: %s" % option, section)
        except:
            error_parsing_option = "Error: Parsing option failed on option: %s!" % option
            raise Exception(error_parsing_option)
    return map

def parse_config(config_file):
    maps = {}
    logging.info("Parsing the config file: %s.." % config_file)
    Config = ConfigParser.ConfigParser()
    Config.read(config_file)
    for section in Config.sections():
        maps[section] = parse_section(Config,section)
    return maps

def db_conn(conn_dict):
    num_db_attempts = 0
    while num_db_attempts < 3:
        try:
            logging.info("Trying to establish a connection to mysql db..")
            server = key_val_strict(conn_dict,'server')
            db = key_val_strict(conn_dict,'db')
            user = key_val_strict(conn_dict,'user')
            password = key_val_strict(conn_dict,'password')
            logging.info("Connecting with MySQL server:%s on db:%s, user:%s, password:<not shown>" % (server, db, user))
            conn = MySQLdb.connect(host=server, user=user, passwd=password, db=db)
            logging.info("Connection established..")
            return conn
        except MySQLdb.Error, e:
            try:
                logging.error("MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
            except IndexError:
                logging.error("MySQL Error: %s" % str(e))
        num_db_attempts += 1
        time.sleep(10)
    raise Exception("Error: could not connect to db!")

def db_execute(query):
    conn = db_conn(conn_dict)
    cur = conn.cursor()
    try:
        logging.debug("Running query: %s" % query)
        cur.execute(query)
        conn.commit()
    except MySQLdb.Error, e:
        try:
            logging.error("MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
        except IndexError:
            logging.error("MySQL Error: %s" % str(e))
        conn.rollback()
        raise Exception("Error: executing sql query: %s" % query)
    finally:
        cur.close()
        conn.close()

def db_execute_select_one(query):
    conn = db_conn(conn_dict)
    cur = conn.cursor()
    try:
        logging.debug("Running query: %s" % query)
        cur.execute(query)
        return cur.fetchone()
    except MySQLdb.Error, e:
        try:
            logging.error("MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
        except IndexError:
            logging.error("MySQL Error: %s" % str(e))
        raise Exception("Error: executing sql query: %s" % query)
    finally:
        cur.close()
        conn.close()

def db_set_job_completed(job_id):
    db_execute("call spUpdateLastJobRunCompleted('%s');" % job_id)
    return 'COMPLETED'

def db_set_job_failed(job_id):
    db_execute("call spUpdateLastJobRunFailed('%s');" % job_id)
    return 'FAILED'

def db_insert_new_job(job_id, execution, cleanup, retries, hostname):
    db_execute("call spInsertNewJobRun('%s','%s','%s','%d', '%s');" % (job_id, execution, cleanup, retries, hostname, os.getpid()))

def run(gearman_worker, gearman_job):
    print "got job %s" % str(os.getpid())
    dir = GEARMAN_DIR + '/' + gearman_job.unique
    run_logger = logging.getLogger("runModule")
    run_logger.info("Going to run a job with id: %s" % gearman_job.unique)
    params = json.loads(gearman_job.data)
    run_logger.debug("Arguments for this job: %s" % str(params))
    execution = key_val_strict(params, 'execution')
    cleanup = key_val_relaxed(params, 'cleanup') or ''
    retries = int(key_val_relaxed(params, 'retries')) or 1
    job_stdin = key_val_relaxed(params, 'stdin') or None
    job_stdout = key_val_relaxed(params, 'stdout') or dir+'/stdout'
    job_stderr = key_val_relaxed(params, 'stderr') or dir + '/stderr'
    run_logger.debug("Execution script: %s, Cleanup script: %s, Number of retries: %d" % (execution, cleanup, retries))
    if not os.path.exists(dir):
        run_logger.debug("creating gearman tracker dir %s" % dir)
        os.makedirs(dir, 0755)
    row = db_execute_select_one("call spGetJobRuns('%s');" % gearman_job.unique)
    attempts = row[0] if row is not None else 0
    status = row[1] if row is not None else None
    run_logger.debug("Attempts: %d, Status = %s" % (attempts,status))
    while status != 'COMPLETED':
        if attempts < retries:
            if status == 'STARTED':
                status = db_set_job_failed(gearman_job.unique)
            if status == 'FAILED' and cleanup.__len__() > 0:
                run_logger.info("Cleaning up after the unsuccessful run of job: %s" % gearman_job.unique)
                p = subprocess.Popen(cleanup,shell=True)
                p.wait()
                if p.returncode != 0:
                    run_logger.error("Failed to clean up job: %s" % gearman_job.unique)
                    break
            db_insert_new_job(gearman_job.unique, execution, cleanup, retries, socket.gethostname())
            run_logger.debug("Executing for job: %s the script: %s" % (gearman_job.unique, execution))
            p = subprocess.Popen(execution, stdin=open(job_stdin, "r") if job_stdin is not None else None, stdout=open(job_stdout, "wb+"), stderr=open(job_stderr,"wb+"), shell=True)
            p.wait()
            if p.returncode == 0:
                status = db_set_job_completed(gearman_job.unique)
                break
            attempts += 1
        else:
            if status != 'FAILED' or status != 'COMPLETED':
                status = db_set_job_failed(gearman_job.unique)
                break
    if status == 'COMPLETED':
        run_logger.info("Job %s completed!" % gearman_job.unique)
        return 0
    else:
        run_logger.error("Job %s failed!" % gearman_job.unique)
        return 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Worker for gearman jobs.',epilog='')
    parser.add_argument('-c', '--config', action='store', nargs=1, default='properties.conf',
                        help="Config file for gearman and database properties. Default: %(default)s",
                        dest='gearman_config')
    parser.add_argument('-g', '--gearman-servers',  action='store', nargs='+', default=['localhost:4730'],
                        help="Gearman servers list. Default: %(default)s", dest='gearman_servers')
    parser.add_argument('-d', '--daemonize',    action='store_const', const=True, default=False, dest='daemonize',
                        help='Daemonizes this worker to run as a deamon in the background and perform tasks for clients. Default: %(default)s')
    parser.add_argument('--version',        action='version', version='%(prog)s 0.1a')
    cmd = parser.parse_args()
    logging.config.fileConfig("logging.conf")
    my_logger = logging.getLogger("mainModule")
    my_logger.debug("Program called with the following arguments:\n%s" % str(cmd))
    if cmd.daemonize:
        try:
            daemon.daemonize()
        except:
            logging.error("Couldn't daemonize process")
    try:
        setup_sighandlers()
        maps = parse_config(cmd.gearman_config[0])
        conn_dict = maps['DB']
        db_conn(conn_dict).close()
        GEARMAN_DIR = maps['GEARMAN']['dir']
        logging.info("Connecting to gearman server(s)")
        gm_worker = gearman.GearmanWorker(cmd.gearman_servers)
        logging.info("Registering tasks with the server")
        gm_worker.register_task("run",run)
        logging.info("Entering the Gearman worker loop")
        gm_worker.work()
        logging.info("Worker is ready ..")
    except (gearman.errors, gearman.errors.ServerUnavailable):
        logging.error("Error connecting to Gearman server: " + str(sys.exc_info()[0]))
    except Exception, e:
        logging.error(e)
    except:
        import traceback
        logging.error("Unhandled exception caught!")
        logging.error(traceback.print_exc())
    finally:
        exit(1)

