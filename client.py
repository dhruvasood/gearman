__author__ = 'sadhan.sood@medialets.com'
import time, MySQLdb, gearman, json, argparse, os, sys, signal, croniter, datetime, socket
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
        maps[section] = parse_section(Config, section)
    return maps

def db_conn(conn_dict):
    num_db_attempts = 0
    while num_db_attempts < 3:
        try:
            logging.info("Trying to establish a connection to mysql db..")
            server = key_val_strict(conn_dict,'server')
            db = key_val_strict(conn_dict,'db')
            user = key_val_strict(conn_dict,'user')
            passwd = key_val_strict(conn_dict,'password')
            logging.info("Connecting with MySQL server:%s on db:%s, user:%s, password:<not shown>" % (server,db,user))
            conn = MySQLdb.connect(host=server, user=user, passwd=passwd, db=db)
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
        #print "query = %s" % query
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

def setup_workflow_in_db(name, num_steps):
    row = db_execute_select_one("call spInsertNewWorkflow('%s','%d', '%s', '%d');" % (name, num_steps, socket.gethostname(), int(os.getpid())))
    return row[0]

def db_set_workflow_step_completed(workflow_id,step_num,job_id):
    db_execute("call spUpdateWorkflowStep('%d','%d','%s');" % (workflow_id,step_num,job_id))

def db_set_workflow_completed(workflow_id):
    db_execute("call spUpdateWorkflowCompleted('%s');" % workflow_id)

def db_set_workflow_failed(workflow_id):
    db_execute("call spUpdateWorkflowFailed('%s');" % workflow_id)

def find_and_replace(val, stdiomap):
    dollar_start = val.find("$$__", 0)
    while dollar_start != -1:
        dollar_end = val.find("__$$", dollar_start)
        underscore = val.find("_", dollar_start+4)
        step = int(val[dollar_start+4:underscore])
        fd = int(val[underscore+1:dollar_end])
        if stdiomap[step].has_key(fd):
            val = val.replace(val[dollar_start:dollar_end+4], stdiomap[step][fd], 1)
        else:
            raise Exception("Error: Invalid macro value %s" % val[dollar_start:dollar_end+4])
        dollar_start = val.find("$$__", dollar_end+4)
    return val

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Client for gearman jobs.', epilog='')
    parser.add_argument('-g', '--gearman-servers',  action='store', nargs='+', default=['localhost:4730'],
                        help="Gearman servers list. Default: %(default)s", dest='gearman_servers')
    parser.add_argument('-c', '--config', action='store', nargs=1, default='properties.conf',
                        help="Config file for gearman and mysql db. Default: %(default)s", dest='gearman_config')
    parser.add_argument('-j', '--job', action='store', nargs=1, default='job.conf',
                        help="Config file for the job run. Default: %(default)s", dest='job_config')
    parser.add_argument('-k', '--kron', action='store', nargs=1, default='NOW',
                        help="Cron entry to schedule this job. Default: %(default)s", dest='cron_entry')
    parser.add_argument('-n', '--name', action='store', nargs=1,
                        help="Name of the job to be run.", dest='job_name')
    parser.add_argument('--version',        action='version', version='%(prog)s 0.1a')
    cmd = parser.parse_args()
    logging.config.fileConfig("logging.conf")
    my_logger = logging.getLogger("mainModule")
    my_logger.debug("Program called with the following arguments:\n%s" % str(cmd))
    counter = 0
    while True:
        try:
            if cmd.cron_entry[0] != 'NOW':
                now = datetime.datetime.now()
                cron = croniter.croniter(cmd.cron_entry[0], now)
                future = cron.get_next(datetime.datetime)
                time.sleep((future-now).total_seconds())
            if counter <= 0:
                maps = parse_config(cmd.gearman_config[0])
                steps = parse_config(cmd.job_config[0])
            counter += 1
            conn_dict = maps['DB']
            db_conn(conn_dict).close()
            GEARMAN_DIR = maps['GEARMAN']['dir']
            logging.info("Connecting to gearman server(s)")
            gm_client = gearman.GearmanClient(cmd.gearman_servers)
            logging.info("Connected to Gearman job server successfully ..")
            workflow_id = setup_workflow_in_db(cmd.job_name[0], len(steps))
            stdio = {}
            num = 1
            while num <= len(steps):
                try:
                    for x in steps[str(num)].keys():
                        steps[str(num)][x] = find_and_replace(steps[str(num)][x], stdio)
                    param = json.dumps(steps[str(num)])
                    logging.debug("job: %d, param: %s", num, str(param))
                    print "Sending job %d", num
                    job_request = gm_client.submit_job("run", param)
                    dir = GEARMAN_DIR + '/' + job_request.gearman_job.unique
                    if job_request.result != "0":
                        raise Exception("Error: step %d failed!" % num)
                    else:
                        print "Step %d is successful!" % num
                    stdio[num]={}
                    stdio[num][0] = steps[str(num)]['stdin'] if steps[str(num)].has_key('stdin') else dir + '/stdin'
                    stdio[num][1] = steps[str(num)]['stdout'] if steps[str(num)].has_key('stdout') else dir + '/stdout'
                    stdio[num][2] = steps[str(num)]['strderr'] if steps[str(num)].has_key('stderr') else dir + '/stderr'
                    db_set_workflow_step_completed(workflow_id, num, job_request.gearman_job.unique)
                    num += 1
                except (gearman.errors, gearman.errors.ServerUnavailable):
                    logging.error("Error connecting to Gearman server: " + str(sys.exc_info()[0]))
                    logging.error("Going to retry step: %d in %d seconds ...", num, 10)
                    time.sleep(10)
            db_set_workflow_completed(workflow_id)
            if cmd.cron_entry[0] != 'NOW':
                break
        except (gearman.errors, gearman.errors.ServerUnavailable):
            logging.error("Error connecting to Gearman server: " + str(sys.exc_info()[0]))
            break
        except Exception, e:
            logging.error(e)
            break
        except:
            import traceback
            logging.error("Unhandled exception caught!")
            logging.error(traceback.print_exc())
            break
        finally:
            gm_client.shutdown()