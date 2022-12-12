from json.decoder import JSONDecodeError
import subprocess
import argshop
import argparse
import time
from queue import Queue, Empty
import sys
import threading
import json
import atexit
import datetime
from argshop import Th
from os.path import expanduser
import signal

def enqueue_output(out, queue):
    for line in iter(out.readline, ''):
        queue.put(line)
    out.close()

def termit( ps ):
    for p in ps:
        p.terminate()

def anyProcessAlive( ps ):
    result = False
    for x in ps:
        if x.poll() is None:
            result = True
    return result


def main():
    parser = argparse.ArgumentParser(description='Multi process load generator')
    argshop.init(parser)
    parser.add_argument('-i', '--instances', dest='instances', type=int, default=1,
                        help='Number of process instanses to spawn (default:1)')
    parser.add_argument('-j', '--journal-file', dest='journal_file', type=str, default='runx_journal.txt',
                        help='Journal file to write start/finish info (default:do not write journal file)')
    args = parser.parse_args()
    acs = {}
    for a in parser._actions:
        acs[a.dest] = a
    print(args)
    # Thread index
    ti = args.tstart
    # Static parameters, all but instance count and first thread index
    sts = ''
    for a in args.__dict__:
        if not a in ['instances','tstart','journal_file']:
            sts = sts + ' ' + acs[a].option_strings[0] + ' ' + str(getattr(args,a))
    
    print(sts)
    ps = []
    th = Th(args.threads)

    jp = vars(args).copy()
    del jp['dbname']
    del jp['journal_file']

    def journal( s ):
        sx = str(datetime.datetime.now()) + ' ' + s.ljust(11) + ' ' + args.dbname.ljust(10) + ' ' + str(jp)
        print( 'RunX ' + sx )
        if not args.journal_file is None:
            with open( expanduser(args.journal_file), 'a') as f:
                f.write( sx.capitalize() + '\n')

    # atexit.register( termit, ps )

    journal('started')

    q = Queue()
    for pi in range(args.instances):
        stt = sts + ' -f ' + str(ti)
        print(stt)
        p = ( subprocess.Popen(('python3 -u shop2.py' + stt).split(), stdout=subprocess.PIPE, bufsize=1, text=True, start_new_session = True ) )
        t = threading.Thread(target=enqueue_output, args=(p.stdout, q))
        t.daemon = True # thread dies with the program
        t.start()
        ps.append(p)
        ti = ti + th.fi_threads

    tcount = {}
    pcount = {}
    interrupted = False
    while anyProcessAlive(ps):
        try:
            try:  
                line = q.get_nowait() # or q.get(timeout=.1)
            except Empty:
                time.sleep(.1)
            else: # got line
                # s = line.decode('utf-8').strip()
                s = line.strip()
                print(s)
                if s > '' and s[0]=='{':
                    try:
                        j = json.loads(s)
                        i = j['i']
                        if i in tcount.keys(): 
                            tcount[i]=tcount[i] + j['success']
                            pcount[i]=pcount[i] + 1
                        else:
                            tcount[i]=j['success']
                            pcount[i]=1
                        if pcount[i]>=args.instances:
                            btps = tcount[i]/args.window
                            print( args.dbname, datetime.datetime.now(), 'BTPS at iteration', i, ':', btps)
                            #for x in range(max(0,i-50)):
                            #    del pcount[0]
                    except JSONDecodeError:
                        print('JSON decode error')
        except KeyboardInterrupt:
            print('RunX keyboard interrupt, sending Ctrl-C to child processes...')
            for p in ps:
                p.send_signal(signal.SIGINT)
            print('RunX keyboard interrupt, child processes asked to stop.')
            interrupted = True

    for x in ps:
        x.wait( 10 )

    print()
    if interrupted:
        journal('interrupted')
    else:
        journal('completed')


main()