import argparse

def init( parser : argparse.ArgumentParser ):
    parser.add_argument('-d', '--dbname', dest='dbname', type=str, required=True,
                        help='YDB CLI profile name to connect to the database')
    parser.add_argument('-c', '--command', dest='command', type=str, default="submitOrder", 
        choices=['submitOrder','submitRandomOrder','submitSameOrder','getCustomerHistory','getRandomCustomerHistory','init',
                 'insertRandomOrder', 'getOrderByID', 'error', 'connect' ],
        help='Command: run, init')
    parser.add_argument('-t', '--threads', dest='threads', type=str, default="1",
                        help='Number of threads start[-finish][,step[,stepseconds]](default: 1,1,5)')
    parser.add_argument('-f', '--firstthread', dest='tstart', type=int, default=0,
                        help='First thread index, to run on different instances (default: 0)')
    parser.add_argument('-s', '--seconds', dest='seconds', type=int, default=10,
                        help='Number of seconds to run the test (default: 10), 0 means indefinite')
    parser.add_argument('-p', '--products', dest='products', type=int, default=5,
                        help='Number of products (default: 5)')
    parser.add_argument('-q', '--quantity', dest='quantity', type=int, default=1,
                        help='Quantity (default:1)')
    parser.add_argument('-a', '--partitions', dest='partitions', type=int, default=1,
                        help='YDB: Number of partitions in the stock table (default:1)')    
    parser.add_argument('-w', '--window', dest='window', type=int, default=1,
                        help='Number of seconds in a window to report statistics, 0 means no window (default:1)')                            
    parser.add_argument('-o', '--orderid', dest='orderid', type=int, default=0,
                        help='Order ID required to query status by order for command <getOrderByID>')                            

class Th:
    st_threads: int
    fi_threads: int
    step_threads: int
    step_seconds: float

    def __init__( self, s: str ):
        sx = s.split(',')
        sy = sx[0].split('-')
        self.st_threads = int(sy[0])
        if len(sy)>1:
            self.fi_threads = int(sy[1])
        else:
            self.fi_threads = self.st_threads
        if len(sx)>1:
            self.step_threads = int(sx[1])
            if len(sx)>2:
                self.step_seconds = float(sx[2])
            else:
                self.step_seconds = 5
        else:
            self.step_threads = 1
            self.step_seconds = 5

