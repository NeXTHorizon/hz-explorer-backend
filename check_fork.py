#!/usr/bin/env python

import json
import urllib2
import sqlite3
import time
import sys
import ConfigParser
import nhz_exp

config = ConfigParser.RawConfigParser()
config.read('config.ini')
conn = sqlite3.connect(config.get("database", "blockexplorer_db"))
c = conn.cursor()

def validate_block():


    counter = 0
    while True:

        nextblock = nhz_exp.last_dbblockdata()

        if nextblock != False and nextblock[0][1] != '' and nextblock[1][1] != '' and nextblock[2][1] != '':
            if (config.get("general", "debug")) == "on":
                print "Checking for blockchain forks."


            prevblock_data = (config.get("api", "getblock_url") + '%s') % nextblock[0][1]
            res_next_block = urllib2.urlopen(prevblock_data).read()
            prevblock_dict = json.loads(res_next_block)

            if 'nextBlock' not in prevblock_dict and 'previousBlock' in prevblock_dict:
                if (config.get("general", "debug")) == "on":
                    print "No new block generated. Exiting."
                sys.exit()


            if 'errorDescription' in prevblock_dict and prevblock_dict['errorDescription'] == 'Unknown block':
                if (config.get("general", "debug")) == "on":
                    print "Block: %s does not exist in blockchain, deleting from database." % (nextblock[0][1])
                c.execute("DELETE FROM blockdata WHERE timestamp >= ?", (nextblock[1][0],))
                conn.commit()

            elif 'previousBlock' in prevblock_dict and nextblock[1][1] == prevblock_dict['previousBlock']:
                if (config.get("general", "debug")) == "on":
                    print "The previous nextblock field is equal to the current previousblock field."
                break

            else:
                print "Block not found, rolling back";
                c.execute("DELETE FROM blockdata WHERE timestamp >= ?", (nextblock[1][0],))
                c.execute("DELETE FROM transactions WHERE timestamp >= ?", (nextblock[1][0],))
                #c.execute("DELETE FROM aliases WHERE timestamp >= ?", (nextblock[1][0],))
                conn.commit()
                counter += 1

                if counter > 9:
                    if (config.get("general", "debug")) == "on":
                        print "Current block might be a fork, exiting."
                    sys.exit()
        else:
            if (config.get("general", "debug")) == "on":
                print "We need at least two previous block entries in database. Not checking fork."
            break

    return True
