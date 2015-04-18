#!/usr/bin/env python

import sqlite3
import sys
import ConfigParser
import urllib2
import json

config = ConfigParser.RawConfigParser()
config.read('config.ini')
conn = sqlite3.connect(config.get("database", "blockexplorer_db"))
c_stats = conn.cursor()

def main():

    generate_stats()


def generate_stats():

    account_balances()

    latest = c_stats.execute("SELECT height, timestamp FROM blockdata ORDER BY timestamp DESC LIMIT 1;")
    timestamp = c_stats.fetchone()[1]
    i = 0
    while i < timestamp:
        fund_stats(i)
        i = i + 3600
        if (config.get("general", "debug")) == "on":
            stat = str(i)+"/"+str(timestamp)
            stat = "Calculating stats (%s)" % stat
            print "\r", stat,

    conn.commit()

def account_balances():

    if (config.get("general", "debug")) == "on":
        print "Getting balances..."

    accounts = c_stats.execute("SELECT distinct(recipient) FROM transactions;")
    account_list = c_stats.fetchall()
    for account in account_list:
        getaccount_url = (config.get("api", "getaccount_url") + '%s') % account[0]

        res_account = urllib2.urlopen(getaccount_url).read()
        account_dictionary = json.loads(res_account)

        c_stats.execute(
            "INSERT OR REPLACE INTO balances (id, rs, balance) VALUES (?,?,?)",
            (account[0],account_dictionary['accountRS'],float(account_dictionary['balanceNQT'])/100000000)
        )

    return True

def fund_stats(timestamp):

    existing = c_stats.execute("SELECT timestamp FROM fundstats WHERE timestamp = %s" % (timestamp,))
    existingtime = c_stats.fetchone()

    if existingtime == None:

        fundstats = { 'bounty' : 0, 'sale' : 0, 'dev' : 0, 'node' : 0, 'giveaway' : 0, 'accounts' : 0, 'blocks' : 0 }

        ba = config.get("account_filtering", "bounty_accounts")
        sa = config.get("account_filtering", "sale_accounts")
        da = config.get("account_filtering", "dev_accounts")
        na = config.get("account_filtering", "node_accounts")
        ga = config.get("account_filtering", "giveaway_accounts")
        xa = config.get("account_filtering", "storage_accounts")
        ya = config.get("account_filtering", "god_accounts")
        ia = config.get("account_filtering", "stake_account")
        nf = config.get("account_filtering", "nfdswap_accounts")

        # sender: ba, xa
        # recipient not: ba,sa,da,na,ga,xa,ya,ia,nf
        c_stats.execute("SELECT SUM(amount) FROM transactions WHERE sender IN (%s,%s) AND recipient NOT IN (%s,%s,%s,%s,%s,%s,%s,%s,%s) AND timestamp < %s" % (xa,ba,ba,sa,da,na,ga,xa,ya,ia,nf,timestamp))
        fundstats['bounty'] = c_stats.fetchone()[0]

        # sender: sa
        # recipient not: ba,sa,da,na,ga,xa,ya
        c_stats.execute("SELECT SUM(amount)+1200000 FROM transactions WHERE sender IN (%s) AND recipient NOT IN (%s,%s,%s,%s,%s,%s,%s) AND timestamp < %s" % (sa,ba,sa,da,na,ga,xa,ya,timestamp))
        fundstats['sale'] = c_stats.fetchone()[0]

        # sender: ba,xa
        # recipient: da
        c_stats.execute("SELECT SUM(amount)-1200000 FROM transactions WHERE (recipient IN (%s) AND sender IN (%s,%s) AND timestamp < %s) OR (sender IN (%s) AND recipient NOT IN (%s,%s,%s,%s,%s,%s) AND timestamp < %s)" % (da,ba,xa,timestamp,ya,ba,sa,na,ga,xa,ya,timestamp))
        fundstats['dev'] = c_stats.fetchone()[0]

        # sender: na
        # recipient not: ba,da,na,xa,ya
        c_stats.execute("SELECT SUM(amount) FROM transactions WHERE sender IN (%s) AND recipient NOT IN (%s,%s,%s,%s,%s) AND timestamp < %s" % (na,ba,da,na,xa,ya,timestamp))
        fundstats['node'] = c_stats.fetchone()[0]

        # sender: ga
        # recipient not: na,ba,ga,ya
        c_stats.execute("SELECT SUM(amount) FROM transactions WHERE sender IN (%s) AND recipient NOT IN (%s,%s,%s,%s) AND timestamp < %s" % (ga,na,ba,ga,ya,timestamp))
        fundstats['giveaway'] = c_stats.fetchone()[0]

        # sender: ia
        c_stats.execute("SELECT SUM(amount) FROM transactions WHERE sender IN (%s) AND timestamp < %s" % (ia,timestamp))
        fundstats['dividends'] = c_stats.fetchone()[0]

        # sender: nf
        # recipient not: nf, ba
        c_stats.execute("SELECT SUM(amount) FROM transactions WHERE sender IN (%s) AND recipient NOT IN (%s,%s) AND timestamp < %s" % (nf,nf,ba,timestamp))
        fundstats['nfd'] = c_stats.fetchone()[0]

        # recipient not: sa,na,da,ga,ba,xa,ya,ia
        c_stats.execute("SELECT count(distinct(recipient)) FROM transactions WHERE recipient NOT IN (%s,%s,%s,%s,%s,%s,%s) AND timestamp < (%s)" % (sa,na,da,ga,ba,xa,ya,timestamp,))
        fundstats['accounts'] = c_stats.fetchone()[0]

        c_stats.execute("SELECT height FROM blockdata WHERE timestamp < (%s) ORDER BY timestamp DESC LIMIT 1" % (timestamp,))
        try:
            fundstats['blocks'] = c_stats.fetchone()[0]
        except:
            fundstats['blocks'] = 0

        c_stats.execute(
            "INSERT INTO fundstats (timestamp, account_bounty, dev_bounty, node_bounty, giveaways, sold, dividends, accounts, blocks, nfd) VALUES" +
                " (?,?,?,?,?,?,?,?,?,?)",
            (timestamp,fundstats['bounty'],fundstats['dev'],fundstats['node'],fundstats['giveaway'],fundstats['sale'],fundstats['dividends'],fundstats['accounts'],fundstats['blocks'],fundstats['nfd'])
        )

        return fundstats

if __name__ == "__main__":
    main()
    sys.exit()
