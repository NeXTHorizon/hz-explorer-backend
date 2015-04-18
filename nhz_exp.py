#!/usr/bin/env python

# ----------------------------------------------------------------------------
# carebeer at freenode wrote this file. As long as you retain this notice you
# can do whatever you want with this stuff. IF you think this stuff is worth it,
# you can send me NHZ, 15175909104401060281 or just say hi at freenode.
# ----------------------------------------------------------------------------


import json
import urllib2
import sqlite3
import time
import sys
import ConfigParser
import check_fork
import generate_stats

config = ConfigParser.RawConfigParser()
config.read('config.ini')
conn = sqlite3.connect(config.get("database", "blockexplorer_db"))
c = conn.cursor()

assets = {}

def main():

    check = check_fork.validate_block()

    if (config.get("general", "debug")) == "on":
        print "Checking the last inserted transaction id in database."

    last_inserted_blockdata = last_dbblockdata()
    if last_inserted_blockdata and len(last_inserted_blockdata[0]) == 2:
        timestamp = last_inserted_blockdata[0][0]

    else:
        timestamp = 0

    if last_inserted_blockdata == False:
        if (config.get("general", "debug")) == "on":
            print "Database seems empty, continuing with looking up genesis account block id."

        genesis_res = urllib2.urlopen(config.get("api", "genesisblockid_url")).read()
        if len(genesis_res) > 0 and 'errorCode' not in genesis_res:
            genesis_block_ids = json.loads(genesis_res)
            block_data_url = (config.get("api", "getblock_url") + '%s') % genesis_block_ids['blockIds'][0]
        else:
            if (config.get("general", "debug")) == "on":
                print "Can not fetch genesis block id. Exiting."
            sys.exit()

    else:
        block_data_url = (config.get("api", "getblock_url") + '%s') % last_inserted_blockdata[1][1]

    if (config.get("general", "debug")) == "on":
        print "Starting to fetch block and transaction data and inserting values to database."

    res_next_block = urllib2.urlopen(block_data_url).read()
    block_dictionary = json.loads(res_next_block)
    res = db_block_insert(timestamp, block_dictionary)

    if 'transactions' in block_dictionary and 'transactions' != '':
        get_transactions(block_dictionary['transactions'])

    block_counter = 0


    while True:

        if (config.get("general", "debug")) == "on":
            status_blocks = "Number of blockchain ids parsed: %d" % block_counter
            print "\r", status_blocks,

        if 'nextBlock' not in block_dictionary:
           if (config.get("general", "debug")) == "on":
                print "\nnextBlock key is missing from dictionary, we might have reached the end of blockchain. Exiting."
           break

        if 'nextBlock' in block_dictionary and block_dictionary['nextBlock'] != '':
            block_data_url = (config.get("api", "getblock_url") + '%s') % block_dictionary['nextBlock']
            res_next_block = urllib2.urlopen(block_data_url).read()
            block_dictionary = json.loads(res_next_block)
            res = db_block_insert(timestamp, block_dictionary)

            if 'transactions' in block_dictionary and 'transactions' != '':
                get_transactions(block_dictionary['transactions'])

            block_counter += 1

    get_assets()

    conn.commit()

    if (config.get("stats", "fundstats")) == "on":
        generate_stats.generate_stats()

    return True


def get_transactions(transactions):

    for ids in transactions:
        trans_data_url = (config.get("api", "gettransactions_url") + '%s') % ids
        res_trans = urllib2.urlopen(trans_data_url).read()
        transaction_dictionary = json.loads(res_trans)

        if transaction_dictionary['sender'] != '':

            if 'senderRS' not in transaction_dictionary or transaction_dictionary['senderRS'] == '':
                transaction_dictionary['senderRS'] = "null"

            if 'recipientRS' not in transaction_dictionary or transaction_dictionary['recipientRS'] == '':
                transaction_dictionary['recipientRS'] = "null"

            if 'fullHash' not in transaction_dictionary or transaction_dictionary['fullHash'] == '':
                transaction_dictionary['fullHash'] = "null"

            if 'amountNQT' not in transaction_dictionary:
                transaction_dictionary['amountNQT'] = transaction_dictionary['totalAmount']

            if 'attachment' not in transaction_dictionary or transaction_dictionary['attachment'] == '':
                transaction_dictionary['attachment'] = "null"

            if 'feeNQT' not in transaction_dictionary:
                transaction_dictionary['feeNQT'] = transaction_dictionary['totalFee']

            if transaction_dictionary['type'] == 1:
                db_message_insert(transaction_dictionary)

            res = db_transaction_insert(ids, transaction_dictionary)

            if res == False and (config.get("general", "debug")) == "on":
                print "Missing fields in transaction data. Transaction id: %s" % (ids)

    return True

def last_dbblockdata():

    c.execute("SELECT timestamp, nextblock FROM blockdata ORDER BY timestamp DESC LIMIT 3;")
    last_inserted_blockdata = c.fetchall()

    if len(last_inserted_blockdata) == 3 and len(last_inserted_blockdata[2]) == 2:
        if (config.get("general", "debug")) == "on":
            print "Last database timestamp: %s, last database prevblock: %s" % (last_inserted_blockdata[0][0], last_inserted_blockdata[0][1])
        return last_inserted_blockdata
    else:
        return False


def validate_blockfields(block_dictionary):

    fields = ['nextBlock','generator','timestamp','numberOfTransactions','transactions',
              'previousBlock','payloadLength','payloadHash','baseTarget','version',
              'previousBlockHash','height','blockSignature','generationSignature']

    if 'height' in block_dictionary and block_dictionary['height'] < 31:
        block_dictionary['previousBlockHash'] = 0
        if 'previousBlock' not in block_dictionary:
            block_dictionary['previousBlock'] = 0

    for key in fields:
         if key in block_dictionary:
             continue
         else:
             return False

    return True


def validate_transfields(trans_dictionary):

    fields = ["sender","timestamp","confirmations",
              "block","senderPublicKey","type","deadline","signature","recipient",
              "signatureHash","fullHash","transaction"]

    for key in fields:
            if key in trans_dictionary:
                continue
            else:
                return False
    return True


def db_block_insert(timestamp, block_dictionary):

    res = validate_blockfields(block_dictionary)

    if 'nextBlock' in block_dictionary and block_dictionary['nextBlock'] != '':
        if res == True and (block_dictionary['timestamp'] >= timestamp):

            n  =   block_dictionary['nextBlock']
            g  =   block_dictionary['generator']
            gr =   block_dictionary['generatorRS']
            t  =   block_dictionary['timestamp']
            nt =   block_dictionary['numberOfTransactions']
            p  =   block_dictionary['previousBlock']
            pl =   block_dictionary['payloadLength']
            ph =   block_dictionary['payloadHash']
            bt =   block_dictionary['baseTarget']
            v  =   block_dictionary['version']
            ta =   block_dictionary['totalAmountNQT']
            pbh=   block_dictionary['payloadHash']
            h  =   block_dictionary['height']
            bs =   block_dictionary['blockSignature']
            tf =   block_dictionary['totalFeeNQT']
            gs =   block_dictionary['generationSignature']
            tids = ' '.join(block_dictionary['transactions'])

            c.execute("INSERT OR IGNORE INTO blockdata (nextblock, generator, generatorrs," +
                      "timestamp, numberoftransactions, previousblock, payloadlength, payloadhash," +
                      "basetarget, version, totalamount, previousblockhash, height, blocksignature," +
                      "totalfee, generationsignature, transactions) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (n,g,gr,t,nt,p,pl,ph,bt,v,float(ta)/100000000,pbh,h,bs,float(tf)/100000000,gs,tids,))
        else:
            return False

    return True


def db_transaction_insert(trans_id, transaction_dictionary):

    res = validate_transfields(transaction_dictionary)

    if res == True:

        f  = transaction_dictionary['feeNQT']
        a  = transaction_dictionary['amountNQT']
        ts  = transaction_dictionary['timestamp']
        c  = transaction_dictionary['confirmations']
        b  = transaction_dictionary['block']
        t  = transaction_dictionary['type']
        s  = transaction_dictionary['sender']
        sr = transaction_dictionary['senderRS']
        sp = transaction_dictionary['senderPublicKey']
        d  = transaction_dictionary['deadline']
        st = transaction_dictionary['signature']
        r  = transaction_dictionary['recipient']
        rs = transaction_dictionary['recipientRS']
        fh = transaction_dictionary['fullHash']
        at = ' '.join(transaction_dictionary['attachment'])

        conn.execute("INSERT OR IGNORE INTO transactions (id, fee, amount, timestamp, confirmations, block," +
                     "type, sender, senderrs, pubkey,  deadline, signature, recipient, recipientrs, fullhash," +
                     "attachment)  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (trans_id, float(f)/100000000,float(a)/100000000,ts,c,b,t,s,sr,sp,d,st,r,rs,fh,at,))
    else:
        return False

    return True

def db_message_insert(transaction_dictionary):
    options = {
        0 : db_message_insert_message,
        1 : db_message_insert_alias,
        2 : db_message_insert_poll,
        3 : db_message_insert_vote,
        4 : db_message_insert_announcement,
        5 : db_message_insert_info
    }

    if transaction_dictionary['subtype'] < 6:
        options[transaction_dictionary['subtype']](transaction_dictionary)

    return True

def db_message_insert_message(transaction_dictionary):

#    tx  = transaction_dictionary['transaction']
#    msg = transaction_dictionary['attachment']['message']

#    conn.execute(
#        "INSERT OR IGNORE INTO messages (tx,message) VALUES (?,?)",
#        (tx,msg)
#    )

    return True

def db_message_insert_alias(transaction_dictionary):
    tx  = transaction_dictionary['transaction']
    ali = transaction_dictionary['attachment']['alias']
    uri = transaction_dictionary['attachment']['uri']

    conn.execute(
        "INSERT OR REPLACE INTO aliases (tx,alias,uri) VALUES (?,?,?)",
        (tx,ali,uri)
    )

    return True

def db_message_insert_poll(transaction_dictionary):

    return True

def db_message_insert_vote(transaction_dictionary):

    return True

def db_message_insert_announcement(transaction_dictionary):

    return True

def db_message_insert_info(transaction_dictionary):

    return True

def get_assets():

    assets_res = urllib2.urlopen(config.get("api", "getassets_url")).read()
    assets_dictionary = json.loads(assets_res)
    for asset in assets_dictionary['assets']:
        ass = asset['asset']
        acc = asset['account']
        ars = asset['accountRS']
        nme = asset['name']
        des = asset['description']
        qnt = asset['quantityQNT']
        dec = asset['decimals']
        trd = asset['numberOfTrades']

        conn.execute(
            "INSERT OR REPLACE INTO assets (asset, account, accountrs, name, description, quantity, decimals, trades) VALUES (?,?,?,?,?,?,?,?)",
            (ass,acc,ars,nme,des,qnt,dec,trd)
        )

    return True

if __name__ == "__main__":
    main()
    sys.exit()
