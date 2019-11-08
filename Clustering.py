from pymongo import MongoClient
import bson
import requests

URL = "https://blockchain.info"
LATEST_BLOCK = '/latestblock'
RAW_BLOCK = '/rawblock/'

client = MongoClient()

database = client['bitcoin-cluster']

Entity = database['Entity']
Blocks = database['Block']
Transaction = database['Transaction']
AddressEntity = database['AddressEntity']
control = database['control']
AddressChange = database['AddressChange']
AddressExchange = database['AddressExchange']

count = 0


def updateLastBlock(lastBlock):
    try:
        control.update_one({"name": "lastblock"}, {"$set": {"value": lastBlock}})
        Blocks.insert(lastBlock)
    except Exception as e:
        print("Fail on update the last block ", e)
        raise e


def getTheLastBlock():
    lastBlockControl = control.find_one({
        "name": "lastblock"
    })

    if lastBlockControl is not None:
        lastBlock = lastBlockControl['value']
    else:
        control.insert({"name": "lastblock"})
        lastBlock = requests.get(URL + LATEST_BLOCK).json()
        lastBlock = requests.get(URL + RAW_BLOCK + lastBlock['hash']).json()
        updateLastBlock(lastBlock)

    return lastBlock


def populateTransactionsDatabaseWhenNecessary(lastBlock, MAX_POPULATION=1e6):
    print("doing count transactions")
    population = Transaction.count_documents({})
    print(population, " transactions!")
    while True:
        if population >= MAX_POPULATION:
            return
        try:
            print("Calling the last block...")
            actualBlock = requests.get(URL + RAW_BLOCK + lastBlock['prev_block']).json()
            Transaction.insert_many(actualBlock['tx'])
            population += len(actualBlock['tx'])
            updateLastBlock(actualBlock)
        except Exception as e:
            print("fail on insert transaction ", e)
            raise e

        lastBlock = actualBlock
        print("Actual Population: ", population)


def executeH1Clustering():
    print("Proccess transactions with h1: ")
    for transaction in Transaction.find():

        count_transactions()

        addresses = get_all_address_in_transaction(transaction)

        entityToMerge = get_all_entity_and_remove_address_already_in_db(addresses)

        newEntityId = get_new_entity_id(entityToMerge)

        update_database_addressEntity(addresses, entityToMerge, newEntityId)

def executeH2Clustering():
    change_wallets = {}
    for transaction in Transaction.find():
        count_transactions()

        addresses = get_all_address_in_transaction(transaction)

        if len(transaction['out']) > 1:
            first_time = 0
            first_address = None

            for output in transaction['out']:
                if output.get('addr') is not None and not (output['addr'] in addresses or output['addr'] in change_wallets):
                    first_address = output['addr']
                    first_time += 1

            if first_time == 1:
                add_change_wallet(addresses, first_address)
                change_wallets[first_address] = True


def add_change_wallet(addresses, first_address):
    if len(addresses):
        entity = AddressEntity.find_one({"address": addresses[0]})
        if entity is not None:
            first_time_on_db = AddressEntity.find_one({"address": first_address})
            if first_time_on_db is None or not len(first_time_on_db):
                AddressEntity.insert_one({"address": first_address, "entity": entity['entity']})

            if AddressChange.find_one({"address": first_address}) is None:
                AddressChange.insert_one({"address": first_address, "entity": entity['entity']})


def update_database_addressEntity(addresses, entityToMerge, newEntityId):
    # if merge, update new entityId
    if len(entityToMerge) > 1:
        AddressEntity.update_many({"entity": {"$in": entityToMerge}}, {"$set": {"entity": newEntityId}})
    # insert all addresses without entity
    newAddressesEntity = [{'address': address, 'entity': newEntityId} for address in addresses]
    if len(newAddressesEntity):
        AddressEntity.insert_many(newAddressesEntity)


def count_transactions():
    global count
    if not count % 10000:
        print(count)
    count += 1


def get_all_address_in_transaction(transaction):
    addresses = []
    for inputAddress in transaction['inputs']:
        address = getAddress(inputAddress)
        if address is not None:
            addresses.append(address)
    addresses = list(set(addresses))
    return addresses

def get_in_out_value_in_transaction(transaction):
    inValue = 0
    outValue = 0
    inAddress = None
    outAddress = []
    if transaction.get('inputs') is not None:
        for input in transaction['inputs']:
            if input.get('prev'):
                inValue += input.get('prev').get('value')
            elif input.get('prev_out'):
                inValue += input.get('prev_out').get('value')
            if inAddress is None:
                inAddress = getAddress(input)
    if transaction.get('out') is not None:
        for out in transaction['out']:
            outValue += out.get('value')
            outAddress.append((out.get('value'), out.get('addr')))
    return (inValue, inAddress, outValue, outAddress)



def get_all_entity_and_remove_address_already_in_db(addresses):
    addressesEntity = AddressEntity.find({"address": {"$in": addresses}})
    entityToMerge = []
    for addressEntity in addressesEntity:
        addresses.remove(addressEntity['address'])
        if addressEntity['entity'] not in entityToMerge:
            entityToMerge.append(addressEntity['entity'])
    return entityToMerge


def get_new_entity_id(entityToMerge):
    if len(entityToMerge) == 1:
        newEntityId = entityToMerge[0]
    else:
        newEntityId = bson.objectid.ObjectId()
    return newEntityId


def getAddress(inputAddress):
    address = None
    if inputAddress.get('prev') is not None:
        address = inputAddress['prev'].get('addr')
    if address is None and inputAddress.get('prev_out') is not None:
        address = inputAddress['prev_out'].get('addr')
    return address


def hasDone(key):
    has = control.find_one({"name": key})

    return has and has['value']


def find_clusters_and_populate():
    if hasDone('find_all_clusters'):
        print('numAddress already populate!')
        return

    AddressExchange.delete_many({})

    entitys = AddressEntity.distinct('entity')

    for entityId in entitys:
        entity = {'entity': entityId, 'numAddresses': AddressEntity.find({'entity': entityId}).count()}

        AddressExchange.insert(entity)

    define_control('find_all_clusters', True)

def populate_num_transactions():

    if hasDone('populate_num_transactions'):
        return

    for transaction in Transaction.find():
        addresses = get_all_address_in_transaction(transaction)
        entitys = list(AddressEntity.find({'address': { '$in': addresses }}))
        entitys = [ entity['entity'] for entity in entitys ]
        for exchange in AddressExchange.find({'entity': {'$in': entitys}}):
            if exchange.get('numTransactions') is None:
                AddressExchange.update_one({'_id': exchange['_id']}, { '$set': { 'numTransactions': 1 } })
            else:
                AddressExchange.update_one({'_id': exchange['_id']}, { '$set': { 'numTransactions': exchange['numTransactions'] + 1 } })

    define_control('populate_num_transactions', True)

def populate_btc_in_out():
    if hasDone('populate_btc_in_out'):
        return

    for transaction in Transaction.find():
        inValue, inAddress, outValue, valuesAndAddress = get_in_out_value_in_transaction(transaction)

        total = inValue - outValue
        update_total_value(inAddress, total)

        for value, address in valuesAndAddress:
            update_total_value(address, value)

    define_control('populate_btc_in_out', True)


def update_total_value(address, total):
    entity = AddressEntity.find_one({'address': address})
    if entity:
        exchange = AddressExchange.find_one({'entity': entity['entity']})
        if exchange:
            if exchange.get('total') is None:
                AddressExchange.update_one({'_id': exchange['_id']}, {'$set': {'total': total}})
            else:
                AddressExchange.update_one({'_id': exchange['_id']}, {'$set': {'total': exchange['total'] + total}})


def populate_all_clusters():
    find_clusters_and_populate()
    populate_num_transactions()
    populate_btc_in_out()

def define_control(key, value):
    element = control.find_one({'name': key})
    if element:
        control.update_one({"name": key}, {"$set": {"value": value}})
    else:
        control.insert({"name": key, "value": value})

def main():
    global count
    # populateTransactionsDatabaseWhenNecessary(getTheLastBlock())
    count = 0
    if not hasDone("h1Done"):
        print("doing h1 clustering")
        executeH1Clustering()
    count = 0
    if not hasDone("h2Done"):
        print("doing h2 clustering")
        executeH2Clustering()

    populate_all_clusters()


main()