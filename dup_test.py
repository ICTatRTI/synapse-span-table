import configparser
import synapseclient
import os
import timeit
from synapse_span_table import SynapseSpanTable

MAX_STRING_LEN = 50
TABLE_PREFIX = 'arc_mnh_'
tableName = TABLE_PREFIX + 'test'
columnLimit = 3

# Config.
config = configparser.ConfigParser()
config.read(os.path.join(os.getcwd(), 'config.ini'))
config.sections()

# Connect to Synapse.
syn = synapseclient.Synapse()
synProjectName = config['SYNAPSE']['ProjectName']
authToken = config['SYNAPSE']['authToken']
syn.login(authToken=authToken)


def afterTest():
    global syn
    global synProjectName
    children = syn.getChildren(synProjectName)
    for entity in children:
        syn.delete(entity['id'])


#
# Queue records with different schema in the same table then flush
#
TestName = 'Upload records then upload more with a different schema'
data1 = {
    "id": "1",
    "a": "1",
    "b": "1",
    "c": "1",
    "d": "1",
    "e": "1",
    "f": "data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAMCAgICAgMCAgIDAwMDBAYEBAQEBAgGBgUGCQgKCgkICQkKDA8MCgsOCwkJDRENDg8QEBEQCgwSExIQEw8QEBD/2wBDAQMDAwQDBAgEBAgQCwkLEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBD/wAARCAEsAlgDASIAAhEBAxEB/8QAHgABAAMBAAMBAQEAAAAAAAAAAAcICQYDBAUCAQr/xABHEAABAwMEAQMCBAMFBQYDCQABAAIDBAUGBwgREiEJEzEiQRQyUWEVI0IWM1JxgRcYJGJyJSdDc4KRNTehRFNjdoSSsbTR/8QAFAEBAAAAAAAAAAAAAAAAAAAAAP/EABQRAQAAAAAAAAAAAAAAAAAAAAD/2gAMAwEAAhEDEQA/ANU0REBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAUebgtZbLt90byrWC+0f4yDHKEzxUfviE1lS5wjggEhDuneV7GduriA4nq7jgyGs9/UUvVVr3rjo/sZxiul6ZBdY8gyw07h2hoYw/oCR92wsq5S13jkQH7ggLg7ddVbvrhoniWrF8wybFavJqI1ptUs5mMUZke2N4eWMLmSRtZK09R9Mjfn5Mjr1rbbqCz26ltFrpIqWioYGU1NBE3qyKJjQ1jGj7AAAAfsvZQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQfHzDLLHgeJ3rN8nrBSWiwW+oudfP1Lvbp4Y3SSO4HkkNafA8n4Wc3pkw3vcVuL1j3oZpSvMtVUfwWytl4eKYS9XuiYfsYKaOliB+7ZXfqVI3rAayv0921U+ndtqzFc9Rbk2heGnh38Ppus1QQfny/8NGR92yuH+cy7B9DXaAbXcPxC4UP4W+XOA36+NczpIK2qAeWSD/HHEIYT/5KCwyIiAiIgIiICIiAiIgIiICIiAiIgrrv63AXHbhtmyPN8buUdFk9e+GzWCVzGvLayd3mRrXAtLo4WzyjsCOYwCCCvQ9OnM9YtRdrGO5xrVkEl5u14q6yagq54gyofb2ymOL3eAA5xcyQtdx5Y6M8lUt9VHML7r9ue042i4JL7k1BNTsqR2JjFzuDmBpkA54bDThjy77CaTx4WqmH4rZcFxOy4TjdL+GtOP2+ntdDCTyY6eCNscbefvw1o8oProiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiiPddrpb9uWgmWaqVMsQrrfRmC0QycH8RcZfopmdf6h3Ic4D4Yx5+yCk+aWun3seqBR4uWGtwHQmlY64/T2gnrIJQ+SNwd45fVvZC5vH1MpX+COStNFT30w9v9fpBoEM+zBkz8z1Smbkd1lqCTM2ncCaWN5Pnt0kfM7nyHVDgfyq4SAiIgIiICIiAiIgIiICIiAiIgLntQs7x3TDBr9qHltYKWz47QTXGsk8c+3G0uLWj7udwGtHyXEAfK6FZzeqJqZlWqGXYLsS0m5qL9m9bS118MbiWxQdz7EUvXktY0sfUyEj6WQxu8glBxnpjaT3/XzXHNd9eqFPKKh91q47EwACF9ZOxzZ3tB8lkEMjYWeOOXnyXRnjUxcVovpRjWh2lmNaUYkwi2Y3QspGSOaGvqJPLpZ3geO8kjnyO48cvK7VAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEXzMlybHsNsNdlOWXuis9ntkLqisrq2dsMEEY+XPe4gAf/6Fn9qj6k+ourmbz6NbB9OJ8xvEXP4jJaum5po4w4NMsUTy0NjBcB705a3n4YQQSGiUkkcMbpZZGsYxpc5zjwGgfJJ+wUeZLuO2+4a+SLK9cMCtMsXPaGryKkjl8Hg8MMnYn9gFSOm9NTchrpOb1u73ZXqrhq5Pflx6ySyz08Xbh3RhlLYIS1xI6sgc36Rw4qVcS9JLZljTo33TFsiyd0fH/wAXvszQ4j7kUvsg/wCXHH7cIO4uXqP7JrSZRVa+Wh/sgF34agrajnnj8vtQu7fP2548/oV86l9T3YxWVDKWHXWJr5DwDLj12iYP83vpQ0f6ldtZtkG0OwhgodumCS9Hdh+NtEVZ5448++H8jx8H7+flfUqtou1SsgdTTba9L2sfxyYsSoIneDz4cyIOHx9ig9zANz+3bVKpioMA1pxC81s8nsxUUN1ibVSP5IAbA8iQ89Txw3yPI8KT1UDUn0rtoua2SWlxXDavBby0",
    "g": "1"
}
data2 = {
    "id": "2",
    "a": "2",
    "b": "2",
    "c": "2",
    "p": "2",
    "d": "2",
    "f": "data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAMCAgICAgMCAgIDAwMDBAYEBAQEBAgGBgUGCQgKCgkICQkKDA8MCgsOCwkJDRENDg8QEBEQCgwSExIQEw8QEBD/2wBDAQMDAwQDBAgEBAgQCwkLEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBD/wAARCAEsAlgDASIAAhEBAxEB/8QAHgABAAMBAAMBAQEAAAAAAAAAAAcICQYDBAUCAQr/xABHEAABAwMEAQMCBAMFBQYDCQABAAIDBAUGBwgREiEJEzEiQRQyUWEVI0IWM1JxgRcYJGJyJSdDc4KRNTehRFNjdoSSsbTR/8QAFAEBAAAAAAAAAAAAAAAAAAAAAP/EABQRAQAAAAAAAAAAAAAAAAAAAAD/2gAMAwEAAhEDEQA/ANU0REBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAUebgtZbLt90byrWC+0f4yDHKEzxUfviE1lS5wjggEhDuneV7GduriA4nq7jgyGs9/UUvVVr3rjo/sZxiul6ZBdY8gyw07h2hoYw/oCR92wsq5S13jkQH7ggLg7ddVbvrhoniWrF8wybFavJqI1ptUs5mMUZke2N4eWMLmSRtZK09R9Mjfn5Mjr1rbbqCz26ltFrpIqWioYGU1NBE3qyKJjQ1jGj7AAAAfsvZQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQfHzDLLHgeJ3rN8nrBSWiwW+oudfP1Lvbp4Y3SSO4HkkNafA8n4Wc3pkw3vcVuL1j3oZpSvMtVUfwWytl4eKYS9XuiYfsYKaOliB+7ZXfqVI3rAayv0921U+ndtqzFc9Rbk2heGnh38Ppus1QQfny/8NGR92yuH+cy7B9DXaAbXcPxC4UP4W+XOA36+NczpIK2qAeWSD/HHEIYT/5KCwyIiAiIgIiICIiAiIgIiICIiAiIgrrv63AXHbhtmyPN8buUdFk9e+GzWCVzGvLayd3mRrXAtLo4WzyjsCOYwCCCvQ9OnM9YtRdrGO5xrVkEl5u14q6yagq54gyofb2ymOL3eAA5xcyQtdx5Y6M8lUt9VHML7r9ue042i4JL7k1BNTsqR2JjFzuDmBpkA54bDThjy77CaTx4WqmH4rZcFxOy4TjdL+GtOP2+ntdDCTyY6eCNscbefvw1o8oProiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiiPddrpb9uWgmWaqVMsQrrfRmC0QycH8RcZfopmdf6h3Ic4D4Yx5+yCk+aWun3seqBR4uWGtwHQmlY64/T2gnrIJQ+SNwd45fVvZC5vH1MpX+COStNFT30w9v9fpBoEM+zBkz8z1Smbkd1lqCTM2ncCaWN5Pnt0kfM7nyHVDgfyq4SAiIgIiICIiAiIgIiICIiAiIgLntQs7x3TDBr9qHltYKWz47QTXGsk8c+3G0uLWj7udwGtHyXEAfK6FZzeqJqZlWqGXYLsS0m5qL9m9bS118MbiWxQdz7EUvXktY0sfUyEj6WQxu8glBxnpjaT3/XzXHNd9eqFPKKh91q47EwACF9ZOxzZ3tB8lkEMjYWeOOXnyXRnjUxcVovpRjWh2lmNaUYkwi2Y3QspGSOaGvqJPLpZ3geO8kjnyO48cvK7VAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEXzMlybHsNsNdlOWXuis9ntkLqisrq2dsMEEY+XPe4gAf/6Fn9qj6k+ourmbz6NbB9OJ8xvEXP4jJaum5po4w4NMsUTy0NjBcB705a3n4YQQSGiUkkcMbpZZGsYxpc5zjwGgfJJ+wUeZLuO2+4a+SLK9cMCtMsXPaGryKkjl8Hg8MMnYn9gFSOm9NTchrpOb1u73ZXqrhq5Pflx6ySyz08Xbh3RhlLYIS1xI6sgc36Rw4qVcS9JLZljTo33TFsiyd0fH/wAXvszQ4j7kUvsg/wCXHH7cIO4uXqP7JrSZRVa+Wh/sgF34agrajnnj8vtQu7fP2548/oV86l9T3YxWVDKWHXWJr5DwDLj12iYP83vpQ0f6ldtZtkG0OwhgodumCS9Hdh+NtEVZ5448++H8jx8H7+flfUqtou1SsgdTTba9L2sfxyYsSoIneDz4cyIOHx9ig9zANz+3bVKpioMA1pxC81s8nsxUUN1ibVSP5IAbA8iQ89Txw3yPI8KT1UDUn0rtoua2SWlxXDavBby0",
    "g": "2",
    "h": "2",
    "i": "2",
    "j": "2"
}
data3 = {
    "id": "3",
    "a": "3",
    "b": "3",
    "c": "3",
    "d": "3",
    "e": "3",
    "g": "3",
    "f": "data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAMCAgICAgMCAgIDAwMDBAYEBAQEBAgGBgUGCQgKCgkICQkKDA8MCgsOCwkJDRENDg8QEBEQCgwSExIQEw8QEBD/2wBDAQMDAwQDBAgEBAgQCwkLEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBD/wAARCAEsAlgDASIAAhEBAxEB/8QAHgABAAMBAAMBAQEAAAAAAAAAAAcICQYDBAUCAQr/xABHEAABAwMEAQMCBAMFBQYDCQABAAIDBAUGBwgREiEJEzEiQRQyUWEVI0IWM1JxgRcYJGJyJSdDc4KRNTehRFNjdoSSsbTR/8QAFAEBAAAAAAAAAAAAAAAAAAAAAP/EABQRAQAAAAAAAAAAAAAAAAAAAAD/2gAMAwEAAhEDEQA/ANU0REBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAUebgtZbLt90byrWC+0f4yDHKEzxUfviE1lS5wjggEhDuneV7GduriA4nq7jgyGs9/UUvVVr3rjo/sZxiul6ZBdY8gyw07h2hoYw/oCR92wsq5S13jkQH7ggLg7ddVbvrhoniWrF8wybFavJqI1ptUs5mMUZke2N4eWMLmSRtZK09R9Mjfn5Mjr1rbbqCz26ltFrpIqWioYGU1NBE3qyKJjQ1jGj7AAAAfsvZQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQfHzDLLHgeJ3rN8nrBSWiwW+oudfP1Lvbp4Y3SSO4HkkNafA8n4Wc3pkw3vcVuL1j3oZpSvMtVUfwWytl4eKYS9XuiYfsYKaOliB+7ZXfqVI3rAayv0921U+ndtqzFc9Rbk2heGnh38Ppus1QQfny/8NGR92yuH+cy7B9DXaAbXcPxC4UP4W+XOA36+NczpIK2qAeWSD/HHEIYT/5KCwyIiAiIgIiICIiAiIgIiICIiAiIgrrv63AXHbhtmyPN8buUdFk9e+GzWCVzGvLayd3mRrXAtLo4WzyjsCOYwCCCvQ9OnM9YtRdrGO5xrVkEl5u14q6yagq54gyofb2ymOL3eAA5xcyQtdx5Y6M8lUt9VHML7r9ue042i4JL7k1BNTsqR2JjFzuDmBpkA54bDThjy77CaTx4WqmH4rZcFxOy4TjdL+GtOP2+ntdDCTyY6eCNscbefvw1o8oProiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiiPddrpb9uWgmWaqVMsQrrfRmC0QycH8RcZfopmdf6h3Ic4D4Yx5+yCk+aWun3seqBR4uWGtwHQmlY64/T2gnrIJQ+SNwd45fVvZC5vH1MpX+COStNFT30w9v9fpBoEM+zBkz8z1Smbkd1lqCTM2ncCaWN5Pnt0kfM7nyHVDgfyq4SAiIgIiICIiAiIgIiICIiAiIgLntQs7x3TDBr9qHltYKWz47QTXGsk8c+3G0uLWj7udwGtHyXEAfK6FZzeqJqZlWqGXYLsS0m5qL9m9bS118MbiWxQdz7EUvXktY0sfUyEj6WQxu8glBxnpjaT3/XzXHNd9eqFPKKh91q47EwACF9ZOxzZ3tB8lkEMjYWeOOXnyXRnjUxcVovpRjWh2lmNaUYkwi2Y3QspGSOaGvqJPLpZ3geO8kjnyO48cvK7VAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEXzMlybHsNsNdlOWXuis9ntkLqisrq2dsMEEY+XPe4gAf/6Fn9qj6k+ourmbz6NbB9OJ8xvEXP4jJaum5po4w4NMsUTy0NjBcB705a3n4YQQSGiUkkcMbpZZGsYxpc5zjwGgfJJ+wUeZLuO2+4a+SLK9cMCtMsXPaGryKkjl8Hg8MMnYn9gFSOm9NTchrpOb1u73ZXqrhq5Pflx6ySyz08Xbh3RhlLYIS1xI6sgc36Rw4qVcS9JLZljTo33TFsiyd0fH/wAXvszQ4j7kUvsg/wCXHH7cIO4uXqP7JrSZRVa+Wh/sgF34agrajnnj8vtQu7fP2548/oV86l9T3YxWVDKWHXWJr5DwDLj12iYP83vpQ0f6ldtZtkG0OwhgodumCS9Hdh+NtEVZ5448++H8jx8H7+flfUqtou1SsgdTTba9L2sfxyYsSoIneDz4cyIOHx9ig9zANz+3bVKpioMA1pxC81s8nsxUUN1ibVSP5IAbA8iQ89Txw3yPI8KT1UDUn0rtoua2SWlxXDavBby0",
    "h": "3",
    "i": "3",
    "j": "3"
}
data4 = {
    "id": "4",
    "a": "4",
    "b": "4",
    "c": "4",
    "p": "4",
    "d": "4",
    "f": "data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAMCAgICAgMCAgIDAwMDBAYEBAQEBAgGBgUGCQgKCgkICQkKDA8MCgsOCwkJDRENDg8QEBEQCgwSExIQEw8QEBD/2wBDAQMDAwQDBAgEBAgQCwkLEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBD/wAARCAEsAlgDASIAAhEBAxEB/8QAHgABAAMBAAMBAQEAAAAAAAAAAAcICQYDBAUCAQr/xABHEAABAwMEAQMCBAMFBQYDCQABAAIDBAUGBwgREiEJEzEiQRQyUWEVI0IWM1JxgRcYJGJyJSdDc4KRNTehRFNjdoSSsbTR/8QAFAEBAAAAAAAAAAAAAAAAAAAAAP/EABQRAQAAAAAAAAAAAAAAAAAAAAD/2gAMAwEAAhEDEQA/ANU0REBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAUebgtZbLt90byrWC+0f4yDHKEzxUfviE1lS5wjggEhDuneV7GduriA4nq7jgyGs9/UUvVVr3rjo/sZxiul6ZBdY8gyw07h2hoYw/oCR92wsq5S13jkQH7ggLg7ddVbvrhoniWrF8wybFavJqI1ptUs5mMUZke2N4eWMLmSRtZK09R9Mjfn5Mjr1rbbqCz26ltFrpIqWioYGU1NBE3qyKJjQ1jGj7AAAAfsvZQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQfHzDLLHgeJ3rN8nrBSWiwW+oudfP1Lvbp4Y3SSO4HkkNafA8n4Wc3pkw3vcVuL1j3oZpSvMtVUfwWytl4eKYS9XuiYfsYKaOliB+7ZXfqVI3rAayv0921U+ndtqzFc9Rbk2heGnh38Ppus1QQfny/8NGR92yuH+cy7B9DXaAbXcPxC4UP4W+XOA36+NczpIK2qAeWSD/HHEIYT/5KCwyIiAiIgIiICIiAiIgIiICIiAiIgrrv63AXHbhtmyPN8buUdFk9e+GzWCVzGvLayd3mRrXAtLo4WzyjsCOYwCCCvQ9OnM9YtRdrGO5xrVkEl5u14q6yagq54gyofb2ymOL3eAA5xcyQtdx5Y6M8lUt9VHML7r9ue042i4JL7k1BNTsqR2JjFzuDmBpkA54bDThjy77CaTx4WqmH4rZcFxOy4TjdL+GtOP2+ntdDCTyY6eCNscbefvw1o8oProiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiICIiAiIgIiiPddrpb9uWgmWaqVMsQrrfRmC0QycH8RcZfopmdf6h3Ic4D4Yx5+yCk+aWun3seqBR4uWGtwHQmlY64/T2gnrIJQ+SNwd45fVvZC5vH1MpX+COStNFT30w9v9fpBoEM+zBkz8z1Smbkd1lqCTM2ncCaWN5Pnt0kfM7nyHVDgfyq4SAiIgIiICIiAiIgIiICIiAiIgLntQs7x3TDBr9qHltYKWz47QTXGsk8c+3G0uLWj7udwGtHyXEAfK6FZzeqJqZlWqGXYLsS0m5qL9m9bS118MbiWxQdz7EUvXktY0sfUyEj6WQxu8glBxnpjaT3/XzXHNd9eqFPKKh91q47EwACF9ZOxzZ3tB8lkEMjYWeOOXnyXRnjUxcVovpRjWh2lmNaUYkwi2Y3QspGSOaGvqJPLpZ3geO8kjnyO48cvK7VAREQEREBERAREQEREBERAREQEREBERAREQEREBERAREQEXzMlybHsNsNdlOWXuis9ntkLqisrq2dsMEEY+XPe4gAf/6Fn9qj6k+ourmbz6NbB9OJ8xvEXP4jJaum5po4w4NMsUTy0NjBcB705a3n4YQQSGiUkkcMbpZZGsYxpc5zjwGgfJJ+wUeZLuO2+4a+SLK9cMCtMsXPaGryKkjl8Hg8MMnYn9gFSOm9NTchrpOb1u73ZXqrhq5Pflx6ySyz08Xbh3RhlLYIS1xI6sgc36Rw4qVcS9JLZljTo33TFsiyd0fH/wAXvszQ4j7kUvsg/wCXHH7cIO4uXqP7JrSZRVa+Wh/sgF34agrajnnj8vtQu7fP2548/oV86l9T3YxWVDKWHXWJr5DwDLj12iYP83vpQ0f6ldtZtkG0OwhgodumCS9Hdh+NtEVZ5448++H8jx8H7+flfUqtou1SsgdTTba9L2sfxyYsSoIneDz4cyIOHx9ig9zANz+3bVKpioMA1pxC81s8nsxUUN1ibVSP5IAbA8iQ89Txw3yPI8KT1UDUn0rtoua2SWlxXDavBby0",
    "g": "4",
    "h": "4",
    "i": "4",
    "j": "4",
    "k": "4"
}

start_time = timeit.default_timer()

synapse_span_table = SynapseSpanTable(syn, synProjectName, columnLimit=columnLimit)
synapse_span_table.queue_span_table_record(tableName, data1)
synapse_span_table.queue_span_table_record(tableName, data2)
synapse_span_table.flush_span_tables()

synapse_span_table.queue_span_table_record(tableName, data3)
synapse_span_table.queue_span_table_record(tableName, data4)
synapse_span_table.flush_span_tables()

for idx in range(1,7):
    _tableName = tableName + "_" + str(idx)
    synId = syn.findEntityId(_tableName, synProjectName)
    if synId:
        for idy in range(1, 4):
            query = syn.tableQuery("select * from " + synId + " where id='" + str(idy) + "'", resultsAs="rowset", limit=1)
            if query.count > 1:
                raise RuntimeError('Failed: ' + TestName)

print('Passed: ' + TestName)
print('Passed: No duplicated detected')

afterTest()
