import json

from jsonbender import bend, K, S


MAPPING = {
    'fullName': {'hello': (S('customer', 'first_name') +
                 K(' ') +
                 S('customer', 'last_name'))},
    'city': S('address', 'city')
}

source = {
    'customer': {
        'first_name': 'Inigo',
        'last_name': 'Montoya',
        'Age': 24,
    },
    'address': {
        'city': 'Sicily',
        'country': 'Florin',
    },
}

result = bend(MAPPING, source)
print(json.dumps(result))
