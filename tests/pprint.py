import pprint

d = { 'name': 'liraz',
      'age': 30 }

s = pprint.pformat(d)
d2 = eval(s)

assert d == d2


