# POP
prediction on prediction

features are:

ETA

week day

on or off

day time bucket

h3 index source and destination

# Runs

nn.Linear(5, 4),
nn.ReLU(),
nn.Linear(4, 3),
nn.ReLU(),
nn.Linear(3, 2),
nn.ReLU(),
nn.Linear(2, 1)


version_0:

estimated y
tensor([1096.6790], grad_fn=<AddBackward0>)
real y
1556

version_2:

without normalization
estimated y
tensor([3.1729e+08], grad_fn=<AddBackward0>)
real y
1556


nn.Linear(5, 4),
nn.ReLU(),
nn.Linear(4, 3),
nn.ReLU(),
nn.Linear(3, 3),
nn.ReLU(),
nn.Linear(3, 2),
nn.ReLU(),
nn.Linear(2, 1)

version_3:

estimated y
tensor([838.9238], grad_fn=<AddBackward0>)
real y
1556


nn.Linear(5, 3),
nn.ReLU(),
nn.Linear(3, 2),
nn.ReLU(),
nn.Linear(2, 1)

version_4:

estimated y
tensor([1151.5865], grad_fn=<AddBackward0>)
real y
1556